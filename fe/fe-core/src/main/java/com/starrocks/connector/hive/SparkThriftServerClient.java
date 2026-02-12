// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.hive;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Functional interface for processing ResultSet objects
 */
@FunctionalInterface
interface ResultSetProcessor<T> {
    T process(ResultSet rs) throws SQLException;
}

/**
 * Client for connecting to Spark Thrift Server using JDBC.
 * Provides connection pooling and SQL execution capabilities.
 */
public class SparkThriftServerClient {
    private static final Logger LOG = LogManager.getLogger(SparkThriftServerClient.class);

    // Connection pool settings
    private final int maxPoolSize;
    private static final int MAX_CONNECTION_POOL_SIZE_DEFAULT = 32;
    private static final int DEFAULT_TIMEOUT_SECONDS = 600;
    private static final ExecutorService NETWORK_TIMEOUT_EXECUTOR = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("spark-thrift-network-timeout-%d").build());

    // JDBC connection settings
    private final String jdbcUrl;
    private final Properties connectionProperties;
    private final int connectionTimeout;

    // Connection pool
    private final LinkedList<PooledConnection> connectionPool = new LinkedList<>();
    private final Object poolLock = new Object();

    // Configuration keys
    public static final String SPARK_THRIFT_SERVER_JDBC_URL = HiveConnector.SPARK_THRIFT_SERVER_JDBC_URL;
    public static final String SPARK_THRIFT_SERVER_USERNAME = HiveConnector.SPARK_THRIFT_SERVER_USERNAME;
    public static final String SPARK_THRIFT_SERVER_PASSWORD = HiveConnector.SPARK_THRIFT_SERVER_PASSWORD;
    public static final String SPARK_THRIFT_SERVER_TIMEOUT = HiveConnector.SPARK_THRIFT_SERVER_TIMEOUT;
    public static final String SPARK_THRIFT_SERVER_CONNECTION_POOL_SIZE = HiveConnector.SPARK_THRIFT_SERVER_CONNECTION_POOL_SIZE;
    public static final String SPARK_THRIFT_SERVER_PRINCIPAL = "spark.thrift.server.principal";

    public SparkThriftServerClient(String jdbcUrl, Properties connectionProperties,
                                   int maxPoolSize, int connectionTimeout) {
        this.jdbcUrl = jdbcUrl;
        this.connectionProperties = connectionProperties;
        this.maxPoolSize = maxPoolSize;
        this.connectionTimeout = connectionTimeout;

        // Load Hive JDBC driver
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            throw new StarRocksConnectorException("Failed to load Hive JDBC driver", e);
        }
    }

    /**
     * Factory method to create SparkThriftServerClient from properties
     */
    public static SparkThriftServerClient create(Map<String, String> properties) {
        String jdbcUrl = properties.get(SPARK_THRIFT_SERVER_JDBC_URL);
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            throw new StarRocksConnectorException("Spark Thrift Server JDBC URL is required");
        }

        // Build JDBC connection properties
        Properties connProps = new Properties();

        // Add username and password if provided
        String username = properties.get(SPARK_THRIFT_SERVER_USERNAME);
        if (username != null && !username.isEmpty()) {
            connProps.setProperty("user", username);
        }

        String password = properties.get(SPARK_THRIFT_SERVER_PASSWORD);
        if (password != null && !password.isEmpty()) {
            connProps.setProperty("password", password);
        }

        // Add Kerberos principal if provided (for secure deployments)
        String principal = properties.get(SPARK_THRIFT_SERVER_PRINCIPAL);
        if (principal != null && !principal.isEmpty()) {
            connProps.setProperty("principal", principal);
        }

        // Get timeout setting
        int timeout = Integer.parseInt(
                properties.getOrDefault(SPARK_THRIFT_SERVER_TIMEOUT,
                        String.valueOf(DEFAULT_TIMEOUT_SECONDS)));

        // Get pool size setting
        int poolSize = Integer.parseInt(
                properties.getOrDefault(SPARK_THRIFT_SERVER_CONNECTION_POOL_SIZE,
                        String.valueOf(MAX_CONNECTION_POOL_SIZE_DEFAULT)));

        return new SparkThriftServerClient(jdbcUrl, connProps, poolSize, timeout);
    }

    /**
     * Pooled connection wrapper that can be recycled
     */
    public class PooledConnection implements AutoCloseable {
        private final Connection connection;
        private boolean inUse;

        private PooledConnection() throws SQLException {
            this.connection = createConnection();
            this.inUse = false;
        }

        /**
         * Execute a query and process the results with proper resource management.
         * The processor function receives the ResultSet and must fully process it
         * before returning, as the ResultSet will be closed after this method returns.
         */
        public <T> T executeQuery(String sql, ResultSetProcessor<T> processor) throws SQLException {
            Statement stmt = null;
            ResultSet rs = null;
            try {
                stmt = connection.createStatement();
                LOG.debug("Executing SQL: {}", sql);
                rs = stmt.executeQuery(sql);
                return processor.process(rs);
            } finally {
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (SQLException e) {
                        LOG.warn("Failed to close ResultSet", e);
                    }
                }
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException e) {
                        LOG.warn("Failed to close Statement", e);
                    }
                }
            }
        }

        public void execute(String sql) throws SQLException {
            try (Statement stmt = connection.createStatement()) {
                LOG.debug("Executing SQL: {}", sql);
                stmt.execute(sql);
            }
        }

        public boolean isValid() {
            try {
                return connection != null && !connection.isClosed() &&
                       connection.isValid(connectionTimeout);
            } catch (SQLException e) {
                LOG.warn("Connection validation failed", e);
                return false;
            }
        }

        @Override
        public void close() {
            recycle(this);
        }

        private void closeConnection() {
            try {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (SQLException e) {
                LOG.warn("Failed to close connection", e);
            }
        }
    }

    /**
     * Create a new JDBC connection
     */
    private Connection createConnection() throws SQLException {
        try {
            LOG.info("Creating new connection to Spark Thrift Server: {}", jdbcUrl);
            Connection conn = DriverManager.getConnection(jdbcUrl, connectionProperties);
            try {
                conn.setNetworkTimeout(NETWORK_TIMEOUT_EXECUTOR, connectionTimeout * 1000);
            } catch (SQLFeatureNotSupportedException e) {
                LOG.debug("JDBC driver does not support setNetworkTimeout", e);
            } catch (SQLException e) {
                LOG.warn("Failed to set network timeout for Spark Thrift Server connection", e);
            }
            return conn;
        } catch (SQLException e) {
            LOG.error("Failed to create connection to Spark Thrift Server", e);
            throw new StarRocksConnectorException(
                "Failed to connect to Spark Thrift Server: " + e.getMessage(), e);
        }
    }

    /**
     * Get a connection from the pool or create a new one
     */
    public PooledConnection getConnection() {
        synchronized (poolLock) {
            // Try to find an available connection in the pool
            while (!connectionPool.isEmpty()) {
                PooledConnection conn = connectionPool.removeFirst();
                if (conn.isValid()) {
                    conn.inUse = true;
                    LOG.debug("Reusing connection from pool. Pool size: {}", connectionPool.size());
                    return conn;
                } else {
                    conn.closeConnection();
                    LOG.debug("Removed invalid connection from pool");
                }
            }

            // No available connection, create a new one
            try {
                PooledConnection conn = new PooledConnection();
                conn.inUse = true;
                LOG.debug("Created new connection. Pool size: {}", connectionPool.size());
                return conn;
            } catch (SQLException e) {
                throw new StarRocksConnectorException("Failed to create new connection", e);
            }
        }
    }

    /**
     * Return a connection to the pool
     */
    private void recycle(PooledConnection conn) {
        synchronized (poolLock) {
            conn.inUse = false;
            if (connectionPool.size() < maxPoolSize && conn.isValid()) {
                connectionPool.addLast(conn);
                LOG.debug("Recycled connection to pool. Pool size: {}", connectionPool.size());
            } else {
                conn.closeConnection();
                LOG.debug("Connection not recycled (pool full or invalid)");
            }
        }
    }

    /**
     * Close all connections in the pool
     */
    public void close() {
        synchronized (poolLock) {
            LOG.info("Closing all connections in pool. Pool size: {}", connectionPool.size());
            for (PooledConnection conn : connectionPool) {
                conn.closeConnection();
            }
            connectionPool.clear();
        }
    }
}
