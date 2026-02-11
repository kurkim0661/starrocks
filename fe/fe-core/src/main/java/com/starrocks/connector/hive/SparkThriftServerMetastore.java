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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metastore.MetastoreTable;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Implementation of IHiveMetastore that uses Spark Thrift Server JDBC
 * connection
 * to query Hive metastore information through SQL queries.
 */
public class SparkThriftServerMetastore implements IHiveMetastore {
    private static final Logger LOG = LogManager.getLogger(SparkThriftServerMetastore.class);

    private final SparkThriftServerClient client;
    private final String catalogName;

    public SparkThriftServerMetastore(SparkThriftServerClient client, String catalogName) {
        this.client = client;
        this.catalogName = catalogName;
        LOG.info("Created SparkThriftServerMetastore for catalog: {}", catalogName);
    }

    @Override
    public List<String> getAllDatabaseNames() {
        try (SparkThriftServerClient.PooledConnection conn = client.getConnection()) {
            return conn.executeQuery("SHOW DATABASES", rs -> {
                return SparkThriftServerConverter.parseDatabaseNames(rs);
            });
        } catch (SQLException e) {
            throw new StarRocksConnectorException("Failed to get all database names: " + e.getMessage(), e);
        }
    }

    @Override
    public List<String> getAllTableNames(String dbName) {
        try (SparkThriftServerClient.PooledConnection conn = client.getConnection()) {
            String sql = String.format("SHOW TABLES IN %s", dbName);
            return conn.executeQuery(sql, rs -> {
                return SparkThriftServerConverter.parseTableNames(rs);
            });
        } catch (SQLException e) {
            throw new StarRocksConnectorException(
                    String.format("Failed to get table names for database %s: %s", dbName, e.getMessage()), e);
        }
    }

    @Override
    public Database getDb(String dbName) {
        try (SparkThriftServerClient.PooledConnection conn = client.getConnection()) {
            String sql = String.format("DESCRIBE DATABASE EXTENDED %s", dbName);
            return conn.executeQuery(sql, rs -> {
                org.apache.hadoop.hive.metastore.api.Database hiveDb =
                        SparkThriftServerConverter.parseDatabase(dbName, rs);
                return HiveMetastoreApiConverter.toDatabase(hiveDb, dbName);
            });
        } catch (SQLException e) {
            throw new StarRocksConnectorException(
                    String.format("Failed to get database %s: %s", dbName, e.getMessage()), e);
        }
    }

    @Override
    public MetastoreTable getMetastoreTable(String dbName, String tableName) {
        org.apache.hadoop.hive.metastore.api.Table hiveTable = getHiveTable(dbName, tableName);
        return HiveMetastoreApiConverter.toMetastoreTable(hiveTable);
    }

    @Override
    public boolean tableExists(String dbName, String tableName) {
        try {
            getHiveTable(dbName, tableName);
            return true;
        } catch (StarRocksConnectorException e) {
            return false;
        }
    }

    @Override
    public Table getTable(String dbName, String tableName) {
        org.apache.hadoop.hive.metastore.api.Table hiveTable = getHiveTable(dbName, tableName);
        StorageDescriptor sd = hiveTable.getSd();
        if (sd == null) {
            throw new StarRocksConnectorException("Table is missing storage descriptor");
        }

        normalizeHiveTableType(hiveTable);

        if (HiveMetastoreApiConverter.isHudiTable(sd.getInputFormat())) {
            return HiveMetastoreApiConverter.toHudiTable(hiveTable, catalogName);
        } else if (HiveMetastoreApiConverter.isKuduTable(sd.getInputFormat())) {
            return HiveMetastoreApiConverter.toKuduTable(hiveTable, catalogName);
        } else {
            HiveMetastoreApiConverter.validateHiveTableType(hiveTable.getTableType());
            if (AcidUtils.isFullAcidTable(hiveTable)) {
                throw new StarRocksConnectorException(String.format(
                        "%s.%s is a hive transactional table(full acid), sr didn't support it yet", dbName, tableName));
            }
            if ("VIRTUAL_VIEW".equalsIgnoreCase(hiveTable.getTableType())) {
                return HiveMetastoreApiConverter.toHiveView(hiveTable, catalogName);
            } else {
                return HiveMetastoreApiConverter.toHiveTable(hiveTable, catalogName);
            }
        }
    }

    /**
     * Get Hive API Table object from Spark Thrift Server
     */
    private org.apache.hadoop.hive.metastore.api.Table getHiveTable(String dbName, String tableName) {
        try (SparkThriftServerClient.PooledConnection conn = client.getConnection()) {
            String sql = String.format("DESCRIBE TABLE EXTENDED %s.%s", dbName, tableName);
            return conn.executeQuery(sql, rs -> {
                return SparkThriftServerConverter.parseTable(dbName, tableName, rs);
            });
        } catch (SQLException e) {
            throw new StarRocksConnectorException(
                    String.format("Failed to get table %s.%s: %s", dbName, tableName, e.getMessage()), e);
        }
    }

    @Override
    public List<String> getPartitionKeysByValue(String dbName, String tableName,
            List<Optional<String>> partitionValues) {
        try (SparkThriftServerClient.PooledConnection conn = client.getConnection()) {
            // Build SQL query
            String sql;
            if (partitionValues == null || partitionValues.isEmpty() ||
                    partitionValues.equals(HivePartitionValue.ALL_PARTITION_VALUES)) {
                // Get all partitions
                sql = String.format("SHOW PARTITIONS %s.%s", dbName, tableName);
            } else {
                // Filter by partition values
                // Build partition filter like: PARTITION(year='2023', month='01')
                List<String> filters = Lists.newArrayList();
                org.apache.hadoop.hive.metastore.api.Table table = getHiveTable(dbName, tableName);
                List<String> partKeys = table.getPartitionKeys().stream()
                        .map(f -> f.getName())
                        .collect(Collectors.toList());

                for (int i = 0; i < partitionValues.size() && i < partKeys.size(); i++) {
                    Optional<String> value = partitionValues.get(i);
                    if (value.isPresent()) {
                        filters.add(String.format("%s='%s'", partKeys.get(i), value.get()));
                    }
                }

                if (filters.isEmpty()) {
                    sql = String.format("SHOW PARTITIONS %s.%s", dbName, tableName);
                } else {
                    sql = String.format("SHOW PARTITIONS %s.%s PARTITION(%s)",
                            dbName, tableName, String.join(", ", filters));
                }
            }

            return conn.executeQuery(sql, rs -> {
                return SparkThriftServerConverter.parsePartitionNames(rs);
            });
        } catch (SQLException e) {
            throw new StarRocksConnectorException(
                    String.format("Failed to get partition keys for %s.%s: %s",
                            dbName, tableName, e.getMessage()),
                    e);
        }
    }

    @Override
    public Partition getPartition(String dbName, String tableName, List<String> partitionValues) {
        try {
            org.apache.hadoop.hive.metastore.api.Table table = getHiveTable(dbName, tableName);

            // Build partition name from values (e.g., "year=2023/month=01")
            List<String> partKeys = table.getPartitionKeys().stream()
                    .map(f -> f.getName())
                    .collect(Collectors.toList());

            if (partKeys.size() != partitionValues.size()) {
                throw new StarRocksConnectorException(
                        String.format("Partition key count mismatch: expected %d, got %d",
                                partKeys.size(), partitionValues.size()));
            }

            StringBuilder partitionName = new StringBuilder();
            for (int i = 0; i < partKeys.size(); i++) {
                if (i > 0) {
                    partitionName.append("/");
                }
                partitionName.append(partKeys.get(i)).append("=").append(partitionValues.get(i));
            }

            return buildPartition(table, partitionName.toString());
        } catch (Exception e) {
            throw new StarRocksConnectorException(
                    String.format("Failed to get partition for %s.%s: %s",
                            dbName, tableName, e.getMessage()),
                    e);
        }
    }

    @Override
    public Map<String, Partition> getPartitionsByNames(String dbName, String tableName,
            List<String> partitionNames) {
        Map<String, Partition> result = new HashMap<>();
        if (partitionNames == null || partitionNames.isEmpty()) {
            return result;
        }
        org.apache.hadoop.hive.metastore.api.Table table = getHiveTable(dbName, tableName);
        List<String> existingPartitionNames =
                getPartitionKeysByValue(dbName, tableName, HivePartitionValue.ALL_PARTITION_VALUES);
        Set<String> existingPartitionNameSet = new HashSet<>(existingPartitionNames);

        for (String partitionName : partitionNames) {
            Partition partition = existingPartitionNameSet.contains(partitionName)
                    ? buildPartition(table, partitionName)
                    : null;
            result.put(partitionName, partition);
        }

        return result;
    }

    @Override
    public HivePartitionStats getTableStatistics(String dbName, String tableName) {
        return getTableStatisticsFromMetadata(dbName, tableName);
    }

    /**
     * Get table statistics by querying table metadata
     * This method extracts statistics from Spark table parameters
     */
    private HivePartitionStats getTableStatisticsFromMetadata(String dbName, String tableName) {
        try {
            // Get the full table metadata which includes parameters
            org.apache.hadoop.hive.metastore.api.Table hiveTable = getHiveTable(dbName, tableName);
            Map<String, String> tableParams = hiveTable.getParameters();

            if (tableParams == null || tableParams.isEmpty()) {
                return HivePartitionStats.empty();
            }

            // Extract statistics from table parameters
            long rowNums = -1;
            long totalFileBytes = -1;
            long numFiles = 0;

            // Try Spark statistics first
            if (tableParams.containsKey("spark.sql.statistics.numRows")) {
                try {
                    rowNums = Long.parseLong(tableParams.get("spark.sql.statistics.numRows"));
                } catch (NumberFormatException e) {
                    LOG.warn("Failed to parse spark.sql.statistics.numRows: {}",
                            tableParams.get("spark.sql.statistics.numRows"));
                }
            }

            if (tableParams.containsKey("spark.sql.statistics.totalSize")) {
                try {
                    totalFileBytes = Long.parseLong(tableParams.get("spark.sql.statistics.totalSize"));
                } catch (NumberFormatException e) {
                    LOG.warn("Failed to parse spark.sql.statistics.totalSize: {}",
                            tableParams.get("spark.sql.statistics.totalSize"));
                }
            }

            // Fallback to standard Hive parameters
            if (rowNums == -1 && tableParams.containsKey("numRows")) {
                try {
                    rowNums = Long.parseLong(tableParams.get("numRows"));
                } catch (NumberFormatException e) {
                    LOG.warn("Failed to parse numRows: {}", tableParams.get("numRows"));
                }
            }

            if (totalFileBytes == -1 && tableParams.containsKey("totalSize")) {
                try {
                    totalFileBytes = Long.parseLong(tableParams.get("totalSize"));
                } catch (NumberFormatException e) {
                    LOG.warn("Failed to parse totalSize: {}", tableParams.get("totalSize"));
                }
            }

            if (tableParams.containsKey("numFiles")) {
                try {
                    numFiles = Long.parseLong(tableParams.get("numFiles"));
                } catch (NumberFormatException e) {
                    LOG.warn("Failed to parse numFiles: {}", tableParams.get("numFiles"));
                }
            }

            LOG.debug("Extracted table statistics for {}.{}: rowNums={}, totalFileBytes={}, numFiles={}",
                    dbName, tableName, rowNums, totalFileBytes, numFiles);

            // Return empty stats if no valid statistics found
            if (rowNums == -1 && totalFileBytes == -1 && numFiles == 0) {
                return HivePartitionStats.empty();
            }

            // Create HivePartitionStats with common stats
            return HivePartitionStats.fromCommonStats(
                    rowNums >= 0 ? rowNums : -1,
                    totalFileBytes >= 0 ? totalFileBytes : -1,
                    numFiles >= 0 ? numFiles : 0);
        } catch (Exception e) {
            LOG.warn("Failed to extract table statistics for {}.{}: {}", dbName, tableName, e.getMessage());
            return HivePartitionStats.empty();
        }
    }

    private void normalizeHiveTableType(org.apache.hadoop.hive.metastore.api.Table hiveTable) {
        String rawType = hiveTable.getTableType();
        if (rawType == null) {
            Map<String, String> params = hiveTable.getParameters();
            if (params != null) {
                rawType = params.get("table_type");
            }
        }

        if (rawType == null) {
            return;
        }

        String normalized = rawType.trim().toUpperCase(Locale.ROOT);
        switch (normalized) {
            case "MANAGED":
            case "MANAGED_TABLE":
                normalized = "MANAGED_TABLE";
                break;
            case "EXTERNAL":
            case "EXTERNAL_TABLE":
                normalized = "EXTERNAL_TABLE";
                break;
            case "VIEW":
            case "VIRTUAL_VIEW":
                normalized = "VIRTUAL_VIEW";
                break;
            case "MATERIALIZED VIEW":
            case "MATERIALIZED_VIEW":
                normalized = "MATERIALIZED_VIEW";
                break;
            default:
                break;
        }

        hiveTable.setTableType(normalized);
    }

    @Override
    public Map<String, HivePartitionStats> getPartitionStatistics(Table table, List<String> partitions) {
        // Basic partition statistics support
        Map<String, HivePartitionStats> result = Maps.newHashMap();
        for (String partition : partitions) {
            result.put(partition, HivePartitionStats.empty());
        }
        return result;
    }

    // Write operations - not supported for Spark Thrift Server (read-only access)

    @Override
    public void createDb(String dbName, Map<String, String> properties) {
        throw new StarRocksConnectorException(
                "CREATE DATABASE is not supported through Spark Thrift Server metastore");
    }

    @Override
    public void dropDb(String dbName, boolean deleteData) {
        throw new StarRocksConnectorException(
                "DROP DATABASE is not supported through Spark Thrift Server metastore");
    }

    @Override
    public void createTable(String dbName, Table table) {
        throw new StarRocksConnectorException(
                "CREATE TABLE is not supported through Spark Thrift Server metastore");
    }

    @Override
    public void dropTable(String dbName, String tableName) {
        throw new StarRocksConnectorException(
                "DROP TABLE is not supported through Spark Thrift Server metastore");
    }

    @Override
    public void addPartitions(String dbName, String tableName, List<HivePartitionWithStats> partitions) {
        throw new StarRocksConnectorException(
                "ADD PARTITION is not supported through Spark Thrift Server metastore");
    }

    @Override
    public void dropPartition(String dbName, String tableName, List<String> partValues, boolean deleteData) {
        throw new StarRocksConnectorException(
                "DROP PARTITION is not supported through Spark Thrift Server metastore");
    }

    @Override
    public boolean partitionExists(Table table, List<String> partitionValues) {
        String dbName = table.getCatalogDBName();
        String tableName = table.getCatalogTableName();
        List<Optional<String>> optionalValues = partitionValues.stream()
                .map(Optional::ofNullable)
                .collect(Collectors.toList());
        return !getPartitionKeysByValue(dbName, tableName, optionalValues).isEmpty();
    }

    @Override
    public void updateTableStatistics(String dbName, String tableName,
            Function<HivePartitionStats, HivePartitionStats> update) {
        throw new StarRocksConnectorException(
                "UPDATE TABLE STATISTICS is not supported through Spark Thrift Server metastore");
    }

    @Override
    public void updatePartitionStatistics(String dbName, String tableName, String partitionName,
            Function<HivePartitionStats, HivePartitionStats> update) {
        throw new StarRocksConnectorException(
                "UPDATE PARTITION STATISTICS is not supported through Spark Thrift Server metastore");
    }

    /**
     * Close the underlying JDBC client and release resources
     */
    public void shutdown() {
        LOG.info("Shutting down SparkThriftServerMetastore for catalog: {}", catalogName);
        client.close();
    }

    private Partition buildPartition(org.apache.hadoop.hive.metastore.api.Table table, String partitionName) {
        StorageDescriptor sd = new StorageDescriptor(table.getSd());
        if (sd.getSerdeInfo() == null) {
            sd.setSerdeInfo(new SerDeInfo());
        }
        if (sd.getSerdeInfo().getParameters() == null) {
            sd.getSerdeInfo().setParameters(new HashMap<>());
        }
        String tableLocation = table.getSd().getLocation();
        if (tableLocation != null && !tableLocation.isEmpty()) {
            sd.setLocation(tableLocation + "/" + partitionName);
        }
        return HiveMetastoreApiConverter.toPartition(sd, new HashMap<>());
    }
}
