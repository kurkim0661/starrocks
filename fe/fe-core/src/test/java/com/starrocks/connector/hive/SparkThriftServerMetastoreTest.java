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
import com.starrocks.catalog.Database;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metastore.MetastoreTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SparkThriftServerMetastoreTest {

    private SparkThriftServerClient mockClient;
    private SparkThriftServerMetastore metastore;
    private SparkThriftServerClient.PooledConnection mockConnection;

    @BeforeEach
    public void setUp() {
        mockClient = mock(SparkThriftServerClient.class);
        mockConnection = mock(SparkThriftServerClient.PooledConnection.class);
        metastore = new SparkThriftServerMetastore(mockClient, "test_catalog");

        when(mockClient.getConnection()).thenReturn(mockConnection);
    }

    @Test
    public void testGetAllDatabaseNames() throws SQLException {
        when(mockConnection.executeQuery(eq("SHOW DATABASES"), any())).thenAnswer(invocation -> {
            ResultSetProcessor<?> processor = invocation.getArgument(1);
            java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
            when(rs.next()).thenReturn(true, true, false);
            when(rs.getString(1)).thenReturn("db1", "db2");
            return processor.process(rs);
        });

        List<String> databases = metastore.getAllDatabaseNames();

        assertEquals(2, databases.size());
        assertEquals("db1", databases.get(0));
        assertEquals("db2", databases.get(1));
        verify(mockConnection).close();
    }

    @Test
    public void testGetAllTableNames() throws SQLException {
        when(mockConnection.executeQuery(eq("SHOW TABLES IN test_db"), any())).thenAnswer(invocation -> {
            ResultSetProcessor<?> processor = invocation.getArgument(1);
            java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
            when(rs.next()).thenReturn(true, true, false);
            when(rs.getString(2)).thenReturn("table1", "table2");
            return processor.process(rs);
        });

        List<String> tables = metastore.getAllTableNames("test_db");

        assertEquals(2, tables.size());
        assertEquals("table1", tables.get(0));
        assertEquals("table2", tables.get(1));
    }

    @Test
    public void testGetDb() throws SQLException {
        when(mockConnection.executeQuery(eq("DESCRIBE DATABASE EXTENDED test_db"), any())).thenAnswer(invocation -> {
            ResultSetProcessor<?> processor = invocation.getArgument(1);
            java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
            when(rs.next()).thenReturn(true, true, false);
            when(rs.getString(1)).thenReturn("Location", "Description");
            when(rs.getString(2)).thenReturn("hdfs://path", "Test DB");
            return processor.process(rs);
        });

        Database db = metastore.getDb("test_db");

        assertEquals("test_db", db.getFullName());
        assertEquals("hdfs://path", db.getLocation());
    }

    @Test
    public void testGetMetastoreTable() throws SQLException {
        when(mockConnection.executeQuery(eq("DESCRIBE TABLE EXTENDED test_db.test_table"), any()))
                .thenAnswer(invocation -> {
                    ResultSetProcessor<?> processor = invocation.getArgument(1);
                    java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
                    when(rs.next()).thenReturn(true, false);
                    when(rs.getString(1)).thenReturn("col1");
                    when(rs.getString(2)).thenReturn("int");
                    when(rs.getString(3)).thenReturn("");
                    return processor.process(rs);
                });

        MetastoreTable table = metastore.getMetastoreTable("test_db", "test_table");

        assertNotNull(table);
        assertEquals("test_db", table.getDbName());
        assertEquals("test_table", table.getTableName());
    }

    @Test
    public void testTableExists() throws SQLException {
        when(mockConnection.executeQuery(anyString(), any())).thenAnswer(invocation -> {
            ResultSetProcessor<?> processor = invocation.getArgument(1);
            java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
            when(rs.next()).thenReturn(false);
            return processor.process(rs);
        });

        assertTrue(metastore.tableExists("test_db", "test_table"));
    }

    @Test
    public void testGetPartitionKeysByValue() throws SQLException {
        // Mock table query
        when(mockConnection.executeQuery(eq("DESCRIBE TABLE EXTENDED test_db.test_table"), any()))
                .thenAnswer(invocation -> {
                    ResultSetProcessor<?> processor = invocation.getArgument(1);
                    java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
                    when(rs.next()).thenReturn(true, true, true, false);
                    when(rs.getString(1)).thenReturn("col1", "# Partition Information", "year");
                    when(rs.getString(2)).thenReturn("int", "", "int");
                    when(rs.getString(3)).thenReturn("", "", "");
                    return processor.process(rs);
                });

        // Mock partition query
        when(mockConnection.executeQuery(eq("SHOW PARTITIONS test_db.test_table"), any())).thenAnswer(invocation -> {
            ResultSetProcessor<?> processor = invocation.getArgument(1);
            java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
            when(rs.next()).thenReturn(true, true, false);
            when(rs.getString(1)).thenReturn("year=2023", "year=2024");
            return processor.process(rs);
        });

        List<String> partitions = metastore.getPartitionKeysByValue("test_db", "test_table",
                HivePartitionValue.ALL_PARTITION_VALUES);

        assertEquals(2, partitions.size());
    }

    @Test
    public void testGetPartition() throws SQLException {
        // Mock table query
        when(mockConnection.executeQuery(eq("DESCRIBE TABLE EXTENDED test_db.test_table"), any()))
                .thenAnswer(invocation -> {
                    ResultSetProcessor<?> processor = invocation.getArgument(1);
                    java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
                    when(rs.next()).thenReturn(true, true, true, true, true, false);
                    when(rs.getString(1)).thenReturn("col1", "# Partition Information", "year",
                            "# Detailed Table Information", "Location");
                    when(rs.getString(2)).thenReturn("int", "", "int", "", "hdfs://path/to/table");
                    when(rs.getString(3)).thenReturn("", "", "", "", "");
                    return processor.process(rs);
                });

        Partition partition = metastore.getPartition("test_db", "test_table", Lists.newArrayList("2023"));

        assertNotNull(partition);
        assertEquals("hdfs://path/to/table/year=2023", partition.getFullPath());
    }

    @Test
    public void testGetPartitionsByNames() throws SQLException {
        // Mock table query
        when(mockConnection.executeQuery(eq("DESCRIBE TABLE EXTENDED test_db.test_table"), any()))
                .thenAnswer(invocation -> {
                    ResultSetProcessor<?> processor = invocation.getArgument(1);
                    java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
                    when(rs.next()).thenReturn(true, true, false);
                    when(rs.getString(1)).thenReturn("# Detailed Table Information", "Location");
                    when(rs.getString(2)).thenReturn("", "hdfs://path/to/table");
                    when(rs.getString(3)).thenReturn("", "");
                    return processor.process(rs);
                });

        when(mockConnection.executeQuery(eq("SHOW PARTITIONS test_db.test_table"), any()))
                .thenAnswer(invocation -> {
                    ResultSetProcessor<?> processor = invocation.getArgument(1);
                    java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
                    when(rs.next()).thenReturn(true, true, false);
                    when(rs.getString(1)).thenReturn("year=2023/month=01", "year=2023/month=02");
                    return processor.process(rs);
                });

        List<String> partitionNames = Lists.newArrayList("year=2023/month=01", "year=2023/month=02");
        Map<String, Partition> partitions = metastore.getPartitionsByNames("test_db", "test_table", partitionNames);

        assertEquals(2, partitions.size());
        assertTrue(partitions.containsKey("year=2023/month=01"));
        assertTrue(partitions.containsKey("year=2023/month=02"));
        assertNotNull(partitions.get("year=2023/month=01"));
        assertNotNull(partitions.get("year=2023/month=02"));
        assertEquals("hdfs://path/to/table/year=2023/month=01",
                partitions.get("year=2023/month=01").getFullPath());
    }

    @Test
    public void testGetTableStatistics() throws SQLException {
        when(mockConnection.executeQuery(anyString(), any())).thenAnswer(invocation -> {
            ResultSetProcessor<?> processor = invocation.getArgument(1);
            java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
            when(rs.next()).thenReturn(false);
            return processor.process(rs);
        });

        HivePartitionStats stats = metastore.getTableStatistics("test_db", "test_table");

        assertNotNull(stats);
        assertEquals(HivePartitionStats.empty(), stats);
    }

    @Test
    public void testCreateDbNotSupported() {
        assertThrows(StarRocksConnectorException.class, () -> {
            metastore.createDb("test_db", null);
        });
    }

    @Test
    public void testDropDbNotSupported() {
        assertThrows(StarRocksConnectorException.class, () -> {
            metastore.dropDb("test_db", false);
        });
    }

    @Test
    public void testCreateTableNotSupported() {
        assertThrows(StarRocksConnectorException.class, () -> {
            metastore.createTable("test_db", null);
        });
    }

    @Test
    public void testDropTableNotSupported() {
        assertThrows(StarRocksConnectorException.class, () -> {
            metastore.dropTable("test_db", "test_table");
        });
    }

    @Test
    public void testAddPartitionsNotSupported() {
        assertThrows(StarRocksConnectorException.class, () -> {
            metastore.addPartitions("test_db", "test_table", null);
        });
    }

    @Test
    public void testDropPartitionNotSupported() {
        assertThrows(StarRocksConnectorException.class, () -> {
            metastore.dropPartition("test_db", "test_table", null, false);
        });
    }

    @Test
    public void testUpdateTableStatisticsNotSupported() {
        assertThrows(StarRocksConnectorException.class, () -> {
            metastore.updateTableStatistics("test_db", "test_table", null);
        });
    }

    @Test
    public void testUpdatePartitionStatisticsNotSupported() {
        assertThrows(StarRocksConnectorException.class, () -> {
            metastore.updatePartitionStatistics("test_db", "test_table", "partition", null);
        });
    }
}
