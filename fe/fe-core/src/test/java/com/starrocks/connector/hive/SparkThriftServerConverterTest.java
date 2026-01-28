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
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class SparkThriftServerConverterTest {

    @Test
    public void testParseDatabaseNames() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true, true, true, false);
        when(rs.getString(1)).thenReturn("db1", "db2", "db3");

        List<String> databases = SparkThriftServerConverter.parseDatabaseNames(rs);

        assertEquals(3, databases.size());
        assertEquals("db1", databases.get(0));
        assertEquals("db2", databases.get(1));
        assertEquals("db3", databases.get(2));
    }

    @Test
    public void testParseTableNames() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString(2)).thenReturn("table1", "table2");

        List<String> tables = SparkThriftServerConverter.parseTableNames(rs);

        assertEquals(2, tables.size());
        assertEquals("table1", tables.get(0));
        assertEquals("table2", tables.get(1));
    }

    @Test
    public void testParseDatabase() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true, true, true, false);
        when(rs.getString(1)).thenReturn("Location", "Description", "Owner");
        when(rs.getString(2)).thenReturn("hdfs://path/to/db", "Test database", "admin");

        Database database = SparkThriftServerConverter.parseDatabase("test_db", rs);

        assertEquals("test_db", database.getName());
        assertEquals("hdfs://path/to/db", database.getLocationUri());
        assertEquals("Test database", database.getDescription());
        assertEquals("admin", database.getOwnerName());
    }

    @Test
    public void testParseTable() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(3);

        // Regular columns
        when(rs.next()).thenReturn(true, true, true, true, true, false);
        when(rs.getString(1)).thenReturn("col1", "col2", "# Detailed Table Information", "Location", "Owner");
        when(rs.getString(2)).thenReturn("int", "string", "", "hdfs://path/to/table", "admin");
        when(rs.getString(3)).thenReturn("", "", "", "", "");

        Table table = SparkThriftServerConverter.parseTable("test_db", "test_table", rs);

        assertEquals("test_db", table.getDbName());
        assertEquals("test_table", table.getTableName());
        assertEquals(2, table.getSd().getCols().size());
        assertEquals("col1", table.getSd().getCols().get(0).getName());
        assertEquals("int", table.getSd().getCols().get(0).getType());
        assertEquals("hdfs://path/to/table", table.getSd().getLocation());
        assertEquals("admin", table.getOwner());
    }

    @Test
    public void testParsePartitionNames() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString(1)).thenReturn("year=2023/month=01", "year=2023/month=02");

        List<String> partitions = SparkThriftServerConverter.parsePartitionNames(rs);

        assertEquals(2, partitions.size());
        assertEquals("year=2023/month=01", partitions.get(0));
        assertEquals("year=2023/month=02", partitions.get(1));
    }

    @Test
    public void testParsePartitionValues() {
        String partitionName = "year=2023/month=01/day=15";
        List<String> values = SparkThriftServerConverter.parsePartitionValues(partitionName);

        assertEquals(3, values.size());
        assertEquals("2023", values.get(0));
        assertEquals("01", values.get(1));
        assertEquals("15", values.get(2));
    }

    @Test
    public void testParsePartitionValuesEmpty() {
        List<String> values = SparkThriftServerConverter.parsePartitionValues("");
        assertTrue(values.isEmpty());

        values = SparkThriftServerConverter.parsePartitionValues(null);
        assertTrue(values.isEmpty());
    }

    @Test
    public void testCreatePartition() {
        Table table = new Table();
        table.setDbName("test_db");
        table.setTableName("test_table");

        FieldSchema partKey1 = new FieldSchema("year", "int", "");
        FieldSchema partKey2 = new FieldSchema("month", "int", "");
        table.setPartitionKeys(Lists.newArrayList(partKey1, partKey2));

        org.apache.hadoop.hive.metastore.api.StorageDescriptor sd = new org.apache.hadoop.hive.metastore.api.StorageDescriptor();
        sd.setLocation("hdfs://path/to/table");
        table.setSd(sd);

        String partitionName = "year=2023/month=01";
        Partition partition = SparkThriftServerConverter.createPartition(table, partitionName);

        assertEquals("test_db", partition.getDbName());
        assertEquals("test_table", partition.getTableName());
        assertEquals(2, partition.getValues().size());
        assertEquals("2023", partition.getValues().get(0));
        assertEquals("01", partition.getValues().get(1));
        assertEquals("hdfs://path/to/table/year=2023/month=01", partition.getSd().getLocation());
    }

    @Test
    public void testParseTableStatistics() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true, true, true, false);
        when(rs.getString(1)).thenReturn("Statistics", "numRows", "totalSize");
        when(rs.getString(2)).thenReturn("1000 rows", "1000", "50000");

        Map<String, String> stats = SparkThriftServerConverter.parseTableStatistics(rs);

        assertEquals(3, stats.size());
        assertTrue(stats.containsKey("Statistics"));
        assertTrue(stats.containsKey("numRows"));
        assertTrue(stats.containsKey("totalSize"));
    }
}
