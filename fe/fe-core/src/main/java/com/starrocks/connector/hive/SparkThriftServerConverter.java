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

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts SQL query results from Spark Thrift Server into Hive metastore
 * objects.
 */
public class SparkThriftServerConverter {
    private static final Logger LOG = LogManager.getLogger(SparkThriftServerConverter.class);

    /**
     * Parse database names from SHOW DATABASES result
     */
    public static List<String> parseDatabaseNames(ResultSet rs) throws SQLException {
        List<String> databases = new ArrayList<>();
        while (rs.next()) {
            String dbName = rs.getString(1);
            databases.add(dbName);
        }
        LOG.debug("Parsed {} databases", databases.size());
        return databases;
    }

    /**
     * Parse table names from SHOW TABLES result
     */
    public static List<String> parseTableNames(ResultSet rs) throws SQLException {
        List<String> tables = new ArrayList<>();
        while (rs.next()) {
            // SHOW TABLES returns: database, tableName, isTemporary
            String tableName = rs.getString(2);
            tables.add(tableName);
        }
        LOG.debug("Parsed {} tables", tables.size());
        return tables;
    }

    /**
     * Parse Database object from DESCRIBE DATABASE EXTENDED result
     */
    public static Database parseDatabase(String dbName, ResultSet rs) throws SQLException {
        Database database = new Database();
        database.setName(dbName);

        Map<String, String> parameters = new HashMap<>();
        String location = null;
        String description = null;

        while (rs.next()) {
            String infoName = rs.getString(1);
            String infoValue = rs.getString(2);

            if (infoName == null || infoValue == null) {
                continue;
            }

            if (infoName.equalsIgnoreCase("Location")) {
                location = infoValue;
            } else if (infoName.equalsIgnoreCase("Description")) {
                description = infoValue;
            } else if (infoName.equalsIgnoreCase("Owner")) {
                database.setOwnerName(infoValue);
            } else {
                parameters.put(infoName, infoValue);
            }
        }

        database.setLocationUri(location != null ? location : "");
        database.setDescription(description != null ? description : "");
        database.setParameters(parameters);

        return database;
    }

    /**
     * Parse Table object from DESCRIBE TABLE EXTENDED result
     */
    public static Table parseTable(String dbName, String tableName, ResultSet rs) throws SQLException {
        Table table = new Table();
        table.setDbName(dbName);
        table.setTableName(tableName);

        List<FieldSchema> columns = new ArrayList<>();
        List<FieldSchema> partitionKeys = new ArrayList<>();
        Map<String, String> parameters = new HashMap<>();
        StorageDescriptor sd = new StorageDescriptor();
        sd.setSerdeInfo(new SerDeInfo());

        boolean inDetailedInfo = false;
        boolean inPartitionInfo = false;

        while (rs.next()) {
            String colName = rs.getString(1);
            String dataType = rs.getString(2);
            String comment = rs.getString(3);

            if (colName == null) {
                continue;
            }

            // Check for section markers
            if (colName.startsWith("# ")) {
                if (colName.contains("Detailed Table Information")) {
                    inDetailedInfo = true;
                    inPartitionInfo = false;
                } else if (colName.contains("Partition Information")) {
                    inPartitionInfo = true;
                    inDetailedInfo = false;
                }
                continue;
            }

            if (inDetailedInfo) {
                // Parse table properties
                parseTableProperty(colName, dataType, table, sd, parameters);
            } else if (inPartitionInfo) {
                // Parse partition columns
                if (!colName.startsWith("#") && dataType != null && !dataType.isEmpty()) {
                    FieldSchema partCol = new FieldSchema();
                    partCol.setName(colName);
                    partCol.setType(dataType);
                    partCol.setComment(comment != null ? comment : "");
                    partitionKeys.add(partCol);
                }
            } else {
                // Parse regular columns
                if (!colName.isEmpty() && dataType != null && !dataType.isEmpty()) {
                    FieldSchema field = new FieldSchema();
                    field.setName(colName);
                    field.setType(dataType);
                    field.setComment(comment != null ? comment : "");
                    columns.add(field);
                }
            }
        }

        sd.setCols(columns);
        sd.setParameters(new HashMap<>());
        table.setSd(sd);
        table.setPartitionKeys(partitionKeys);
        table.setParameters(parameters);

        LOG.debug("Parsed table {}.{} with {} columns and {} partition keys",
                dbName, tableName, columns.size(), partitionKeys.size());

        return table;
    }

    /**
     * Parse individual table property from DESCRIBE TABLE EXTENDED output
     */
    private static void parseTableProperty(String propName, String propValue,
            Table table, StorageDescriptor sd,
            Map<String, String> parameters) {
        if (propValue == null) {
            return;
        }

        String normalizedPropName = propName.trim();
        switch (normalizedPropName) {
            case "Location":
                sd.setLocation(propValue);
                break;
            case "Owner":
                table.setOwner(propValue);
                break;
            case "Created Time":
            case "Created By":
                // Parse timestamp if needed
                break;
            case "Type":
                parameters.put("table_type", propValue);
                break;
            case "Provider":
            case "Table Properties":
            case "Table Parameters":
                parameters.put(normalizedPropName, propValue);
                parseTableProperties(propValue, parameters);
                break;
            case "View Text":
                table.setViewExpandedText(propValue);
                break;
            case "View Original Text":
                table.setViewOriginalText(propValue);
                break;
            case "Serde Library":
                sd.getSerdeInfo().setSerializationLib(propValue);
                break;
            case "InputFormat":
                sd.setInputFormat(propValue);
                break;
            case "OutputFormat":
                sd.setOutputFormat(propValue);
                break;
            default:
                if (!normalizedPropName.startsWith("#")) {
                    parameters.put(normalizedPropName, propValue);
                }
        }
    }

    private static void parseTableProperties(String propValue, Map<String, String> parameters) {
        if (propValue == null) {
            return;
        }
        String normalized = propValue.trim();
        if (normalized.isEmpty()) {
            return;
        }
        if (normalized.startsWith("[") && normalized.endsWith("]")) {
            normalized = normalized.substring(1, normalized.length() - 1).trim();
        } else if (normalized.startsWith("Map(") && normalized.endsWith(")")) {
            normalized = normalized.substring(4, normalized.length() - 1).trim();
        }
        if (normalized.isEmpty()) {
            return;
        }
        for (String entry : splitTablePropertyEntries(normalized)) {
            String trimmedEntry = entry.trim();
            if (trimmedEntry.isEmpty()) {
                continue;
            }
            String key;
            String value;
            int eqIndex = trimmedEntry.indexOf('=');
            if (eqIndex >= 0) {
                key = trimmedEntry.substring(0, eqIndex).trim();
                value = trimmedEntry.substring(eqIndex + 1).trim();
            } else {
                int arrowIndex = trimmedEntry.indexOf("->");
                if (arrowIndex < 0) {
                    continue;
                }
                key = trimmedEntry.substring(0, arrowIndex).trim();
                value = trimmedEntry.substring(arrowIndex + 2).trim();
            }
            if (!key.isEmpty()) {
                parameters.putIfAbsent(key, stripOptionalQuotes(value));
            }
        }
    }

    private static List<String> splitTablePropertyEntries(String value) {
        List<String> entries = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int bracketDepth = 0;
        int braceDepth = 0;
        int parenDepth = 0;
        char quote = 0;
        boolean escape = false;

        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (quote != 0) {
                current.append(c);
                if (escape) {
                    escape = false;
                    continue;
                }
                if (c == '\\') {
                    escape = true;
                } else if (c == quote) {
                    quote = 0;
                }
                continue;
            }

            if (c == '\'' || c == '"') {
                quote = c;
                current.append(c);
                continue;
            }

            switch (c) {
                case '[':
                    bracketDepth++;
                    break;
                case ']':
                    if (bracketDepth > 0) {
                        bracketDepth--;
                    }
                    break;
                case '{':
                    braceDepth++;
                    break;
                case '}':
                    if (braceDepth > 0) {
                        braceDepth--;
                    }
                    break;
                case '(':
                    parenDepth++;
                    break;
                case ')':
                    if (parenDepth > 0) {
                        parenDepth--;
                    }
                    break;
                case ',':
                    if (bracketDepth == 0 && braceDepth == 0 && parenDepth == 0) {
                        entries.add(current.toString());
                        current.setLength(0);
                        continue;
                    }
                    break;
                default:
                    break;
            }
            current.append(c);
        }

        if (current.length() > 0) {
            entries.add(current.toString());
        }
        return entries;
    }

    private static String stripOptionalQuotes(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.length() >= 2) {
            char first = trimmed.charAt(0);
            char last = trimmed.charAt(trimmed.length() - 1);
            if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
                return trimmed.substring(1, trimmed.length() - 1);
            }
        }
        return trimmed;
    }

    /**
     * Parse partition names from SHOW PARTITIONS result
     */
    public static List<String> parsePartitionNames(ResultSet rs) throws SQLException {
        List<String> partitions = new ArrayList<>();
        while (rs.next()) {
            String partition = rs.getString(1);
            partitions.add(partition);
        }
        LOG.debug("Parsed {} partition names", partitions.size());
        return partitions;
    }

    /**
     * Parse partition values from partition name string (e.g.,
     * "year=2023/month=01")
     */
    public static List<String> parsePartitionValues(String partitionName) {
        List<String> values = new ArrayList<>();
        if (partitionName == null || partitionName.isEmpty()) {
            return values;
        }

        String[] parts = partitionName.split("/");
        for (String part : parts) {
            String[] keyValue = part.split("=", 2);
            if (keyValue.length == 2) {
                values.add(keyValue[1]);
            }
        }
        return values;
    }

    /**
     * Create Partition object from partition name and table metadata
     */
    public static Partition createPartition(Table table, String partitionName) {
        Partition partition = new Partition();
        partition.setDbName(table.getDbName());
        partition.setTableName(table.getTableName());
        partition.setValues(parsePartitionValues(partitionName));

        // Copy storage descriptor from table
        StorageDescriptor sd = new StorageDescriptor(table.getSd());

        // Append partition path to table location
        String tableLocation = table.getSd().getLocation();
        if (tableLocation != null && !tableLocation.isEmpty()) {
            String partitionPath = tableLocation + "/" + partitionName;
            sd.setLocation(partitionPath);
        }

        partition.setSd(sd);
        partition.setParameters(new HashMap<>());

        return partition;
    }

    /**
     * Parse table statistics from DESCRIBE EXTENDED result and create
     * HivePartitionStats
     * Extracts Spark statistics parameters like:
     * - spark.sql.statistics.numRows
     * - spark.sql.statistics.totalSize or numFiles/totalSize
     */
    public static HivePartitionStats parseTableStatisticsToStats(ResultSet rs,
            Map<String, String> tableParams) throws SQLException {
        // First collect all statistics from result set
        Map<String, String> statistics = new HashMap<>();

        while (rs.next()) {
            String statName = rs.getString(1);
            String statValue = rs.getString(2);

            if (statName != null && statValue != null) {
                if (statName.contains("Statistics") || statName.contains("numRows") ||
                        statName.contains("totalSize") || statName.contains("rawDataSize")) {
                    statistics.put(statName, statValue);
                }
            }
        }

        // Extract statistics from table parameters (Spark format)
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

        LOG.debug("Parsed table statistics: rowNums={}, totalFileBytes={}, numFiles={}",
                rowNums, totalFileBytes, numFiles);

        // Return empty stats if no valid statistics found
        if (rowNums == -1 && totalFileBytes == -1 && numFiles == 0) {
            return HivePartitionStats.empty();
        }

        // Create HivePartitionStats with common stats
        return HivePartitionStats.fromCommonStats(
                rowNums >= 0 ? rowNums : -1,
                totalFileBytes >= 0 ? totalFileBytes : -1,
                numFiles >= 0 ? numFiles : 0);
    }

    /**
     * Parse table statistics from DESCRIBE EXTENDED result (legacy method)
     * 
     * @deprecated Use parseTableStatisticsToStats instead
     */
    @Deprecated
    public static Map<String, String> parseTableStatistics(ResultSet rs) throws SQLException {
        Map<String, String> statistics = new HashMap<>();

        while (rs.next()) {
            String statName = rs.getString(1);
            String statValue = rs.getString(2);

            if (statName != null && statValue != null) {
                if (statName.contains("Statistics") || statName.contains("numRows") ||
                        statName.contains("totalSize") || statName.contains("rawDataSize")) {
                    statistics.put(statName, statValue);
                }
            }
        }

        LOG.debug("Parsed {} table statistics", statistics.size());
        return statistics;
    }
}
