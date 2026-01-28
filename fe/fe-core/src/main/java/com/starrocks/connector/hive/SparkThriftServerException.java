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

import com.starrocks.connector.exception.StarRocksConnectorException;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;

/**
 * Custom exception for Spark Thrift Server operations
 * Provides better error classification and retry logic
 */
public class SparkThriftServerException extends StarRocksConnectorException {

    /**
     * Error type classification
     */
    public enum ErrorType {
        /** JDBC connection errors (network, authentication, etc.) */
        CONNECTION_ERROR,

        /** SQL query errors (syntax, table not found, etc.) */
        QUERY_ERROR,

        /** Timeout errors */
        TIMEOUT_ERROR,

        /** Authentication/authorization errors */
        AUTHENTICATION_ERROR,

        /** Unknown or unclassified errors */
        UNKNOWN_ERROR
    }

    private final ErrorType errorType;
    private final boolean retryable;

    public SparkThriftServerException(String message, ErrorType errorType, boolean retryable) {
        super(message);
        this.errorType = errorType;
        this.retryable = retryable;
    }

    public SparkThriftServerException(String message, Throwable cause, ErrorType errorType, boolean retryable) {
        super(message, cause);
        this.errorType = errorType;
        this.retryable = retryable;
    }

    public ErrorType getErrorType() {
        return errorType;
    }

    public boolean isRetryable() {
        return retryable;
    }

    /**
     * Create exception from SQLException with automatic error type classification
     */
    public static SparkThriftServerException fromSQLException(String message, SQLException e) {
        ErrorType errorType;
        boolean retryable;

        if (e instanceof SQLTimeoutException) {
            errorType = ErrorType.TIMEOUT_ERROR;
            retryable = true;
        } else {
            String sqlState = e.getSQLState();
            int errorCode = e.getErrorCode();
            String errorMessage = e.getMessage().toLowerCase();

            // Classify based on SQL state and error message
            if (sqlState != null && (sqlState.startsWith("08") || sqlState.startsWith("HY"))) {
                // Connection errors
                errorType = ErrorType.CONNECTION_ERROR;
                retryable = true;
            } else if (errorMessage.contains("authentication") || errorMessage.contains("access denied") ||
                    errorMessage.contains("permission")) {
                // Authentication errors
                errorType = ErrorType.AUTHENTICATION_ERROR;
                retryable = false;
            } else if (errorMessage.contains("timeout")) {
                // Timeout errors
                errorType = ErrorType.TIMEOUT_ERROR;
                retryable = true;
            } else if (errorMessage.contains("syntax") || errorMessage.contains("not found") ||
                    errorMessage.contains("does not exist")) {
                // Query errors
                errorType = ErrorType.QUERY_ERROR;
                retryable = false;
            } else {
                // Unknown errors - be conservative and don't retry
                errorType = ErrorType.UNKNOWN_ERROR;
                retryable = false;
            }
        }

        return new SparkThriftServerException(message, e, errorType, retryable);
    }

    @Override
    public String toString() {
        return String.format("SparkThriftServerException{errorType=%s, retryable=%s, message=%s}",
                errorType, retryable, getMessage());
    }
}
