/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.thriftserver.cli;

import org.apache.spark.sql.thriftserver.auth.HiveAuthFactory;
import org.apache.spark.sql.thriftserver.cli.thrift.TOperationHandle;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

public interface ICLIService {

    SessionHandle openSession(String username, String password,
                              Map<String, String> configuration)
            throws SparkThriftServerSQLException;

    SessionHandle openSessionWithImpersonation(String username, String password,
                                               Map<String, String> configuration, String delegationToken)
            throws SparkThriftServerSQLException;

    void closeSession(SessionHandle sessionHandle)
            throws SparkThriftServerSQLException;

    GetInfoValue getInfo(SessionHandle sessionHandle, GetInfoType infoType)
            throws SparkThriftServerSQLException;

    OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
                                     Map<String, String> confOverlay) throws SparkThriftServerSQLException;

    OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
                                     Map<String, String> confOverlay, long queryTimeout) throws SparkThriftServerSQLException;

    OperationHandle executeStatementAsync(SessionHandle sessionHandle, String statement,
                                          Map<String, String> confOverlay) throws SparkThriftServerSQLException;

    OperationHandle executeStatementAsync(SessionHandle sessionHandle, String statement,
                                          Map<String, String> confOverlay, long queryTimeout) throws SparkThriftServerSQLException;

    OperationHandle getTypeInfo(SessionHandle sessionHandle)
            throws SparkThriftServerSQLException;

    OperationHandle getCatalogs(SessionHandle sessionHandle)
            throws SparkThriftServerSQLException;

    OperationHandle getSchemas(SessionHandle sessionHandle,
                               String catalogName, String schemaName)
            throws SparkThriftServerSQLException;

    OperationHandle getTables(SessionHandle sessionHandle,
                              String catalogName, String schemaName, String tableName, List<String> tableTypes)
            throws SparkThriftServerSQLException;

    OperationHandle getTableTypes(SessionHandle sessionHandle)
            throws SparkThriftServerSQLException;

    OperationHandle getColumns(SessionHandle sessionHandle,
                               String catalogName, String schemaName, String tableName, String columnName)
            throws SparkThriftServerSQLException;

    OperationHandle getFunctions(SessionHandle sessionHandle,
                                 String catalogName, String schemaName, String functionName)
            throws SparkThriftServerSQLException;

    OperationStatus getOperationStatus(OperationHandle opHandle)
            throws SparkThriftServerSQLException;

    String getQueryId(TOperationHandle operationHandle) throws SparkThriftServerSQLException;

    void cancelOperation(OperationHandle opHandle)
            throws SparkThriftServerSQLException;

    void closeOperation(OperationHandle opHandle)
            throws SparkThriftServerSQLException;

    StructType getResultSetMetadata(OperationHandle opHandle)
            throws SparkThriftServerSQLException;

    RowSet fetchResults(OperationHandle opHandle)
            throws SparkThriftServerSQLException;

    RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation,
                        long maxRows, FetchType fetchType) throws SparkThriftServerSQLException;

    String getDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
                              String owner, String renewer) throws SparkThriftServerSQLException;

    void cancelDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
                               String tokenStr) throws SparkThriftServerSQLException;

    void renewDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
                              String tokenStr) throws SparkThriftServerSQLException;

    OperationHandle getPrimaryKeys(SessionHandle sessionHandle, String catalog,
                                   String schema, String table) throws SparkThriftServerSQLException;

    OperationHandle getCrossReference(SessionHandle sessionHandle,
                                      String primaryCatalog, String primarySchema, String primaryTable,
                                      String foreignCatalog, String foreignSchema, String foreignTable)
            throws SparkThriftServerSQLException;
}
