/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.service.cli;

import java.util.List;
import java.util.Map;

import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TRowSet;
import org.apache.hive.service.rpc.thrift.TTableSchema;

public interface ICLIService {

  SessionHandle openSession(String username, String password,
      Map<String, String> configuration)
          throws HiveSQLException;

  SessionHandle openSessionWithImpersonation(String username, String password,
      Map<String, String> configuration, String delegationToken)
          throws HiveSQLException;

  void closeSession(SessionHandle sessionHandle)
      throws HiveSQLException;

  GetInfoValue getInfo(SessionHandle sessionHandle, GetInfoType infoType)
      throws HiveSQLException;

  OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay) throws HiveSQLException;

  OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay, long queryTimeout) throws HiveSQLException;

  OperationHandle executeStatementAsync(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay) throws HiveSQLException;
  OperationHandle executeStatementAsync(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay, long queryTimeout) throws HiveSQLException;

  OperationHandle getTypeInfo(SessionHandle sessionHandle)
      throws HiveSQLException;

  OperationHandle getCatalogs(SessionHandle sessionHandle)
      throws HiveSQLException;

  OperationHandle getSchemas(SessionHandle sessionHandle,
      String catalogName, String schemaName)
          throws HiveSQLException;

  OperationHandle getTables(SessionHandle sessionHandle,
      String catalogName, String schemaName, String tableName, List<String> tableTypes)
          throws HiveSQLException;

  OperationHandle getTableTypes(SessionHandle sessionHandle)
      throws HiveSQLException;

  OperationHandle getColumns(SessionHandle sessionHandle,
      String catalogName, String schemaName, String tableName, String columnName)
          throws HiveSQLException;

  OperationHandle getFunctions(SessionHandle sessionHandle,
      String catalogName, String schemaName, String functionName)
          throws HiveSQLException;

  OperationStatus getOperationStatus(OperationHandle opHandle)
      throws HiveSQLException;

  void cancelOperation(OperationHandle opHandle)
      throws HiveSQLException;

  void closeOperation(OperationHandle opHandle)
      throws HiveSQLException;

  TTableSchema getResultSetMetadata(OperationHandle opHandle)
      throws HiveSQLException;

  TRowSet fetchResults(OperationHandle opHandle)
      throws HiveSQLException;

  TRowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation,
      long maxRows, FetchType fetchType) throws HiveSQLException;

  String getDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String owner, String renewer) throws HiveSQLException;

  String getQueryId(TOperationHandle operationHandle) throws HiveSQLException;

  void cancelDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String tokenStr) throws HiveSQLException;

  void renewDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String tokenStr) throws HiveSQLException;

  OperationHandle getPrimaryKeys(SessionHandle sessionHandle, String catalog,
      String schema, String table) throws HiveSQLException;

  OperationHandle getCrossReference(SessionHandle sessionHandle,
      String primaryCatalog, String primarySchema, String primaryTable,
      String foreignCatalog, String foreignSchema, String foreignTable) throws HiveSQLException;
}
