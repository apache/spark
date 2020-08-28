/**
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

package org.apache.hive.service.cli.session;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.*;

public interface HiveSession extends HiveSessionBase {

  void open(Map<String, String> sessionConfMap) throws Exception;

  IMetaStoreClient getMetaStoreClient() throws HiveSQLException;

  /**
   * getInfo operation handler
   * @param getInfoType
   * @return
   * @throws HiveSQLException
   */
  GetInfoValue getInfo(GetInfoType getInfoType) throws HiveSQLException;

  /**
   * execute operation handler
   * @param statement
   * @param confOverlay
   * @return
   * @throws HiveSQLException
   */
  OperationHandle executeStatement(String statement,
      Map<String, String> confOverlay) throws HiveSQLException;

  /**
   * execute operation handler
   * @param statement
   * @param confOverlay
   * @param queryTimeout
   * @return
   * @throws HiveSQLException
   */
  OperationHandle executeStatement(String statement, Map<String, String> confOverlay,
      long queryTimeout) throws HiveSQLException;

  /**
   * execute operation handler
   * @param statement
   * @param confOverlay
   * @return
   * @throws HiveSQLException
   */
  OperationHandle executeStatementAsync(String statement, Map<String, String> confOverlay) throws HiveSQLException;

  /**
   * execute operation handler
   * @param statement
   * @param confOverlay
   * @param queryTimeout
   * @return
   * @throws HiveSQLException
   */
  OperationHandle executeStatementAsync(String statement, Map<String, String> confOverlay,
      long queryTimeout) throws HiveSQLException;

  /**
   * getTypeInfo operation handler
   * @return
   * @throws HiveSQLException
   */
  OperationHandle getTypeInfo() throws HiveSQLException;

  /**
   * getCatalogs operation handler
   * @return
   * @throws HiveSQLException
   */
  OperationHandle getCatalogs() throws HiveSQLException;

  /**
   * getSchemas operation handler
   * @param catalogName
   * @param schemaName
   * @return
   * @throws HiveSQLException
   */
  OperationHandle getSchemas(String catalogName, String schemaName)
      throws HiveSQLException;

  /**
   * getTables operation handler
   * @param catalogName
   * @param schemaName
   * @param tableName
   * @param tableTypes
   * @return
   * @throws HiveSQLException
   */
  OperationHandle getTables(String catalogName, String schemaName,
      String tableName, List<String> tableTypes) throws HiveSQLException;

  /**
   * getTableTypes operation handler
   * @return
   * @throws HiveSQLException
   */
  OperationHandle getTableTypes() throws HiveSQLException ;

  /**
   * getColumns operation handler
   * @param catalogName
   * @param schemaName
   * @param tableName
   * @param columnName
   * @return
   * @throws HiveSQLException
   */
  OperationHandle getColumns(String catalogName, String schemaName,
      String tableName, String columnName)  throws HiveSQLException;

  /**
   * getFunctions operation handler
   * @param catalogName
   * @param schemaName
   * @param functionName
   * @return
   * @throws HiveSQLException
   */
  OperationHandle getFunctions(String catalogName, String schemaName,
      String functionName) throws HiveSQLException;

  /**
   * getPrimaryKeys operation handler
   * @param catalog
   * @param schema
   * @param table
   * @return
   * @throws HiveSQLException
   */
  OperationHandle getPrimaryKeys(String catalog, String schema,
      String table) throws HiveSQLException;


  /**
   * getCrossReference operation handler
   * @param primaryCatalog
   * @param primarySchema
   * @param primaryTable
   * @param foreignCatalog
   * @param foreignSchema
   * @param foreignTable
   * @return
   * @throws HiveSQLException
   */
  OperationHandle getCrossReference(String primaryCatalog,
      String primarySchema, String primaryTable, String foreignCatalog,
      String foreignSchema, String foreignTable) throws HiveSQLException;

  /**
   * close the session
   * @throws HiveSQLException
   */
  void close() throws HiveSQLException;

  void cancelOperation(OperationHandle opHandle) throws HiveSQLException;

  void closeOperation(OperationHandle opHandle) throws HiveSQLException;

  TableSchema getResultSetMetadata(OperationHandle opHandle)
      throws HiveSQLException;

  RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation,
      long maxRows, FetchType fetchType) throws HiveSQLException;

  String getDelegationToken(HiveAuthFactory authFactory, String owner,
      String renewer) throws HiveSQLException;

  void cancelDelegationToken(HiveAuthFactory authFactory, String tokenStr)
      throws HiveSQLException;

  void renewDelegationToken(HiveAuthFactory authFactory, String tokenStr)
      throws HiveSQLException;

  void closeExpiredOperations();

  long getNoOperationTime();
}
