/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.cli.session

import org.apache.spark.sql.hive.thriftserver.auth.HiveAuthFactory
import org.apache.spark.sql.hive.thriftserver.cli._
import org.apache.spark.sql.hive.thriftserver.server.cli.SparkThriftServerSQLException
import org.apache.spark.sql.types.StructType

trait ThriftSession extends ThriftSessionBase {

  @throws[SparkThriftServerSQLException]
  def open(sessionConfMap: Map[String, String]): Unit

  /**
   * getInfo operation handler
   *
   * @param getInfoType
   * @return
   * @throws SparkThriftServerSQLException
   */
  @throws[SparkThriftServerSQLException]
  def getInfo(getInfoType: GetInfoType): GetInfoValue

  /**
   * execute operation handler
   *
   * @param statement
   * @param confOverlay
   * @return
   * @throws SparkThriftServerSQLException
   */
  @throws[SparkThriftServerSQLException]
  def executeStatement(statement: String,
                       confOverlay: Map[String, String]): OperationHandle

  @throws[SparkThriftServerSQLException]
  def executeStatementAsync(statement: String,
                            confOverlay: Map[String, String]): OperationHandle

  /**
   * getTypeInfo operation handler
   *
   * @return
   * @throws SparkThriftServerSQLException
   */
  @throws[SparkThriftServerSQLException]
  def getTypeInfo: OperationHandle

  /**
   * getCatalogs operation handler
   *
   * @return
   * @throws SparkThriftServerSQLException
   */
  @throws[SparkThriftServerSQLException]
  def getCatalogs: OperationHandle

  /**
   * getSchemas operation handler
   *
   * @param catalogName
   * @param schemaName
   * @return
   * @throws SparkThriftServerSQLException
   */
  @throws[SparkThriftServerSQLException]
  def getSchemas(catalogName: String,
                 schemaName: String): OperationHandle

  /**
   * getTables operation handler
   *
   * @param catalogName
   * @param schemaName
   * @param tableName
   * @param tableTypes
   * @return
   * @throws SparkThriftServerSQLException
   */
  @throws[SparkThriftServerSQLException]
  def getTables(catalogName: String,
                schemaName: String,
                tableName: String,
                tableTypes: List[String]): OperationHandle

  /**
   * getTableTypes operation handler
   *
   * @return
   * @throws SparkThriftServerSQLException
   */
  @throws[SparkThriftServerSQLException]
  def getTableTypes: OperationHandle

  /**
   * getColumns operation handler
   *
   * @param catalogName
   * @param schemaName
   * @param tableName
   * @param columnName
   * @return
   * @throws SparkThriftServerSQLException
   */
  @throws[SparkThriftServerSQLException]
  def getColumns(catalogName: String,
                 schemaName: String,
                 tableName: String,
                 columnName: String): OperationHandle

  /**
   * getFunctions operation handler
   *
   * @param catalogName
   * @param schemaName
   * @param functionName
   * @return
   * @throws SparkThriftServerSQLException
   */
  @throws[SparkThriftServerSQLException]
  def getFunctions(catalogName: String,
                   schemaName: String,
                   functionName: String): OperationHandle

  /**
   * getPrimaryKeys operation handler
   *
   * @param catalog
   * @param schema
   * @param table
   * @return
   * @throws SparkThriftServerSQLException
   */
  @throws[SparkThriftServerSQLException]
  def getPrimaryKeys(catalog: String,
                     schema: String,
                     table: String): OperationHandle


  /**
   * getCrossReference operation handler
   *
   * @param primaryCatalog
   * @param primarySchema
   * @param primaryTable
   * @param foreignCatalog
   * @param foreignSchema
   * @param foreignTable
   * @return
   * @throws SparkThriftServerSQLException
   */
  @throws[SparkThriftServerSQLException]
  def getCrossReference(primaryCatalog: String,
                        primarySchema: String,
                        primaryTable: String,
                        foreignCatalog: String,
                        foreignSchema: String,
                        foreignTable: String): OperationHandle

  /**
   * close the session
   *
   * @throws SparkThriftServerSQLException
   */
  @throws[SparkThriftServerSQLException]
  def close(): Unit

  @throws[SparkThriftServerSQLException]
  def cancelOperation(opHandle: OperationHandle): Unit

  @throws[SparkThriftServerSQLException]
  def closeOperation(opHandle: OperationHandle): Unit

  @throws[SparkThriftServerSQLException]
  def getResultSetMetadata(opHandle: OperationHandle): StructType

  @throws[SparkThriftServerSQLException]
  def fetchResults(opHandle: OperationHandle,
                   orientation: FetchOrientation,
                   maxRows: Long,
                   fetchType: FetchType): RowSet

  @throws[SparkThriftServerSQLException]
  def getDelegationToken(authFactory:
                         HiveAuthFactory,
                         owner: String,
                         renewer: String,
                         remoteAddr: String): String

  @throws[SparkThriftServerSQLException]
  def cancelDelegationToken(authFactory: HiveAuthFactory, tokenStr: String): Unit

  @throws[SparkThriftServerSQLException]
  def renewDelegationToken(authFactory: HiveAuthFactory, tokenStr: String): Unit

  def closeExpiredOperations(): Unit

  def getNoOperationTime: Long
}
