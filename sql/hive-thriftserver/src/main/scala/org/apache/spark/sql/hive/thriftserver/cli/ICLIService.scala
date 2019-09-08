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

package org.apache.spark.sql.hive.thriftserver.cli

import org.apache.spark.sql.hive.thriftserver.auth.HiveAuthFactory
import org.apache.spark.sql.hive.thriftserver.cli.operation.OperationStatus
import org.apache.spark.sql.hive.thriftserver.server.cli.SparkThriftServerSQLException
import org.apache.spark.sql.types.StructType


trait ICLIService {

  @throws[SparkThriftServerSQLException]
  def openSession(username: String,
                  password: String,
                  configuration: Map[String, String]): SessionHandle

  @throws[SparkThriftServerSQLException]
  def openSessionWithImpersonation(username: String,
                                   password: String,
                                   configuration: Map[String, String],
                                   delegationToken: String): SessionHandle

  @throws[SparkThriftServerSQLException]
  def closeSession(sessionHandle: SessionHandle): Unit

  @throws[SparkThriftServerSQLException]
  def getInfo(sessionHandle: SessionHandle,
              infoType: GetInfoType): GetInfoValue

  @throws[SparkThriftServerSQLException]
  def executeStatement(sessionHandle: SessionHandle,
                       statement: String,
                       confOverlay: Map[String, String]): OperationHandle

  @throws[SparkThriftServerSQLException]
  def executeStatementAsync(sessionHandle: SessionHandle,
                            statement: String,
                            confOverlay: Map[String, String]): OperationHandle

  @throws[SparkThriftServerSQLException]
  def getTypeInfo(sessionHandle: SessionHandle): OperationHandle

  @throws[SparkThriftServerSQLException]
  def getCatalogs(sessionHandle: SessionHandle): OperationHandle

  @throws[SparkThriftServerSQLException]
  def getSchemas(sessionHandle: SessionHandle,
                 catalogName: String,
                 schemaName: String): OperationHandle

  @throws[SparkThriftServerSQLException]
  def getTables(sessionHandle: SessionHandle,
                catalogName: String,
                schemaName: String,
                tableName: String,
                tableTypes: List[String]): OperationHandle

  @throws[SparkThriftServerSQLException]
  def getTableTypes(sessionHandle: SessionHandle): OperationHandle

  @throws[SparkThriftServerSQLException]
  def getColumns(sessionHandle: SessionHandle,
                 catalogName: String,
                 schemaName: String,
                 tableName: String,
                 columnName: String): OperationHandle

  @throws[SparkThriftServerSQLException]
  def getFunctions(sessionHandle: SessionHandle,
                   catalogName: String,
                   schemaName: String,
                   functionName: String): OperationHandle

  @throws[SparkThriftServerSQLException]
  def getPrimaryKeys(sessionHandle: SessionHandle,
                     catalog: String,
                     schema: String,
                     table: String): OperationHandle

  @throws[SparkThriftServerSQLException]
  def getCrossReference(sessionHandle: SessionHandle,
                        primaryCatalog: String,
                        primarySchema: String,
                        primaryTable: String,
                        foreignCatalog: String,
                        foreignSchema: String,
                        foreignTable: String): OperationHandle

  @throws[SparkThriftServerSQLException]
  def getOperationStatus(opHandle: OperationHandle): OperationStatus

  @throws[SparkThriftServerSQLException]
  def cancelOperation(opHandle: OperationHandle): Unit

  @throws[SparkThriftServerSQLException]
  def closeOperation(opHandle: OperationHandle): Unit

  @throws[SparkThriftServerSQLException]
  def getResultSetMetadata(opHandle: OperationHandle): StructType

  @throws[SparkThriftServerSQLException]
  def fetchResults(opHandle: OperationHandle): RowSet

  @throws[SparkThriftServerSQLException]
  def fetchResults(opHandle: OperationHandle,
                   orientation: FetchOrientation,
                   maxRows: Long,
                   fetchType: FetchType): RowSet

  @throws[SparkThriftServerSQLException]
  def getDelegationToken(sessionHandle: SessionHandle,
                         authFactory: HiveAuthFactory,
                         owner: String,
                         renewer: String,
                         remoteAddr: String): String

  @throws[SparkThriftServerSQLException]
  def cancelDelegationToken(sessionHandle: SessionHandle,
                            authFactory: HiveAuthFactory,
                            tokenStr: String): Unit

  @throws[SparkThriftServerSQLException]
  def renewDelegationToken(sessionHandle: SessionHandle,
                           authFactory: HiveAuthFactory,
                           tokenStr: String): Unit

}
