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

package org.apache.spark.sql.thriftserver.cli.thrift

import java.util.{List => JList, Map => JMap}

import org.apache.thrift.TException

import org.apache.spark.sql.thriftserver.auth.HiveAuthFactory
import org.apache.spark.sql.thriftserver.cli._
import org.apache.spark.sql.types.StructType


/**
 * ThriftCLIServiceClient.
 *
 */
class ThriftCLIServiceClient(val cliService: TCLIService.Iface) extends CLIServiceClient {
  @throws[SparkThriftServerSQLException]
  def checkStatus(status: TStatus): Unit = {
    if (TStatusCode.ERROR_STATUS == status.getStatusCode) {
      throw new SparkThriftServerSQLException(status)
    }
  }


  @throws[SparkThriftServerSQLException]
  override def openSession(username: String,
                           password: String,
                           configuration: JMap[String, String]): SessionHandle = try {
    val req = new TOpenSessionReq
    req.setUsername(username)
    req.setPassword(password)
    req.setConfiguration(configuration)
    val resp = cliService.OpenSession(req)
    checkStatus(resp.getStatus)
    new SessionHandle(resp.getSessionHandle, resp.getServerProtocolVersion)
  } catch {
    case e: SparkThriftServerSQLException =>
      throw e
    case e: Exception =>
      throw new SparkThriftServerSQLException(e)
  }

  @throws[SparkThriftServerSQLException]
  override def openSessionWithImpersonation(username: String,
                                            password: String,
                                            configuration: JMap[String, String],
                                            delegationToken: String): SessionHandle =
    throw new SparkThriftServerSQLException("open with impersonation operation " +
      "is not supported in the client")

  @throws[SparkThriftServerSQLException]
  override def closeSession(sessionHandle: SessionHandle): Unit = {
    try {
      val req = new TCloseSessionReq(sessionHandle.toTSessionHandle)
      val resp = cliService.CloseSession(req)
      checkStatus(resp.getStatus)
    } catch {
      case e: SparkThriftServerSQLException =>
        throw e
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }
  }

  @throws[SparkThriftServerSQLException]
  override def getInfo(sessionHandle: SessionHandle,
                       infoType: GetInfoType): GetInfoValue = {
    try {
      // FIXME extract the right info type
      val req = new TGetInfoReq(sessionHandle.toTSessionHandle, infoType.toTGetInfoType)
      val resp = cliService.GetInfo(req)
      checkStatus(resp.getStatus)
      new GetInfoValue(resp.getInfoValue)
    } catch {
      case e: SparkThriftServerSQLException =>
        throw e
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }
  }

  @throws[SparkThriftServerSQLException]
  override def executeStatement(sessionHandle: SessionHandle,
                                statement: String,
                                confOverlay: JMap[String, String]): OperationHandle =
    executeStatementInternal(sessionHandle, statement, confOverlay, false, 0)

  @throws[SparkThriftServerSQLException]
  override def executeStatement(sessionHandle: SessionHandle,
                                statement: String,
                                confOverlay: JMap[String, String],
                                queryTimeout: Long): OperationHandle =
    executeStatementInternal(sessionHandle, statement, confOverlay, false, queryTimeout)

  @throws[SparkThriftServerSQLException]
  override def executeStatementAsync(sessionHandle: SessionHandle,
                                     statement: String,
                                     confOverlay: JMap[String, String]): OperationHandle =
    executeStatementInternal(sessionHandle, statement, confOverlay, true, 0)

  @throws[SparkThriftServerSQLException]
  override def executeStatementAsync(sessionHandle: SessionHandle,
                                     statement: String,
                                     confOverlay: JMap[String, String],
                                     queryTimeout: Long): OperationHandle =
    executeStatementInternal(sessionHandle, statement, confOverlay, true, queryTimeout)

  @throws[SparkThriftServerSQLException]
  private def executeStatementInternal(sessionHandle: SessionHandle,
                                       statement: String,
                                       confOverlay: JMap[String, String],
                                       isAsync: Boolean,
                                       queryTimeout: Long): OperationHandle = {
    try {
      val req = new TExecuteStatementReq(sessionHandle.toTSessionHandle, statement)
      req.setConfOverlay(confOverlay)
      req.setRunAsync(isAsync)
      req.setQueryTimeout(queryTimeout)
      val resp = cliService.ExecuteStatement(req)
      checkStatus(resp.getStatus)
      val protocol = sessionHandle.getProtocolVersion
      new OperationHandle(resp.getOperationHandle, protocol)
    } catch {
      case e: SparkThriftServerSQLException =>
        throw e
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }
  }

  @throws[SparkThriftServerSQLException]
  override def getTypeInfo(sessionHandle: SessionHandle): OperationHandle = try {
      val req = new TGetTypeInfoReq(sessionHandle.toTSessionHandle)
      val resp = cliService.GetTypeInfo(req)
      checkStatus(resp.getStatus)
      val protocol = sessionHandle.getProtocolVersion
      new OperationHandle(resp.getOperationHandle, protocol)
    } catch {
      case e: SparkThriftServerSQLException =>
        throw e
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }

  @throws[SparkThriftServerSQLException]
  override def getCatalogs(sessionHandle: SessionHandle): OperationHandle = try {
      val req = new TGetCatalogsReq(sessionHandle.toTSessionHandle)
      val resp = cliService.GetCatalogs(req)
      checkStatus(resp.getStatus)
      val protocol = sessionHandle.getProtocolVersion
      new OperationHandle(resp.getOperationHandle, protocol)
    } catch {
      case e: SparkThriftServerSQLException =>
        throw e
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }

  @throws[SparkThriftServerSQLException]
  override def getSchemas(sessionHandle: SessionHandle,
                          catalogName: String,
                          schemaName: String): OperationHandle = try {
    val req = new TGetSchemasReq(sessionHandle.toTSessionHandle)
    req.setCatalogName(catalogName)
    req.setSchemaName(schemaName)
    val resp = cliService.GetSchemas(req)
    checkStatus(resp.getStatus)
    val protocol = sessionHandle.getProtocolVersion
    new OperationHandle(resp.getOperationHandle, protocol)
  } catch {
    case e: SparkThriftServerSQLException =>
      throw e
    case e: Exception =>
      throw new SparkThriftServerSQLException(e)
  }

  @throws[SparkThriftServerSQLException]
  override def getTables(sessionHandle: SessionHandle,
                         catalogName: String,
                         schemaName: String,
                         tableName: String,
                         tableTypes: JList[String]): OperationHandle = try {
    val req = new TGetTablesReq(sessionHandle.toTSessionHandle)
    req.setTableName(tableName)
    req.setTableTypes(tableTypes)
    req.setSchemaName(schemaName)
    val resp = cliService.GetTables(req)
    checkStatus(resp.getStatus)
    val protocol = sessionHandle.getProtocolVersion
    new OperationHandle(resp.getOperationHandle, protocol)
  } catch {
    case e: SparkThriftServerSQLException =>
      throw e
    case e: Exception =>
      throw new SparkThriftServerSQLException(e)
  }

  @throws[SparkThriftServerSQLException]
  override def getTableTypes(sessionHandle: SessionHandle): OperationHandle = try {
      val req = new TGetTableTypesReq(sessionHandle.toTSessionHandle)
      val resp = cliService.GetTableTypes(req)
      checkStatus(resp.getStatus)
      val protocol = sessionHandle.getProtocolVersion
      new OperationHandle(resp.getOperationHandle, protocol)
    } catch {
      case e: SparkThriftServerSQLException =>
        throw e
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }

  @throws[SparkThriftServerSQLException]
  override def getColumns(sessionHandle: SessionHandle,
                          catalogName: String,
                          schemaName: String,
                          tableName: String,
                          columnName: String): OperationHandle = {
    try {
      val req = new TGetColumnsReq
      req.setSessionHandle(sessionHandle.toTSessionHandle)
      req.setCatalogName(catalogName)
      req.setSchemaName(schemaName)
      req.setTableName(tableName)
      req.setColumnName(columnName)
      val resp = cliService.GetColumns(req)
      checkStatus(resp.getStatus)
      val protocol = sessionHandle.getProtocolVersion
      new OperationHandle(resp.getOperationHandle, protocol)
    } catch {
      case e: SparkThriftServerSQLException =>
        throw e
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }
  }

  @throws[SparkThriftServerSQLException]
  override def getFunctions(sessionHandle: SessionHandle,
                            catalogName: String,
                            schemaName: String,
                            functionName: String): OperationHandle = {
    try {
      val req = new TGetFunctionsReq(sessionHandle.toTSessionHandle, functionName)
      req.setCatalogName(catalogName)
      req.setSchemaName(schemaName)
      val resp = cliService.GetFunctions(req)
      checkStatus(resp.getStatus)
      val protocol = sessionHandle.getProtocolVersion
      new OperationHandle(resp.getOperationHandle, protocol)
    } catch {
      case e: SparkThriftServerSQLException =>
        throw e
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }
  }

  @throws[SparkThriftServerSQLException]
  override def getOperationStatus(opHandle: OperationHandle): OperationStatus = try {
      val req = new TGetOperationStatusReq(opHandle.toTOperationHandle)
      val resp = cliService.GetOperationStatus(req)
      // Checks the status of the RPC call, throws an exception in case of error
      checkStatus(resp.getStatus)
      val opState = OperationState.getOperationState(resp.getOperationState)
      var opException: SparkThriftServerSQLException = null
      if (opState eq ERROR) {
        opException =
          new SparkThriftServerSQLException(
            resp.getErrorMessage,
            resp.getSqlState,
            resp.getErrorCode)
      }
      new OperationStatus(opState, opException)
    } catch {
      case e: SparkThriftServerSQLException =>
        throw e
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }

  @throws[SparkThriftServerSQLException]
  override def cancelOperation(opHandle: OperationHandle): Unit = {
    try {
      val req = new TCancelOperationReq(opHandle.toTOperationHandle)
      val resp = cliService.CancelOperation(req)
      checkStatus(resp.getStatus)
    } catch {
      case e: SparkThriftServerSQLException =>
        throw e
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }
  }

  @throws[SparkThriftServerSQLException]
  override def closeOperation(opHandle: OperationHandle): Unit = {
    try {
      val req = new TCloseOperationReq(opHandle.toTOperationHandle)
      val resp = cliService.CloseOperation(req)
      checkStatus(resp.getStatus)
    } catch {
      case e: SparkThriftServerSQLException =>
        throw e
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }
  }

  @throws[SparkThriftServerSQLException]
  override def getResultSetMetadata(opHandle: OperationHandle): StructType = try {
    val req = new TGetResultSetMetadataReq(opHandle.toTOperationHandle)
    val resp = cliService.GetResultSetMetadata(req)
    checkStatus(resp.getStatus)
    SchemaMapper.toStructType(resp.getSchema)
  } catch {
    case e: SparkThriftServerSQLException =>
      throw e
    case e: Exception =>
      throw new SparkThriftServerSQLException(e)
  }

  @throws[SparkThriftServerSQLException]
  override def fetchResults(opHandle: OperationHandle,
                            orientation: FetchOrientation,
                            maxRows: Long,
                            fetchType: FetchType): RowSet = try {
    val req = new TFetchResultsReq
    req.setOperationHandle(opHandle.toTOperationHandle)
    req.setOrientation(orientation.toTFetchOrientation)
    req.setMaxRows(maxRows)
    req.setFetchType(fetchType.toTFetchType)
    val resp = cliService.FetchResults(req)
    checkStatus(resp.getStatus)
    RowSetFactory.create(resp.getResults, opHandle.getProtocolVersion)
  } catch {
    case e: SparkThriftServerSQLException =>
      throw e
    case e: Exception =>
      throw new SparkThriftServerSQLException(e)
  }

  @throws[SparkThriftServerSQLException]
  override def fetchResults(opHandle: OperationHandle): RowSet = {
    // TODO: set the correct default fetch size
    fetchResults(opHandle, FetchOrientation.FETCH_NEXT, 10000, FetchType.QUERY_OUTPUT)
  }

  @throws[SparkThriftServerSQLException]
  override def getDelegationToken(sessionHandle: SessionHandle,
                                  authFactory: HiveAuthFactory,
                                  owner: String,
                                  renewer: String): String = {
    val req = new TGetDelegationTokenReq(sessionHandle.toTSessionHandle, owner, renewer)
    try {
      val tokenResp = cliService.GetDelegationToken(req)
      checkStatus(tokenResp.getStatus)
      tokenResp.getDelegationToken
    } catch {
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }
  }

  @throws[SparkThriftServerSQLException]
  override def cancelDelegationToken(sessionHandle: SessionHandle,
                                     authFactory: HiveAuthFactory,
                                     tokenStr: String): Unit = {
    val cancelReq = new TCancelDelegationTokenReq(sessionHandle.toTSessionHandle, tokenStr)
    try {
      val cancelResp = cliService.CancelDelegationToken(cancelReq)
      checkStatus(cancelResp.getStatus)
    } catch {
      case e: TException =>
        throw new SparkThriftServerSQLException(e)
    }
  }

  @throws[SparkThriftServerSQLException]
  override def renewDelegationToken(sessionHandle: SessionHandle,
                                    authFactory: HiveAuthFactory,
                                    tokenStr: String): Unit = {
    val cancelReq = new TRenewDelegationTokenReq(sessionHandle.toTSessionHandle, tokenStr)
    try {
      val renewResp = cliService.RenewDelegationToken(cancelReq)
      checkStatus(renewResp.getStatus)
    } catch {
      case e: Exception =>
        throw new SparkThriftServerSQLException(e)
    }
  }

  @throws[SparkThriftServerSQLException]
  override def getPrimaryKeys(sessionHandle: SessionHandle,
                              catalog: String,
                              schema: String,
                              table: String): OperationHandle = try {
    val req = new TGetPrimaryKeysReq(sessionHandle.toTSessionHandle)
    req.setCatalogName(catalog)
    req.setSchemaName(schema)
    req.setTableName(table)
    val resp = cliService.GetPrimaryKeys(req)
    checkStatus(resp.getStatus)
    val protocol = sessionHandle.getProtocolVersion
    new OperationHandle(resp.getOperationHandle, protocol)
  } catch {
    case e: SparkThriftServerSQLException =>
      throw e
    case e: Exception =>
      throw new SparkThriftServerSQLException(e)
  }

  @throws[SparkThriftServerSQLException]
  override def getCrossReference(sessionHandle: SessionHandle,
                                 primaryCatalog: String,
                                 primarySchema: String,
                                 primaryTable: String,
                                 foreignCatalog: String,
                                 foreignSchema: String,
                                 foreignTable: String): OperationHandle = try {
    val req = new TGetCrossReferenceReq(sessionHandle.toTSessionHandle)
    req.setParentCatalogName(primaryCatalog)
    req.setParentSchemaName(primarySchema)
    req.setParentTableName(primaryTable)
    req.setForeignCatalogName(foreignCatalog)
    req.setForeignSchemaName(foreignSchema)
    req.setForeignTableName(foreignTable)
    val resp = cliService.GetCrossReference(req)
    checkStatus(resp.getStatus)
    val protocol = sessionHandle.getProtocolVersion
    new OperationHandle(resp.getOperationHandle, protocol)
  } catch {
    case e: SparkThriftServerSQLException =>
      throw e
    case e: Exception =>
      throw new SparkThriftServerSQLException(e)
  }

  override def getQueryId(opHandle: TOperationHandle): String = try
    return cliService.GetQueryId(new TGetQueryIdReq(opHandle)).getQueryId
  catch {
    case e: TException =>
      throw new SparkThriftServerSQLException(e)
  }
}
