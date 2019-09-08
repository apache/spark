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

package org.apache.spark.sql.hive.thriftserver.cli.thrift

import java.io.IOException
import java.net.{InetAddress, UnknownHostException}
import java.util
import java.util.concurrent.TimeUnit
import javax.security.auth.login.LoginException

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.thrift.TException
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.server.{ServerContext, TServer, TServerEventHandler}
import org.apache.thrift.transport.TTransport

import org.apache.spark.internal.Logging
import org.apache.spark.service.cli.thrift._
import org.apache.spark.sql.hive.thriftserver.{AbstractService, ServiceException, ServiceUtils}
import org.apache.spark.sql.hive.thriftserver.auth.{HiveAuthFactory, KERBEROS, NONE, TSetIpAddressProcessor}
import org.apache.spark.sql.hive.thriftserver.cli._
import org.apache.spark.sql.hive.thriftserver.cli.operation.OperationStatus
import org.apache.spark.sql.hive.thriftserver.cli.session.SessionManager
import org.apache.spark.sql.hive.thriftserver.server.SparkThriftServer
import org.apache.spark.sql.hive.thriftserver.server.cli.SparkThriftServerSQLException
import org.apache.spark.sql.types.StructType


abstract class ThriftCLIService(cliService: CLIService, serviceName: String)
  extends AbstractService(serviceName)
    with TCLIService.Iface
    with Runnable
    with Logging {


  private val OK_STATUS = new TStatus(TStatusCode.SUCCESS_STATUS)
  protected var hiveAuthFactory: HiveAuthFactory = new HiveAuthFactory()

  protected var portNum: Int = 0
  protected var serverIPAddress: InetAddress = null
  protected var hiveHost: String = null
  protected var server: TServer = null
  protected var httpServer: org.eclipse.jetty.server.Server = null

  private var isStarted = false
  protected var isEmbedded = false

  protected var hiveConf: HiveConf = null

  protected var minWorkerThreads: Int = 0
  protected var maxWorkerThreads: Int = 0
  protected var workerKeepAliveTime: Long = 0L

  private class ThriftCLIServerContext extends ServerContext {
    private var sessionHandle: SessionHandle = null

    def setSessionHandle(sessionHandle: SessionHandle): Unit = {
      this.sessionHandle = sessionHandle
    }

    def getSessionHandle: SessionHandle = sessionHandle
  }

  protected var currentServerContext = new ThreadLocal[ServerContext]

  protected var serverEventHandler: TServerEventHandler = new TServerEventHandler {
    def createContext(input: TProtocol, output: TProtocol): ServerContext = {
      new ThriftCLIServerContext()
    }

    def deleteContext(serverContext: ServerContext, input: TProtocol, output: TProtocol): Unit = {
      val context: ThriftCLIServerContext = serverContext.asInstanceOf[ThriftCLIServerContext]
      val sessionHandle = context.getSessionHandle
      if (sessionHandle != null) {
        logInfo("Session disconnected without closing properly, close it now")
        try {
          cliService.closeSession(sessionHandle)
        } catch {
          case e: SparkThriftServerSQLException =>
            logWarning("Failed to close session: " + e, e)
        }
      }
    }

    def preServe(): Unit = {}

    def processContext(serverContext: ServerContext,
                       input: TTransport,
                       output: TTransport): Unit = {
      currentServerContext.set(serverContext)
    }
  }


  override def init(hiveConf: HiveConf): Unit = {
    this.hiveConf = hiveConf
    // Initialize common server configs needed in both binary & http modes
    var portString: String = null
    hiveHost = System.getenv("HIVE_SERVER2_THRIFT_BIND_HOST")
    if (hiveHost == null) {
      hiveHost = hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST)
    }
    try {
      if (hiveHost != null && !hiveHost.isEmpty) {
        serverIPAddress = InetAddress.getByName(hiveHost)
      } else {
        serverIPAddress = InetAddress.getLocalHost
      }
    } catch {
      case e: UnknownHostException =>
        throw new ServiceException(e)
    }
    // HTTP mode
    if (SparkThriftServer.isHTTPTransportMode(hiveConf)) {
      workerKeepAliveTime = hiveConf.getTimeVar(
        ConfVars.HIVE_SERVER2_THRIFT_HTTP_WORKER_KEEPALIVE_TIME,
        TimeUnit.SECONDS)
      portString = System.getenv("HIVE_SERVER2_THRIFT_HTTP_PORT")
      if (portString != null) {
        portNum = Integer.valueOf(portString)
      } else {
        portNum = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT)
      }
    } else { // Binary mode
      workerKeepAliveTime = hiveConf.getTimeVar(ConfVars.HIVE_SERVER2_THRIFT_WORKER_KEEPALIVE_TIME,
        TimeUnit.SECONDS)
      portString = System.getenv("HIVE_SERVER2_THRIFT_PORT")
      if (portString != null) {
        portNum = Integer.valueOf(portString)
      } else {
        portNum = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT)
      }
    }
    minWorkerThreads = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_MIN_WORKER_THREADS)
    maxWorkerThreads = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_MAX_WORKER_THREADS)
    super.init(hiveConf)
  }

  override def start(): Unit = {
    super.start()
    if (!isStarted && !isEmbedded) {
      new Thread(this).start()
      isStarted = true
    }
  }

  override def stop(): Unit = {
    if (isStarted && !isEmbedded) {
      if (server != null) {
        server.stop()
        logInfo("Thrift server has stopped")
      }
      if ((httpServer != null) && httpServer.isStarted) {
        try {
          httpServer.stop()
          logInfo("Http server has stopped")
        } catch {
          case e: Exception =>
            logError("Error stopping Http server: ", e)
        }
      }
      isStarted = false
    }
    super.stop()
  }

  def getPortNumber: Int = portNum

  def getServerIPAddress: InetAddress = serverIPAddress

  @throws[TException]
  override def GetDelegationToken(req: TGetDelegationTokenReq): TGetDelegationTokenResp = {
    val resp: TGetDelegationTokenResp = new TGetDelegationTokenResp
    if (hiveAuthFactory == null) {
      resp.setStatus(notSupportTokenErrorStatus)
    } else {
      try {
        val token = cliService.getDelegationToken(
          new SessionHandle(req.getSessionHandle),
          hiveAuthFactory,
          req.getOwner,
          req.getRenewer,
          getIpAddress)
        resp.setDelegationToken(token)
        resp.setStatus(OK_STATUS)
      } catch {
        case e: SparkThriftServerSQLException =>
          logError("Error obtaining delegation token", e)
          val tokenErrorStatus = SparkThriftServerSQLException.toTStatus(e)
          tokenErrorStatus.setSqlState(e.getSQLState)
          tokenErrorStatus.setErrorCode(e.getErrorCode)
          tokenErrorStatus.setErrorMessage(e.getMessage)
          resp.setStatus(tokenErrorStatus)
      }
    }
    resp
  }

  @throws[TException]
  override def CancelDelegationToken(req: TCancelDelegationTokenReq): TCancelDelegationTokenResp = {
    val resp: TCancelDelegationTokenResp = new TCancelDelegationTokenResp
    if (hiveAuthFactory == null) {
      resp.setStatus(notSupportTokenErrorStatus)
    } else {
      try {
        cliService.cancelDelegationToken(
          new SessionHandle(req.getSessionHandle),
          hiveAuthFactory,
          req.getDelegationToken)
        resp.setStatus(OK_STATUS)
      } catch {
        case e: SparkThriftServerSQLException =>
          logError("Error cancel delegation token", e)
          val tokenErrorStatus = SparkThriftServerSQLException.toTStatus(e)
          tokenErrorStatus.setSqlState(e.getSQLState)
          tokenErrorStatus.setErrorCode(e.getErrorCode)
          tokenErrorStatus.setErrorMessage(e.getMessage)
          resp.setStatus(tokenErrorStatus)
      }
    }
    resp
  }

  @throws[TException]
  override def RenewDelegationToken(req: TRenewDelegationTokenReq): TRenewDelegationTokenResp = {
    val resp: TRenewDelegationTokenResp = new TRenewDelegationTokenResp
    if (hiveAuthFactory == null) {
      resp.setStatus(notSupportTokenErrorStatus)
    } else {
      try {
        cliService.renewDelegationToken(
          new SessionHandle(req.getSessionHandle),
          hiveAuthFactory,
          req.getDelegationToken)
        resp.setStatus(OK_STATUS)
      } catch {
        case e: SparkThriftServerSQLException =>
          logError("Error renew delegation token", e)
          val tokenErrorStatus = SparkThriftServerSQLException.toTStatus(e)
          tokenErrorStatus.setSqlState(e.getSQLState)
          tokenErrorStatus.setErrorCode(e.getErrorCode)
          tokenErrorStatus.setErrorMessage(e.getMessage)
          resp.setStatus(tokenErrorStatus)
      }
    }
    resp
  }

  private def notSupportTokenErrorStatus: TStatus = {
    val errorStatus: TStatus = new TStatus(TStatusCode.ERROR_STATUS)
    errorStatus.setErrorMessage("Delegation token is not supported")
    errorStatus
  }

  @throws[TException]
  override def OpenSession(req: TOpenSessionReq): TOpenSessionResp = {
    logInfo("Client protocol version: " + req.getClient_protocol)
    val resp: TOpenSessionResp = new TOpenSessionResp
    try {
      val sessionHandle: SessionHandle = getSessionHandle(req, resp)
      resp.setSessionHandle(sessionHandle.toTSessionHandle)
      // TODO: set real configuration map
      resp.setConfiguration(new util.HashMap[String, String])
      resp.setStatus(OK_STATUS)
      val context: ThriftCLIServerContext =
        currentServerContext.get.asInstanceOf[ThriftCLIServerContext]
      if (context != null) {
        context.setSessionHandle(sessionHandle)
      }
    } catch {
      case e: Exception =>
        logWarning("Error opening session: ", e)
        resp.setStatus(SparkThriftServerSQLException.toTStatus(e))
    }
    resp
  }

  private def getIpAddress: String = {
    var clientIpAddress: String = null
    // Http transport mode.
    // We set the thread local ip address, in ThriftHttpServlet.
    if (cliService.getHiveConf.getVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE)
      .equalsIgnoreCase("http")) {
      clientIpAddress = SessionManager.getIpAddress
    } else { // Kerberos
      if (isKerberosAuthMode) {
        clientIpAddress = hiveAuthFactory.getIpAddress
      } else { // Except kerberos, NOSASL
        clientIpAddress = TSetIpAddressProcessor.getUserIpAddress
      }
    }
    logDebug("Client's IP Address: " + clientIpAddress)
    clientIpAddress
  }

  /**
   * Returns the effective username.
   * 1. If hive.server2.allow.user.substitution = false: the username of the connecting user
   * 2. If hive.server2.allow.user.substitution = true: the username of the end user,
   * that the connecting user is trying to proxy for.
   * This includes a check whether the connecting user is allowed to proxy for the end user.
   *
   * @param req
   * @return
   * @throws SparkThriftServerSQLException
   */
  @throws[SparkThriftServerSQLException]
  private def getUserName(req: TOpenSessionReq): String = {
    var userName: String = null
    if (isKerberosAuthMode) {
      userName = hiveAuthFactory.getRemoteUser
    }
    if (userName == null) {
      userName = TSetIpAddressProcessor.getUserName
    }
    // We set the thread local username, in ThriftHttpServlet.
    if (cliService.getHiveConf.getVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE)
      .equalsIgnoreCase("http")) {
      userName = SessionManager.getUserName
    }
    if (userName == null) {
      userName = req.getUsername
    }
    userName = getShortName(userName)
    val effectiveClientUser: String = getProxyUser(userName, req.getConfiguration, getIpAddress)
    logDebug("Client's username: " + effectiveClientUser)
    effectiveClientUser
  }

  private def getShortName(userName: String): String = {
    var ret: String = null
    if (userName != null) {
      val indexOfDomainMatch: Int = ServiceUtils.indexOfDomainMatch(userName)
      ret = if (indexOfDomainMatch <= 0) {
        userName
      } else {
        userName.substring(0, indexOfDomainMatch)
      }
    }
    ret
  }

  /**
   * Create a session handle
   *
   * @param req
   * @param res
   * @return
   * @throws SparkThriftServerSQLException
   * @throws LoginException
   * @throws IOException
   */
  @throws[SparkThriftServerSQLException]
  @throws[LoginException]
  @throws[IOException]
  private[thrift] def getSessionHandle(req: TOpenSessionReq,
                                       res: TOpenSessionResp): SessionHandle = {
    val userName: String = getUserName(req)
    val ipAddress: String = getIpAddress
    val protocol: TProtocolVersion =
      getMinVersion(CLIService.SERVER_VERSION, req.getClient_protocol)
    res.setServerProtocolVersion(protocol)
    var sessionHandle: SessionHandle = null
    if (cliService.getHiveConf.getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS) &&
      (userName != null)) {
      val delegationTokenStr: String = getDelegationToken(userName)
      sessionHandle = cliService.openSessionWithImpersonation(
        protocol,
        userName,
        req.getPassword,
        ipAddress,
        req.getConfiguration.asScala.toMap,
        delegationTokenStr)
    } else sessionHandle = cliService.openSession(
      protocol,
      userName,
      req.getPassword,
      ipAddress,
      req.getConfiguration.asScala.toMap)
    sessionHandle
  }


  @throws[SparkThriftServerSQLException]
  @throws[LoginException]
  @throws[IOException]
  private def getDelegationToken(userName: String): String = {
    if (userName == null ||
      !cliService.getHiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION)
        .equalsIgnoreCase(KERBEROS.toString)) {
      return null
    }
    try {
      return cliService.getDelegationTokenFromMetaStore(userName)
    } catch {
      case e: UnsupportedOperationException =>
      // The delegation token is not applicable in the given deployment mode
    }
    null
  }

  private def getMinVersion(versions: TProtocolVersion*): TProtocolVersion = {
    val values: Array[TProtocolVersion] = TProtocolVersion.values
    var current: Int = values(values.length - 1).getValue
    for (version <- versions) {
      if (current > version.getValue) {
        current = version.getValue
      }
    }
    for (version <- values) {
      if (version.getValue == current) {
        return version
      }
    }
    throw new IllegalArgumentException("never")
  }

  @throws[TException]
  override def CloseSession(req: TCloseSessionReq): TCloseSessionResp = {
    val resp: TCloseSessionResp = new TCloseSessionResp
    try {
      val sessionHandle: SessionHandle = new SessionHandle(req.getSessionHandle)
      cliService.closeSession(sessionHandle)
      resp.setStatus(OK_STATUS)
      val context: ThriftCLIServerContext =
        currentServerContext.get.asInstanceOf[ThriftCLIServerContext]
      if (context != null) {
        context.setSessionHandle(null)
      }
    } catch {
      case e: Exception =>
        logWarning("Error closing session: ", e)
        resp.setStatus(SparkThriftServerSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetInfo(req: TGetInfoReq): TGetInfoResp = {
    val resp: TGetInfoResp = new TGetInfoResp
    try {
      val getInfoValue: GetInfoValue =
        cliService.getInfo(
          new SessionHandle(req.getSessionHandle),
          GetInfoType.getGetInfoType(req.getInfoType))
      resp.setInfoValue(getInfoValue.toTGetInfoValue)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        logWarning("Error getting info: ", e)
        resp.setStatus(SparkThriftServerSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def ExecuteStatement(req: TExecuteStatementReq): TExecuteStatementResp = {
    val resp: TExecuteStatementResp = new TExecuteStatementResp
    try {
      val sessionHandle: SessionHandle = new SessionHandle(req.getSessionHandle)
      val statement: String = req.getStatement
      val confOverlay: util.Map[String, String] = req.getConfOverlay
      val runAsync: Boolean = req.isRunAsync
      val operationHandle: OperationHandle = if (runAsync) {
        cliService.executeStatementAsync(sessionHandle, statement, confOverlay.asScala.toMap)
      } else {
        cliService.executeStatement(sessionHandle, statement, confOverlay.asScala.toMap)
      }
      resp.setOperationHandle(operationHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        logWarning("Error executing statement: ", e)
        resp.setStatus(SparkThriftServerSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetTypeInfo(req: TGetTypeInfoReq): TGetTypeInfoResp = {
    val resp: TGetTypeInfoResp = new TGetTypeInfoResp
    try {
      val operationHandle: OperationHandle =
        cliService.getTypeInfo(new SessionHandle(req.getSessionHandle))
      resp.setOperationHandle(operationHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        logWarning("Error getting type info: ", e)
        resp.setStatus(SparkThriftServerSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetCatalogs(req: TGetCatalogsReq): TGetCatalogsResp = {
    val resp: TGetCatalogsResp = new TGetCatalogsResp
    try {
      val opHandle: OperationHandle =
        cliService.getCatalogs(new SessionHandle(req.getSessionHandle))
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        logWarning("Error getting catalogs: ", e)
        resp.setStatus(SparkThriftServerSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetSchemas(req: TGetSchemasReq): TGetSchemasResp = {
    val resp: TGetSchemasResp = new TGetSchemasResp
    try {
      val opHandle: OperationHandle =
        cliService.getSchemas(new SessionHandle(req.getSessionHandle),
          req.getCatalogName, req.getSchemaName)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        logWarning("Error getting schemas: ", e)
        resp.setStatus(SparkThriftServerSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetTables(req: TGetTablesReq): TGetTablesResp = {
    val resp: TGetTablesResp = new TGetTablesResp
    try {
      val opHandle: OperationHandle =
        cliService.getTables(
          new SessionHandle(req.getSessionHandle),
          req.getCatalogName,
          req.getSchemaName,
          req.getTableName,
          req.getTableTypes.asScala.toList)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        logWarning("Error getting tables: ", e)
        resp.setStatus(SparkThriftServerSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetTableTypes(req: TGetTableTypesReq): TGetTableTypesResp = {
    val resp: TGetTableTypesResp = new TGetTableTypesResp
    try {
      val opHandle: OperationHandle =
        cliService.getTableTypes(new SessionHandle(req.getSessionHandle))
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        logWarning("Error getting table types: ", e)
        resp.setStatus(SparkThriftServerSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetColumns(req: TGetColumnsReq): TGetColumnsResp = {
    val resp: TGetColumnsResp = new TGetColumnsResp
    try {
      val opHandle: OperationHandle =
        cliService.getColumns(
          new SessionHandle(req.getSessionHandle),
          req.getCatalogName,
          req.getSchemaName,
          req.getTableName,
          req.getColumnName)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        logWarning("Error getting columns: ", e)
        resp.setStatus(SparkThriftServerSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetFunctions(req: TGetFunctionsReq): TGetFunctionsResp = {
    val resp: TGetFunctionsResp = new TGetFunctionsResp
    try {
      val opHandle: OperationHandle = cliService.getFunctions(
        new SessionHandle(req.getSessionHandle),
        req.getCatalogName,
        req.getSchemaName,
        req.getFunctionName)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        logWarning("Error getting functions: ", e)
        resp.setStatus(SparkThriftServerSQLException.toTStatus(e))
    }
    resp
  }

  override def GetPrimaryKeys(req: TGetPrimaryKeysReq): TGetPrimaryKeysResp = {
    val resp = new TGetPrimaryKeysResp
    try {
      val opHandle: OperationHandle =
        cliService.getPrimaryKeys(new SessionHandle(req.getSessionHandle),
          req.getCatalogName,
          req.getSchemaName,
          req.getTableName)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        logWarning("Error getting functions: ", e)
        resp.setStatus(SparkThriftServerSQLException.toTStatus(e))
    }
    resp
  }

  override def GetCrossReference(req: TGetCrossReferenceReq): TGetCrossReferenceResp = {
    val resp = new TGetCrossReferenceResp
    try {
      val opHandle =
        cliService.getCrossReference(
          new SessionHandle(req.getSessionHandle),
          req.getParentCatalogName,
          req.getParentSchemaName,
          req.getParentTableName,
          req.getForeignCatalogName,
          req.getForeignSchemaName,
          req.getForeignTableName)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        logInfo("Error getting functions: ", e)
        resp.setStatus(SparkThriftServerSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetOperationStatus(req: TGetOperationStatusReq): TGetOperationStatusResp = {
    val resp: TGetOperationStatusResp = new TGetOperationStatusResp
    try {
      val operationStatus: OperationStatus =
        cliService.getOperationStatus(new OperationHandle(req.getOperationHandle))
      resp.setOperationState(operationStatus.getState.toTOperationState)
      val opException: SparkThriftServerSQLException = operationStatus.getOperationException
      if (opException != null) {
        resp.setSqlState(opException.getSQLState)
        resp.setErrorCode(opException.getErrorCode)
        resp.setErrorMessage(opException.getMessage)
      }
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        logWarning("Error getting operation status: ", e)
        resp.setStatus(SparkThriftServerSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def CancelOperation(req: TCancelOperationReq): TCancelOperationResp = {
    val resp: TCancelOperationResp = new TCancelOperationResp
    try {
      cliService.cancelOperation(new OperationHandle(req.getOperationHandle))
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        logWarning("Error cancelling operation: ", e)
        resp.setStatus(SparkThriftServerSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def CloseOperation(req: TCloseOperationReq): TCloseOperationResp = {
    val resp: TCloseOperationResp = new TCloseOperationResp
    try {
      cliService.closeOperation(new OperationHandle(req.getOperationHandle))
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        logWarning("Error closing operation: ", e)
        resp.setStatus(SparkThriftServerSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetResultSetMetadata(req: TGetResultSetMetadataReq): TGetResultSetMetadataResp = {
    val resp: TGetResultSetMetadataResp = new TGetResultSetMetadataResp
    try {
      val schema: StructType =
        cliService.getResultSetMetadata(new OperationHandle(req.getOperationHandle))
      resp.setSchema(SchemaMapper.toTTableSchema(schema))
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        logWarning("Error getting result set metadata: ", e)
        resp.setStatus(SparkThriftServerSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def FetchResults(req: TFetchResultsReq): TFetchResultsResp = {
    val resp: TFetchResultsResp = new TFetchResultsResp
    try {
      val rowSet: RowSet =
        cliService.fetchResults(
          new OperationHandle(req.getOperationHandle),
          FetchOrientation.getFetchOrientation(req.getOrientation),
          req.getMaxRows,
          FetchType.getFetchType(req.getFetchType))
      resp.setResults(rowSet.toTRowSet)
      resp.setHasMoreRows(false)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        logWarning("Error fetching results: ", e)
        resp.setStatus(SparkThriftServerSQLException.toTStatus(e))
    }
    resp
  }

  override def run(): Unit

  /**
   * If the proxy user name is provided then check privileges to substitute the user.
   *
   * @param realUser
   * @param sessionConf
   * @param ipAddress
   * @return
   * @throws SparkThriftServerSQLException
   */
  @throws[SparkThriftServerSQLException]
  private def getProxyUser(realUser: String,
                           sessionConf: util.Map[String, String],
                           ipAddress: String): String = {
    var proxyUser: String = null
    // We set the thread local proxy username, in ThriftHttpServlet.
    if (cliService.getHiveConf.getVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE)
      .equalsIgnoreCase("http")) {
      proxyUser = SessionManager.getProxyUserName
      logDebug("Proxy user from query string: " + proxyUser)
    }
    if (proxyUser == null &&
      sessionConf != null &&
      sessionConf.containsKey(HiveAuthFactory.HS2_PROXY_USER)) {
      val proxyUserFromThriftBody: String = sessionConf.get(HiveAuthFactory.HS2_PROXY_USER)
      logDebug("Proxy user from thrift body: " + proxyUserFromThriftBody)
      proxyUser = proxyUserFromThriftBody
    }
    if (proxyUser == null) return realUser
    // check whether substitution is allowed
    if (!hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ALLOW_USER_SUBSTITUTION)) {
      throw new SparkThriftServerSQLException("Proxy user substitution is not allowed")
    }
    // If there's no authentication, then directly substitute the user
    if (NONE.toString.equalsIgnoreCase(hiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION))) {
      return proxyUser
    }
    // Verify proxy user privilege of the realUser for the proxyUser
    HiveAuthFactory.verifyProxyAccess(realUser, proxyUser, ipAddress, hiveConf)
    logDebug("Verified proxy user: " + proxyUser)
    proxyUser
  }

  private def isKerberosAuthMode: Boolean = {
    cliService.getHiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION)
      .equalsIgnoreCase(KERBEROS.toString)
  }


}
