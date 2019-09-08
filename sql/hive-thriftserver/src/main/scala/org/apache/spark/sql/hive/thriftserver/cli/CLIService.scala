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

package org.apache.spark.sql.hive.thriftserver.cli

import java.io.IOException
import java.util.concurrent.{CancellationException, ExecutionException, TimeoutException, TimeUnit}
import javax.security.auth.login.LoginException

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.shims.Utils
import org.apache.hadoop.security.{SecurityUtil, UserGroupInformation}

import org.apache.spark.internal.Logging
import org.apache.spark.service.cli.thrift.TProtocolVersion
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.thriftserver.{CompositeService, ServiceException}
import org.apache.spark.sql.hive.thriftserver.auth.HiveAuthFactory
import org.apache.spark.sql.hive.thriftserver.cli.operation.{Operation, OperationStatus}
import org.apache.spark.sql.hive.thriftserver.cli.session.{SessionManager, ThriftSession}
import org.apache.spark.sql.hive.thriftserver.server.SparkThriftServer
import org.apache.spark.sql.hive.thriftserver.server.cli.SparkThriftServerSQLException
import org.apache.spark.sql.types.StructType

class CLIService(hiveServer2: SparkThriftServer, sqlContext: SQLContext)
  extends CompositeService(classOf[CLIService].getSimpleName)
    with ICLIService with Logging {

  import CLIService._

  private var hiveConf: HiveConf = null
  private var sessionManager: SessionManager = null
  private var serviceUGI: UserGroupInformation = null
  private var httpUGI: UserGroupInformation = null

  private var delegationTokenFetchThread: Thread = null

  override def init(hiveConf: HiveConf): Unit = {
    this.hiveConf = hiveConf
    sessionManager = new SessionManager(hiveServer2, sqlContext)
    addService(sessionManager)

    if (UserGroupInformation.isSecurityEnabled) {
      try {
        val principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL)
        val keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB)
        if (principal.isEmpty || keyTabFile.isEmpty) {
          throw new IOException(
            "HiveServer2 Kerberos principal or keytab is not correctly configured")
        }
        val originalUgi = UserGroupInformation.getCurrentUser
        serviceUGI =
          if (HiveAuthFactory.needUgiLogin(originalUgi,
            SecurityUtil.getServerPrincipal(principal, "0.0.0.0"), keyTabFile)) {
            HiveAuthFactory.loginFromKeytab(hiveConf)
            Utils.getUGI()
          } else {
            originalUgi
          }
      } catch {
        case e@(_: IOException | _: LoginException) =>
          throw new ServiceException("Unable to login to kerberos with given principal/keytab", e)
      }

      // Try creating spnego UGI if it is configured.
      val principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_PRINCIPAL).trim
      val keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_KEYTAB).trim
      if (principal.nonEmpty && keyTabFile.nonEmpty) {
        try {
          httpUGI = HiveAuthFactory.loginFromSpnegoKeytabAndReturnUGI(hiveConf)
        } catch {
          case e: IOException =>
            throw new ServiceException("Unable to login to spnego with given principal " +
              s"$principal and keytab $keyTabFile: $e", e)
        }
      }
    }

    delegationTokenFetcher = new DelegationTokenHandler(new HiveConf())
    delegationTokenFetchThread = new Thread(delegationTokenFetcher)
    super.init(hiveConf)
  }

  override def start(): Unit = {
    try {
      delegationTokenFetchThread.start()
    } catch {
      case e: Exception =>
        logError("Start DelegationTokenThread failed.", e)
    }
    super.start()
  }

  def getServiceUGI: UserGroupInformation = this.serviceUGI

  def getHttpUGI: UserGroupInformation = this.httpUGI

  def openSession(protocol: TProtocolVersion,
                  username: String,
                  password: String,
                  configuration: Predef.Map[String, String]): SessionHandle = {
    val sessionHandle: SessionHandle =
      sessionManager.openSession(protocol,
        username,
        password,
        null,
        configuration,
        false,
        null)
    logDebug(sessionHandle + ": openSession()")
    sessionHandle
  }

  def openSession(protocol: TProtocolVersion,
                  username: String,
                  password: String,
                  ipAddress: String,
                  configuration: Predef.Map[String, String]): SessionHandle = {
    val sessionHandle =
      sessionManager.openSession(protocol,
        username,
        password,
        ipAddress,
        configuration,
        false,
        null)
    logDebug(sessionHandle + ": openSession()")
    sessionHandle
  }

  def openSessionWithImpersonation(protocol: TProtocolVersion,
                                   username: String,
                                   password: String,
                                   configuration: Predef.Map[String, String],
                                   delegationToken: String): SessionHandle = {
    val sessionHandle =
      sessionManager.openSession(protocol,
        username,
        password,
        null,
        configuration,
        true,
        delegationToken)
    logDebug(sessionHandle + ": openSessionWithImpersonation()")
    sessionHandle
  }

  def openSessionWithImpersonation(protocol: TProtocolVersion,
                                   username: String,
                                   password: String,
                                   ipAddress: String,
                                   configuration: Predef.Map[String, String],
                                   delegationToken: String): SessionHandle = {
    val sessionHandle =
      sessionManager.openSession(protocol,
        username,
        password,
        ipAddress,
        configuration,
        true,
        delegationToken)
    logDebug(sessionHandle + ": openSessionWithImpersonation()")
    sessionHandle
  }

  override def openSession(username: String,
                           password: String,
                           configuration: Predef.Map[String, String]): SessionHandle = {
    val sessionHandle =
      sessionManager.openSession(
        SERVER_VERSION,
        username,
        password,
        null,
        configuration,
        false,
        null)
    logDebug(sessionHandle + ": openSession()")
    sessionHandle
  }

  override def openSessionWithImpersonation(username: String,
                                            password: String,
                                            configuration: Predef.Map[String, String],
                                            delegationToken: String): SessionHandle = {
    val sessionHandle =
      sessionManager.openSession(SERVER_VERSION,
        username,
        password,
        null,
        configuration,
        true,
        delegationToken)
    logDebug(sessionHandle + ": openSessionWithImpersonation()")
    sessionHandle
  }

  override def closeSession(sessionHandle: SessionHandle): Unit = {
    val session: ThriftSession = sessionManager.getSession(sessionHandle)
    sessionManager.closeSession(sessionHandle)
    logDebug(sessionHandle + ": closeSession()")
  }


  override def getInfo(sessionHandle: SessionHandle,
                       infoType: GetInfoType): GetInfoValue = {
    val infoValue = sessionManager.getSession(sessionHandle).getInfo(infoType)
    logDebug(sessionHandle + ": getInfo()")
    infoValue
  }

  override def executeStatement(sessionHandle: SessionHandle,
                                statement: String,
                                confOverlay: Predef.Map[String, String]): OperationHandle = {
    val session = sessionManager.getSession(sessionHandle)
    val opHandle: OperationHandle = session.executeStatement(statement, confOverlay)
    logDebug(sessionHandle + ": executeStatement()")
    opHandle
  }

  override def executeStatementAsync(sessionHandle: SessionHandle,
                                     statement: String,
                                     confOverlay: Predef.Map[String, String]): OperationHandle = {
    val opHandle: OperationHandle =
      sessionManager.getSession(sessionHandle)
        .executeStatementAsync(statement, confOverlay)
    logDebug(sessionHandle + ": executeStatementAsync()")
    opHandle
  }

  override def getTypeInfo(sessionHandle: SessionHandle): OperationHandle = {
    val opHandle: OperationHandle = sessionManager.getSession(sessionHandle).getTypeInfo
    logDebug(sessionHandle + ": getTypeInfo()")
    opHandle
  }

  override def getCatalogs(sessionHandle: SessionHandle): OperationHandle = {
    val opHandle: OperationHandle = sessionManager.getSession(sessionHandle).getTypeInfo
    logDebug(sessionHandle + ": getTypeInfo()")
    opHandle
  }


  override def getSchemas(sessionHandle: SessionHandle,
                          catalogName: String,
                          schemaName: String): OperationHandle = {
    val opHandle: OperationHandle =
      sessionManager.getSession(sessionHandle)
        .getSchemas(catalogName, schemaName)
    logDebug(sessionHandle + ": getSchemas()")
    opHandle
  }

  override def getTables(sessionHandle: SessionHandle,
                         catalogName: String,
                         schemaName: String,
                         tableName: String,
                         tableTypes: List[String]): OperationHandle = {
    val opHandle: OperationHandle =
      sessionManager.getSession(sessionHandle)
        .getTables(catalogName, schemaName, tableName, tableTypes)
    logDebug(sessionHandle + ": getTables()")
    opHandle
  }


  override def getTableTypes(sessionHandle: SessionHandle): OperationHandle = {
    val opHandle: OperationHandle = sessionManager.getSession(sessionHandle).getTableTypes
    logDebug(sessionHandle + ": getTableTypes()")
    opHandle
  }

  override def getColumns(sessionHandle: SessionHandle,
                          catalogName: String,
                          schemaName: String,
                          tableName: String,
                          columnName: String): OperationHandle = {
    val opHandle: OperationHandle =
      sessionManager.getSession(sessionHandle)
        .getColumns(catalogName, schemaName, tableName, columnName)
    logDebug(sessionHandle + ": getColumns()")
    opHandle
  }


  override def getFunctions(sessionHandle: SessionHandle,
                            catalogName: String,
                            schemaName: String,
                            functionName: String): OperationHandle = {
    val opHandle: OperationHandle =
      sessionManager.getSession(sessionHandle)
        .getFunctions(catalogName, schemaName, functionName)
    logDebug(sessionHandle + ": getFunctions()")
    opHandle
  }

  @throws[SparkThriftServerSQLException]
  override def getPrimaryKeys(sessionHandle: SessionHandle,
                              catalog: String,
                              schema: String,
                              table: String): OperationHandle = {
    val opHandle = sessionManager.getSession(sessionHandle).getPrimaryKeys(catalog, schema, table)
    logDebug(sessionHandle + ": getPrimaryKeys()")
    opHandle
  }

  @throws[SparkThriftServerSQLException]
  override def getCrossReference(sessionHandle: SessionHandle,
                                 primaryCatalog: String,
                                 primarySchema: String,
                                 primaryTable: String,
                                 foreignCatalog: String,
                                 foreignSchema: String,
                                 foreignTable: String): OperationHandle = {
    val opHandle = sessionManager.getSession(sessionHandle)
      .getCrossReference(primaryCatalog,
        primarySchema,
        primaryTable,
        foreignCatalog,
        foreignSchema,
        foreignTable)
    logDebug(sessionHandle + ": getCrossReference()")
    opHandle
  }

  override def getOperationStatus(opHandle: OperationHandle): OperationStatus = {
    val operation = sessionManager.getOperationManager.getOperation(opHandle)

    /*
     * If this is a background operation run asynchronously,
     * we block for a configured duration, before we return
     * (duration: HIVE_SERVER2_LONG_POLLING_TIMEOUT).
     * However, if the background operation is complete, we return immediately.
     */
    if (operation.shouldRunAsync) {
      val conf = operation.getParentSession.getHiveConf
      val timeout =
        HiveConf.getTimeVar(conf,
          HiveConf.ConfVars.HIVE_SERVER2_LONG_POLLING_TIMEOUT,
          TimeUnit.MILLISECONDS)
      try
        operation.getBackgroundHandle.get(timeout, TimeUnit.MILLISECONDS)
      catch {
        case e: TimeoutException =>
          // No Op, return to the caller since long polling timeout has expired
          logTrace(opHandle + ": Long polling timed out")
        case e: CancellationException =>
          // The background operation thread was cancelled
          logTrace(opHandle + ": The background operation was cancelled", e)
        case e: ExecutionException =>
          // The background operation thread was aborted
          logTrace(opHandle + ": The background operation was aborted", e)
        case e: InterruptedException =>

        // No op, this thread was interrupted
        // In this case, the call might return sooner than long polling timeout
      }
    }
    val opStatus = operation.getStatus
    logDebug(opHandle + ": getOperationStatus()")
    opStatus
  }

  override def cancelOperation(opHandle: OperationHandle): Unit = {
    sessionManager.getOperationManager
      .getOperation(opHandle)
      .getParentSession
      .cancelOperation(opHandle)
    logDebug(opHandle + ": cancelOperation()")
  }

  override def closeOperation(opHandle: OperationHandle): Unit = {
    sessionManager.getOperationManager
      .getOperation(opHandle)
      .getParentSession
      .closeOperation(opHandle)
    logDebug(opHandle + ": closeOperation")
  }

  override def getResultSetMetadata(opHandle: OperationHandle): StructType = {
    val tableSchema = sessionManager
      .getOperationManager
      .getOperation(opHandle)
      .getParentSession
      .getResultSetMetadata(opHandle)
    logDebug(opHandle + ": getResultSetMetadata()")
    tableSchema
  }

  override def fetchResults(opHandle: OperationHandle): RowSet = {
    fetchResults(opHandle,
      Operation.DEFAULT_FETCH_ORIENTATION,
      Operation.DEFAULT_FETCH_MAX_ROWS,
      FetchType.QUERY_OUTPUT)
  }

  override def fetchResults(opHandle: OperationHandle,
                            orientation: FetchOrientation,
                            maxRows: Long,
                            fetchType: FetchType): RowSet = {
    val rowSet: RowSet = sessionManager
      .getOperationManager
      .getOperation(opHandle)
      .getParentSession
      .fetchResults(opHandle, orientation, maxRows, fetchType)
    logDebug(opHandle + ": fetchResults()")
    rowSet
  }

  // obtain delegation token for the give user from metastore
  @throws[SparkThriftServerSQLException]
  @throws[UnsupportedOperationException]
  @throws[LoginException]
  @throws[IOException]
  def getDelegationTokenFromMetaStore(owner: String): String = {
    if (!hiveConf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL) ||
      !hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS)) {
      throw new UnsupportedOperationException("delegation token is can only " +
        "be obtained for a secure remote metastore")
    }
    try {
      delegationTokenFetcher.getDelegationToken(owner)
    } catch {
      case e: HiveException =>
        if (e.getCause.isInstanceOf[UnsupportedOperationException]) {
          throw e.getCause.asInstanceOf[UnsupportedOperationException]
        } else {
          throw new SparkThriftServerSQLException("Error connect metastore to " +
            "setup impersonation", e)
        }
    }
  }


  @throws[SparkThriftServerSQLException]
  override def getDelegationToken(sessionHandle: SessionHandle,
                                  authFactory: HiveAuthFactory,
                                  owner: String,
                                  renewer: String,
                                  remoteAddr: String): String = {
    val delegationToken = sessionManager.getSession(sessionHandle)
      .getDelegationToken(authFactory, owner, renewer, remoteAddr)
    logInfo(sessionHandle + ": getDelegationToken()")
    delegationToken
  }

  @throws[SparkThriftServerSQLException]
  override def cancelDelegationToken(sessionHandle: SessionHandle,
                                     authFactory: HiveAuthFactory,
                                     tokenStr: String): Unit = {
    sessionManager.getSession(sessionHandle).cancelDelegationToken(authFactory, tokenStr)
    logInfo(sessionHandle + ": cancelDelegationToken()")
  }

  @throws[SparkThriftServerSQLException]
  override def renewDelegationToken(sessionHandle: SessionHandle,
                                    authFactory: HiveAuthFactory,
                                    tokenStr: String): Unit = {
    sessionManager.getSession(sessionHandle).renewDelegationToken(authFactory, tokenStr)
    logInfo(sessionHandle + ": renewDelegationToken()")
  }

  def getSessionManager: SessionManager = sessionManager
}

object CLIService {
  val protocols: Array[TProtocolVersion] = TProtocolVersion.values()
  val SERVER_VERSION: TProtocolVersion = protocols(protocols.length - 1)
  var delegationTokenFetcher: DelegationTokenHandler = null
}