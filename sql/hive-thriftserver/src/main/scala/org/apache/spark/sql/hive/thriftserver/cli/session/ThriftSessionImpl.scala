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

import java.io._

import java.util
import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.SystemVariables._
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.internal.Logging
import org.apache.spark.service.cli.thrift.TProtocolVersion
import org.apache.spark.sql.hive.thriftserver.auth.HiveAuthFactory
import org.apache.spark.sql.hive.thriftserver.cli._
import org.apache.spark.sql.hive.thriftserver.cli.operation.{Operation, OperationManager}
import org.apache.spark.sql.hive.thriftserver.server.cli.SparkThriftServerSQLException
import org.apache.spark.sql.hive.thriftserver.utils.VariableSubstitution
import org.apache.spark.sql.types.StructType


class ThriftSessionImpl(_protocol: TProtocolVersion,
                        var _username: String,
                        var _password: String,
                        serverHiveConf: HiveConf,
                        var _ipAddress: String) extends ThriftSession with Logging {

  private val _sessionHandle: SessionHandle = new SessionHandle(_protocol)
  private val _hiveConf: HiveConf = new HiveConf(serverHiveConf)
  private var _sessionState: SessionState = null
  private var _sessionManager: SessionManager = null
  private var _operationManager: OperationManager = new OperationManager()
  private val _opHandleSet: util.Set[OperationHandle] = new util.HashSet[OperationHandle]()
  private var _isOperationLogEnabled: Boolean = false
  private var _sessionLogDir: File = null
  private var _lastAccessTime: Long = 0L
  private var _lastIdleTime: Long = 0L

  @throws[SparkThriftServerSQLException]
  override def open(sessionConfMap: Map[String, String]): Unit = {
    _sessionState = new SessionState(_hiveConf, _username)
    _sessionState.setUserIpAddress(_ipAddress)
    _sessionState.setIsHiveServerQuery(true)
    SessionState.start(_sessionState)
    if (sessionConfMap != null) {
      configureSession(sessionConfMap.asJava)
    }
    _lastAccessTime = System.currentTimeMillis
    _lastIdleTime = _lastAccessTime
  }

  @throws[SparkThriftServerSQLException]
  private def configureSession(sessionConfMap: util.Map[String, String]): Unit = {
    SessionState.setCurrentSessionState(_sessionState)

    sessionConfMap.entrySet().forEach(entry => {
      val key = entry.getKey
      if (key.startsWith("set:")) {
        try
          setVariable(key.substring(4), entry.getValue)
        catch {
          case e: Exception =>
            throw new SparkThriftServerSQLException(e)
        }
      } else if (key.startsWith("use:")) {
        SessionState.get.setCurrentDatabase(entry.getValue)
      } else {
        _hiveConf.verifyAndSet(key, entry.getValue)
      }
    })
  }

  // Copy from org.apache.hadoop.hive.ql.processors.SetProcessor, only change:
  // setConf(varname, propName, varvalue, true) when varname.startsWith(HIVECONF_PREFIX)
  @throws[Exception]
  def setVariable(key: String, value: String): Int = {
    var varname = key
    val ss = SessionState.get
    if (value.contains("\n")) {
      logError("Warning: Value had a \\n character in it.")
    }
    val substitution = new VariableSubstitution(ss.getHiveVariables)
    varname = varname.trim
    if (varname.startsWith(ENV_PREFIX)) {
      logError("env:* variables can not be set.")
      return 1
    } else if (varname.startsWith(SYSTEM_PREFIX)) {
      val propName = varname.substring(SYSTEM_PREFIX.length)
      System.getProperties.setProperty(propName, substitution.substitute(ss.getConf, value))
    } else if (varname.startsWith(HIVECONF_PREFIX)) {
      val propName = varname.substring(HIVECONF_PREFIX.length)
      setConf(varname, propName, value, true)
    } else if (varname.startsWith(HIVEVAR_PREFIX)) {
      val propName = varname.substring(HIVEVAR_PREFIX.length)
      ss.getHiveVariables.put(propName, substitution.substitute(ss.getConf, value))
    } else if (varname.startsWith(METACONF_PREFIX)) {
      val propName = varname.substring(METACONF_PREFIX.length)
      val hive = Hive.get(ss.getConf)
      hive.setMetaConf(propName, substitution.substitute(ss.getConf, value))
    } else {
      setConf(varname, varname, value, true)
    }
    0
  }

  // returns non-null string for validation fail
  @throws[IllegalArgumentException]
  private def setConf(varname: String, key: String, varvalue: String, register: Boolean): Unit = {
    val substitution = new VariableSubstitution(SessionState.get.getHiveVariables)
    val conf = SessionState.get.getConf
    val value = substitution.substitute(conf, varvalue)
    if (conf.getBoolVar(HiveConf.ConfVars.HIVECONFVALIDATION)) {
      val confVars = HiveConf.getConfVars(key)
      if (confVars != null) {
        if (!confVars.isType(value)) {
          val message = new StringBuilder
          message.append("'SET ").append(varname).append('=').append(varvalue)
          message.append("' FAILED because ").append(key).append(" expects ")
          message.append(confVars.typeString).append(" type value.")
          throw new IllegalArgumentException(message.toString)
        }
        val fail = confVars.validate(value)
        if (fail != null) {
          val message = new StringBuilder
          message.append("'SET ").append(varname).append('=').append(varvalue)
          message.append("' FAILED in validation : ").append(fail).append('.')
          throw new IllegalArgumentException(message.toString)
        }
      } else if (key.startsWith("hive.")) {
        throw new IllegalArgumentException("hive configuration " + key + " does not exists.")
      }
    }
    conf.verifyAndSet(key, value)
    if (register) {
      SessionState.get.getOverriddenConfigurations.put(key, value)
    }
  }

  /**
   * getInfo operation handler
   *
   * @param getInfoType
   * @return
   * @throws SparkThriftServerSQLException
   */
  override def getInfo(getInfoType: GetInfoType): GetInfoValue = {
    import org.apache.hive.common.util.HiveVersionInfo
    acquire(true)
    try {
      getInfoType match {
        case GetInfoType.CLI_SERVER_NAME =>
          return new GetInfoValue("Spark Thrift Server")
        case GetInfoType.CLI_DBMS_NAME =>
          return new GetInfoValue("Apache Spark")
        case GetInfoType.CLI_DBMS_VER =>
          return new GetInfoValue(HiveVersionInfo.getVersion)
        case _ =>
          throw new SparkThriftServerSQLException("Unrecognized GetInfoType value: " +
            getInfoType.toString)
      }
    } finally {
      release(true)
    }
  }

  /**
   * execute operation handler
   *
   * @param statement
   * @param confOverlay
   * @return
   * @throws SparkThriftServerSQLException
   */
  override def executeStatement(statement: String,
                                confOverlay: Map[String, String]): OperationHandle = {
    executeStatementInternal(statement, confOverlay, false)
  }

  override def executeStatementAsync(statement: String,
                                     confOverlay: Map[String, String]): OperationHandle = {
    executeStatementInternal(statement, confOverlay, true)
  }

  @throws[SparkThriftServerSQLException]
  private def executeStatementInternal(statement: String,
                                       confOverlay: Map[String, String],
                                       runAsync: Boolean): OperationHandle = {
    acquire(true)
    val operationManager = getOperationManager
    val operation = operationManager
      .newExecuteStatementOperation(
        getSession,
        statement,
        confOverlay,
        runAsync)
    val opHandle = operation.getHandle
    try {
      operation.run
      _opHandleSet.add(opHandle)
      opHandle
    } catch {
      case e: SparkThriftServerSQLException =>
        // Referring to SQLOperation.java, there is no chance that a
        // HiveSQLException throws and the asyn background operation
        // submits to thread pool successfully at the same time. So, Cleanup
        // opHandle directly when got SparkThriftServerSQLException
        operationManager.closeOperation(opHandle)
        throw e
    } finally {
      release(true)
    }
  }

  /**
   * getTypeInfo operation handler
   *
   * @return
   * @throws SparkThriftServerSQLException
   */
  override def getTypeInfo: OperationHandle = {
    acquire(true)

    val operationManager = getOperationManager
    val operation = operationManager.newGetTypeInfoOperation(getSession)
    val opHandle = operation.getHandle
    try {
      operation.run
      _opHandleSet.add(opHandle)
      return opHandle
    } catch {
      case e: SparkThriftServerSQLException =>
        operationManager.closeOperation(opHandle)
        throw e
    } finally {
      release(true)
    }
  }

  /**
   * getCatalogs operation handler
   *
   * @return
   * @throws SparkThriftServerSQLException
   */
  override def getCatalogs: OperationHandle = {
    acquire(true)

    val operationManager = getOperationManager
    val operation = operationManager.newGetCatalogsOperation(getSession)
    val opHandle = operation.getHandle
    try {
      operation.run
      _opHandleSet.add(opHandle)
      return opHandle
    } catch {
      case e: SparkThriftServerSQLException =>
        operationManager.closeOperation(opHandle)
        throw e
    } finally {
      release(true)
    }
  }

  /**
   * getSchemas operation handler
   *
   * @param catalogName
   * @param schemaName
   * @return
   * @throws SparkThriftServerSQLException
   */
  override def getSchemas(catalogName: String,
                          schemaName: String): OperationHandle = {
    acquire(true)
    val operationManager = getOperationManager
    val operation = operationManager.newGetSchemasOperation(getSession, catalogName, schemaName)
    val opHandle = operation.getHandle
    try {
      operation.run
      _opHandleSet.add(opHandle)
      return opHandle
    } catch {
      case e: SparkThriftServerSQLException =>
        operationManager.closeOperation(opHandle)
        throw e
    } finally {
      release(true)
    }
  }

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
  override def getTables(catalogName: String,
                         schemaName: String,
                         tableName: String,
                         tableTypes: List[String]): OperationHandle = {
    acquire(true)
    val operationManager = getOperationManager
    val operation =
      operationManager.newGetTablesOperation(
        getSession,
        catalogName,
        schemaName,
        tableName,
        tableTypes)
    val opHandle = operation.getHandle
    try {
      operation.run
      _opHandleSet.add(opHandle)
      return opHandle
    } catch {
      case e: SparkThriftServerSQLException =>
        operationManager.closeOperation(opHandle)
        throw e
    } finally {
      release(true)
    }
  }

  /**
   * getTableTypes operation handler
   *
   * @return
   * @throws SparkThriftServerSQLException
   */
  override def getTableTypes: OperationHandle = {
    acquire(true)

    val operationManager = getOperationManager
    val operation = operationManager.newGetTableTypesOperation(getSession)
    val opHandle = operation.getHandle
    try {
      operation.run
      _opHandleSet.add(opHandle)
      return opHandle
    } catch {
      case e: SparkThriftServerSQLException =>
        operationManager.closeOperation(opHandle)
        throw e
    } finally {
      release(true)
    }
  }

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
  override def getColumns(catalogName: String,
                          schemaName: String,
                          tableName: String,
                          columnName: String): OperationHandle = {
    acquire(true)
    val operationManager = getOperationManager
    val operation = operationManager
      .newGetColumnsOperation(
        getSession,
        catalogName,
        schemaName,
        tableName,
        columnName)
    val opHandle = operation.getHandle
    try {
      operation.run
      _opHandleSet.add(opHandle)
      opHandle
    } catch {
      case e: SparkThriftServerSQLException =>
        operationManager.closeOperation(opHandle)
        throw e
    } finally {
      release(true)
    }
  }

  /**
   * getFunctions operation handler
   *
   * @param catalogName
   * @param schemaName
   * @param functionName
   * @return
   * @throws SparkThriftServerSQLException
   */
  override def getFunctions(catalogName: String,
                            schemaName: String,
                            functionName: String): OperationHandle = {
    acquire(true)
    val operationManager = getOperationManager
    val operation = operationManager
      .newGetFunctionsOperation(
        getSession,
        catalogName,
        schemaName,
        functionName)
    val opHandle = operation.getHandle
    try {
      operation.run
      _opHandleSet.add(opHandle)
      opHandle
    } catch {
      case e: SparkThriftServerSQLException =>
        operationManager.closeOperation(opHandle)
        throw e
    } finally {
      release(true)
    }
  }

  @throws[SparkThriftServerSQLException]
  override def getPrimaryKeys(catalog: String, schema: String, table: String): OperationHandle = {
    acquire(true)
    val operationManager = getOperationManager
    val operation: Operation = operationManager
      .newGetPrimaryKeysOperation(
        getSession,
        catalog,
        schema,
        table)
    val opHandle = operation.getHandle
    try {
      operation.run
      _opHandleSet.add(opHandle)
      opHandle
    } catch {
      case e: SparkThriftServerSQLException =>
        operationManager.closeOperation(opHandle)
        throw e
    } finally {
      release(true)
    }
  }

  @throws[SparkThriftServerSQLException]
  override def getCrossReference(primaryCatalog: String,
                                 primarySchema: String,
                                 primaryTable: String,
                                 foreignCatalog: String,
                                 foreignSchema: String,
                                 foreignTable: String): OperationHandle = {
    acquire(true)
    val operationManager = getOperationManager
    val operation: Operation = operationManager
      .newGetCrossReferenceOperation(
        getSession,
        primaryCatalog,
        primarySchema,
        primaryTable,
        foreignCatalog,
        foreignSchema,
        foreignTable)
    val opHandle = operation.getHandle
    try {
      operation.run
      _opHandleSet.add(opHandle)
      opHandle
    } catch {
      case e: SparkThriftServerSQLException =>
        operationManager.closeOperation(opHandle)
        throw e
    } finally {
      release(true)
    }
  }

  /**
   * close the session
   *
   * @throws SparkThriftServerSQLException
   */
  override def close(): Unit = {
    try {
      acquire(true)
      // Iterate through the opHandles and close their operations
      for (opHandle <- _opHandleSet.asScala) {
        _operationManager.closeOperation(opHandle)
      }
      _opHandleSet.clear
      // Cleanup session log directory.
      cleanupSessionLogDir
      val hiveHist = _sessionState.getHiveHistory
      if (null != hiveHist) {
        hiveHist.closeStream()
      }
      try {
        _sessionState.close
      } finally {
        _sessionState = null
      }
    } catch {
      case ioe: IOException =>
        throw new SparkThriftServerSQLException("Failure to close", ioe)
    } finally {
      if (_sessionState != null) {
        try {
          _sessionState.close
        } catch {
          case t: Throwable =>
            logWarning("Error closing session", t)
        }
        _sessionState = null
      }
      release(true)
    }
  }

  override def cancelOperation(opHandle: OperationHandle): Unit = {
    acquire(true)
    try {
      _sessionManager.getOperationManager.cancelOperation(opHandle)
    } finally {
      release(true)
    }
  }

  override def closeOperation(opHandle: OperationHandle): Unit = {
    acquire(true)
    try {
      _operationManager.closeOperation(opHandle)
      _opHandleSet.remove(opHandle)
    } finally {
      release(true)
    }
  }

  private def cleanupSessionLogDir(): Unit = {
    if (isOperationLogEnabled) {
      try {
        FileUtils.forceDelete(_sessionLogDir)
      }
      catch {
        case e: Exception =>
          logError("Failed to cleanup session log dir: " + _sessionHandle, e)
      }
    }
  }

  override def getResultSetMetadata(opHandle: OperationHandle): StructType = {
    acquire(true)
    try {
      _sessionManager.getOperationManager.getOperationResultSetSchema(opHandle)
    } finally {
      release(true)
    }
  }

  override def fetchResults(opHandle: OperationHandle,
                            orientation: FetchOrientation,
                            maxRows: Long,
                            fetchType: FetchType): RowSet = {
    acquire(true)
    try {
      if (fetchType eq FetchType.QUERY_OUTPUT) {
        return _operationManager.getOperationNextRowSet(opHandle, orientation, maxRows)
      }
      return _operationManager.getOperationLogRowSet(opHandle, orientation, maxRows)
    } finally {
      release(true)
    }
  }

  override def getDelegationToken(authFactory: HiveAuthFactory,
                                  owner: String,
                                  renewer: String,
                                  remoteAddr: String): String = {
    HiveAuthFactory.verifyProxyAccess(
      getUsername,
      owner,
      getIpAddress,
      getHiveConf)
    authFactory.getDelegationToken(owner, renewer, remoteAddr)
  }

  override def cancelDelegationToken(authFactory: HiveAuthFactory, tokenStr: String): Unit = {
    HiveAuthFactory.verifyProxyAccess(
      getUsername,
      getUserFromToken(authFactory, tokenStr),
      getIpAddress,
      getHiveConf)
    authFactory.cancelDelegationToken(tokenStr)
  }

  override def renewDelegationToken(authFactory: HiveAuthFactory, tokenStr: String): Unit = {
    HiveAuthFactory.verifyProxyAccess(
      getUsername,
      getUserFromToken(authFactory, tokenStr),
      getIpAddress,
      getHiveConf)
    authFactory.renewDelegationToken(tokenStr)
  }


  // extract the real user from the given token string
  @throws[SparkThriftServerSQLException]
  private def getUserFromToken(authFactory: HiveAuthFactory, tokenStr: String) = {
    authFactory.getUserFromToken(tokenStr)
  }

  protected def acquire(userAccess: Boolean): Unit = {
    // Need to make sure that the this HiveServer2's session's SessionState is
    // stored in the thread local for the handler thread.
    SessionState.setCurrentSessionState(_sessionState)
    if (userAccess) {
      _lastAccessTime = System.currentTimeMillis
    }
  }

  /**
   * 1. We'll remove the ThreadLocal SessionState as this thread might now serve
   * other requests.
   * 2. We'll cache the ThreadLocal RawStore object for this background thread
   * for an orderly cleanup when this thread is garbage collected later.
   */
  protected def release(userAccess: Boolean): Unit = {
    SessionState.detachSession()
    if (userAccess) {
      _lastAccessTime = System.currentTimeMillis
    }
    if (_opHandleSet.isEmpty) {
      _lastIdleTime = System.currentTimeMillis
    } else {
      _lastIdleTime = 0
    }
  }


  override def closeExpiredOperations(): Unit = {
    val handles = _opHandleSet.toArray(new Array[OperationHandle](_opHandleSet.size))
    if (handles.length > 0) {
      val operations = _operationManager.removeExpiredOperations(handles)
      if (!operations.isEmpty) {
        closeTimedOutOperations(operations)
      }
    }
  }

  private def closeTimedOutOperations(operations: List[Operation]): Unit = {
    acquire(false)
    try {
      for (operation <- operations) {
        _opHandleSet.remove(operation.getHandle)
        try {
          operation.close
        } catch {
          case e: Exception =>
            logWarning("Exception is thrown closing timed-out operation " + operation.getHandle, e)
        }
      }
    } finally {
      release(false)
    }
  }

  override def getNoOperationTime: Long = {
    if (_lastIdleTime > 0) {
      System.currentTimeMillis - _lastIdleTime
    } else {
      0
    }
  }

  override def getProtocolVersion: TProtocolVersion = _protocol

  /**
   * Set the session manager for the session
   *
   * @param sessionManager
   */
  override def setSessionManager(sessionManager: SessionManager): Unit = {
    _sessionManager = sessionManager
  }

  /**
   * Get the session manager for the session
   */
  override def getSessionManager: SessionManager = _sessionManager


  private def getOperationManager: OperationManager = _operationManager

  /**
   * Set operation manager for the session
   *
   * @param operationManager
   */
  override def setOperationManager(operationManager: OperationManager): Unit = {
    _operationManager = operationManager
  }

  /**
   * Check whether operation logging is enabled and session dir is created successfully
   */
  override def isOperationLogEnabled: Boolean = _isOperationLogEnabled

  /**
   * Get the session dir, which is the parent dir of operation logs
   *
   * @return a file representing the parent directory of operation logs
   */
  override def getOperationLogSessionDir: File = _sessionLogDir

  /**
   * Set the session dir, which is the parent dir of operation logs
   *
   * @param operationLogRootDir the parent dir of the session dir
   */
  override def setOperationLogSessionDir(operationLogRootDir: File): Unit = {
    if (!operationLogRootDir.exists) {
      logWarning("The operation log root directory is removed, recreating: " +
        operationLogRootDir.getAbsolutePath)
      if (!operationLogRootDir.mkdirs) {
        logWarning("Unable to create operation log root directory: " +
          operationLogRootDir.getAbsolutePath)
      }
    }
    if (!operationLogRootDir.canWrite) {
      logWarning("The operation log root directory is not writable: " +
        operationLogRootDir.getAbsolutePath)
    }
    _sessionLogDir = new File(operationLogRootDir, _sessionHandle.getHandleIdentifier.toString)
    _isOperationLogEnabled = true
    if (!_sessionLogDir.exists) {
      if (!_sessionLogDir.mkdir) {
        logWarning("Unable to create operation log session directory: " +
          _sessionLogDir.getAbsolutePath)
        _isOperationLogEnabled = false
      }
    }
    if (isOperationLogEnabled) {
      logInfo("Operation log session directory is created: " + _sessionLogDir.getAbsolutePath)
    }
  }

  protected def getSession: ThriftSession = this

  override def getSessionHandle: SessionHandle = _sessionHandle

  override def getUsername: String = _username

  override def getPassword: String = _password

  override def getHiveConf: HiveConf = _hiveConf

  override def getSessionState: SessionState = _sessionState

  override def getUserName: String = _username

  override def setUserName(userName: String): Unit = _username = userName

  override def getIpAddress: String = _ipAddress

  override def setIpAddress(ipAddress: String): Unit = _ipAddress = ipAddress

  override def getLastAccessTime: Long = _lastAccessTime

}
