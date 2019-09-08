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

import java.io.{File, IOException}
import java.util.Date
import java.util.concurrent._

import org.apache.commons.io.FileUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars

import org.apache.spark.internal.Logging
import org.apache.spark.service.cli.thrift.TProtocolVersion
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.thriftserver.{CompositeService, HiveThriftServer2}
import org.apache.spark.sql.hive.thriftserver.cli.SessionHandle
import org.apache.spark.sql.hive.thriftserver.cli.operation.OperationManager
import org.apache.spark.sql.hive.thriftserver.server.{NamedThreadFactory, SparkThriftServer}
import org.apache.spark.sql.hive.thriftserver.server.cli.SparkThriftServerSQLException

class SessionManager(hiveServer2: SparkThriftServer, sqlContext: SQLContext)
  extends CompositeService(classOf[SessionManager].getSimpleName)
    with Logging {

  private var hiveConf: HiveConf = null
  private val handleToSession: ConcurrentHashMap[SessionHandle, ThriftSession] =
    new ConcurrentHashMap[SessionHandle, ThriftSession]
  private val operationManager: OperationManager = new OperationManager()
  private var backgroundOperationPool: ThreadPoolExecutor = null
  private var isOperationLogEnabled: Boolean = false
  private var operationLogRootDir: File = null

  private var checkInterval: Long = 0L
  private var sessionTimeout: Long = 0L
  private var checkOperation: Boolean = false

  private var shutdown: Boolean = false


  override def init(hiveConf: HiveConf): Unit = {
    this.hiveConf = hiveConf
    // Create operation log root directory, if operation logging is enabled
    if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED)) {
      initOperationLogRootDir
    }
    createBackgroundOperationPool
    addService(operationManager)
    super.init(hiveConf)
  }

  private def createBackgroundOperationPool(): Unit = {
    val poolSize = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_THREADS)
    logInfo("HiveServer2: Background operation thread pool size: " + poolSize)
    val poolQueueSize = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_WAIT_QUEUE_SIZE)
    logInfo("HiveServer2: Background operation thread wait queue size: " + poolQueueSize)
    val keepAliveTime = HiveConf.getTimeVar(hiveConf,
      ConfVars.HIVE_SERVER2_ASYNC_EXEC_KEEPALIVE_TIME, TimeUnit.SECONDS)
    logInfo("HiveServer2: Background operation thread keepalive time: " +
      keepAliveTime + " seconds")
    // Create a thread pool with #poolSize threads
    // Threads terminate when they are idle for more than the keepAliveTime
    // A bounded blocking queue is used to queue incoming operations, if #operations > poolSize
    val threadPoolName = "HiveServer2-Background-Pool"
    backgroundOperationPool =
      new ThreadPoolExecutor(poolSize,
        poolSize, keepAliveTime,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue[Runnable](poolQueueSize),
        new NamedThreadFactory(threadPoolName))
    backgroundOperationPool.allowCoreThreadTimeOut(true)
    checkInterval = HiveConf.getTimeVar(hiveConf,
      ConfVars.HIVE_SERVER2_SESSION_CHECK_INTERVAL, TimeUnit.MILLISECONDS)
    sessionTimeout = HiveConf.getTimeVar(hiveConf,
      ConfVars.HIVE_SERVER2_IDLE_SESSION_TIMEOUT, TimeUnit.MILLISECONDS)
    checkOperation = HiveConf.getBoolVar(hiveConf,
      ConfVars.HIVE_SERVER2_IDLE_SESSION_CHECK_OPERATION)
  }

  private def initOperationLogRootDir(): Unit = {
    operationLogRootDir =
      new File(hiveConf.getVar(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION))
    isOperationLogEnabled = true
    if (operationLogRootDir.exists && !operationLogRootDir.isDirectory) {
      logWarning("The operation log root directory exists, " +
        "but it is not a directory: " + operationLogRootDir.getAbsolutePath)
      isOperationLogEnabled = false
    }
    if (!operationLogRootDir.exists) if (!operationLogRootDir.mkdirs) {
      logWarning("Unable to create operation log root directory: " +
        operationLogRootDir.getAbsolutePath)
      isOperationLogEnabled = false
    }
    if (isOperationLogEnabled) {
      logWarning("Operation log root directory is created: " + operationLogRootDir.getAbsolutePath)
      try
        FileUtils.forceDeleteOnExit(operationLogRootDir)
      catch {
        case e: IOException =>
          logWarning("Failed to schedule cleanup HS2 operation logging root dir: " +
            operationLogRootDir.getAbsolutePath, e)
      }
    }
  }

  override def start(): Unit = {
    super.start()
    if (checkInterval > 0) {
      startTimeoutChecker()
    }
  }

  private def startTimeoutChecker(): Unit = {
    val interval: Long = Math.max(checkInterval, 3000L)
    // minimum 3 seconds
    val timeoutChecker: Runnable = new Runnable() {
      override def run(): Unit = {
        sleepInterval(interval)
        while ( {
          !shutdown
        }) {
          val current: Long = System.currentTimeMillis
          handleToSession.values().forEach(session => {
            if (sessionTimeout > 0 &&
              session.getLastAccessTime + sessionTimeout <= current &&
              (!checkOperation || session.getNoOperationTime > sessionTimeout)) {
              val handle: SessionHandle = session.getSessionHandle
              logWarning("Session " + handle + " is Timed-out (last access : " +
                new Date(session.getLastAccessTime) + ") and will be closed")
              try
                closeSession(handle)
              catch {
                case e: SparkThriftServerSQLException =>
                  logWarning("Exception is thrown closing session " + handle, e)
              }
            }
            else session.closeExpiredOperations
          })
          sleepInterval(interval)
        }
      }

      private

      def sleepInterval(interval: Long): Unit = {
        try
          Thread.sleep(interval)
        catch {
          case e: InterruptedException =>

          // ignore
        }
      }
    }
    backgroundOperationPool.execute(timeoutChecker)
  }

  override def stop(): Unit = {
    super.stop()
    shutdown = true
    if (backgroundOperationPool != null) {
      backgroundOperationPool.shutdown()
      val timeout =
        hiveConf.getTimeVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)
      try
        backgroundOperationPool.awaitTermination(timeout, TimeUnit.SECONDS)
      catch {
        case e: InterruptedException =>
          logWarning("HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT = " + timeout +
            " seconds has been exceeded. RUNNING background operations will be shut down", e)
      }
      backgroundOperationPool = null
    }
    cleanupLoggingRootDir()
  }

  private def cleanupLoggingRootDir(): Unit = {
    if (isOperationLogEnabled) try
      FileUtils.forceDelete(operationLogRootDir)
    catch {
      case e: Exception =>
        logWarning("Failed to cleanup root dir of HS2 logging: " +
          operationLogRootDir.getAbsolutePath, e)
    }
  }

  @throws[SparkThriftServerSQLException]
  def openSession(protocol: TProtocolVersion,
                  username: String,
                  password: String,
                  ipAddress: String,
                  sessionConf: Map[String, String]): SessionHandle =
    openSession(protocol, username, password, ipAddress, sessionConf, false, null)

  @throws[SparkThriftServerSQLException]
  def openSession(protocol: TProtocolVersion,
                  username: String,
                  password: String,
                  ipAddress: String,
                  sessionConf: Map[String, String],
                  withImpersonation: Boolean,
                  delegationToken: String): SessionHandle = {
    var session: ThriftSession = null
    // If doAs is set to true for HiveServer2, we will create a proxy object for the session impl.
    // Within the proxy object, we wrap the method call in a UserGroupInformation#doAs
    if (withImpersonation) {
      val sessionWithUGI =
        new ThriftSessionImplWithUgi(protocol,
          username,
          password,
          hiveConf,
          ipAddress,
          delegationToken)
      session = ThriftSessionProxy.getProxy(sessionWithUGI, sessionWithUGI.getSessionUgi)
      sessionWithUGI.setProxySession(session)
    } else {
      session = new ThriftSessionImpl(protocol, username, password, hiveConf, ipAddress)
    }
    session.setSessionManager(this)
    session.setOperationManager(operationManager)
    try
      session.open(sessionConf)
    catch {
      case e: Exception =>
        try
          session.close
        catch {
          case t: Throwable =>
            logWarning("Error closing session", t)
        }
        session = null
        throw new SparkThriftServerSQLException("Failed to open new session: " + e, e)
    }
    if (isOperationLogEnabled) {
      session.setOperationLogSessionDir(operationLogRootDir)
    }
    handleToSession.put(session.getSessionHandle, session)
    val sessionHandle = session.getSessionHandle
    HiveThriftServer2.listener.onSessionCreated(
      session.getIpAddress, sessionHandle.getSessionId.toString, session.getUsername)
    val ctx = if (sqlContext.conf.hiveThriftServerSingleSession) {
      sqlContext
    } else {
      sqlContext.newSession()
    }
    ctx.setConf(HiveUtils.FAKE_HIVE_VERSION.key, HiveUtils.builtinHiveVersion)
    if (sessionConf != null && sessionConf.contains("use:database")) {
      ctx.sql(s"use ${sessionConf.get("use:database").get}")
    }
    operationManager.sessionToContexts.put(sessionHandle, ctx)
    sessionHandle
  }


  @throws[SparkThriftServerSQLException]
  def closeSession(sessionHandle: SessionHandle): Unit = {
    val session = handleToSession.remove(sessionHandle)
    operationManager.sessionToActivePool.remove(sessionHandle)
    operationManager.sessionToContexts.remove(sessionHandle)
    if (session == null) {
      throw new SparkThriftServerSQLException("Session does not exist!")
    }
    session.close
  }

  @throws[SparkThriftServerSQLException]
  def getSession(sessionHandle: SessionHandle): ThriftSession = {
    val session = handleToSession.get(sessionHandle)
    if (session == null) {
      throw new SparkThriftServerSQLException("Invalid SessionHandle: " + sessionHandle)
    }
    session
  }

  def getOperationManager: OperationManager = operationManager


  def submitBackgroundOperation(r: Runnable): Future[_] = backgroundOperationPool.submit(r)

  def getOpenSessionCount: Int = handleToSession.size
}

object SessionManager extends Logging {

  private val threadLocalProxyUserName = new ThreadLocal[String]() {
    override protected def initialValue: String = null
  }

  private val threadLocalIpAddress = new ThreadLocal[String]() {
    override protected def initialValue: String = null
  }

  private val threadLocalUserName = new ThreadLocal[String]() {
    override protected def initialValue: String = null
  }

  def setProxyUserName(userName: String): Unit = {
    logDebug("setting proxy user name based on query param to: " + userName)
    threadLocalProxyUserName.set(userName)
  }

  def getProxyUserName: String = threadLocalProxyUserName.get

  def clearProxyUserName(): Unit = {
    threadLocalProxyUserName.remove()
  }


  def setIpAddress(ipAddress: String): Unit = {
    threadLocalIpAddress.set(ipAddress)
  }

  def clearIpAddress(): Unit = {
    threadLocalIpAddress.remove()
  }

  def getIpAddress: String = threadLocalIpAddress.get


  def setUserName(userName: String): Unit = {
    threadLocalUserName.set(userName)
  }

  def clearUserName(): Unit = {
    threadLocalUserName.remove()
  }

  def getUserName: String = threadLocalUserName.get

}