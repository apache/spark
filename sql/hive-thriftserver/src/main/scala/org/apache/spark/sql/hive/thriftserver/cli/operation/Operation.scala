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

package org.apache.spark.sql.hive.thriftserver.cli.operation

import java.io.{File, FileNotFoundException}
import java.util.concurrent.Future

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse
import org.apache.hadoop.hive.ql.session.OperationLog

import org.apache.spark.internal.Logging
import org.apache.spark.service.cli.thrift.TProtocolVersion
import org.apache.spark.sql.hive.thriftserver.cli._
import org.apache.spark.sql.hive.thriftserver.cli.session.ThriftSession
import org.apache.spark.sql.hive.thriftserver.server.cli.SparkThriftServerSQLException
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

abstract class Operation(session: ThriftSession,
                         opType: OperationType,
                         runInBackground: Boolean) extends Logging {
  private[this] var _state: OperationState = INITIALIZED
  private[this] val _opHandle: OperationHandle =
    new OperationHandle(opType, session.getProtocolVersion)
  private[this] var _conf: HiveConf = session.getHiveConf

  protected var _hasResultSet = false
  protected var _operationException: SparkThriftServerSQLException = _
  protected val _runAsync: Boolean = runInBackground
  protected var _backgroundHandle: Future[_] = _
  protected var _operationLog: OperationLog = _
  protected var _isOperationLogEnabled = false

  private var _operationTimeout: Long =
    Utils.timeStringAsMs(_conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_IDLE_OPERATION_TIMEOUT))
  private var _lastAccessTime = System.currentTimeMillis()

  protected val DEFAULT_FETCH_ORIENTATION_SET: Set[FetchOrientation] =
    Set(FetchOrientation.FETCH_NEXT, FetchOrientation.FETCH_FIRST)


  def getBackgroundHandle: Future[_] = _backgroundHandle

  protected def setBackgroundHandle(backgroundHandle: Future[_]): Unit = {
    this._backgroundHandle = backgroundHandle
  }

  def shouldRunAsync: Boolean = _runAsync

  def setConfiguration(conf: HiveConf): Unit = {
    this._conf = conf
  }

  def getConfiguration: HiveConf = _conf

  def getParentSession: ThriftSession = session

  def getHandle: OperationHandle = _opHandle

  def getProtocolVersion: TProtocolVersion = _opHandle.getProtocolVersion

  def getType: OperationType = _opHandle.getOperationType

  def getStatus: OperationStatus = new OperationStatus(_state, _operationException)

  def hasResultSet: Boolean = _hasResultSet

  protected def setHasResultSet(hasResultSet: Boolean): Unit = {
    this._hasResultSet = hasResultSet
    _opHandle.setHasResultSet(this._hasResultSet)
  }

  def getOperationLog: OperationLog = _operationLog


  @throws[SparkThriftServerSQLException]
  protected def setState(newState: OperationState): OperationState = {
    _state.validateTransition(newState)
    this._state = newState
    this._lastAccessTime = System.currentTimeMillis()
    this._state
  }

  def isTimedOut(current: Long): Boolean = {
    if (_operationTimeout == 0) {
      return false
    }
    if (_operationTimeout > 0) { // check only when it's in terminal state
      return _state.isTerminal && _lastAccessTime + _operationTimeout <= current
    }
    _lastAccessTime + -_operationTimeout <= current
  }

  def getLastAccessTime: Long = _lastAccessTime

  def getOperationTimeout: Long = _operationTimeout

  def setOperationTimeout(operationTimeout: Long): Unit = {
    this._operationTimeout = operationTimeout
  }

  protected def setOperationException(opEx: SparkThriftServerSQLException): Unit = {
    this._operationException = opEx
  }

  @throws[SparkThriftServerSQLException]
  protected final def assertState(state: OperationState): Unit = {
    if (this._state ne state) {
      throw new SparkThriftServerSQLException("Expected state " +
        state + ", but found " + this._state)
    }
    this._lastAccessTime = System.currentTimeMillis
  }

  def isRunning: Boolean = RUNNING == _state

  def isFinished: Boolean = FINISHED == _state

  def isCanceled: Boolean = CANCELED == _state

  def isFailed: Boolean = ERROR == _state


  protected def createOperationLog(): Unit = {
    if (session.isOperationLogEnabled) {
      val logFile =
        new File(session.getOperationLogSessionDir, _opHandle.getHandleIdentifier.toString)
      val logFilePath = logFile.getAbsolutePath
      this._isOperationLogEnabled = true
      // create log file
      try {
        if (logFile.exists) {
          logWarning(
            s"""
               |The operation log file should not exist, but it is already there: $logFilePath"
             """.stripMargin)
          logFile.delete
        }
        if (!logFile.createNewFile) {
          // the log file already exists and cannot be deleted.
          // If it can be read/written, keep its contents and use it.
          if (!logFile.canRead || !logFile.canWrite) {
            logWarning(
              s"""
                 |The already existed operation log file cannot be recreated,
                 |and it cannot be read or written: $logFilePath"
               """.stripMargin)
            this._isOperationLogEnabled = false
            return
          }
        }
      } catch {
        case e: Exception =>
          logWarning("Unable to create operation log file: " + logFilePath, e)
          this._isOperationLogEnabled = false
          return
      }
      // create OperationLog object with above log file
      try
        this._operationLog = new OperationLog(this._opHandle.toString, logFile, new HiveConf())
      catch {
        case e: FileNotFoundException =>
          logWarning("Unable to instantiate OperationLog object for operation: " +
            this._opHandle, e)
          this._isOperationLogEnabled = false
          return
      }
      // register this operationLog
      //      session.getSessionMgr.getOperationMgr
      //        .setOperationLog(session.getUserName, this._operationLog)
      OperationLog.setCurrentOperationLog(_operationLog)
    }
  }

  protected def unregisterOperationLog(): Unit = {
    if (_isOperationLogEnabled) {
      OperationLog.removeCurrentOperationLog()
    }
  }


  /**
   * Invoked before runInternal().
   * Set up some preconditions, or configurations.
   */
  protected def beforeRun(): Unit = {
    createOperationLog()
  }

  /**
   * Invoked after runInternal(), even if an exception is thrown in runInternal().
   * Clean up resources, which was set up in beforeRun().
   */
  protected def afterRun(): Unit = {
    unregisterOperationLog()
  }

  /**
   * Implemented by subclass of Operation class to execute specific behaviors.
   *
   * @throws SparkThriftServerSQLException
   */
  @throws[SparkThriftServerSQLException]
  protected def runInternal(): Unit

  @throws[SparkThriftServerSQLException]
  def run(): Unit = {
    beforeRun()
    try
      runInternal()
    finally afterRun()
  }


  protected def cleanupOperationLog(): Unit = {
    if (_isOperationLogEnabled) {
      if (_operationLog == null) {
        logError("Operation [ " + _opHandle.getHandleIdentifier + " ] " +
          "logging is enabled, but its OperationLog object cannot be found.")
      } else {
        _operationLog.close()
      }
    }
  }

  // TODO: make this abstract and implement in subclasses.
  @throws[SparkThriftServerSQLException]
  def cancel(): Unit = {
    setState(CANCELED)
    throw new UnsupportedOperationException("SQLOperation.cancel()")
  }

  @throws[SparkThriftServerSQLException]
  def close(): Unit

  @throws[SparkThriftServerSQLException]
  def getResultSetSchema: StructType

  @throws[SparkThriftServerSQLException]
  def getNextRowSet(orientation: FetchOrientation, maxRows: Long): RowSet

  @throws[SparkThriftServerSQLException]
  def getNextRowSet: RowSet =
    getNextRowSet(FetchOrientation.FETCH_NEXT, Operation.DEFAULT_FETCH_MAX_ROWS)


  /**
   * Verify if the given fetch orientation is part of the default orientation types.
   */
  @throws[SparkThriftServerSQLException]
  protected def validateDefaultFetchOrientation(orientation: FetchOrientation): Unit = {
    validateFetchOrientation(orientation, DEFAULT_FETCH_ORIENTATION_SET)
  }

  /**
   * Verify if the given fetch orientation is part of the supported orientation types.
   */
  @throws[SparkThriftServerSQLException]
  protected def validateFetchOrientation(orientation: FetchOrientation,
                                         supportedOrientations: Set[FetchOrientation]): Unit = {
    if (!supportedOrientations.contains(orientation)) {
      throw new SparkThriftServerSQLException(
        "The fetch type " + orientation.toString + " is not supported for this resultset", "HY106")
    }
  }

  protected def toSQLException(
       prefix: String,
       response: CommandProcessorResponse): SparkThriftServerSQLException = {
    val ex = new SparkThriftServerSQLException(prefix + ": " +
      response.getErrorMessage, response.getSQLState, response.getResponseCode)
    if (response.getException != null) {
      ex.initCause(response.getException)
    }
    ex
  }
}

object Operation {
  final val DEFAULT_FETCH_ORIENTATION: FetchOrientation = FetchOrientation.FETCH_NEXT
  final val DEFAULT_FETCH_MAX_ROWS = 100
}
