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

package org.apache.spark.sql.connect.service

import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import com.google.common.base.Ticker
import com.google.common.cache.CacheBuilder

import org.apache.spark.{SparkException, SparkSQLException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.common.InvalidPlanInput
import org.apache.spark.sql.connect.planner.PythonStreamingQueryListener
import org.apache.spark.sql.connect.planner.StreamingForeachBatchHelper
import org.apache.spark.sql.connect.service.SessionHolder.{ERROR_CACHE_SIZE, ERROR_CACHE_TIMEOUT_SEC}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.util.{SystemClock, Utils}

// Unique key identifying session by combination of user, and session id
case class SessionKey(userId: String, sessionId: String)

/**
 * Object used to hold the Spark Connect session state.
 */
case class SessionHolder(userId: String, sessionId: String, session: SparkSession)
    extends Logging {

  // Time when the session was started.
  private val startTimeMs: Long = System.currentTimeMillis()

  // Time when the session was last accessed (retrieved from SparkConnectSessionManager)
  @volatile private var lastAccessTimeMs: Long = System.currentTimeMillis()

  // Time when the session was closed.
  // Set only by close(), and only once.
  @volatile private var closedTimeMs: Option[Long] = None

  // Custom timeout after a session expires due to inactivity.
  // Used by SparkConnectSessionManager instead of default timeout if set.
  // Setting it to -1 indicated forever.
  @volatile private var customInactiveTimeoutMs: Option[Long] = None

  private val executions: ConcurrentMap[String, ExecuteHolder] =
    new ConcurrentHashMap[String, ExecuteHolder]()

  // The cache that maps an error id to a throwable. The throwable in cache is independent to
  // each other.
  private[connect] val errorIdToError = CacheBuilder
    .newBuilder()
    .ticker(Ticker.systemTicker())
    .maximumSize(ERROR_CACHE_SIZE)
    .expireAfterAccess(ERROR_CACHE_TIMEOUT_SEC, TimeUnit.SECONDS)
    .build[String, Throwable]()

  val eventManager: SessionEventsManager = SessionEventsManager(this, new SystemClock())

  // Mapping from relation ID (passed to client) to runtime dataframe. Used for callbacks like
  // foreachBatch() in Streaming. Lazy since most sessions don't need it.
  private lazy val dataFrameCache: ConcurrentMap[String, DataFrame] = new ConcurrentHashMap()

  // Mapping from id to StreamingQueryListener. Used for methods like removeListener() in
  // StreamingQueryManager.
  private lazy val listenerCache: ConcurrentMap[String, StreamingQueryListener] =
    new ConcurrentHashMap()

  // Handles Python process clean up for streaming queries. Initialized on first use in a query.
  private[connect] lazy val streamingForeachBatchRunnerCleanerCache =
    new StreamingForeachBatchHelper.CleanerCache(this)

  def key: SessionKey = SessionKey(userId, sessionId)

  // Returns the server side session ID and asserts that it must be different from the client-side
  // session ID.
  def serverSessionId: String = {
    if (Utils.isTesting && session == null) {
      // Testing-only: Some sessions created by SessionHolder.forTesting are not fully initialized
      // and don't have an underlying SparkSession.
      ""
    } else {
      assert(session.sessionUUID != sessionId)
      session.sessionUUID
    }
  }

  /**
   * Add ExecuteHolder to this session.
   *
   * Called only by SparkConnectExecutionManager under executionsLock.
   */
  @GuardedBy("SparkConnectService.executionManager.executionsLock")
  private[service] def addExecuteHolder(executeHolder: ExecuteHolder): Unit = {
    if (closedTimeMs.isDefined) {
      // Do not accept new executions if the session is closing.
      throw new SparkSQLException(
        errorClass = "INVALID_HANDLE.SESSION_CLOSED",
        messageParameters = Map("handle" -> sessionId))
    }

    val oldExecute = executions.putIfAbsent(executeHolder.operationId, executeHolder)
    if (oldExecute != null) {
      // the existence of this should already be checked by SparkConnectExecutionManager
      throw new IllegalStateException(
        s"ExecuteHolder with opId=${executeHolder.operationId} already exists!")
    }
  }

  /**
   * Remove ExecuteHolder from this session.
   *
   * Called only by SparkConnectExecutionManager under executionsLock.
   */
  @GuardedBy("SparkConnectService.executionManager.executionsLock")
  private[service] def removeExecuteHolder(operationId: String): Unit = {
    executions.remove(operationId)
  }

  private[connect] def executeHolder(operationId: String): Option[ExecuteHolder] = {
    Option(executions.get(operationId))
  }

  /**
   * Interrupt all executions in the session.
   * @return
   *   list of operationIds of interrupted executions
   */
  private[service] def interruptAll(): Seq[String] = {
    val interruptedIds = new mutable.ArrayBuffer[String]()
    executions.asScala.values.foreach { execute =>
      if (execute.interrupt()) {
        interruptedIds += execute.operationId
      }
    }
    interruptedIds.toSeq
  }

  /**
   * Interrupt executions in the session with a given tag.
   * @return
   *   list of operationIds of interrupted executions
   */
  private[service] def interruptTag(tag: String): Seq[String] = {
    val interruptedIds = new mutable.ArrayBuffer[String]()
    executions.asScala.values.foreach { execute =>
      if (execute.sparkSessionTags.contains(tag)) {
        if (execute.interrupt()) {
          interruptedIds += execute.operationId
        }
      }
    }
    interruptedIds.toSeq
  }

  /**
   * Interrupt the execution with the given operation_id
   * @return
   *   list of operationIds of interrupted executions (one element or empty)
   */
  private[service] def interruptOperation(operationId: String): Seq[String] = {
    val interruptedIds = new mutable.ArrayBuffer[String]()
    Option(executions.get(operationId)).foreach { execute =>
      if (execute.interrupt()) {
        interruptedIds += execute.operationId
      }
    }
    interruptedIds.toSeq
  }

  private[connect] def artifactManager = session.artifactManager

  /**
   * Add an artifact to this SparkConnect session.
   *
   * @param remoteRelativePath
   * @param serverLocalStagingPath
   * @param fragment
   */
  private[connect] def addArtifact(
      remoteRelativePath: Path,
      serverLocalStagingPath: Path,
      fragment: Option[String]): Unit = {
    artifactManager.addArtifact(remoteRelativePath, serverLocalStagingPath, fragment)
  }

  /**
   * A [[ClassLoader]] for jar/class file resources specific to this SparkConnect session.
   */
  def classloader: ClassLoader = artifactManager.classloader

  private[connect] def updateAccessTime(): Unit = {
    lastAccessTimeMs = System.currentTimeMillis()
    logInfo(s"Session $key accessed, time $lastAccessTimeMs.")
  }

  private[connect] def setCustomInactiveTimeoutMs(newInactiveTimeoutMs: Option[Long]): Unit = {
    customInactiveTimeoutMs = newInactiveTimeoutMs
    logInfo(s"Session $key inactive timeout set to $customInactiveTimeoutMs ms.")
  }

  /**
   * Initialize the session.
   *
   * Called only by SparkConnectSessionManager.
   */
  private[connect] def initializeSession(): Unit = {
    eventManager.postStarted()
  }

  /**
   * Expire this session and trigger state cleanup mechanisms.
   *
   * Called only by SparkConnectSessionManager.shutdownSessionHolder.
   */
  private[connect] def close(): Unit = {
    // Called only by SparkConnectSessionManager.shutdownSessionHolder.
    // It is not called under SparkConnectSessionManager.sessionsLock, but it's guaranteed to be
    // called only once, since removing the session from SparkConnectSessionManager.sessionStore is
    // synchronized and guaranteed to happen only once.
    if (closedTimeMs.isDefined) {
      throw new IllegalStateException(s"Session $key is already closed.")
    }
    logInfo(s"Closing session with userId: $userId and sessionId: $sessionId")
    closedTimeMs = Some(System.currentTimeMillis())

    if (Utils.isTesting && eventManager.status == SessionStatus.Pending) {
      // Testing-only: Some sessions created by SessionHolder.forTesting are not fully initialized
      // and can't be closed.
      return
    }

    // Note on the below notes about concurrency:
    // While closing the session can potentially race with operations started on the session, the
    // intended use is that the client session will get closed when it's really not used anymore,
    // or that it expires due to inactivity, in which case there should be no races.

    // Clean up all artifacts.
    // Note: there can be concurrent AddArtifact calls still adding something.
    artifactManager.cleanUpResources()

    // Clean up running streaming queries.
    // Note: there can be concurrent streaming queries being started.
    SparkConnectService.streamingSessionManager.cleanupRunningQueries(this)
    streamingForeachBatchRunnerCleanerCache.cleanUpAll() // Clean up any streaming workers.
    removeAllListeners() // removes all listener and stop python listener processes if necessary.

    // Clean up all executions.
    // After closedTimeMs is defined, SessionHolder.addExecuteHolder() will not allow new executions
    // to be added for this session anymore. Because both SessionHolder.addExecuteHolder() and
    // SparkConnectExecutionManager.removeAllExecutionsForSession() are executed under
    // executionsLock, this guarantees that removeAllExecutionsForSession triggered here will
    // remove all executions and no new executions will be added in the meanwhile.
    SparkConnectService.executionManager.removeAllExecutionsForSession(this.key)

    eventManager.postClosed()
  }

  /**
   * Execute a block of code with this session as the active SparkConnect session.
   * @param f
   * @tparam T
   */
  def withSession[T](f: SparkSession => T): T = {
    artifactManager.withResources {
      session.withActive {
        f(session)
      }
    }
  }

  /** Get SessionInfo with information about this SessionHolder. */
  def getSessionHolderInfo: SessionHolderInfo =
    SessionHolderInfo(
      userId = userId,
      sessionId = sessionId,
      serverSessionId = serverSessionId,
      status = eventManager.status,
      startTimeMs = startTimeMs,
      lastAccessTimeMs = lastAccessTimeMs,
      customInactiveTimeoutMs = customInactiveTimeoutMs,
      closedTimeMs = closedTimeMs)

  /**
   * Caches given DataFrame with the ID. The cache does not expire. The entry needs to be
   * explicitly removed by the owners of the DataFrame once it is not needed.
   */
  private[connect] def cacheDataFrameById(dfId: String, df: DataFrame): Unit = {
    if (dataFrameCache.putIfAbsent(dfId, df) != null) {
      throw SparkException.internalError(s"A dataframe is already associated with id $dfId")
    }
  }

  /**
   * Returns [[DataFrame]] cached for DataFrame ID `dfId`. If it is not found, throw
   * [[InvalidPlanInput]].
   */
  private[connect] def getDataFrameOrThrow(dfId: String): DataFrame = {
    Option(dataFrameCache.get(dfId))
      .getOrElse {
        throw InvalidPlanInput(s"No DataFrame with id $dfId is found in the session $sessionId")
      }
  }

  private[connect] def removeCachedDataFrame(dfId: String): DataFrame = {
    dataFrameCache.remove(dfId)
  }

  /**
   * Caches given StreamingQueryListener with the ID.
   */
  private[connect] def cacheListenerById(id: String, listener: StreamingQueryListener): Unit = {
    if (listenerCache.putIfAbsent(id, listener) != null) {
      throw SparkException.internalError(s"A listener is already associated with id $id")
    }
  }

  /**
   * Returns [[StreamingQueryListener]] cached for Listener ID `id`. If it is not found, return
   * None.
   */
  private[connect] def getListener(id: String): Option[StreamingQueryListener] = {
    Option(listenerCache.get(id))
  }

  /**
   * Removes corresponding StreamingQueryListener by ID. Terminates the python process if it's a
   * Spark Connect PythonStreamingQueryListener.
   */
  private[connect] def removeCachedListener(id: String): Unit = {
    Option(listenerCache.remove(id)) match {
      case Some(pyListener: PythonStreamingQueryListener) => pyListener.stopListenerProcess()
      case _ => // do nothing
    }
  }

  /**
   * Stop all streaming listener threads, and removes all python process if applicable. Only
   * called when session is expired.
   */
  private def removeAllListeners(): Unit = {
    listenerCache.forEach((id, listener) => {
      session.streams.removeListener(listener)
      removeCachedListener(id)
    })
  }

  /**
   * List listener IDs that have been register on server side.
   */
  private[connect] def listListenerIds(): Seq[String] = {
    listenerCache.keySet().asScala.toSeq
  }
}

object SessionHolder {

  // The maximum number of distinct errors in the cache.
  private val ERROR_CACHE_SIZE = 20

  // The maximum time for an error to stay in the cache.
  private val ERROR_CACHE_TIMEOUT_SEC = 60

  /** Creates a dummy session holder for use in tests. */
  def forTesting(session: SparkSession): SessionHolder = {
    val ret =
      SessionHolder(
        userId = "testUser",
        sessionId = UUID.randomUUID().toString,
        session = session)
    SparkConnectService.sessionManager.putSessionForTesting(ret)
    ret
  }
}

/** Basic information about SessionHolder. */
case class SessionHolderInfo(
    userId: String,
    sessionId: String,
    serverSessionId: String,
    status: SessionStatus,
    customInactiveTimeoutMs: Option[Long],
    startTimeMs: Long,
    lastAccessTimeMs: Long,
    closedTimeMs: Option[Long]) {
  def key: SessionKey = SessionKey(userId, sessionId)
}
