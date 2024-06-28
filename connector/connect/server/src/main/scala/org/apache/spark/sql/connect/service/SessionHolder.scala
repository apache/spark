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
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.{JobArtifactSet, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.artifact.SparkConnectArtifactManager
import org.apache.spark.sql.connect.common.InvalidPlanInput
import org.apache.spark.sql.connect.planner.PythonStreamingQueryListener
import org.apache.spark.sql.connect.planner.StreamingForeachBatchHelper
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.util.SystemClock
import org.apache.spark.util.Utils

/**
 * Object used to hold the Spark Connect session state.
 */
case class SessionHolder(userId: String, sessionId: String, session: SparkSession)
    extends Logging {

  private val executions: ConcurrentMap[String, ExecuteHolder] =
    new ConcurrentHashMap[String, ExecuteHolder]()

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

  /** Add ExecuteHolder to this session. Called only by SparkConnectExecutionManager. */
  private[service] def addExecuteHolder(executeHolder: ExecuteHolder): Unit = {
    val oldExecute = executions.putIfAbsent(executeHolder.operationId, executeHolder)
    if (oldExecute != null) {
      // the existance of this should alrady be checked by SparkConnectExecutionManager
      throw new IllegalStateException(
        s"ExecuteHolder with opId=${executeHolder.operationId} already exists!")
    }
  }

  /** Remove ExecuteHolder to this session. Called only by SparkConnectExecutionManager. */
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

  private[connect] lazy val artifactManager = new SparkConnectArtifactManager(this)

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

  private[connect] def initializeSession(): Unit = {
    eventManager.postStarted()
  }

  /**
   * Expire this session and trigger state cleanup mechanisms.
   */
  private[connect] def expireSession(): Unit = {
    logDebug(s"Expiring session with userId: $userId and sessionId: $sessionId")
    artifactManager.cleanUpResources()
    eventManager.postClosed()
    // Clean up running queries
    SparkConnectService.streamingSessionManager.cleanupRunningQueries(this)
    streamingForeachBatchRunnerCleanerCache.cleanUpAll() // Clean up any streaming workers.
    removeAllListeners() // removes all listener and stop python listener processes if necessary.
  }

  /**
   * Execute a block of code using this session's classloader.
   * @param f
   * @tparam T
   */
  def withContextClassLoader[T](f: => T): T = {
    // Needed for deserializing and evaluating the UDF on the driver
    Utils.withContextClassLoader(classloader) {
      JobArtifactSet.withActiveJobArtifactState(artifactManager.state) {
        f
      }
    }
  }

  /**
   * Execute a block of code with this session as the active SparkConnect session.
   * @param f
   * @tparam T
   */
  def withSession[T](f: SparkSession => T): T = {
    withContextClassLoader {
      session.withActive {
        f(session)
      }
    }
  }

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

  /** Creates a dummy session holder for use in tests. */
  def forTesting(session: SparkSession): SessionHolder = {
    val ret =
      SessionHolder(
        userId = "testUser",
        sessionId = UUID.randomUUID().toString,
        session = session)
    SparkConnectService.putSessionForTesting(ret)
    ret
  }
}
