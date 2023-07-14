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
import scala.util.control.NonFatal

import org.apache.spark.JobArtifactSet
import org.apache.spark.SparkException
import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.artifact.SparkConnectArtifactManager
import org.apache.spark.sql.connect.common.InvalidPlanInput
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.util.Utils

/**
 * Object used to hold the Spark Connect session state.
 */
case class SessionHolder(userId: String, sessionId: String, session: SparkSession)
    extends Logging {

  val executions: ConcurrentMap[String, ExecuteHolder] =
    new ConcurrentHashMap[String, ExecuteHolder]()

  // Mapping from relation ID (passed to client) to runtime dataframe. Used for callbacks like
  // foreachBatch() in Streaming. Lazy since most sessions don't need it.
  private lazy val dataFrameCache: ConcurrentMap[String, DataFrame] = new ConcurrentHashMap()

  // Mapping from id to StreamingQueryListener. Used for methods like removeListener() in
  // StreamingQueryManager.
  private lazy val listenerCache: ConcurrentMap[String, StreamingQueryListener] =
  new ConcurrentHashMap()

  private[connect] def createExecuteHolder(request: proto.ExecutePlanRequest): ExecuteHolder = {
    val operationId = UUID.randomUUID().toString
    val executePlanHolder = new ExecuteHolder(request, operationId, this)
    assert(executions.putIfAbsent(operationId, executePlanHolder) == null)
    executePlanHolder
  }

  private[connect] def removeExecuteHolder(operationId: String): Unit = {
    executions.remove(operationId)
  }

  private[connect] def interruptAll(): Unit = {
    executions.asScala.values.foreach { execute =>
      // Eat exception while trying to interrupt a given execution and move forward.
      try {
        logDebug(s"Interrupting execution ${execute.operationId}")
        execute.interrupt()
      } catch {
        case NonFatal(e) =>
          logWarning(s"Exception $e while trying to interrupt execution ${execute.operationId}")
      }
    }
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

  /**
   * Expire this session and trigger state cleanup mechanisms.
   */
  private[connect] def expireSession(): Unit = {
    logDebug(s"Expiring session with userId: $userId and sessionId: $sessionId")
    artifactManager.cleanUpResources()
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
      SparkException.internalError(s"A dataframe is already associated with id $dfId")
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
      SparkException.internalError(s"A listener is already associated with id $id")
    }
  }

  /**
   * Returns [[StreamingQueryListener]] cached for Listener ID `id`. If it is not found, throw
   * [[InvalidPlanInput]].
   */
  private[connect] def getListenerOrThrow(id: String): StreamingQueryListener = {
    Option(listenerCache.get(id))
      .getOrElse {
        throw InvalidPlanInput(s"No listener with id $id is found in the session $sessionId")
      }
  }

  /**
   * Removes corresponding StreamingQueryListener by ID.
   */
  private[connect] def removeCachedListener(id: String): StreamingQueryListener = {
    listenerCache.remove(id)
  }
}

object SessionHolder {

  /** Creates a dummy session holder for use in tests. */
  def forTesting(session: SparkSession): SessionHolder = {
    SessionHolder(userId = "testUser", sessionId = UUID.randomUUID().toString, session = session)
  }
}
