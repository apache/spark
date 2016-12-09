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

package org.apache.spark.sql.streaming

import java.util.UUID
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.state.StateStoreCoordinatorRef
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.{Clock, SystemClock, Utils}

/**
 * :: Experimental ::
 * A class to manage all the [[StreamingQuery]] active on a `SparkSession`.
 *
 * @since 2.0.0
 */
@Experimental
class StreamingQueryManager private[sql] (sparkSession: SparkSession) {

  private[sql] val stateStoreCoordinator =
    StateStoreCoordinatorRef.forDriver(sparkSession.sparkContext.env)
  private val listenerBus = new StreamingQueryListenerBus(sparkSession.sparkContext.listenerBus)

  @GuardedBy("activeQueriesLock")
  private val activeQueries = new mutable.HashMap[UUID, StreamingQuery]

  /** Track names of queries that are going to start but not yet put into `activeQueries` */
  @GuardedBy("activeQueriesLock")
  private val pendingQueryNames = mutable.HashSet[String]()

  /** Track ids of queries that are going to start but not yet put into `activeQueries` */
  @GuardedBy("activeQueriesLock")
  private val pendingQueryIds = mutable.HashSet[UUID]()

  private val activeQueriesLock = new Object

  private val awaitTerminationLock = new Object

  @GuardedBy("awaitTerminationLock")
  private var lastTerminatedQuery: StreamingQuery = null

  /**
   * Returns a list of active queries associated with this SQLContext
   *
   * @since 2.0.0
   */
  def active: Array[StreamingQuery] = activeQueriesLock.synchronized {
    activeQueries.values.toArray
  }

  /**
   * Returns the query if there is an active query with the given id, or null.
   *
   * @since 2.1.0
   */
  def get(id: UUID): StreamingQuery = activeQueriesLock.synchronized {
    activeQueries.get(id).orNull
  }

  /**
   * Returns the query if there is an active query with the given id, or null.
   *
   * @since 2.1.0
   */
  def get(id: String): StreamingQuery = get(UUID.fromString(id))

  /**
   * Wait until any of the queries on the associated SQLContext has terminated since the
   * creation of the context, or since `resetTerminated()` was called. If any query was terminated
   * with an exception, then the exception will be thrown.
   *
   * If a query has terminated, then subsequent calls to `awaitAnyTermination()` will either
   * return immediately (if the query was terminated by `query.stop()`),
   * or throw the exception immediately (if the query was terminated with exception). Use
   * `resetTerminated()` to clear past terminations and wait for new terminations.
   *
   * In the case where multiple queries have terminated since `resetTermination()` was called,
   * if any query has terminated with exception, then `awaitAnyTermination()` will
   * throw any of the exception. For correctly documenting exceptions across multiple queries,
   * users need to stop all of them after any of them terminates with exception, and then check the
   * `query.exception()` for each query.
   *
   * @throws StreamingQueryException if any query has terminated with an exception
   *
   * @since 2.0.0
   */
  @throws[StreamingQueryException]
  def awaitAnyTermination(): Unit = {
    awaitTerminationLock.synchronized {
      while (lastTerminatedQuery == null) {
        awaitTerminationLock.wait(10)
      }
      if (lastTerminatedQuery != null && lastTerminatedQuery.exception.nonEmpty) {
        throw lastTerminatedQuery.exception.get
      }
    }
  }

  /**
   * Wait until any of the queries on the associated SQLContext has terminated since the
   * creation of the context, or since `resetTerminated()` was called. Returns whether any query
   * has terminated or not (multiple may have terminated). If any query has terminated with an
   * exception, then the exception will be thrown.
   *
   * If a query has terminated, then subsequent calls to `awaitAnyTermination()` will either
   * return `true` immediately (if the query was terminated by `query.stop()`),
   * or throw the exception immediately (if the query was terminated with exception). Use
   * `resetTerminated()` to clear past terminations and wait for new terminations.
   *
   * In the case where multiple queries have terminated since `resetTermination()` was called,
   * if any query has terminated with exception, then `awaitAnyTermination()` will
   * throw any of the exception. For correctly documenting exceptions across multiple queries,
   * users need to stop all of them after any of them terminates with exception, and then check the
   * `query.exception()` for each query.
   *
   * @throws StreamingQueryException if any query has terminated with an exception
   *
   * @since 2.0.0
   */
  @throws[StreamingQueryException]
  def awaitAnyTermination(timeoutMs: Long): Boolean = {

    val startTime = System.currentTimeMillis
    def isTimedout = System.currentTimeMillis - startTime >= timeoutMs

    awaitTerminationLock.synchronized {
      while (!isTimedout && lastTerminatedQuery == null) {
        awaitTerminationLock.wait(10)
      }
      if (lastTerminatedQuery != null && lastTerminatedQuery.exception.nonEmpty) {
        throw lastTerminatedQuery.exception.get
      }
      lastTerminatedQuery != null
    }
  }

  /**
   * Forget about past terminated queries so that `awaitAnyTermination()` can be used again to
   * wait for new terminations.
   *
   * @since 2.0.0
   */
  def resetTerminated(): Unit = {
    awaitTerminationLock.synchronized {
      lastTerminatedQuery = null
    }
  }

  /**
   * Register a [[StreamingQueryListener]] to receive up-calls for life cycle events of
   * [[StreamingQuery]].
   *
   * @since 2.0.0
   */
  def addListener(listener: StreamingQueryListener): Unit = {
    listenerBus.addListener(listener)
  }

  /**
   * Deregister a [[StreamingQueryListener]].
   *
   * @since 2.0.0
   */
  def removeListener(listener: StreamingQueryListener): Unit = {
    listenerBus.removeListener(listener)
  }

  /** Post a listener event */
  private[sql] def postListenerEvent(event: StreamingQueryListener.Event): Unit = {
    listenerBus.post(event)
  }

  private def prepareAndCreateQuery(
      userSpecifiedName: Option[String],
      userSpecifiedCheckpointLocation: Option[String],
      df: DataFrame,
      sink: Sink,
      outputMode: OutputMode,
      useTempCheckpointLocation: Boolean,
      recoverFromCheckpointLocation: Boolean,
      trigger: Trigger,
      triggerClock: Clock): StreamExecution = {
    val checkpointLocation = userSpecifiedCheckpointLocation.map { userSpecified =>
      new Path(userSpecified).toUri.toString
    }.orElse {
      df.sparkSession.sessionState.conf.checkpointLocation.map { location =>
        new Path(location, userSpecifiedName.getOrElse(UUID.randomUUID().toString)).toUri.toString
      }
    }.getOrElse {
      if (useTempCheckpointLocation) {
        Utils.createTempDir(namePrefix = s"temporary").getCanonicalPath
      } else {
        throw new AnalysisException(
          "checkpointLocation must be specified either " +
            """through option("checkpointLocation", ...) or """ +
            s"""SparkSession.conf.set("${SQLConf.CHECKPOINT_LOCATION.key}", ...)""")
      }
    }

    // If offsets have already been created, we trying to resume a query.
    if (!recoverFromCheckpointLocation) {
      val checkpointPath = new Path(checkpointLocation, "offsets")
      val fs = checkpointPath.getFileSystem(df.sparkSession.sessionState.newHadoopConf())
      if (fs.exists(checkpointPath)) {
        throw new AnalysisException(
          s"This query does not support recovering from checkpoint location. " +
            s"Delete $checkpointPath to start over.")
      }
    }

    val analyzedPlan = df.queryExecution.analyzed
    df.queryExecution.assertAnalyzed()

    if (sparkSession.sessionState.conf.isUnsupportedOperationCheckEnabled) {
      UnsupportedOperationChecker.checkForStreaming(analyzedPlan, outputMode)
    }

    var nextSourceId = 0L

    val logicalPlan = analyzedPlan.transform {
      case StreamingRelation(dataSource, _, output) =>
        // Materialize source to avoid creating it in every batch
        val metadataPath = s"$checkpointLocation/sources/$nextSourceId"
        val source = dataSource.createSource(metadataPath)
        nextSourceId += 1
        // We still need to use the previous `output` instead of `source.schema` as attributes in
        // "df.logicalPlan" has already used attributes of the previous `output`.
        StreamingExecutionRelation(source, output)
    }
    new StreamExecution(
      sparkSession,
      userSpecifiedName.orNull,
      checkpointLocation,
      logicalPlan,
      sink,
      trigger,
      triggerClock,
      outputMode)
  }

  /**
   * Start a [[StreamingQuery]].
   * @param userSpecifiedName Query name optionally specified by the user.
   * @param userSpecifiedCheckpointLocation  Checkpoint location optionally specified by the user.
   * @param df Streaming DataFrame.
   * @param sink  Sink to write the streaming outputs.
   * @param outputMode  Output mode for the sink.
   * @param useTempCheckpointLocation  Whether to use a temporary checkpoint location when the user
   *                                   has not specified one. If false, then error will be thrown.
   * @param recoverFromCheckpointLocation  Whether to recover query from the checkpoint location.
   *                                       If false and the checkpoint location exists, then error
   *                                       will be thrown.
   * @param trigger [[Trigger]] for the query.
   * @param triggerClock [[Clock]] to use for the triggering.
   */
  private[sql] def startQuery(
      userSpecifiedName: Option[String],
      userSpecifiedCheckpointLocation: Option[String],
      df: DataFrame,
      sink: Sink,
      outputMode: OutputMode,
      useTempCheckpointLocation: Boolean = false,
      recoverFromCheckpointLocation: Boolean = true,
      trigger: Trigger = ProcessingTime(0),
      triggerClock: Clock = new SystemClock()): StreamingQuery = {
    // `queryName` and `queryId` will be set when we add them into `pendingQueryNames` and
    // `pendingQueryIds` so that we can remove them at the end of this method when an error happens.
    var queryName: String = null
    var queryId: UUID = null
    try {
      userSpecifiedName.foreach { name =>
        activeQueriesLock.synchronized {
          if (activeQueries.values.exists(_.name == name) || pendingQueryNames.contains(name)) {
            throw new IllegalArgumentException(
              s"Cannot start query with name $name as a query with that name is already active")
          }
          pendingQueryNames += name
          queryName = name
        }
      }
      val query = prepareAndCreateQuery(
        userSpecifiedName,
        userSpecifiedCheckpointLocation,
        df,
        sink,
        outputMode,
        useTempCheckpointLocation,
        recoverFromCheckpointLocation,
        trigger,
        triggerClock)
      activeQueriesLock.synchronized {
        if (activeQueries.values.exists(_.id == query.id) || pendingQueryIds.contains(query.id)) {
          throw new IllegalStateException(
            s"Cannot start query with id ${query.id} as another query with same id is " +
              s"already active. Perhaps you are attempting to restart a query from checkpoint " +
              s"that is already active.")
        }
        pendingQueryIds += query.id
        queryId = query.id
      }
      query.start()
      activeQueriesLock.synchronized {
        // It's possible that `notifyQueryTermination` is called before we reach here. So we
        // need to check the query status before adding it into `activeQueries`.
        if (query.isActive) {
          activeQueries.put(query.id, query)
          pendingQueryIds -= query.id
          queryId = null
          if (queryName != null) {
            pendingQueryNames -= queryName
            queryName = null
          }
        }
      }
      query
    } finally {
      activeQueriesLock.synchronized {
        if (queryName != null) pendingQueryNames -= queryName
        if (queryId != null) pendingQueryIds -= queryId
      }
    }
  }

  /** Notify (by the StreamingQuery) that the query has been terminated */
  private[sql] def notifyQueryTermination(terminatedQuery: StreamingQuery): Unit = {
    activeQueriesLock.synchronized {
      activeQueries -= terminatedQuery.id
      pendingQueryIds -= terminatedQuery.id
      Option(terminatedQuery.name).foreach(name => pendingQueryNames -= name)
    }
    awaitTerminationLock.synchronized {
      if (lastTerminatedQuery == null || terminatedQuery.exception.nonEmpty) {
        lastTerminatedQuery = terminatedQuery
      }
      awaitTerminationLock.notifyAll()
    }
  }
}
