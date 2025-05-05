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

package org.apache.spark.sql.classic

import java.util.UUID
import java.util.concurrent.{TimeoutException, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.annotation.Evolving
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{CLASS_NAME, QUERY_ID, RUN_ID}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.streaming.{WriteToStream, WriteToStreamStatement}
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsWrite, Table, TableCatalog}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.continuous.ContinuousExecution
import org.apache.spark.sql.execution.streaming.state.StateStoreCoordinatorRef
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.STREAMING_QUERY_LISTENERS
import org.apache.spark.sql.streaming
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException, StreamingQueryListener, Trigger}
import org.apache.spark.util.{Clock, SystemClock, Utils}

/**
 * A class to manage all the [[StreamingQuery]] active in a `SparkSession`.
 *
 * @since 2.0.0
 */
@Evolving
class StreamingQueryManager private[sql] (
    sparkSession: SparkSession,
    sqlConf: SQLConf)
  extends streaming.StreamingQueryManager
  with Logging {

  private[sql] val stateStoreCoordinator =
    StateStoreCoordinatorRef.forDriver(sparkSession.sparkContext.env, sqlConf)
  private val listenerBus =
    new StreamingQueryListenerBus(Some(sparkSession.sparkContext.listenerBus))

  @GuardedBy("activeQueriesSharedLock")
  private val activeQueries = new mutable.HashMap[UUID, StreamingQuery]
  // A global lock to keep track of active streaming queries across Spark sessions
  private val activeQueriesSharedLock = sparkSession.sharedState.activeQueriesLock
  private val awaitTerminationLock = new Object

  /**
   * Track the last terminated query and remember the last failure since the creation of the
   * context, or since `resetTerminated()` was called. There are three possible values:
   *
   * - null: no query has been been terminated.
   * - None: some queries have been terminated and no one has failed.
   * - Some(StreamingQueryException): Some queries have been terminated and at least one query has
   *   failed. The exception is the exception of the last failed query.
   */
  @GuardedBy("awaitTerminationLock")
  private var lastTerminatedQueryException: Option[StreamingQueryException] = _

  try {
    sparkSession.sparkContext.conf.get(STREAMING_QUERY_LISTENERS).foreach { classNames =>
      SQLConf.withExistingConf(sqlConf) {
        Utils.loadExtensions(classOf[StreamingQueryListener], classNames,
          sparkSession.sparkContext.conf).foreach { listener =>
          addListener(listener)
          logInfo(log"Registered listener ${MDC(CLASS_NAME, listener.getClass.getName)}")
        }
      }
    }
    sparkSession.sharedState.streamingQueryStatusListener.foreach { listener =>
      addListener(listener)
    }
  } catch {
    case e: Exception =>
      throw QueryExecutionErrors.registeringStreamingQueryListenerError(e)
  }

  /** @inheritdoc */
  def active: Array[StreamingQuery] = activeQueriesSharedLock.synchronized {
    activeQueries.values.toArray
  }

  /** @inheritdoc */
  def get(id: UUID): StreamingQuery = activeQueriesSharedLock.synchronized {
    activeQueries.get(id).orNull
  }

  /** @inheritdoc */
  def get(id: String): StreamingQuery = get(UUID.fromString(id))

  /** @inheritdoc */
  @throws[StreamingQueryException]
  def awaitAnyTermination(): Unit = {
    awaitTerminationLock.synchronized {
      while (lastTerminatedQueryException == null) {
        awaitTerminationLock.wait(10)
      }
      if (lastTerminatedQueryException != null && lastTerminatedQueryException.nonEmpty) {
        throw lastTerminatedQueryException.get
      }
    }
  }

  /** @inheritdoc */
  @throws[StreamingQueryException]
  def awaitAnyTermination(timeoutMs: Long): Boolean = {

    val startTime = System.nanoTime()
    def isTimedout = {
      System.nanoTime() - startTime >= TimeUnit.MILLISECONDS.toNanos(timeoutMs)
    }

    awaitTerminationLock.synchronized {
      while (!isTimedout && lastTerminatedQueryException == null) {
        awaitTerminationLock.wait(10)
      }
      if (lastTerminatedQueryException != null && lastTerminatedQueryException.nonEmpty) {
        throw lastTerminatedQueryException.get
      }
      lastTerminatedQueryException != null
    }
  }

  /** @inheritdoc */
  def resetTerminated(): Unit = {
    awaitTerminationLock.synchronized {
      lastTerminatedQueryException = null
    }
  }

  /** @inheritdoc */
  def addListener(listener: StreamingQueryListener): Unit = {
    listenerBus.addListener(listener)
  }

  /** @inheritdoc */
  def removeListener(listener: StreamingQueryListener): Unit = {
    listenerBus.removeListener(listener)
  }

  /** @inheritdoc */
  def listListeners(): Array[StreamingQueryListener] = {
    listenerBus.listeners.asScala.toArray
  }

  /** Post a listener event */
  private[sql] def postListenerEvent(event: StreamingQueryListener.Event): Unit = {
    listenerBus.post(event)
  }

  private def useAsyncProgressTracking(extraOptions: Map[String, String]): Boolean = {
    extraOptions.getOrElse(
      AsyncProgressTrackingMicroBatchExecution.ASYNC_PROGRESS_TRACKING_ENABLED, "false").toBoolean
  }

  // scalastyle:off argcount
  private def createQuery(
      userSpecifiedName: Option[String],
      userSpecifiedCheckpointLocation: Option[String],
      df: Dataset[_],
      extraOptions: Map[String, String],
      sink: Table,
      outputMode: OutputMode,
      useTempCheckpointLocation: Boolean,
      recoverFromCheckpointLocation: Boolean,
      trigger: Trigger,
      triggerClock: Clock,
      catalogAndIdent: Option[(TableCatalog, Identifier)] = None,
      catalogTable: Option[CatalogTable] = None): StreamingQueryWrapper = {
    val analyzedPlan = df.queryExecution.analyzed
    df.queryExecution.assertAnalyzed()

    val dataStreamWritePlan = WriteToStreamStatement(
      userSpecifiedName,
      userSpecifiedCheckpointLocation,
      useTempCheckpointLocation,
      recoverFromCheckpointLocation,
      sink,
      outputMode,
      df.sparkSession.sessionState.newHadoopConf(),
      trigger.isInstanceOf[ContinuousTrigger],
      analyzedPlan,
      catalogAndIdent,
      catalogTable)

    val analyzedStreamWritePlan =
      sparkSession.sessionState.executePlan(dataStreamWritePlan).analyzed
        .asInstanceOf[WriteToStream]

    (sink, trigger) match {
      case (_: SupportsWrite, trigger: ContinuousTrigger) =>
        new StreamingQueryWrapper(new ContinuousExecution(
          sparkSession,
          trigger,
          triggerClock,
          extraOptions,
          analyzedStreamWritePlan))
      case _ =>
        val microBatchExecution = if (useAsyncProgressTracking(extraOptions)) {
          new AsyncProgressTrackingMicroBatchExecution(
            sparkSession,
            trigger,
            triggerClock,
            extraOptions,
            analyzedStreamWritePlan)
        } else {
          new MicroBatchExecution(
            sparkSession,
            trigger,
            triggerClock,
            extraOptions,
            analyzedStreamWritePlan)
        }
        new StreamingQueryWrapper(microBatchExecution)
    }
  }
  // scalastyle:on argcount

  // scalastyle:off argcount
  /**
   * Start a [[StreamingQuery]].
   *
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
   * @param catalogAndIdent Catalog and identifier for the sink, set when it is a V2 catalog table
   */
  @throws[TimeoutException]
  private[sql] def startQuery(
      userSpecifiedName: Option[String],
      userSpecifiedCheckpointLocation: Option[String],
      df: Dataset[_],
      extraOptions: Map[String, String],
      sink: Table,
      outputMode: OutputMode,
      useTempCheckpointLocation: Boolean = false,
      recoverFromCheckpointLocation: Boolean = true,
      trigger: Trigger = Trigger.ProcessingTime(0),
      triggerClock: Clock = new SystemClock(),
      catalogAndIdent: Option[(TableCatalog, Identifier)] = None,
      catalogTable: Option[CatalogTable] = None): StreamingQuery = {
    val query = createQuery(
      userSpecifiedName,
      userSpecifiedCheckpointLocation,
      df,
      extraOptions,
      sink,
      outputMode,
      useTempCheckpointLocation,
      recoverFromCheckpointLocation,
      trigger,
      triggerClock,
      catalogAndIdent,
      catalogTable)
    // scalastyle:on argcount

    // The following code block checks if a stream with the same name or id is running. Then it
    // returns an Option of an already active stream to stop outside of the lock
    // to avoid a deadlock.
    val activeRunOpt = activeQueriesSharedLock.synchronized {
      // Make sure no other query with same name is active
      userSpecifiedName.foreach { name =>
        if (activeQueries.values.exists(_.name == name)) {
          throw new IllegalArgumentException(s"Cannot start query with name $name as a query " +
            s"with that name is already active in this SparkSession")
        }
      }

      // Make sure no other query with same id is active across all sessions
      val activeOption = Option(sparkSession.sharedState.activeStreamingQueries.get(query.id))
        .orElse(activeQueries.get(query.id)) // shouldn't be needed but paranoia ...

      val shouldStopActiveRun =
        sparkSession.sessionState.conf.getConf(SQLConf.STREAMING_STOP_ACTIVE_RUN_ON_RESTART)
      if (activeOption.isDefined) {
        if (shouldStopActiveRun) {
          val oldQuery = activeOption.get
          logWarning(log"Stopping existing streaming query [id=${MDC(QUERY_ID, query.id)}, " +
            log"runId=${MDC(RUN_ID, oldQuery.runId)}], as a new run is being started.")
          Some(oldQuery)
        } else {
          throw new IllegalStateException(
            s"Cannot start query with id ${query.id} as another query with same id is " +
              s"already active. Perhaps you are attempting to restart a query from checkpoint " +
              s"that is already active. You may stop the old query by setting the SQL " +
              "configuration: " +
              s"""spark.conf.set("${SQLConf.STREAMING_STOP_ACTIVE_RUN_ON_RESTART.key}", true) """ +
              "and retry.")
        }
      } else {
        // nothing to stop so, no-op
        None
      }
    }

    // stop() will clear the queryId from activeStreamingQueries as well as activeQueries
    activeRunOpt.foreach(_.stop())

    activeQueriesSharedLock.synchronized {
      // We still can have a race condition when two concurrent instances try to start the same
      // stream, while a third one was already active and stopped above. In this case, we throw a
      // ConcurrentModificationException.
      val oldActiveQuery = sparkSession.sharedState.activeStreamingQueries.put(
        query.id, query.streamingQuery) // we need to put the StreamExecution, not the wrapper
      if (oldActiveQuery != null) {
        throw QueryExecutionErrors.concurrentQueryInstanceError()
      }
      activeQueries.put(query.id, query)
    }

    try {
      // When starting a query, it will call `StreamingQueryListener.onQueryStarted` synchronously.
      // As it's provided by the user and can run arbitrary codes, we must not hold any lock here.
      // Otherwise, it's easy to cause dead-lock, or block too long if the user codes take a long
      // time to finish.
      query.streamingQuery.start()
    } catch {
      case e: Throwable =>
        unregisterTerminatedStream(query)
        throw e
    }
    query
  }

  /** Notify (by the StreamingQuery) that the query has been terminated */
  private[sql] def notifyQueryTermination(terminatedQuery: StreamingQuery): Unit = {
    unregisterTerminatedStream(terminatedQuery)
    awaitTerminationLock.synchronized {
      if (lastTerminatedQueryException == null || terminatedQuery.exception.nonEmpty) {
        lastTerminatedQueryException = terminatedQuery.exception
      }
      awaitTerminationLock.notifyAll()
    }
    stateStoreCoordinator.deactivateInstances(terminatedQuery.runId)
  }

  private def unregisterTerminatedStream(terminatedQuery: StreamingQuery): Unit = {
    activeQueriesSharedLock.synchronized {
      // remove from shared state only if the streaming execution also matches
      sparkSession.sharedState.activeStreamingQueries.remove(
        terminatedQuery.id, terminatedQuery)
      activeQueries -= terminatedQuery.id
    }
  }
}
