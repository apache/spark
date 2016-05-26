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

package org.apache.spark.sql

import scala.collection.mutable

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.analysis.{Append, OutputMode, UnsupportedOperationChecker}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.state.StateStoreCoordinatorRef
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.ContinuousQueryListener
import org.apache.spark.util.{Clock, SystemClock}

/**
 * :: Experimental ::
 * A class to manage all the [[org.apache.spark.sql.ContinuousQuery ContinuousQueries]] active
 * on a [[SparkSession]].
 *
 * @since 2.0.0
 */
@Experimental
class ContinuousQueryManager(sparkSession: SparkSession) {

  private[sql] val stateStoreCoordinator =
    StateStoreCoordinatorRef.forDriver(sparkSession.sparkContext.env)
  private val listenerBus = new ContinuousQueryListenerBus(sparkSession.sparkContext.listenerBus)
  private val activeQueries = new mutable.HashMap[String, ContinuousQuery]
  private val activeQueriesLock = new Object
  private val awaitTerminationLock = new Object

  private var lastTerminatedQuery: ContinuousQuery = null

  /**
   * Returns a list of active queries associated with this SQLContext
   *
   * @since 2.0.0
   */
  def active: Array[ContinuousQuery] = activeQueriesLock.synchronized {
    activeQueries.values.toArray
  }

  /**
   * Returns an active query from this SQLContext or throws exception if bad name
   *
   * @since 2.0.0
   */
  def get(name: String): ContinuousQuery = activeQueriesLock.synchronized {
    activeQueries.getOrElse(name,
      throw new IllegalArgumentException(s"There is no active query with name $name"))
  }

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
   * @throws ContinuousQueryException, if any query has terminated with an exception
   *
   * @since 2.0.0
   */
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
   * @throws ContinuousQueryException, if any query has terminated with an exception
   *
   * @since 2.0.0
   */
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
   * Register a [[ContinuousQueryListener]] to receive up-calls for life cycle events of
   * [[org.apache.spark.sql.ContinuousQuery ContinuousQueries]].
   *
   * @since 2.0.0
   */
  def addListener(listener: ContinuousQueryListener): Unit = {
    listenerBus.addListener(listener)
  }

  /**
   * Deregister a [[ContinuousQueryListener]].
   *
   * @since 2.0.0
   */
  def removeListener(listener: ContinuousQueryListener): Unit = {
    listenerBus.removeListener(listener)
  }

  /** Post a listener event */
  private[sql] def postListenerEvent(event: ContinuousQueryListener.Event): Unit = {
    listenerBus.post(event)
  }

  /** Start a query */
  private[sql] def startQuery(
      name: String,
      checkpointLocation: String,
      df: DataFrame,
      sink: Sink,
      trigger: Trigger = ProcessingTime(0),
      triggerClock: Clock = new SystemClock(),
      outputMode: OutputMode = Append): ContinuousQuery = {
    activeQueriesLock.synchronized {
      if (activeQueries.contains(name)) {
        throw new IllegalArgumentException(
          s"Cannot start query with name $name as a query with that name is already active")
      }
      val analyzedPlan = df.queryExecution.analyzed
      df.queryExecution.assertAnalyzed()

      if (sparkSession.conf.get(SQLConf.UNSUPPORTED_OPERATION_CHECK_ENABLED)) {
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
      val query = new StreamExecution(
        sparkSession,
        name,
        checkpointLocation,
        logicalPlan,
        sink,
        trigger,
        triggerClock,
        outputMode)
      query.start()
      activeQueries.put(name, query)
      query
    }
  }

  /** Notify (by the ContinuousQuery) that the query has been terminated */
  private[sql] def notifyQueryTermination(terminatedQuery: ContinuousQuery): Unit = {
    activeQueriesLock.synchronized {
      activeQueries -= terminatedQuery.name
    }
    awaitTerminationLock.synchronized {
      if (lastTerminatedQuery == null || terminatedQuery.exception.nonEmpty) {
        lastTerminatedQuery = terminatedQuery
      }
      awaitTerminationLock.notifyAll()
    }
  }
}
