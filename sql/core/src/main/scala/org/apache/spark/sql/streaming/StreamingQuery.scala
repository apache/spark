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

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.SparkSession

/**
 * :: Experimental ::
 * A handle to a query that is executing continuously in the background as new data arrives.
 * All these methods are thread-safe.
 * @since 2.0.0
 */
@Experimental
trait StreamingQuery {

  /**
   * Returns the name of the query. This name is unique across all active queries. This can be
   * set in the [[org.apache.spark.sql.DataStreamWriter DataStreamWriter]] as
   * `dataframe.writeStream.queryName("query").start()`.
   * @since 2.0.0
   */
  def name: String

  /**
   * Returns the unique id of this query. This id is automatically generated and is unique across
   * all queries that have been started in the current process.
   * @since 2.0.0
   */
  def id: Long

  /**
   * Returns the [[SparkSession]] associated with `this`.
   * @since 2.0.0
   */
  def sparkSession: SparkSession

  /**
   * Whether the query is currently active or not
   * @since 2.0.0
   */
  def isActive: Boolean

  /**
   * Returns the [[StreamingQueryException]] if the query was terminated by an exception.
   * @since 2.0.0
   */
  def exception: Option[StreamingQueryException]

  /**
   * Returns the current status of the query.
   * @since 2.0.2
   */
  def status: StreamingQueryStatus

  /**
   * Returns current status of all the sources.
   * @since 2.0.0
   */
  @deprecated("use status.sourceStatuses", "2.0.2")
  def sourceStatuses: Array[SourceStatus]

  /**
   * Returns current status of the sink.
   * @since 2.0.0
   */
  @deprecated("use status.sinkStatus", "2.0.2")
  def sinkStatus: SinkStatus

  /**
   * Waits for the termination of `this` query, either by `query.stop()` or by an exception.
   * If the query has terminated with an exception, then the exception will be thrown.
   *
   * If the query has terminated, then all subsequent calls to this method will either return
   * immediately (if the query was terminated by `stop()`), or throw the exception
   * immediately (if the query has terminated with exception).
   *
   * @throws StreamingQueryException, if `this` query has terminated with an exception.
   *
   * @since 2.0.0
   */
  def awaitTermination(): Unit

  /**
   * Waits for the termination of `this` query, either by `query.stop()` or by an exception.
   * If the query has terminated with an exception, then the exception will be thrown.
   * Otherwise, it returns whether the query has terminated or not within the `timeoutMs`
   * milliseconds.
   *
   * If the query has terminated, then all subsequent calls to this method will either return
   * `true` immediately (if the query was terminated by `stop()`), or throw the exception
   * immediately (if the query has terminated with exception).
   *
   * @throws StreamingQueryException, if `this` query has terminated with an exception
   *
   * @since 2.0.0
   */
  def awaitTermination(timeoutMs: Long): Boolean

  /**
   * Blocks until all available data in the source has been processed and committed to the sink.
   * This method is intended for testing. Note that in the case of continually arriving data, this
   * method may block forever. Additionally, this method is only guaranteed to block until data that
   * has been synchronously appended data to a [[org.apache.spark.sql.execution.streaming.Source]]
   * prior to invocation. (i.e. `getOffset` must immediately reflect the addition).
   * @since 2.0.0
   */
  def processAllAvailable(): Unit

  /**
   * Stops the execution of this query if it is running. This method blocks until the threads
   * performing execution has stopped.
   * @since 2.0.0
   */
  def stop(): Unit

  /**
   * Prints the physical plan to the console for debugging purposes.
   * @since 2.0.0
   */
  def explain(): Unit

  /**
   * Prints the physical plan to the console for debugging purposes.
   *
   * @param extended whether to do extended explain or not
   * @since 2.0.0
   */
  def explain(extended: Boolean): Unit
}
