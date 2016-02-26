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

import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * A handle to a query that is executing continuously in the background as new data arrives.
 * All these methods are thread-safe.
 * @since 2.0.0
 */
@Experimental
trait ContinuousQuery {

  /**
   * Returns the name of the query.
   * @since 2.0.0
   */
  def name: String

  /**
   * Returns the SQLContext associated with `this` query
   * @since 2.0.0
   */
  def sqlContext: SQLContext

  /**
   * Whether the query is currently active or not
   * @since 2.0.0
   */
  def isActive: Boolean

  /**
   * Returns the [[ContinuousQueryException]] if the query was terminated by an exception.
   * @since 2.0.0
   */
  def exception: Option[ContinuousQueryException]

  /**
   * Returns current status of all the sources.
   * @since 2.0.0
   */
   def sourceStatuses: Array[SourceStatus]

  /** Returns current status of the sink. */
  def sinkStatus: SinkStatus

  /**
   * Waits for the termination of `this` query, either by `query.stop()` or by an exception.
   * If the query has terminated with an exception, then the exception will be thrown.
   *
   * If the query has terminated, then all subsequent calls to this method will either return
   * immediately (if the query was terminated by `stop()`), or throw the exception
   * immediately (if the query has terminated with exception).
   *
   * @throws ContinuousQueryException, if `this` query has terminated with an exception.
   *
   * @since 2.0.0
   */
  def awaitTermination(): Unit

  /**
   * Waits for the termination of `this` query, either by `query.stop()` or by an exception.
   * If the query has terminated with an exception, then the exception will be throw.
   * Otherwise, it returns whether the query has terminated or not within the `timeoutMs`
   * milliseconds.
   *
   * If the query has terminated, then all subsequent calls to this method will either return
   * `true` immediately (if the query was terminated by `stop()`), or throw the exception
   * immediately (if the query has terminated with exception).
   *
   * @throws ContinuousQueryException, if `this` query has terminated with an exception
   *
   * @since 2.0.0
   */
  def awaitTermination(timeoutMs: Long): Boolean

  /**
   * Stops the execution of this query if it is running. This method blocks until the threads
   * performing execution has stopped.
   * @since 2.0.0
   */
  def stop(): Unit
}
