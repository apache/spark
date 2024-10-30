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
package org.apache.spark.sql.api

import _root_.java.util.UUID

import org.apache.spark.annotation.Evolving
import org.apache.spark.sql.streaming.{StreamingQueryException, StreamingQueryListener}

/**
 * A class to manage all the [[StreamingQuery]] active in a `SparkSession`.
 *
 * @since 2.0.0
 */
@Evolving
abstract class StreamingQueryManager {

  /**
   * Returns a list of active queries associated with this SQLContext
   *
   * @since 2.0.0
   */
  def active: Array[_ <: StreamingQuery]

  /**
   * Returns the query if there is an active query with the given id, or null.
   *
   * @since 2.1.0
   */
  def get(id: UUID): StreamingQuery

  /**
   * Returns the query if there is an active query with the given id, or null.
   *
   * @since 2.1.0
   */
  def get(id: String): StreamingQuery

  /**
   * Wait until any of the queries on the associated SQLContext has terminated since the creation
   * of the context, or since `resetTerminated()` was called. If any query was terminated with an
   * exception, then the exception will be thrown.
   *
   * If a query has terminated, then subsequent calls to `awaitAnyTermination()` will either
   * return immediately (if the query was terminated by `query.stop()`), or throw the exception
   * immediately (if the query was terminated with exception). Use `resetTerminated()` to clear
   * past terminations and wait for new terminations.
   *
   * In the case where multiple queries have terminated since `resetTermination()` was called, if
   * any query has terminated with exception, then `awaitAnyTermination()` will throw any of the
   * exception. For correctly documenting exceptions across multiple queries, users need to stop
   * all of them after any of them terminates with exception, and then check the
   * `query.exception()` for each query.
   *
   * @throws org.apache.spark.sql.streaming.StreamingQueryException
   *   if any query has terminated with an exception
   * @since 2.0.0
   */
  @throws[StreamingQueryException]
  def awaitAnyTermination(): Unit

  /**
   * Wait until any of the queries on the associated SQLContext has terminated since the creation
   * of the context, or since `resetTerminated()` was called. Returns whether any query has
   * terminated or not (multiple may have terminated). If any query has terminated with an
   * exception, then the exception will be thrown.
   *
   * If a query has terminated, then subsequent calls to `awaitAnyTermination()` will either
   * return `true` immediately (if the query was terminated by `query.stop()`), or throw the
   * exception immediately (if the query was terminated with exception). Use `resetTerminated()`
   * to clear past terminations and wait for new terminations.
   *
   * In the case where multiple queries have terminated since `resetTermination()` was called, if
   * any query has terminated with exception, then `awaitAnyTermination()` will throw any of the
   * exception. For correctly documenting exceptions across multiple queries, users need to stop
   * all of them after any of them terminates with exception, and then check the
   * `query.exception()` for each query.
   *
   * @throws org.apache.spark.sql.streaming.StreamingQueryException
   *   if any query has terminated with an exception
   * @since 2.0.0
   */
  @throws[StreamingQueryException]
  def awaitAnyTermination(timeoutMs: Long): Boolean

  /**
   * Forget about past terminated queries so that `awaitAnyTermination()` can be used again to
   * wait for new terminations.
   *
   * @since 2.0.0
   */
  def resetTerminated(): Unit

  /**
   * Register a [[org.apache.spark.sql.streaming.StreamingQueryListener]] to receive up-calls for
   * life cycle events of [[StreamingQuery]].
   *
   * @since 2.0.0
   */
  def addListener(listener: StreamingQueryListener): Unit

  /**
   * Deregister a [[org.apache.spark.sql.streaming.StreamingQueryListener]].
   *
   * @since 2.0.0
   */
  def removeListener(listener: StreamingQueryListener): Unit

  /**
   * List all [[org.apache.spark.sql.streaming.StreamingQueryListener]]s attached to this
   * [[StreamingQueryManager]].
   *
   * @since 3.0.0
   */
  def listListeners(): Array[StreamingQueryListener]
}
