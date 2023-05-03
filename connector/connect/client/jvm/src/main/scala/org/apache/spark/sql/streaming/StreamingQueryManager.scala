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

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Evolving
import org.apache.spark.connect.proto.Command
import org.apache.spark.connect.proto.Command.StreamingQueryManagerCommand
import org.apache.spark.connect.proto.Command.StreamingQueryManagerCommandResult
import org.apache.spark.sql.SparkSession

/**
 * A class to manage all the [[StreamingQuery]] active in a `SparkSession`.
 *
 * @since 3.5.0
 */
@Evolving
class StreamingQueryManager private[sql] (sparkSession: SparkSession) {

  /**
   * Returns a list of active queries associated with this SQLContext
   *
   * @since 3.5.0
   */
  def active: Array[StreamingQuery] = {
    executeManagerCmd(_.setActive(true)).getActive.getActiveQueries.map {
      q => RemoteStreamingQuery.fromStreamingQueryInstanceResponse(sparkSession, q)
    }
  } // TODO: null check, empty array check

  /**
   * Returns the query if there is an active query with the given id, or null.
   *
   * @since 3.5.0
   */
  def get(id: UUID): StreamingQuery = get(id.toString)

  /**
   * Returns the query if there is an active query with the given id, or null.
   *
   * @since 3.5.0
   */
  def get(id: String): StreamingQuery = {
    executeManagerCmd(_.setGet(id)).getQuery match {
      case null => null // TODO: needed?
      case q => RemoteStreamingQuery.fromStreamingQueryInstanceResponse(sparkSession, q)
    }
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
  *  For correctly documenting exceptions across multiple queries,
   * users need to stop all of them after any of them terminates with exception, and then check the
   * `query.exception()` for each query.
   *
   * @since 3.5.0
   */
  // TODO(SPARK-43299): verity the behavior of this method after JVM client-side error-handling
  // framework is supported and modify the doc accordingly.
  def awaitAnyTermination(): Unit = {
    executeManagerCmd(_.getAwaitAnyTerminationBuilder.build())
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
   * For correctly documenting exceptions across multiple queries,
   * users need to stop all of them after any of them terminates with exception, and then check the
   * `query.exception()` for each query.
   *
   * @since 3.5.0
   */
  // TODO(SPARK-43299): verity the behavior of this method after JVM client-side error-handling
  // framework is supported and modify the doc accordingly.
  def awaitAnyTermination(timeoutMs: Long): Boolean = {
    require(timeoutMs > 0, "Timeout has to be positive")
    executeManagerCmd(
      _.getAwaitAnyTerminationBuilder.setTimeoutMs(timeoutMs)).getAwaitAnyTermination.getTerminated
  }

  /**
   * Forget about past terminated queries so that `awaitAnyTermination()` can be used again to
   * wait for new terminations.
   *
   * @since 3.5.0
   */
  def resetTerminated(): Unit = {
    executeManagerCmd(_.setResetTerminated(true))
  }

  private def executeManagerCmd(
    setCmdFn: StreamingQueryManagerCommand.Builder => Unit // Sets the command field, like stop().
  ): StreamingQueryManagerCommandResult = {

    val cmdBuilder = Command.newBuilder()
    val managerCmdBuilder = cmdBuilder.getStreamingQueryManagerCommandBuilder

    // Set command.
    setCmdFn(managerCmdBuilder)

    val resp = sparkSession.execute(cmdBuilder.build()).head

    if (!resp.hasStreamingQueryManagerCommandResult) {
      throw new RuntimeException("Unexpected missing response for streaming query manager command")
    }

    resp.getStreamingQueryManagerCommandResult
  }
}