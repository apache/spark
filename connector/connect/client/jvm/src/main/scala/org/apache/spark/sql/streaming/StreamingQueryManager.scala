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
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import scala.jdk.CollectionConverters._

import org.apache.spark.annotation.Evolving
import org.apache.spark.connect.proto.Command
import org.apache.spark.connect.proto.StreamingQueryManagerCommand
import org.apache.spark.connect.proto.StreamingQueryManagerCommandResult
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{api, SparkSession}
import org.apache.spark.sql.connect.common.InvalidPlanInput

/**
 * A class to manage all the [[StreamingQuery]] active in a `SparkSession`.
 *
 * @since 3.5.0
 */
@Evolving
class StreamingQueryManager private[sql] (sparkSession: SparkSession)
    extends api.StreamingQueryManager
    with Logging {

  // Mapping from id to StreamingQueryListener. There's another mapping from id to
  // StreamingQueryListener on server side. This is used by removeListener() to find the id
  // of previously added StreamingQueryListener and pass it to server side to find the
  // corresponding listener on server side. We use id to StreamingQueryListener mapping
  // here to make sure there's no hash collision as well as handling the case that adds and
  // removes the same listener instance multiple times properly.
  private lazy val listenerCache: ConcurrentMap[String, StreamingQueryListener] =
    new ConcurrentHashMap()

  private[spark] val streamingQueryListenerBus = new StreamingQueryListenerBus(sparkSession)

  private[spark] def close(): Unit = {
    streamingQueryListenerBus.close()
  }

  /** @inheritdoc */
  def active: Array[StreamingQuery] = {
    executeManagerCmd(_.setActive(true)).getActive.getActiveQueriesList.asScala.map { q =>
      RemoteStreamingQuery.fromStreamingQueryInstanceResponse(sparkSession, q)
    }.toArray
  }

  /** @inheritdoc */
  def get(id: UUID): StreamingQuery = get(id.toString)

  /** @inheritdoc */
  def get(id: String): StreamingQuery = {
    val response = executeManagerCmd(_.setGetQuery(id))
    if (response.hasQuery) {
      RemoteStreamingQuery.fromStreamingQueryInstanceResponse(sparkSession, response.getQuery)
    } else {
      null
    }
  }

  /** @inheritdoc */
  @throws[StreamingQueryException]
  def awaitAnyTermination(): Unit = {
    executeManagerCmd(_.getAwaitAnyTerminationBuilder.build())
  }

  /** @inheritdoc */
  @throws[StreamingQueryException]
  def awaitAnyTermination(timeoutMs: Long): Boolean = {
    require(timeoutMs > 0, "Timeout has to be positive")
    executeManagerCmd(
      _.getAwaitAnyTerminationBuilder.setTimeoutMs(
        timeoutMs)).getAwaitAnyTermination.getTerminated
  }

  /** @inheritdoc */
  def resetTerminated(): Unit = {
    executeManagerCmd(_.setResetTerminated(true))
  }

  /** @inheritdoc */
  def addListener(listener: StreamingQueryListener): Unit = {
    streamingQueryListenerBus.append(listener)
  }

  /** @inheritdoc */
  def removeListener(listener: StreamingQueryListener): Unit = {
    streamingQueryListenerBus.remove(listener)
  }

  /** @inheritdoc */
  def listListeners(): Array[StreamingQueryListener] = {
    streamingQueryListenerBus.list()
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
      throw new RuntimeException(
        "Unexpected missing response for streaming query manager command")
    }

    resp.getStreamingQueryManagerCommandResult
  }

  private def cacheListenerById(id: String, listener: StreamingQueryListener): Unit = {
    listenerCache.putIfAbsent(id, listener)
  }

  private def getIdByListener(listener: StreamingQueryListener): String = {
    listenerCache.forEach((k, v) => if (listener.equals(v)) return k)
    throw InvalidPlanInput(s"No id with listener $listener is found.")
  }

  private def removeCachedListener(id: String): StreamingQueryListener = {
    listenerCache.remove(id)
  }
}
