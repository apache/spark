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

import scala.jdk.CollectionConverters._

import org.json4s.{DefaultFormats, JNothing, JObject, JString, JValue}
import org.json4s.JsonDSL.{jobject2assoc, pair2Assoc}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import org.apache.spark.annotation.Evolving
import org.apache.spark.sql.Row

/**
 * Interface for listening to events related to [[StreamingQuery StreamingQueries]].
 * @note
 *   The methods are not thread-safe as they may be called from different threads.
 *
 * @since 3.5.0
 */
@Evolving
abstract class StreamingQueryListener extends Serializable {

  import StreamingQueryListener._

  /**
   * Called when a query is started.
   * @note
   *   This is called synchronously with
   *   [[org.apache.spark.sql.streaming.DataStreamWriter `DataStreamWriter.start()`]], that is,
   *   `onQueryStart` will be called on all listeners before `DataStreamWriter.start()` returns
   *   the corresponding [[StreamingQuery]]. Please don't block this method as it will block your
   *   query.
   * @since 3.5.0
   */
  def onQueryStarted(event: QueryStartedEvent): Unit

  /**
   * Called when there is some status update (ingestion rate updated, etc.)
   *
   * @note
   *   This method is asynchronous. The status in [[StreamingQuery]] will always be latest no
   *   matter when this method is called. Therefore, the status of [[StreamingQuery]] may be
   *   changed before/when you process the event. E.g., you may find [[StreamingQuery]] is
   *   terminated when you are processing `QueryProgressEvent`.
   * @since 3.5.0
   */
  def onQueryProgress(event: QueryProgressEvent): Unit

  /**
   * Called when the query is idle and waiting for new data to process.
   * @since 3.5.0
   */
  def onQueryIdle(event: QueryIdleEvent): Unit = {}

  /**
   * Called when a query is stopped, with or without error.
   * @since 3.5.0
   */
  def onQueryTerminated(event: QueryTerminatedEvent): Unit
}

/**
 * Companion object of [[StreamingQueryListener]] that defines the listener events.
 * @since 3.5.0
 */
@Evolving
object StreamingQueryListener extends Serializable {

  /**
   * Base type of [[StreamingQueryListener]] events
   * @since 3.5.0
   */
  @Evolving
  trait Event

  /**
   * Event representing the start of a query
   * @param id
   *   A unique query id that persists across restarts. See `StreamingQuery.id()`.
   * @param runId
   *   A query id that is unique for every start/restart. See `StreamingQuery.runId()`.
   * @param name
   *   User-specified name of the query, null if not specified.
   * @param timestamp
   *   The timestamp to start a query.
   * @since 3.5.0
   */
  @Evolving
  class QueryStartedEvent private[sql] (
      val id: UUID,
      val runId: UUID,
      val name: String,
      val timestamp: String)
      extends Event
      with Serializable {

    def json: String = compact(render(jsonValue))

    private def jsonValue: JValue = {
      ("id" -> JString(id.toString)) ~
        ("runId" -> JString(runId.toString)) ~
        ("name" -> JString(name)) ~
        ("timestamp" -> JString(timestamp))
    }
  }

  private[spark] object QueryStartedEvent {
    private implicit val formats: DefaultFormats = DefaultFormats

    def fromJson(input: String): QueryStartedEvent = {
      val json = parse(input)
      val id = UUID.fromString((json \ "id").extract[String])
      val runId = UUID.fromString((json \ "runId").extract[String])
      val name = (json \ "name").extract[String]
      val timestamp = (json \ "timestamp").extract[String]
      new QueryStartedEvent(id, runId, name, timestamp)
    }
  }

  /**
   * Event representing any progress updates in a query.
   * @param progress
   *   The query progress updates.
   * @since 3.5.0
   */
  @Evolving
  class QueryProgressEvent private[sql] (val progress: StreamingQueryProgress)
      extends Event
      with Serializable {

    def json: String = compact(render(jsonValue))

    private def jsonValue: JValue = JObject("progress" -> progress.jsonValue)
  }

  private[spark] object QueryProgressEvent {
    private implicit val formats: DefaultFormats = DefaultFormats

    def fromJson(input: String): QueryProgressEvent = {
      val json = parse(input)
      val progress = (json \ "progress").extract[JObject]
      new QueryProgressEvent(toStreamingQueryProgress(progress))
    }

    private def toStreamingQueryProgress(input: JObject): StreamingQueryProgress = {
      val json = input
      val id = UUID.fromString((json \ "id").extract[String])
      val runId = UUID.fromString((json \ "runId").extract[String])
      val name = (json \ "name").extract[String]
      val timestamp = (json \ "timestamp").extract[String]
      val batchId = (json \ "batchId").extract[Int]
      val batchDuration = (json \ "batchDuration").extract[Long]
      val durationMs = (json \ "durationMs").extract[Map[String, Long]].asJava
        .asInstanceOf[java.util.Map[String, java.lang.Long]]
      val eventTime = (json \ "eventTime").extract[Map[String, String]].asJava
      val stateOperators = (json \ "stateOperators").extract[Array[JObject]]
        .map(toStateOperatorProgress)
      val sources = (json \ "sources").extract[Array[JObject]].map(toSourceProgress)
      val sink = toSinkProgress((json \ "sink").extract[JObject])
      val observedMetrics = (json \ "observedMetrics").extract[Map[String, Row]].asJava
      new StreamingQueryProgress(
        id,
        runId,
        name,
        timestamp,
        batchId,
        batchDuration,
        durationMs,
        eventTime,
        stateOperators,
        sources,
        sink,
        observedMetrics)
    }

    private def toStateOperatorProgress(input: JObject): StateOperatorProgress = {
      val json = input
      val operatorName = (json \ "operatorName").extract[String]
      val numRowsTotal = (json \ "numRowsTotal").extract[Int]
      val numRowsUpdated = (json \ "numRowsUpdated").extract[Int]
      val allUpdatesTimeMs = (json \ "allUpdatesTimeMs").extract[Int]
      val numRowsRemoved = (json \ "numRowsRemoved").extract[Int]
      val allRemovalsTimeMs = (json \ "allRemovalsTimeMs").extract[Int]
      val commitTimeMs = (json \ "commitTimeMs").extract[Int]
      val memoryUsedBytes = (json \ "memoryUsedBytes").extract[Int]
      val numRowsDroppedByWatermark = (json \ "numRowsDroppedByWatermark").extract[Int]
      val numShufflePartitions = (json \ "numShufflePartitions").extract[Int]
      val numStateStoreInstances = (json \ "numStateStoreInstances").extract[Int]
      val customMetrics = (json \ "customMetrics").extract[Map[String, Long]].asJava
        .asInstanceOf[java.util.Map[String, java.lang.Long]]
      new StateOperatorProgress(
        operatorName,
        numRowsTotal,
        numRowsUpdated,
        allUpdatesTimeMs,
        numRowsRemoved,
        allRemovalsTimeMs,
        commitTimeMs,
        memoryUsedBytes,
        numRowsDroppedByWatermark,
        numShufflePartitions,
        numStateStoreInstances,
        customMetrics)
    }

    private def toSourceProgress(input: JObject): SourceProgress = {
      val json = input
      val description = (json \ "description").extract[String]
      val startOffset = (json \ "startOffset").extract[Int]
      val endOffset = (json \ "endOffset").extract[Int]
      val latestOffset = (json \ "latestOffset").extract[Int]
      val numInputRows = (json \ "numInputRows").extract[Long]
      val inputRowsPerSecond = (json \ "inputRowsPerSecond").extract[Double]
      val processedRowsPerSecond = (json \ "processedRowsPerSecond").extract[Double]
      val metrics = (json \ "metrics").extract[Map[String, String]].asJava
      new SourceProgress(description, startOffset.toString, endOffset.toString,
        latestOffset.toString, numInputRows, inputRowsPerSecond, processedRowsPerSecond, metrics)
    }

    private def toSinkProgress(input: JObject): SinkProgress = {
      val json = input
      val description = (json \ "description").extract[String]
      val numOutputRows = (json \ "numOutputRows").extract[Long]
      val metrics = (json \ "metrics").extract[Map[String, String]].asJava
      new SinkProgress(description, numOutputRows, metrics)
    }
  }

  /**
   * Event representing that query is idle and waiting for new data to process.
   *
   * @param id
   *   A unique query id that persists across restarts. See `StreamingQuery.id()`.
   * @param runId
   *   A query id that is unique for every start/restart. See `StreamingQuery.runId()`.
   * @param timestamp
   *   The timestamp when the latest no-batch trigger happened.
   * @since 3.5.0
   */
  @Evolving
  class QueryIdleEvent private[sql] (val id: UUID, val runId: UUID, val timestamp: String)
      extends Event
      with Serializable {

    def json: String = compact(render(jsonValue))

    private def jsonValue: JValue = {
      ("id" -> JString(id.toString)) ~
        ("runId" -> JString(runId.toString)) ~
        ("timestamp" -> JString(timestamp))
    }
  }

  private[spark] object QueryIdleEvent {
    private implicit val formats: DefaultFormats = DefaultFormats

    def fromJson(input: String): QueryIdleEvent = {
      val json = parse(input)
      val id = UUID.fromString((json \ "id").extract[String])
      val runId = UUID.fromString((json \ "runId").extract[String])
      val timestamp = (json \ "timestamp").extract[String]
      new QueryIdleEvent(id, runId, timestamp)
    }
  }

  /**
   * Event representing that termination of a query.
   *
   * @param id
   *   A unique query id that persists across restarts. See `StreamingQuery.id()`.
   * @param runId
   *   A query id that is unique for every start/restart. See `StreamingQuery.runId()`.
   * @param exception
   *   The exception message of the query if the query was terminated with an exception.
   *   Otherwise, it will be `None`.
   * @param errorClassOnException
   *   The error class from the exception if the query was terminated with an exception which is a
   *   part of error class framework. If the query was terminated without an exception, or the
   *   exception is not a part of error class framework, it will be `None`.
   * @since 3.5.0
   */
  @Evolving
  class QueryTerminatedEvent private[sql] (
      val id: UUID,
      val runId: UUID,
      val exception: Option[String],
      val errorClassOnException: Option[String])
      extends Event
      with Serializable {
    // compatibility with versions in prior to 3.5.0
    def this(id: UUID, runId: UUID, exception: Option[String]) = {
      this(id, runId, exception, None)
    }

    def json: String = compact(render(jsonValue))

    private def jsonValue: JValue = {
      ("id" -> JString(id.toString)) ~
        ("runId" -> JString(runId.toString)) ~
        ("exception" -> JString(exception.orNull)) ~
        ("errorClassOnException" -> JString(errorClassOnException.orNull))
    }
  }

  private[spark] object QueryTerminatedEvent {
    private implicit val formats: DefaultFormats = DefaultFormats

    def fromJson(input: String): QueryTerminatedEvent = {
      val json = parse(input)
      val id = UUID.fromString((json \ "id").extract[String])
      val runId = UUID.fromString((json \ "runId").extract[String])
      val exception = jsonOption(json \ "exception").map(_.extract[String])
      val errorClassOnException = jsonOption(json \ "errorClassOnException").map(_.extract[String])
      new QueryTerminatedEvent(id, runId, exception, errorClassOnException)
    }

    private def jsonOption(json: JValue): Option[JValue] = {
      json match {
        case JNothing => None
        case value: JValue => Some(value)
      }
    }
  }
}
