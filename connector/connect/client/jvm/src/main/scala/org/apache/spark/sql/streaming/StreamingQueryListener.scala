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

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}
import org.json4s.{JObject, JString, JValue}
import org.json4s.JsonDSL.{jobject2assoc, pair2Assoc}
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.annotation.Evolving

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
    private val mapper = {
      val ret = new ObjectMapper() with ClassTagExtensions
      ret.registerModule(DefaultScalaModule)
      ret.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      ret
    }

    private[spark] def jsonString(event: QueryStartedEvent): String =
      mapper.writeValueAsString(event)

    private[spark] def fromJson(json: String): QueryStartedEvent =
      mapper.readValue[QueryStartedEvent](json)
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
    private val mapper = {
      val ret = new ObjectMapper() with ClassTagExtensions
      ret.registerModule(DefaultScalaModule)
      ret.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      ret
    }

    private[spark] def jsonString(event: QueryProgressEvent): String =
      mapper.writeValueAsString(event)

    private[spark] def fromJson(json: String): QueryProgressEvent =
      mapper.readValue[QueryProgressEvent](json)
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
    private val mapper = {
      val ret = new ObjectMapper() with ClassTagExtensions
      ret.registerModule(DefaultScalaModule)
      ret.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      ret
    }

    private[spark] def jsonString(event: QueryTerminatedEvent): String =
      mapper.writeValueAsString(event)

    private[spark] def fromJson(json: String): QueryTerminatedEvent =
      mapper.readValue[QueryTerminatedEvent](json)
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
    private val mapper = {
      val ret = new ObjectMapper() with ClassTagExtensions
      ret.registerModule(DefaultScalaModule)
      ret.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      ret
    }

    private[spark] def jsonString(event: QueryTerminatedEvent): String =
      mapper.writeValueAsString(event)

    private[spark] def fromJson(json: String): QueryTerminatedEvent =
      mapper.readValue[QueryTerminatedEvent](json)
  }
}
