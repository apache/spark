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

package org.apache.spark.sql.pipelines.logging

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

/**
 * A factory object that is used to construct [[PipelineEvent]]s with common fields
 * automatically filled in. Developers should always use this factory rather than construct
 * an event directly from an empty proto.
 */
object ConstructPipelineEvent {

  /**
   * Converts an exception that was thrown during a pipeline run to a more structured and standard
   * internal representation.
   */
  private[pipelines] def serializeException(t: Throwable): Seq[SerializedException] = {
    val className = t.getClass.getName
    val stacks = Option(t.getStackTrace).map(_.toSeq).getOrElse(Nil).map { f =>
      StackFrame(declaringClass = f.getClassName, methodName = f.getMethodName)
    }
    SerializedException(className = className, message = t.getMessage, stack = stacks) +:
    Option(t.getCause).map(serializeException).getOrElse(Nil)
  }

  private def constructErrorDetails(t: Throwable): ErrorDetail = ErrorDetail(serializeException(t))

  /**
   * Returns a new event with the current or provided timestamp and the given origin/message.
   */
  def apply(
      origin: PipelineEventOrigin,
      level: EventLevel,
      message: String,
      details: EventDetails,
      exception: Throwable = null,
      eventTimestamp: Option[Timestamp] = None
  ): PipelineEvent = {
    ConstructPipelineEvent(
      origin = origin,
      level = level,
      message = message,
      details = details,
      errorDetails = Option(exception).map(constructErrorDetails),
      eventTimestamp = eventTimestamp
    )
  }

  /**
   * Returns a new event with the current or given timestamp and the given origin / message.
   */
  def apply(
      origin: PipelineEventOrigin,
      level: EventLevel,
      message: String,
      details: EventDetails,
      errorDetails: Option[ErrorDetail],
      eventTimestamp: Option[Timestamp]
  ): PipelineEvent = synchronized {

    val eventUUID = UUID.randomUUID()
    val timestamp = Timestamp.from(Instant.now())

    PipelineEvent(
      id = eventUUID.toString,
      timestamp = EventHelpers.formatTimestamp(eventTimestamp.getOrElse(timestamp)),
      message = message,
      details = details,
      error = errorDetails,
      origin = origin,
      level = level
    )
  }
}
