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
   * Returns a new event with the current or provided timestamp and the given origin/message.
   */
  def apply(
      origin: PipelineEventOrigin,
      level: EventLevel,
      message: String,
      details: EventDetails,
      exception: Option[Throwable] = None,
      eventTimestamp: Option[Timestamp] = None
  ): PipelineEvent = {
    val eventUUID = UUID.randomUUID()
    val timestamp = Timestamp.from(Instant.now())

    PipelineEvent(
      id = eventUUID.toString,
      timestamp = eventTimestamp.getOrElse(timestamp),
      message = message,
      details = details,
      error = exception,
      origin = origin,
      level = level
    )
  }
}
