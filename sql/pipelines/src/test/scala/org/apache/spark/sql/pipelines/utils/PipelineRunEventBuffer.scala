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

package org.apache.spark.sql.pipelines.utils

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.pipelines.logging.PipelineEvent

/**
 * An in-memory buffer which contains the internal events that are emitted during a run of a
 * pipeline.
 */
class PipelineRunEventBuffer extends Logging {

  /**
   * A buffer to hold the events emitted during a pipeline run.
   * This buffer is thread-safe and can be accessed concurrently.
   */
  private val events = ArrayBuffer[PipelineEvent]()

  def addEvent(event: PipelineEvent): Unit = synchronized {
    val eventToAdd = event
    events.append(eventToAdd)
  }

  def clear(): Unit = synchronized {
    events.clear()
  }

  def getEvents: Seq[PipelineEvent] = events.toSeq
}
