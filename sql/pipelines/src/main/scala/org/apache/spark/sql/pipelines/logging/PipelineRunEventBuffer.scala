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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging

/**
 * An in-memory buffer which contains the internal events that are emitted during a run of a
 * pipeline.
 *
 * @param eventCallback A callback function to be called when an event is added to the buffer.
 */
class PipelineRunEventBuffer(eventCallback: PipelineEvent => Unit) extends Logging {

  /**
   * A buffer to hold the events emitted during a pipeline run.
   * This buffer is thread-safe and can be accessed concurrently.
   *
   * TODO(SPARK-52409): Deprecate this class to be used in test only and use a more
   *                    robust event logging system in production.
   */
  private val events = ArrayBuffer[PipelineEvent]()

  def addEvent(event: PipelineEvent): Unit = synchronized {
    val eventToAdd = event
    events.append(eventToAdd)
    eventCallback(event)
  }

  def clear(): Unit = synchronized {
    events.clear()
  }

  def getEvents: Seq[PipelineEvent] = events.toSeq
}
