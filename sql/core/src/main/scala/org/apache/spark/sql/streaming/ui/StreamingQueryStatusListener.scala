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

package org.apache.spark.sql.streaming.ui

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress}

class StreamingQueryStatusListener(sqlConf: SQLConf) extends StreamingQueryListener {

  private val activeQueryStatus = new ConcurrentHashMap[UUID, StreamingQueryUIData]()
  private val inactiveQueryStatus = new ConcurrentHashMap[UUID, StreamingQueryUIData]()

  private val streamingProgressRetention = sqlConf.streamingProgressRetention

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    activeQueryStatus.putIfAbsent(event.id,
      new StreamingQueryUIData(event.name, event.id, event.runId))
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val queryStatus = activeQueryStatus.getOrDefault(
      event.progress.id,
      new StreamingQueryUIData(event.progress.name, event.progress.id, event.progress.runId))
    queryStatus.updateProcess(event.progress, streamingProgressRetention)
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    val queryStatus = activeQueryStatus.remove(event.id)
    if (queryStatus != null) {
      queryStatus.queryTerminated(event)
      inactiveQueryStatus.put(event.id, queryStatus)
    }
  }

  def allQueryStatus: Seq[StreamingQueryUIData] = {
    activeQueryStatus.values().asScala.toSeq ++ inactiveQueryStatus.values().asScala.toSeq
  }
}

private[ui] class StreamingQueryUIData(
    val name: String,
    val id: UUID,
    val runId: UUID) {
  val submitTime = System.currentTimeMillis()

  /** Holds the most recent query progress updates. */
  private val progressBuffer = new mutable.Queue[StreamingQueryProgress]()

  private var _isActive = true
  private var _exception: Option[String] = None

  def isActive: Boolean = isActive

  def exception: Option[String] = _exception

  def queryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = synchronized {
    _isActive = false
    _exception = event.exception
  }

  def updateProcess(
      newProgress: StreamingQueryProgress, retentionNum: Int): Unit = progressBuffer.synchronized {
    progressBuffer += newProgress
    while (progressBuffer.length >= retentionNum) {
      progressBuffer.dequeue()
    }
  }

  def recentProgress: Array[StreamingQueryProgress] = progressBuffer.synchronized {
    progressBuffer.toArray
  }

  def lastProgress: StreamingQueryProgress = progressBuffer.synchronized {
    progressBuffer.lastOption.orNull
  }
}