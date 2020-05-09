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

import scala.collection.mutable

import com.fasterxml.jackson.annotation.JsonIgnore

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress}
import org.apache.spark.sql.streaming.ui.StreamingQueryProgressWrapper._
import org.apache.spark.sql.streaming.ui.UIUtils.parseProgressTimestamp
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.status.KVUtils.KVIndexParam
import org.apache.spark.util.kvstore.KVIndex

/**
 * A customized StreamingQueryListener used in structured streaming UI, which contains all
 * UI data for both active and inactive query.
 * TODO: Add support for history server.
 */
private[sql] class StreamingQueryStatusListener(conf: SparkConf) extends StreamingQueryListener {

  private var store: ElementTrackingStore = _
  private val streamingProgressRetention =
    conf.get(StaticSQLConf.STREAMING_UI_RETAINED_PROGRESS_UPDATES)
  private val inactiveQueryStatusRetention = conf.get(StaticSQLConf.STREAMING_UI_RETAINED_QUERIES)
  // activeQueries map: (queryId, queryRunId) -> queryName
  private[ui] val activeQueries = new mutable.HashMap[(UUID, UUID), String]()
  // inactive query queue tuple: (queryId, queryRunID, queryName, exception)
  private[ui] val inactiveQueryQueue = new mutable.Queue[(UUID, UUID, String, Option[String])]()
  private val startTimeOfQuery = new mutable.HashMap[UUID, Long]()
  private val progressUniqueIdOfQuery = new mutable.HashMap[UUID, mutable.Queue[String]]()

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    val startTimestamp = parseProgressTimestamp(event.timestamp)
    activeQueries.put((event.id, event.runId), event.name)
    startTimeOfQuery.put(event.runId, startTimestamp)
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val runId = event.progress.runId
    val batchId = event.progress.batchId
    val timestamp = event.progress.timestamp
    val uniqueIdQueue =
      progressUniqueIdOfQuery.getOrElseUpdate(runId, new mutable.Queue[String]())
    uniqueIdQueue += getUniqueId(runId, batchId, timestamp)
    store.write(new StreamingQueryProgressWrapper(event.progress))
    while(uniqueIdQueue.length >= streamingProgressRetention) {
      val uniqueId = uniqueIdQueue.dequeue()
      store.delete(classOf[StreamingQueryProgressWrapper], uniqueId)
    }
  }

  override def onQueryTerminated(
      event: StreamingQueryListener.QueryTerminatedEvent): Unit = synchronized {
    val name = activeQueries.remove((event.id, event.runId)).getOrElse("")
    inactiveQueryQueue += ((event.id, event.runId, name, event.exception))
    while (inactiveQueryQueue.length >= inactiveQueryStatusRetention) {
      val (_, runId, _, _) = inactiveQueryQueue.dequeue()
      store.removeAllByIndexValues(classOf[StreamingQueryProgressWrapper], "runId", runId.toString)
    }
  }

  def allQueryStatus: Seq[StreamingQueryUIData] = synchronized {
    activeQueries.toSeq.map { case ((id, runId), name) =>
      new StreamingQueryUIData(
        name,
        id,
        runId,
        progressUniqueIdOfQuery.getOrElse(runId, Seq.empty),
        startTimeOfQuery(runId),
        true,
        None,
        store)} ++
      inactiveQueryQueue.toIterator.map { case (id, runId, name, exception) =>
        new StreamingQueryUIData(
          name,
          id,
          runId,
          progressUniqueIdOfQuery.getOrElse(runId, Seq.empty),
          startTimeOfQuery(runId),
          false,
          exception,
          store)
      }
  }

  def setStore(_store: ElementTrackingStore): Unit = {
    store = _store
  }

  def recentProgressByRunId(runId: UUID): Array[StreamingQueryProgress] = {
    progressUniqueIdOfQuery.get(runId).map(
      new StreamingQueryUIData(
        null,
        null,
        runId,
        _,
        -1L,
        false,
        None,
        store).recentProgress
    ).getOrElse(Array.empty)
  }

  def lastProgressByRunId(runId: UUID): StreamingQueryProgress = {
    progressUniqueIdOfQuery.get(runId).map(
      new StreamingQueryUIData(
        null,
        null,
        runId,
        _,
        -1L,
        false,
        None,
        store).lastProgress
    ).orNull
  }
}

/**
 * This class contains all message related to UI display, each instance corresponds to a single
 * [[org.apache.spark.sql.streaming.StreamingQuery]].
 */
private[sql] class StreamingQueryUIData(
    val name: String,
    val id: UUID,
    val runId: UUID,
    uniqueIdSeq: Seq[String],
    val startTimestamp: Long,
    val isActive: Boolean,
    val exception: Option[String],
    store: ElementTrackingStore) {

  def recentProgress: Array[StreamingQueryProgress] = {
    uniqueIdSeq.map { uniqueId =>
      store.read(classOf[StreamingQueryProgressWrapper], uniqueId).progress
    }.toArray
  }

  def lastProgress: StreamingQueryProgress = {
    if (uniqueIdSeq.nonEmpty) {
      store.read(classOf[StreamingQueryProgressWrapper], uniqueIdSeq.last).progress
    } else {
      null
    }
  }
}

private[sql] class StreamingQueryProgressWrapper(val progress: StreamingQueryProgress) {
  @KVIndexParam("batchId") val batchId: Long = progress.batchId
  @KVIndexParam("runId") val runId: String = progress.runId.toString

  @JsonIgnore @KVIndex
  def uniqueId: String = getUniqueId(progress.runId, progress.batchId, progress.timestamp)
}

private[sql] object StreamingQueryProgressWrapper {
  /**
   * Adding `timestamp` into unique id to support reporting `empty` query progress
   * when no data comes.
   */
  def getUniqueId(
      runId: UUID,
      batchId: Long,
      timestamp: String): String = {
    s"${runId}_${batchId}_$timestamp"
  }
}
