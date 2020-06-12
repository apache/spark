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

import scala.collection.immutable.Queue

import com.fasterxml.jackson.annotation.JsonIgnore

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress}
import org.apache.spark.sql.streaming.ui.StreamingQueryProgressWrapper._
import org.apache.spark.sql.streaming.ui.UIUtils.parseProgressTimestamp
import org.apache.spark.status.{ElementTrackingStore, KVUtils}
import org.apache.spark.status.KVUtils.KVIndexParam
import org.apache.spark.util.kvstore.KVIndex

/**
 * A customized StreamingQueryListener used in structured streaming UI, which contains all
 * UI data for both active and inactive query.
 */
private[sql] class StreamingQueryStatusListener(
    conf: SparkConf,
    store: ElementTrackingStore) extends StreamingQueryListener {

  private val streamingProgressRetention =
    conf.get(StaticSQLConf.STREAMING_UI_RETAINED_PROGRESS_UPDATES)
  private val inactiveQueryStatusRetention = conf.get(StaticSQLConf.STREAMING_UI_RETAINED_QUERIES)

  store.addTrigger(classOf[StreamingQuerySummary], inactiveQueryStatusRetention) { count =>
    cleanupInactiveQueries(count)
  }

  private def cleanupInactiveQueries(count: Long): Unit = {
    val countToDelete = count - inactiveQueryStatusRetention
    if (countToDelete <= 0) {
      return
    }

    val view = store.view(classOf[StreamingQuerySummary]).index("startTimestamp").first(0L)
    val toDelete = KVUtils.viewToSeq(view, countToDelete.toInt)(_.isActive == false)
    toDelete.foreach { e =>
      store.delete(e.getClass, e.runId)
      store.removeAllByIndexValues(
        classOf[StreamingQueryProgressWrapper], "runId", e.runId.toString)
    }
  }

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    val startTimestamp = parseProgressTimestamp(event.timestamp)
    val querySummary = new StreamingQuerySummary(
      event.name,
      event.id,
      event.runId,
      Queue.empty[String],
      startTimestamp,
      true,
      None)
    store.write(querySummary)
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val runId = event.progress.runId
    val batchId = event.progress.batchId
    val timestamp = event.progress.timestamp
    val querySummary = store.read(classOf[StreamingQuerySummary], runId)
    val progressIds =
      querySummary.progressIds ++ Seq(getUniqueId(runId, batchId, timestamp))
    store.write(new StreamingQueryProgressWrapper(event.progress))
    while(progressIds.length >= streamingProgressRetention) {
      val uniqueId = progressIds.dequeue
      store.delete(classOf[StreamingQueryProgressWrapper], uniqueId)
    }
    store.delete(classOf[StreamingQuerySummary], runId)
    store.write(new StreamingQuerySummary(
      querySummary.name,
      querySummary.id,
      querySummary.runId,
      progressIds,
      querySummary.startTimestamp,
      querySummary.isActive,
      querySummary.exception
    ))
  }

  override def onQueryTerminated(
      event: StreamingQueryListener.QueryTerminatedEvent): Unit = synchronized {
    val querySummary = store.read(classOf[StreamingQuerySummary], event.runId)
    store.delete(classOf[StreamingQuerySummary], event.runId)
    store.write(new StreamingQuerySummary(
      querySummary.name,
      querySummary.id,
      querySummary.runId,
      querySummary.progressIds,
      querySummary.startTimestamp,
      false,
      querySummary.exception
    ))
  }
}

private[sql] class StreamingQuerySummary(
    val name: String,
    val id: UUID,
    @KVIndexParam val runId: UUID,
    val progressIds: Queue[String],
    val startTimestamp: Long,
    val isActive: Boolean,
    val exception: Option[String]) {
  @JsonIgnore @KVIndex("startTimestamp")
  private def startTimestampIndex: Long = startTimestamp
}

/**
 * This class contains all message related to UI display, each instance corresponds to a single
 * [[org.apache.spark.sql.streaming.StreamingQuery]].
 */
private[sql] case class StreamingQueryUIData(
    summary: StreamingQuerySummary,
    recentProgress: Array[StreamingQueryProgress],
    lastProgress: StreamingQueryProgress)

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
