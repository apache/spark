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

import scala.collection.mutable

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

  store.addTrigger(classOf[StreamingQueryData], inactiveQueryStatusRetention) { count =>
    cleanupInactiveQueries(count)
  }

  // Events from the same query run will never be processed concurrently, so it's safe to
  // access `progressIds` without any protection.
  private val queryToProgress = new ConcurrentHashMap[UUID, mutable.Queue[String]]()

  private def cleanupInactiveQueries(count: Long): Unit = {
    val view = store.view(classOf[StreamingQueryData]).index("active").first(false).last(false)
    val inactiveQueries = KVUtils.viewToSeq(view, Int.MaxValue)(_ => true)
    val numInactiveQueries = inactiveQueries.size
    if (numInactiveQueries <= inactiveQueryStatusRetention) {
      return
    }
    val toDelete = inactiveQueries.sortBy(_.endTimestamp.get)
      .take(numInactiveQueries - inactiveQueryStatusRetention)
    val runIds = toDelete.map { e =>
      store.delete(e.getClass, e.runId)
      e.runId.toString
    }
    // Delete wrappers in one pass, as deleting them for each summary is slow
    store.removeAllByIndexValues(classOf[StreamingQueryProgressWrapper], "runId", runIds)
  }

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    val startTimestamp = parseProgressTimestamp(event.timestamp)
    store.write(new StreamingQueryData(
      event.name,
      event.id,
      event.runId,
      isActive = true,
      None,
      startTimestamp
    ), checkTriggers = true)
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val runId = event.progress.runId
    val batchId = event.progress.batchId
    val timestamp = event.progress.timestamp
    if (!queryToProgress.containsKey(runId)) {
      queryToProgress.put(runId, mutable.Queue.empty[String])
    }
    val progressIds = queryToProgress.get(runId)
    progressIds.enqueue(getUniqueId(runId, batchId, timestamp))
    store.write(new StreamingQueryProgressWrapper(event.progress))
    while (progressIds.length > streamingProgressRetention) {
      val uniqueId = progressIds.dequeue
      store.delete(classOf[StreamingQueryProgressWrapper], uniqueId)
    }
  }

  override def onQueryTerminated(
      event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    val querySummary = store.read(classOf[StreamingQueryData], event.runId)
    val curTime = System.currentTimeMillis()
    store.write(new StreamingQueryData(
      querySummary.name,
      querySummary.id,
      querySummary.runId,
      isActive = false,
      querySummary.exception,
      querySummary.startTimestamp,
      Some(curTime)
    ), checkTriggers = true)
    queryToProgress.remove(event.runId)
  }
}

private[sql] class StreamingQueryData(
    val name: String,
    val id: UUID,
    @KVIndexParam val runId: UUID,
    @KVIndexParam("active") val isActive: Boolean,
    val exception: Option[String],
    @KVIndexParam("startTimestamp") val startTimestamp: Long,
    val endTimestamp: Option[Long] = None)

/**
 * This class contains all message related to UI display, each instance corresponds to a single
 * [[org.apache.spark.sql.streaming.StreamingQuery]].
 */
private[sql] case class StreamingQueryUIData(
    summary: StreamingQueryData,
    recentProgress: Array[StreamingQueryProgress]) {

  def lastProgress: StreamingQueryProgress = {
    if (recentProgress.nonEmpty) {
      recentProgress.last
    } else {
      null
    }
  }
}

private[sql] class StreamingQueryProgressWrapper(val progress: StreamingQueryProgress) {
  @JsonIgnore @KVIndex
  private val uniqueId: String = getUniqueId(progress.runId, progress.batchId, progress.timestamp)

  @JsonIgnore @KVIndex("runId")
  private def runIdIndex: String = progress.runId.toString
}

private[sql] object StreamingQueryProgressWrapper {
  /**
   * Adding `timestamp` into unique id to support reporting `empty` query progress
   * in which no data comes but with the same batchId.
   */
  def getUniqueId(
      runId: UUID,
      batchId: Long,
      timestamp: String): String = {
    s"${runId}_${batchId}_$timestamp"
  }
}
