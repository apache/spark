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

package org.apache.spark.sql.execution.ui

import java.util.UUID

import org.apache.spark.sql.streaming.ui.{StreamingQueryData, StreamingQueryProgressWrapper, StreamingQueryUIData}
import org.apache.spark.status.KVUtils
import org.apache.spark.util.kvstore.KVStore

/**
 * Provides a view of a KVStore with methods that make it easy to query Streaming Query state.
 * There's no state kept in this class, so it's ok to have multiple instances of it in an
 * application.
 */
class StreamingQueryStatusStore(store: KVStore) {

  def allQueryUIData: Seq[StreamingQueryUIData] = {
    val view = store.view(classOf[StreamingQueryData]).index("startTimestamp").first(0L)
    KVUtils.viewToSeq(view, Int.MaxValue)(_ => true).map(makeUIData)
  }

  // visible for test
  private[sql] def getQueryProgressData(runId: UUID): Seq[StreamingQueryProgressWrapper] = {
    val view = store.view(classOf[StreamingQueryProgressWrapper])
      .index("runId").first(runId.toString).last(runId.toString)
    KVUtils.viewToSeq(view, Int.MaxValue)(_ => true)
  }

  private def makeUIData(summary: StreamingQueryData): StreamingQueryUIData = {
    val runId = summary.runId
    val view = store.view(classOf[StreamingQueryProgressWrapper])
      .index("runId").first(runId).last(runId)
    val recentProgress = KVUtils.viewToSeq(view, Int.MaxValue)(_ => true)
      .map(_.progress).sortBy(_.timestamp).toArray
    StreamingQueryUIData(summary, recentProgress)
  }
}
