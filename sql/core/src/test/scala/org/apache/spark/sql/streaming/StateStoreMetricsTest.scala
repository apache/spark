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

import org.apache.spark.sql.execution.streaming.StreamExecution

trait StateStoreMetricsTest extends StreamTest {

  private var lastCheckedRecentProgressIndex = -1
  private var lastQuery: StreamExecution = null

  override def beforeEach(): Unit = {
    super.beforeEach()
    lastCheckedRecentProgressIndex = -1
  }

  def assertNumStateRows(
      total: Seq[Long],
      updated: Seq[Long],
      droppedByWatermark: Seq[Long]): AssertOnQuery =
    AssertOnQuery(s"Check total state rows = $total, updated state rows = $updated" +
      s", rows dropped by watermark = $droppedByWatermark") { q =>
      // This assumes that the streaming query will not make any progress while the eventually
      // is being executed.
      eventually(timeout(streamingTimeout)) {
        val recentProgress = q.recentProgress
        require(recentProgress.nonEmpty, "No progress made, cannot check num state rows")
        require(recentProgress.length < spark.sessionState.conf.streamingProgressRetention,
          "This test assumes that all progresses are present in q.recentProgress but " +
            "some may have been dropped due to retention limits")

        if (q.ne(lastQuery)) lastCheckedRecentProgressIndex = -1
        lastQuery = q

        val numStateOperators = recentProgress.last.stateOperators.length
        val progressesSinceLastCheck = recentProgress
          .slice(lastCheckedRecentProgressIndex + 1, recentProgress.length)
          .filter(_.stateOperators.length == numStateOperators)

        val allNumUpdatedRowsSinceLastCheck =
          progressesSinceLastCheck.map(_.stateOperators.map(_.numRowsUpdated))

        val allNumRowsDroppedByWatermarkSinceLastCheck =
          progressesSinceLastCheck.map(_.stateOperators.map(_.numRowsDroppedByWatermark))

        lazy val debugString = "recent progresses:\n" +
          progressesSinceLastCheck.map(_.prettyJson).mkString("\n\n")

        val numTotalRows = recentProgress.last.stateOperators.map(_.numRowsTotal)
        assert(numTotalRows === total, s"incorrect total rows, $debugString")

        val numUpdatedRows = arraySum(allNumUpdatedRowsSinceLastCheck, numStateOperators)
        assert(numUpdatedRows === updated, s"incorrect updates rows, $debugString")

        val numRowsDroppedByWatermark = arraySum(allNumRowsDroppedByWatermarkSinceLastCheck,
          numStateOperators)
        assert(numRowsDroppedByWatermark === droppedByWatermark,
          s"incorrect dropped rows by watermark, $debugString")

        lastCheckedRecentProgressIndex = recentProgress.length - 1
      }
      true
    }

  def assertNumStateRows(total: Seq[Long], updated: Seq[Long]): AssertOnQuery = {
    assert(total.length === updated.length)
    assertNumStateRows(total, updated, droppedByWatermark = (0 until total.length).map(_ => 0L))
  }

  def assertNumStateRows(
      total: Long,
      updated: Long,
      droppedByWatermark: Long = 0): AssertOnQuery = {
    assertNumStateRows(Seq(total), Seq(updated), Seq(droppedByWatermark))
  }

  def arraySum(arraySeq: Seq[Array[Long]], arrayLength: Int): Seq[Long] = {
    if (arraySeq.isEmpty) return Seq.fill(arrayLength)(0L)

    assert(arraySeq.forall(_.length == arrayLength),
      "Arrays are of different lengths:\n" + arraySeq.map(_.toSeq).mkString("\n"))
    (0 until arrayLength).map { index => arraySeq.map(_.apply(index)).sum }
  }
}
