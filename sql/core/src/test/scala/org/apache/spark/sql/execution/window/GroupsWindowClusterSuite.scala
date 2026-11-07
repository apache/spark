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

package org.apache.spark.sql.execution.window

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.ExtendedSQLTest

/**
 * Runs GROUPS window frames on a `local-cluster`, so frames are built, code-generated and fed
 * shuffled rows in executor JVMs separate from the driver. Each query is compared with a
 * DENSE_RANK + RANGE oracle computed on default settings, and the window metrics, which are
 * aggregated from the executors, assert which frame implementation actually ran. Without them
 * a path whose settings never reached the executors would still produce correct results.
 */
@ExtendedSQLTest
class GroupsWindowClusterSuite extends SparkFunSuite {

  private var spark: SparkSession = _

  // Every account is one window partition of 40 rows, spread over all input partitions.
  // Accounts differ in peer-group size (1 to 4 rows), key gaps, and whether they have a NULL
  // peer group, so a task processing several accounts must not carry group state across them.
  private val numAccounts = 16

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local-cluster[2,1,1024]")
      .appName("GroupsWindowClusterSuite")
      .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      .config(SQLConf.SHUFFLE_PARTITIONS.key, "4")
      .getOrCreate()
    spark.sql(
      s"""
         |SELECT account, id,
         |  CASE WHEN account % 5 = 0 AND pos % 13 = 0 THEN NULL
         |    ELSE CAST(pos DIV (account % 4 + 1) AS INT) * (account % 3 + 1) END AS batch,
         |  CAST(id * 7 % 101 AS INT) AS amount
         |FROM (
         |  SELECT id, id % $numAccounts AS account, id DIV $numAccounts AS pos
         |  FROM range(0, ${numAccounts * 40}, 1, 8)
         |)
         |""".stripMargin).createOrReplaceTempView("t")
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.stop()
        spark = null
      }
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    } finally {
      super.afterAll()
    }
  }

  private def withSessionConf[T](pairs: (String, String)*)(f: => T): T = {
    val previous = pairs.map { case (k, _) => k -> spark.conf.getOption(k) }
    pairs.foreach { case (k, v) => spark.conf.set(k, v) }
    try f finally {
      previous.foreach {
        case (k, Some(v)) => spark.conf.set(k, v)
        case (k, None) => spark.conf.unset(k)
      }
    }
  }

  private def windowQuery(functions: Seq[String], frame: String, orderBy: String,
      from: String): String = {
    val columns = functions.zipWithIndex.map { case (f, i) =>
      s"$f OVER (PARTITION BY account ORDER BY $orderBy $frame) AS c$i"
    }
    s"SELECT account, id, ${columns.mkString(", ")} FROM $from"
  }

  /**
   * Runs `functions` over `GROUPS <frame>` with `confs` set, checks the result against the
   * oracle, and returns the window metrics of that single run.
   */
  private def checkGroups(
      functions: Seq[String],
      frame: String,
      confs: (String, String)*): Map[String, Long] = {
    val oracleFrom =
      "(SELECT *, dense_rank() OVER (PARTITION BY account ORDER BY batch) AS peer_group FROM t)"
    val expected = spark.sql(
      windowQuery(functions, s"RANGE $frame", "peer_group", oracleFrom)).collect().toSeq

    withSessionConf(confs: _*) {
      val df = spark.sql(windowQuery(functions, s"GROUPS $frame", "batch", "t"))
      val actual = df.collect().toSeq
      QueryTest.sameRows(expected, actual).foreach { error =>
        fail(s"GROUPS $frame with ${confs.mkString(", ")}:\n$error")
      }
      val windows = df.queryExecution.executedPlan.collect { case w: WindowExec => w }
      assert(windows.size == 1, s"expected one Window node:\n${df.queryExecution.executedPlan}")
      windows.head.metrics.map { case (name, metric) => name -> metric.value }
    }
  }

  private val sumOnly = Seq("sum(amount)")
  private val minMax = Seq("min(amount)", "max(amount)")

  // Two-sided frames share one output cursor between their bounds; shrinking frames use a
  // single bound. Under 2 PRECEDING AND 1 PRECEDING the lower bound is not consulted until the
  // frame first becomes non-empty.
  private val movingFrames = Seq(
    "BETWEEN 2 PRECEDING AND 1 PRECEDING",
    "BETWEEN 1 PRECEDING AND 2 FOLLOWING",
    "BETWEEN CURRENT ROW AND CURRENT ROW")
  private val shrinkingFrame = "BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING"
  private val growingFrame = "BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING"

  private val segmentTreeOff = Seq(
    SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "false",
    SQLConf.WINDOW_MONOTONIC_DEQUE_ENABLED.key -> "false")

  test("GROUPS frames on the sliding and unbounded frame paths") {
    (movingFrames :+ shrinkingFrame :+ growingFrame).foreach { frame =>
      val metrics = checkGroups(sumOnly, frame, segmentTreeOff: _*)
      assert(metrics("numSegmentTreeFrames") == 0, s"$frame: $metrics")
      assert(metrics("numSegmentTreeFallbackFrames") == 0, s"$frame: $metrics")
    }
  }

  test("GROUPS frames on the segment-tree path") {
    (movingFrames :+ shrinkingFrame).foreach { frame =>
      val metrics = checkGroups(sumOnly, frame,
        SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
        SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "1")
      assert(metrics("numSegmentTreeFrames") == numAccounts, s"$frame: $metrics")
      assert(metrics("numSegmentTreeFallbackFrames") == 0, s"$frame: $metrics")
    }
  }

  // The partition-size threshold is read when the frame is prepared on the executor.
  test("GROUPS segment-tree fallback threshold is honoured on executors") {
    (movingFrames :+ shrinkingFrame).foreach { frame =>
      val metrics = checkGroups(sumOnly, frame,
        SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
        SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "1000")
      assert(metrics("numSegmentTreeFrames") == 0, s"$frame: $metrics")
      assert(metrics("numSegmentTreeFallbackFrames") == numAccounts, s"$frame: $metrics")
    }
  }

  test("GROUPS min/max frames on the monotonic-deque path") {
    movingFrames.foreach { frame =>
      val metrics = checkGroups(minMax, frame,
        SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "false",
        SQLConf.WINDOW_MONOTONIC_DEQUE_ENABLED.key -> "true")
      assert(metrics("numMonotonicDequeFrames") == numAccounts, s"$frame: $metrics")
    }
  }

  // Spilled iterators reuse a single row buffer, which the peer-group cursors must copy from.
  // The shrinking frame is only spilled on the segment-tree path: off that path,
  // UnboundedFollowingWindowFunctionFrame opens a fresh spill reader per output row, which
  // exhausts executor heap at this spill threshold for RANGE frames just the same.
  test("GROUPS frames over spilled partitions") {
    val spill = Seq(
      SQLConf.WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
      SQLConf.WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "8")
    val segmentTreeOn = Seq(
      SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
      SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "1")
    val cases = movingFrames.map(segmentTreeOff -> _) ++
      (movingFrames :+ shrinkingFrame).map(segmentTreeOn -> _)
    for ((path, frame) <- cases) {
      val metrics = checkGroups(sumOnly, frame, spill ++ path: _*)
      assert(metrics("spillSize") > 0, s"$frame with $path did not spill: $metrics")
    }
  }
}
