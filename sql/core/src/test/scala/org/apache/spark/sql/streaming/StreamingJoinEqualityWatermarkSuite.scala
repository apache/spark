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

import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
class StreamingJoinEqualityWatermarkSuite extends StreamTest {
  import testImplicits._

  private def withJoinConf(version: Int)(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false")(f)
  }

  // The watermarks are on different equality keys: la on the left and rb on the right. A
  // negative left b gives a NULL lb.
  private def crossedKeyJoin()
      : (MemoryStream[(Int, Int)], MemoryStream[(Int, Int)], DataFrame) = {
    val leftInput = MemoryStream[(Int, Int)]
    val rightInput = MemoryStream[(Int, Int)]
    val left = leftInput.toDF().toDF("a", "b")
      .select(timestamp_seconds($"a").as("la"),
        when($"b" >= 0, timestamp_seconds($"b")).as("lb"))
      .withWatermark("la", "10 seconds")
    val right = rightInput.toDF().toDF("a", "b")
      .select(timestamp_seconds($"a").as("ra"), timestamp_seconds($"b").as("rb"))
      .withWatermark("rb", "10 seconds")
    val joined = left.join(right, expr("la = ra AND lb = rb"), "full_outer")
      .select($"la".cast("int"), $"lb".cast("int"), $"ra".cast("int"), $"rb".cast("int"))
    (leftInput, rightInput, joined)
  }

  for {
    version <- Seq(2, 3, 4)
    restart <- Seq(false, true)
  } {
    test(s"equality eviction follows the opposite watermark, V$version restart=$restart") {
      withJoinConf(version) {
        val (leftInput, rightInput, joined) = crossedKeyJoin()
        withTempDir { checkpoint =>
          val restartActions = if (restart) {
            Seq(StopStream, StartStream(checkpointLocation = checkpoint.getCanonicalPath))
          } else {
            Nil
          }
          testStream(joined)((Seq(
            StartStream(checkpointLocation = checkpoint.getCanonicalPath),
            MultiAddData(leftInput, (5, 50))(rightInput, (80, 50)),
            CheckNewAnswer(),
            MultiAddData(leftInput, (60, 100))(rightInput, (70, 50)),
            CheckNewAnswer(),
            AddData(leftInput, (70, 100)),
            // At watermark 40, left (5, 50) can still match fresh right input on rb = 50.
            CheckNewAnswer()) ++ restartActions ++ Seq(
            AddData(rightInput, (5, 50)),
            CheckNewAnswer(Row(5, 50, 5, 50)),
            // Fresh on la, but already expired on lb: V4 must visit it below the old cutoff.
            AddData(leftInput, (90, 10)),
            CheckNewAnswer(Row(90, 10, null, null)),
            MultiAddData(leftInput, (200, 200))(rightInput, (200, 200)),
            CheckNewAnswer(Row(200, 200, 200, 200)),
            MultiAddData(leftInput, (210, 210))(rightInput, (210, 210)),
            CheckNewAnswer(Row(210, 210, 210, 210), Row(60, 100, null, null),
              Row(70, 100, null, null), Row(null, null, 80, 50), Row(null, null, 70, 50))
          )): _*)
        }
      }
    }
  }

  // The legacy mode evicts both inputs by la and ra, as earlier versions did.
  for (version <- Seq(2, 4)) {
    test(s"equality eviction in the legacy mode keeps the earlier behavior, V$version") {
      withJoinConf(version) {
        withSQLConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> "false") {
          val (leftInput, rightInput, joined) = crossedKeyJoin()
          testStream(joined)(
            MultiAddData(leftInput, (5, 50))(rightInput, (80, 50)),
            CheckNewAnswer(),
            MultiAddData(leftInput, (60, 100))(rightInput, (70, 50)),
            CheckNewAnswer(),
            AddData(leftInput, (70, 100)),
            CheckNewAnswer(Row(5, 50, null, null)),
            AddData(rightInput, (5, 50)),
            CheckNewAnswer(Row(null, null, 5, 50)),
            MultiAddData(leftInput, (200, 200))(rightInput, (200, 200)),
            CheckNewAnswer(Row(200, 200, 200, 200)))
        }
      }
    }
  }

  // An earlier version, like the legacy mode, stored a row whose lb is NULL and evicted it by la.
  for (version <- Seq(2, 3)) {
    test(s"a stored row whose eviction key is NULL is evicted, V$version") {
      withJoinConf(version) {
        val (leftInput, rightInput, joined) = crossedKeyJoin()
        withTempDir { checkpoint =>
          withSQLConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> "false") {
            testStream(joined)(
              StartStream(checkpointLocation = checkpoint.getCanonicalPath),
              MultiAddData(leftInput, (5, -1))(rightInput, (80, 50)),
              CheckNewAnswer(),
              MultiAddData(leftInput, (60, 100))(rightInput, (70, 50)),
              CheckNewAnswer(),
              StopStream)
          }
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint.getCanonicalPath),
            AddData(leftInput, (70, 100)),
            CheckNewAnswer(Row(5, null, null, null)))
        }
      }
    }
  }

  for (version <- Seq(2, 4)) {
    test(s"a row whose eviction key is NULL is emitted on arrival, V$version") {
      withJoinConf(version) {
        val (leftInput, rightInput, joined) = crossedKeyJoin()
        testStream(joined)(
          // The row can never match, so it is not stored, although the next eviction would remove
          // it. Only the two rows with key 6 are stored.
          MultiAddData(leftInput, (5, -1), (6, 60))(rightInput, (6, 60)),
          CheckNewAnswer(Row(5, null, null, null), Row(6, 60, 6, 60)),
          AssertOnQuery { q =>
            q.lastProgress.stateOperators.head.numRowsUpdated == 2
          })
      }
    }
  }

  // A window aggregation on la after a join on la = ra AND lb = rb, or on la = ra only.
  private def countJoined(crossed: Boolean): (MemoryStream[(Int, Int)], DataFrame) = {
    val leftInput = MemoryStream[(Int, Int)]
    val rightInput = MemoryStream[(Int, Int)]
    val left = leftInput.toDF().toDF("a", "b")
      .select(timestamp_seconds($"a").as("la"), timestamp_seconds($"b").as("lb"))
      .withWatermark("la", "10 seconds")
    val right = rightInput.toDF().toDF("a", "b")
      .select(timestamp_seconds($"a").as("ra"), timestamp_seconds($"b").as("rb"))
      .withWatermark(if (crossed) "rb" else "ra", "10 seconds")
    val condition = if (crossed) "la = ra AND lb = rb" else "la = ra"
    val result = left.join(right, expr(condition))
      .groupBy(window($"la", "10 seconds")).count()
      .select($"window.start".cast("long"), $"count")
    (leftInput, result)
  }

  // The left input is evicted by lb, so its state keeps rows of any la.
  for (version <- Seq(2, 4)) {
    test(s"reject a window aggregation after a join with crossed watermarked keys, V$version") {
      withJoinConf(version) {
        val (leftInput, result) = countJoined(crossed = true)
        testStream(result)(
          AddData(leftInput, (1, 5)),
          ExpectFailure[AnalysisException](e => assert(e.getMessage.contains(
            "the state of the left input of a stream-stream join has no cutoff")),
            typeIsSuperClass = true))
      }
    }

    test(s"accept a window aggregation after a join on one watermarked key, V$version") {
      withJoinConf(version) {
        val (leftInput, result) = countJoined(crossed = false)
        testStream(result)(AddData(leftInput, (1, 5)), CheckNewAnswer())
      }
    }
  }
}
