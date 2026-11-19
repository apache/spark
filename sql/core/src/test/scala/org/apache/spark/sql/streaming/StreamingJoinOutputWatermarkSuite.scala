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

import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.Level

import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeq, OffsetSeqLog}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

/**
 * A stateful operator after a stream-stream join whose state of an input has no cutoff. The join
 * forwards its input watermark, so the operator would drop the old rows that the join emits.
 */
class StreamingJoinOutputWatermarkSuite extends StreamTest {
  import testImplicits._

  private def withJoinConf(version: Int)(f: => Unit): Unit = withSQLConf(
    SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
    SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
    SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false")(f)

  private def input(side: String): (MemoryStream[(Int, Long)], DataFrame) = {
    val stream = MemoryStream[(Int, Long)]
    (stream, stream.toDF().toDF(s"${side}Key", "seconds")
      .select(col(s"${side}Key"), timestamp_seconds($"seconds").as(s"${side}Time"))
      .withWatermark(s"${side}Time", "10 seconds"))
  }

  // A window count over the join of the two inputs.
  private def countJoined(condition: String, time: String)
      : (MemoryStream[(Int, Long)], MemoryStream[(Int, Long)], DataFrame) = {
    val (leftInput, left) = input("left")
    val (rightInput, right) = input("right")
    val result = left.join(right, expr(condition))
      .groupBy(window(col(time), "10 seconds")).count()
      .select($"window.start".cast("long"), $"count")
    (leftInput, rightInput, result)
  }

  private val keyOnly = "leftKey = rightKey"

  private def expectRejected(side: String): StreamAction = ExpectFailure[AnalysisException](
    e => assert(e.getMessage.contains(
      s"the state of the $side input of a stream-stream join has no cutoff")),
    typeIsSuperClass = true)

  for (version <- Seq(2, 4)) {
    test(s"reject a key-only join before a window aggregation, V$version") {
      withJoinConf(version) {
        val (leftInput, _, result) = countJoined(keyOnly, "leftTime")
        testStream(result)(AddData(leftInput, (1, 5L)), expectRejected("left"))
      }
    }

    test(s"reject a range join bounded on one side only before a window aggregation, V$version") {
      withJoinConf(version) {
        val (leftInput, _, result) = countJoined(
          "leftKey = rightKey AND rightTime <= leftTime + INTERVAL 10 SECONDS", "rightTime")
        testStream(result)(AddData(leftInput, (1, 5L)), expectRejected("right"))
      }
    }

    test(s"accept a time-interval join before a window aggregation, V$version") {
      withJoinConf(version) {
        val (leftInput, rightInput, result) = countJoined(
          "leftKey = rightKey AND rightTime BETWEEN leftTime AND leftTime + INTERVAL 10 SECONDS",
          "leftTime")
        testStream(result)(
          MultiAddData(leftInput, (1, 5L))(rightInput, (1, 8L)), CheckNewAnswer(),
          MultiAddData(leftInput, (2, 40L))(rightInput, (2, 40L)), CheckNewAnswer(),
          AddData(rightInput, (2, 41L)), CheckNewAnswer((0L, 1L)))
      }
    }

    test(s"accept a window-key join before a window aggregation, V$version") {
      withJoinConf(version) {
        val (leftInput, left) = input("left")
        val (rightInput, right) = input("right")
        val result = left.select($"leftKey".as("key"), window($"leftTime", "10 seconds"))
          .join(right.select($"rightKey".as("key"), window($"rightTime", "10 seconds")),
            Seq("key", "window"))
          .groupBy("window").count()
          .select($"window.start".cast("long"), $"count")
        testStream(result)(
          MultiAddData(leftInput, (1, 5L))(rightInput, (1, 8L)), CheckNewAnswer(),
          MultiAddData(leftInput, (2, 40L))(rightInput, (2, 40L)), CheckNewAnswer(),
          AddData(rightInput, (2, 41L)), CheckNewAnswer((0L, 1L)))
      }
    }
  }

  test("a key-only join before a window aggregation only warns with the check disabled") {
    withJoinConf(4) {
      withSQLConf(SQLConf.STATEFUL_OPERATOR_CHECK_CORRECTNESS_ENABLED.key -> "false") {
        val (leftInput, rightInput, result) = countJoined(keyOnly, "leftTime")
        val logAppender = new LogAppender("join output watermark")
        withLogAppender(logAppender, level = Some(Level.WARN)) {
          testStream(result)(
            MultiAddData(leftInput, (1, 5L))(rightInput, (1, 5L)), CheckNewAnswer())
        }
        assert(logAppender.loggingEvents.exists(_.getMessage.getFormattedMessage.contains(
          "the state of the left input of a stream-stream join has no cutoff")))
      }
    }
  }

  test("a key-only join before a window aggregation keeps running from an older checkpoint") {
    withJoinConf(4) {
      withTempDir { checkpoint =>
        val (leftInput, rightInput, result) = countJoined(keyOnly, "leftTime")
        val start = StartStream(checkpointLocation = checkpoint.getCanonicalPath)
        withSQLConf(SQLConf.STATEFUL_OPERATOR_CHECK_CORRECTNESS_ENABLED.key -> "false") {
          testStream(result)(start,
            MultiAddData(leftInput, (1, 5L))(rightInput, (1, 5L)), CheckNewAnswer(), StopStream)
        }
        removeCheckJoinOutputWatermark(checkpoint)
        testStream(result)(start,
          MultiAddData(leftInput, (2, 40L))(rightInput, (2, 40L)), CheckNewAnswer())
      }
    }
  }

  // Removes the conf from the latest offset log entry, as versions without it wrote it.
  private def removeCheckJoinOutputWatermark(checkpoint: File): Unit = {
    val key = SQLConf.STATEFUL_OPERATOR_CHECK_JOIN_OUTPUT_WATERMARK.key
    val offsetsPath = new File(checkpoint, "offsets").getCanonicalPath
    val log = new OffsetSeqLog(spark, offsetsPath)
    val (batchId, offsets) = log.getLatest().get
    val updated = offsets match {
      case o: OffsetSeq =>
        assert(o.metadataOpt.exists(_.conf.contains(key)))
        o.copy(metadataOpt = o.metadataOpt.map(m => m.copy(conf = m.conf - key)))
      case o: OffsetMap =>
        assert(o.metadata.conf.contains(key))
        o.copy(metadata = o.metadata.copy(conf = o.metadata.conf - key))
    }
    val path = new Path(offsetsPath, batchId.toString)
    assert(path.getFileSystem(spark.sessionState.newHadoopConf()).delete(path, false))
    assert(log.add(batchId, updated))
  }
}
