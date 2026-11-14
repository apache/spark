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
import java.nio.file.Files

import scala.jdk.CollectionConverters._
import scala.util.Using

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeq, OffsetSeqLog}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
class StreamingJoinRangeEvictionCheckpointSuite extends StreamTest {
  import testImplicits._

  private val markerKey = "spark.sql.streaming.join.checkpointWatermarkSemanticsVersion"
  private val currentMarker =
    SQLConf.STREAMING_JOIN_CHECKPOINT_WATERMARK_SEMANTICS_VERSION.defaultValue.get.toString

  private def withJoinConf(version: Int, logVersion: Int)(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
      SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> logVersion.toString,
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false")(f)
  }

  private def setMarker(checkpoint: String, marker: Option[String]): Unit = {
    def update(conf: Map[String, String]): Map[String, String] = {
      (conf - markerKey) ++ marker.map(markerKey -> _)
    }
    val log = new OffsetSeqLog(spark, checkpoint + "/offsets")
    val (batchId, offsets) = log.getLatest().get
    val updated = offsets match {
      case o: OffsetSeq =>
        o.copy(metadataOpt = o.metadataOpt.map(m => m.copy(conf = update(m.conf))))
      case o: OffsetMap =>
        o.copy(metadata = o.metadata.copy(conf = update(o.metadata.conf)))
    }
    val path = new Path(checkpoint + "/offsets", batchId.toString)
    assert(path.getFileSystem(spark.sessionState.newHadoopConf()).delete(path, false))
    assert(log.add(batchId, updated))
  }

  private def snapshot(directory: File): Map[String, Seq[Byte]] = {
    Using.resource(Files.walk(directory.toPath)) { paths =>
      paths.iterator().asScala.filter(Files.isRegularFile(_)).map { path =>
        directory.toPath.relativize(path).toString -> Files.readAllBytes(path).toSeq
      }.toMap
    }
  }

  private def inputs(expiryBound: Boolean):
      (MemoryStream[(Int, Long, Long)], MemoryStream[(Int, Long)], DataFrame) = {
    val leftInput = MemoryStream[(Int, Long, Long)]
    val rightInput = MemoryStream[(Int, Long)]
    val left = leftInput.toDF().toDF("leftKey", "start", "end")
      .select($"leftKey", timestamp_seconds($"start").as("leftTime"),
        timestamp_seconds($"end").as("expiry"))
      .withWatermark("leftTime", "60 seconds")
    val right = rightInput.toDF().toDF("rightKey", "time")
      .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
      .withWatermark("rightTime", "60 seconds")
    val upper = if (expiryBound) "expiry" else "leftTime + INTERVAL 10 MINUTES"
    val joined = left.join(right,
      expr(s"leftKey = rightKey AND rightTime BETWEEN leftTime AND $upper"), "left_outer")
      .select($"leftKey", $"rightTime".cast("long"), $"leftTime".cast("long"))
    (leftInput, rightInput, joined)
  }

  private def inputsWithNumericExpiry(useEventTimeLowerBound: Boolean):
      (MemoryStream[(Int, Long, Long)], MemoryStream[(Int, Long)], DataFrame) = {
    val leftInput = MemoryStream[(Int, Long, Long)]
    val rightInput = MemoryStream[(Int, Long)]
    val left = leftInput.toDF().toDF("leftKey", "start", "end")
      .select($"leftKey", timestamp_seconds($"start").as("leftTime"), $"end".as("expiry"))
      .withWatermark("leftTime", "60 seconds")
    val right = rightInput.toDF().toDF("rightKey", "time")
      .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
      .withWatermark("rightTime", "60 seconds")
    val timeRange = if (useEventTimeLowerBound) {
      "rightTime BETWEEN leftTime AND CAST(expiry AS TIMESTAMP)"
    } else {
      "rightTime <= CAST(expiry AS TIMESTAMP)"
    }
    val joined = left.join(right, expr(s"leftKey = rightKey AND $timeRange"), "inner")
      .select($"leftKey", $"rightTime".cast("long"), $"leftTime".cast("long"))
    (leftInput, rightInput, joined)
  }

  for {
    version <- Seq(2, 3, 4)
    logVersion <- Seq(1, 2)
    // Every marker below the current one takes the same rejection branch.
    oldMarker <- Seq(None, Some("0"), Some((currentMarker.toInt - 1).toString)).distinct
  } {
    test(s"reject old range-column checkpoint, V$version log=$logVersion marker=$oldMarker") {
      withJoinConf(version, logVersion) {
        val (leftInput, rightInput, joined) = inputs(expiryBound = true)
        withTempDir { checkpointDir =>
          val checkpoint = checkpointDir.getCanonicalPath
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint),
            StreamProgressLockedActions(Seq(
              AddData(leftInput, (1, 100L, 700L)), AddData(rightInput, (1, 100L)))),
            CheckNewAnswer(Row(1, 100L, 100L)), StopStream)
          // Only offset metadata is historical. Rejection must precede opening the state.
          setMarker(checkpoint, oldMarker)
          val stateBefore = snapshot(new File(checkpoint, "state"))
          val commitsBefore = snapshot(new File(checkpoint, "commits"))
          withSQLConf(markerKey -> currentMarker) {
            testStream(joined)(
              StartStream(checkpointLocation = checkpoint),
              AddData(rightInput, (9, 400L)),
              ExpectFailure[AnalysisException](assertFailure = error => {
                checkError(error.asInstanceOf[AnalysisException],
                  condition = "STREAMING_JOIN_CHECKPOINT_INCOMPATIBLE",
                  parameters = Map(
                    "joinCondition" -> ".*leftKey.*rightKey.*",
                    "leftWatermarkColumns" -> "leftTime",
                    "rightWatermarkColumns" -> "rightTime"),
                  matchPVals = true)
              }))
          }
          assert(snapshot(new File(checkpoint, "state")) == stateBefore)
          assert(snapshot(new File(checkpoint, "commits")) == commitsBefore)
        }
      }
    }
  }

  for {
    version <- Seq(2, 3, 4)
    logVersion <- Seq(1, 2)
    oldMarker <- Seq(None, Some("0"), Some((currentMarker.toInt - 1).toString)).distinct
    useEventTimeLowerBound <- Seq(false, true)
  } {
    val boundDescription = if (useEventTimeLowerBound) "with event-time bound" else "only"
    test(s"reject old numeric-expiry $boundDescription checkpoint, " +
        s"V$version log=$logVersion marker=$oldMarker") {
      withJoinConf(version, logVersion) {
        val (leftInput, rightInput, joined) =
          inputsWithNumericExpiry(useEventTimeLowerBound)
        withTempDir { checkpointDir =>
          val checkpoint = checkpointDir.getCanonicalPath
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint),
            StreamProgressLockedActions(Seq(
              AddData(leftInput, (1, 100L, 700L)), AddData(rightInput, (1, 100L)))),
            CheckNewAnswer(Row(1, 100L, 100L)), StopStream)
          val offsetLog = new OffsetSeqLog(spark, checkpoint + "/offsets")
          val pinned = offsetLog.getLatest().get._2.metadataOpt.get.conf.get(markerKey)
          assert(pinned.contains(currentMarker))
          setMarker(checkpoint, oldMarker)
          val stateBefore = snapshot(new File(checkpoint, "state"))
          val commitsBefore = snapshot(new File(checkpoint, "commits"))
          withSQLConf(markerKey -> currentMarker) {
            testStream(joined)(
              StartStream(checkpointLocation = checkpoint),
              AddData(rightInput, (9, 400L)),
              ExpectFailure[AnalysisException](assertFailure = error => {
                checkError(error.asInstanceOf[AnalysisException],
                  condition = "STREAMING_JOIN_CHECKPOINT_INCOMPATIBLE",
                  parameters = Map(
                    "joinCondition" -> ".*leftKey.*rightKey.*",
                    "leftWatermarkColumns" -> "leftTime",
                    "rightWatermarkColumns" -> "rightTime"),
                  matchPVals = true)
              }))
          }
          assert(snapshot(new File(checkpoint, "state")) == stateBefore)
          assert(snapshot(new File(checkpoint, "commits")) == commitsBefore)
        }
      }
    }
  }

  for {
    version <- Seq(2, 3, 4)
    logVersion <- Seq(1, 2)
    expiryBound <- Seq(false, true)
  } {
    test(s"resume range checkpoint, V$version log=$logVersion expiry=$expiryBound") {
      withJoinConf(version, logVersion) {
        val (leftInput, rightInput, joined) = inputs(expiryBound)
        withTempDir { checkpointDir =>
          val checkpoint = checkpointDir.getCanonicalPath
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint),
            StreamProgressLockedActions(Seq(
              AddData(leftInput, (1, 100L, 700L)), AddData(rightInput, (9, 100L)))),
            CheckNewAnswer(),
            StreamProgressLockedActions(Seq(
              AddData(leftInput, (8, 400L, 1000L)), AddData(rightInput, (9, 400L)))),
            CheckNewAnswer(),
            AddData(rightInput, (9, 400L)), CheckNewAnswer(), StopStream)
          val offsetLog = new OffsetSeqLog(spark, checkpoint + "/offsets")
          val pinned = offsetLog.getLatest().get._2.metadataOpt.get.conf.get(markerKey)
          assert(pinned.contains(currentMarker))
          // The checkpoint value must override a changed session default on restart.
          withSQLConf(markerKey -> "0") {
            testStream(joined)(
              StartStream(checkpointLocation = checkpoint),
              AddData(rightInput, (1, 460L)), CheckNewAnswer(Row(1, 460L, 100L)), StopStream)
          }
        }
      }
    }
  }

  for {
    version <- Seq(2, 3, 4)
    logVersion <- Seq(1, 2)
  } {
    test(s"resume current numeric-expiry checkpoint, V$version log=$logVersion") {
      withJoinConf(version, logVersion) {
        val (leftInput, rightInput, joined) =
          inputsWithNumericExpiry(useEventTimeLowerBound = true)
        withTempDir { checkpointDir =>
          val checkpoint = checkpointDir.getCanonicalPath
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint),
            StreamProgressLockedActions(Seq(
              AddData(leftInput, (1, 100L, 700L)), AddData(rightInput, (9, 100L)))),
            CheckNewAnswer(),
            StreamProgressLockedActions(Seq(
              AddData(leftInput, (8, 400L, 1000L)), AddData(rightInput, (9, 400L)))),
            CheckNewAnswer(),
            AddData(rightInput, (9, 400L)), CheckNewAnswer(), StopStream)
          val offsetLog = new OffsetSeqLog(spark, checkpoint + "/offsets")
          assert(offsetLog.getLatest().get._2.metadataOpt.get.conf.get(markerKey)
            .contains(currentMarker))
          withSQLConf(markerKey -> "0") {
            testStream(joined)(
              StartStream(checkpointLocation = checkpoint),
              AddData(rightInput, (1, 460L)), CheckNewAnswer(Row(1, 460L, 100L)), StopStream)
          }
        }
      }
    }
  }

  for {
    version <- Seq(2, 3, 4)
    logVersion <- Seq(1, 2)
  } {
    test(s"reject old range-column checkpoint retrying batch zero, V$version log=$logVersion") {
      withJoinConf(version, logVersion) {
        val (leftInput, rightInput, joined) = inputs(expiryBound = true)
        withTempDir { checkpointDir =>
          val checkpoint = checkpointDir.getCanonicalPath
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint),
            StreamProgressLockedActions(Seq(
              AddData(leftInput, (1, 100L, 700L)), AddData(rightInput, (1, 100L)))),
            CheckNewAnswer(Row(1, 100L, 100L)), StopStream)
          val offsetLog = new OffsetSeqLog(spark, checkpoint + "/offsets")
          assert(offsetLog.getLatest().get._1 == 0L)
          setMarker(checkpoint, None)
          val commit = new Path(checkpoint + "/commits/0")
          assert(commit.getFileSystem(spark.sessionState.newHadoopConf()).delete(commit, false))
          val stateBefore = snapshot(new File(checkpoint, "state"))
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint),
            ExpectFailure[AnalysisException](assertFailure = error => {
              checkError(error.asInstanceOf[AnalysisException],
                condition = "STREAMING_JOIN_CHECKPOINT_INCOMPATIBLE",
                parameters = Map(
                  "joinCondition" -> ".*leftKey.*rightKey.*",
                  "leftWatermarkColumns" -> "leftTime",
                  "rightWatermarkColumns" -> "rightTime"),
                matchPVals = true)
            }))
          assert(snapshot(new File(checkpoint, "state")) == stateBefore)
        }
      }
    }
  }
}
