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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeq, OffsetSeqLog}
import org.apache.spark.sql.execution.streaming.operators.stateful.join.{StreamingSymmetricHashJoinExec, StreamingSymmetricHashJoinHelper}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StructField, StructType,
  TimestampType}
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.util.Utils

@SlowSQLTest
class StreamingJoinCheckpointCompatibilitySuite extends StreamTest {
  import testImplicits._

  private val markerKey = SQLConf.STREAMING_JOIN_CHECKPOINT_WATERMARK_SEMANTICS_VERSION.key
  private val currentMarker =
    SQLConf.STREAMING_JOIN_CHECKPOINT_WATERMARK_SEMANTICS_VERSION.defaultValue.get.toString

  private def setMarker(checkpoint: String, marker: Option[String]): Unit = {
    def update(conf: Map[String, String]): Map[String, String] = {
      (conf - markerKey) ++ marker.map(markerKey -> _)
    }
    val offsetsPath = checkpoint + "/offsets"
    val log = new OffsetSeqLog(spark, offsetsPath)
    val (batchId, offsets) = log.getLatest().get
    val updated = offsets match {
      case o: OffsetSeq =>
        o.copy(metadataOpt = o.metadataOpt.map(m => m.copy(conf = update(m.conf))))
      case o: OffsetMap =>
        o.copy(metadata = o.metadata.copy(conf = update(o.metadata.conf)))
    }
    val path = new Path(offsetsPath, batchId.toString)
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

  private def pinnedMarker(checkpoint: String): Option[String] = {
    new OffsetSeqLog(spark, checkpoint + "/offsets").getLatest().get._2.metadataOpt.get
      .conf.get(markerKey)
  }

  private def markerAt(checkpoint: String, batchId: Long): Option[String] = {
    new OffsetSeqLog(spark, checkpoint + "/offsets").get(batchId).get.metadataOpt.get
      .conf.get(markerKey)
  }

  private def withJoinConf(version: Int, logVersion: Int)(f: => Unit): Unit = {
    withSQLConf(
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
        SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> logVersion.toString,
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
        SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false")(f)
  }

  private def assertNonNullableOutput(dataFrame: DataFrame, columnNames: String*): Unit = {
    val outputByName = dataFrame.queryExecution.analyzed.output.map(a => a.name -> a).toMap
    columnNames.foreach { columnName =>
      val attribute = outputByName(columnName)
      assert(!attribute.nullable, s"Expected $columnName to be non-null in $attribute")
    }
  }

  private def joinInputs(shape: String): (MemoryStream[(Int, Int)],
      MemoryStream[(Int, Int)], DataFrame) = {
    val leftInput = MemoryStream[(Int, Int)]
    val rightInput = MemoryStream[(Int, Int)]
    val leftData = leftInput.toDF().toDF("leftKey", "time")
      .select($"leftKey", timestamp_seconds($"time").as("leftTime"),
        timestamp_seconds($"time" + 10).as("expiry"))
    val rightData = rightInput.toDF().toDF("rightKey", "time")
      .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
    val left = if (Seq("equality", "expiry", "pruned expiry", "unbounded")
        .contains(shape)) {
      leftData.withWatermark("leftTime", "100 seconds")
    } else {
      leftData
    }
    val right = if (shape != "no watermark") {
      rightData.withWatermark("rightTime", "100 seconds")
    } else {
      rightData
    }
    val condition = shape match {
      case "range" => "leftKey = rightKey AND leftTime > rightTime - INTERVAL 5 SECONDS"
      case "expiry" => "leftKey = rightKey AND rightTime BETWEEN leftTime AND expiry"
      case "pruned expiry" => "leftKey = rightKey AND rightTime <= expiry"
      case "unbounded" => "leftKey = rightKey"
      case _ => "leftKey = rightKey AND leftTime = rightTime"
    }
    val joined = left.join(right, expr(condition))
    val result = if (shape == "unbounded") {
      joined.select($"leftKey", $"rightKey", $"leftTime".cast("long"),
        $"rightTime".cast("long"))
    } else {
      joined.select($"leftKey", $"rightKey")
    }
    (leftInput, rightInput, result)
  }

  private def nullableAlignedJoinInputs(): (MemoryStream[Int], MemoryStream[Int], DataFrame) = {
    val leftInput = MemoryStream[Int]
    val rightInput = MemoryStream[Int]
    val left = leftInput.toDF().toDF("value")
      .select(when($"value" < 0, lit(null).cast(TimestampType))
        .otherwise(timestamp_seconds($"value")).as("leftTime"))
      .withWatermark("leftTime", "100 seconds")
    val right = rightInput.toDF().toDF("value")
      .select(timestamp_seconds($"value").as("rightTime"))
      .withWatermark("rightTime", "100 seconds")
    val joined = left.join(right, expr("leftTime = rightTime"), "full_outer")
      .select($"leftTime".cast("long"), $"rightTime".cast("long"))
    (leftInput, rightInput, joined)
  }

  private def nonNullableAlignedJoinInputs(): (MemoryStream[Long], MemoryStream[Long],
      DataFrame) = {
    val leftInput = MemoryStream[Long]
    val rightInput = MemoryStream[Long]
    val left = leftInput.toDF().toDF("value")
      .select(coalesce(timestamp_seconds($"value"), lit(new java.sql.Timestamp(0L)))
        .as("leftTime"))
      .withWatermark("leftTime", "100 seconds")
    val right = rightInput.toDF().toDF("value")
      .select(coalesce(timestamp_seconds($"value"), lit(new java.sql.Timestamp(0L)))
        .as("rightTime"))
      .withWatermark("rightTime", "100 seconds")
    assertNonNullableOutput(left, "leftTime")
    assertNonNullableOutput(right, "rightTime")
    val joined = left.join(right, expr("leftTime = rightTime"), "full_outer")
      .select($"leftTime".cast("long"), $"rightTime".cast("long"))
    (leftInput, rightInput, joined)
  }

  test("checkpoint compatibility requires one aligned own-event-time equality key") {
    val watermarkMetadata = new MetadataBuilder()
      .putLong(EventTimeWatermark.delayKey, 0L).build()
    val leftId = AttributeReference("leftId", IntegerType)()
    val rightId = AttributeReference("rightId", IntegerType)()
    val leftTime = AttributeReference("leftTime", TimestampType,
      metadata = watermarkMetadata)()
    val rightTime = AttributeReference("rightTime", TimestampType,
      metadata = watermarkMetadata)()
    val compatible = StreamingSymmetricHashJoinHelper.isWatermarkCheckpointCompatible(
      _: Seq[Attribute], _: Seq[Attribute], _: Seq[Expression], _: Seq[Expression], 3)

    assert(compatible(Seq(leftId), Seq(rightId), Seq(leftId), Seq(rightId)))
    assert(compatible(Seq(leftId, leftTime), Seq(rightId, rightTime),
      Seq(leftId, leftTime), Seq(rightId, rightTime)))
    assert(!compatible(Seq(leftId, leftTime), Seq(rightId, rightTime),
      Seq(leftId, leftTime), Seq(rightTime, rightId)))
    assert(!compatible(Seq(leftId, leftTime), Seq(rightId, rightTime),
      Seq(leftId, leftTime, leftTime), Seq(rightId, rightTime, rightTime)))
    assert(!compatible(Seq(leftId, leftTime), Seq(rightId, rightTime),
      Seq(leftId), Seq(rightId)))
    assert(!compatible(Seq(leftId, leftTime), Seq(rightId),
      Seq(leftId, leftTime), Seq(rightId, rightId)))
    assert(!compatible(Seq(leftId), Seq(rightId, rightTime),
      Seq(leftId, leftId), Seq(rightId, rightTime)))
    assert(!compatible(Seq(leftTime, rightTime), Seq(rightTime),
      Seq(leftTime), Seq(rightTime)))
    assert(!compatible(Seq(leftTime), Seq(rightTime, leftTime),
      Seq(leftTime), Seq(rightTime)))
    // Metadata alone must not select an earlier key or hide a computed watermarked key.
    val computed = Alias(leftTime, "computed")()
    assert(!compatible(Seq(leftTime), Seq(rightTime),
      Seq(computed, leftTime), Seq(rightTime, rightTime)))
    assert(!compatible(Seq(leftId), Seq(rightId), Seq(computed), Seq(rightId)))
  }

  test("checkpoint compatibility for old V4 requires non-null selected key timestamp") {
    val watermarkMetadata = new MetadataBuilder()
      .putLong(EventTimeWatermark.delayKey, 0L).build()
    val nullableLeftTime = AttributeReference("leftTime", TimestampType,
      metadata = watermarkMetadata)()
    val nullableRightTime = AttributeReference("rightTime", TimestampType,
      metadata = watermarkMetadata)()
    val nonNullLeftTime = AttributeReference("leftTime", TimestampType, nullable = false,
      metadata = watermarkMetadata)()
    val nonNullRightTime = AttributeReference("rightTime", TimestampType, nullable = false,
      metadata = watermarkMetadata)()
    val nullableEndWindowType = StructType(Seq(
      StructField("start", TimestampType, nullable = false),
      StructField("end", TimestampType, nullable = true)))
    val nonNullWindowType = StructType(Seq(
      StructField("start", TimestampType, nullable = false),
      StructField("end", TimestampType, nullable = false)))
    val nullableEndWindow = AttributeReference("window", nullableEndWindowType,
      nullable = false, metadata = watermarkMetadata)()
    val nullableStructWindow = AttributeReference("window", nonNullWindowType,
      nullable = true, metadata = watermarkMetadata)()
    val nonNullWindow = AttributeReference("window", nonNullWindowType,
      nullable = false, metadata = watermarkMetadata)()
    val nonNullKeyNullableInput = nonNullLeftTime.withNullability(true)
    val nullableInputNonNullKey = nullableLeftTime.withNullability(false)

    def compatible(
        leftAttribute: Attribute,
        rightAttribute: Attribute,
        stateFormatVersion: Int,
        leftKey: Option[Attribute] = None,
        rightKey: Option[Attribute] = None): Boolean = {
      StreamingSymmetricHashJoinHelper.isWatermarkCheckpointCompatible(
        Seq(leftAttribute), Seq(rightAttribute),
        Seq(leftKey.getOrElse(leftAttribute)), Seq(rightKey.getOrElse(rightAttribute)),
        stateFormatVersion)
    }

    assert(compatible(nullableLeftTime, nullableRightTime, stateFormatVersion = 2))
    assert(compatible(nullableLeftTime, nullableRightTime, stateFormatVersion = 3))
    assert(!compatible(nullableLeftTime, nonNullRightTime, stateFormatVersion = 4))
    assert(!compatible(nonNullLeftTime, nullableRightTime, stateFormatVersion = 4))
    assert(compatible(nonNullLeftTime, nonNullRightTime, stateFormatVersion = 4))
    assert(!compatible(nullableEndWindow, nonNullWindow, stateFormatVersion = 4))
    assert(!compatible(nullableStructWindow, nonNullWindow, stateFormatVersion = 4))
    assert(compatible(nonNullWindow, nonNullWindow, stateFormatVersion = 4))
    assert(compatible(nonNullLeftTime, nonNullRightTime, stateFormatVersion = 4,
      leftKey = Some(nonNullKeyNullableInput), rightKey = Some(nonNullRightTime)))
    assert(!compatible(nullableLeftTime, nonNullRightTime, stateFormatVersion = 4,
      leftKey = Some(nullableInputNonNullKey), rightKey = Some(nonNullRightTime)))
  }

  for {
    version <- Seq(1, 2, 3, 4)
    logVersion <- Seq(1, 2)
    // Every marker below the current one takes the same rejection branch.
    oldMarker <- Seq(None, Some("0"), Some((currentMarker.toInt - 1).toString)).distinct
    shape <- Seq("range", "expiry", "pruned expiry", "unbounded")
  } {
    test(s"reject old $shape checkpoint, V$version log=$logVersion old=$oldMarker") {
      withJoinConf(version, logVersion) {
        val (leftInput, rightInput, joined) = joinInputs(shape)
        withTempDir { checkpointDir =>
          val checkpoint = checkpointDir.getCanonicalPath
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint),
            MultiAddData(leftInput, (1, 10))(rightInput, (1, 10)),
            CheckNewAnswer(if (shape == "unbounded") Row(1, 1, 10L, 10L)
              else Row(1, 1)),
            StopStream)
          // Emulate old offset metadata. State is written by the current implementation;
          // rejection must not open or reinterpret it, regardless of its physical layout.
          setMarker(checkpoint, oldMarker)
          val originalBatchId = new OffsetSeqLog(spark, checkpoint + "/offsets")
            .getLatest().get._1
          val stateBefore = snapshot(new File(checkpoint, "state"))
          val commitsBefore = snapshot(new File(checkpoint, "commits"))
          for (attempt <- 1 to 2) {
            // Session defaults must not bypass the checkpoint-pinned marker or state format.
            withSQLConf(markerKey -> currentMarker,
                SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> "2") {
              testStream(joined)(
                StartStream(checkpointLocation = checkpoint),
                AddData(rightInput, (2, 20 + attempt)),
                ExpectFailure[AnalysisException](assertFailure = error => {
                  checkError(error.asInstanceOf[AnalysisException],
                    condition = "STREAMING_JOIN_CHECKPOINT_INCOMPATIBLE",
                    parameters = Map(
                      "joinCondition" -> ".*leftKey.*rightKey.*",
                      "leftWatermarkColumns" ->
                        (if (shape == "expiry" || shape == "unbounded") "leftTime" else "none"),
                      "rightWatermarkColumns" -> "rightTime"),
                    matchPVals = true)
                }))
            }
            assert(snapshot(new File(checkpoint, "state")) == stateBefore)
            assert(snapshot(new File(checkpoint, "commits")) == commitsBefore)
            assert(markerAt(checkpoint, originalBatchId) == oldMarker)
          }
        }
      }
    }
  }

  for {
    version <- Seq(1, 2, 3, 4)
    logVersion <- Seq(1, 2)
    shape <- Seq("equality", "no watermark", "range", "expiry", "pruned expiry")
  } {
    test(s"resume compatible $shape checkpoint, V$version log=$logVersion") {
      withJoinConf(version, logVersion) {
        val (leftInput, rightInput, joined) = joinInputs(shape)
        withTempDir { checkpointDir =>
          val checkpoint = checkpointDir.getCanonicalPath
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint),
            MultiAddData(leftInput, (1, 10))(rightInput, (2, 10)),
            CheckNewAnswer(),
            StopStream)
          assert(pinnedMarker(checkpoint).contains(currentMarker))
          val oldCompatible = Seq("equality", "no watermark").contains(shape)
          val expectedMarker = if (oldCompatible) "0" else currentMarker
          if (oldCompatible) setMarker(checkpoint, None)
          withSQLConf(markerKey -> "0",
              SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> "2") {
            testStream(joined)(
              StartStream(checkpointLocation = checkpoint),
              AddData(rightInput, (1, 10)),
              CheckNewAnswer(Row(1, 1)),
              Execute { query =>
                val join = query.lastExecution.executedPlan.collect {
                  case j: StreamingSymmetricHashJoinExec => j
                }.head
                assert(join.stateFormatVersion == version)
              },
              StopStream)
          }
          assert(pinnedMarker(checkpoint).contains(expectedMarker))
        }
      }
    }
  }

  test("reject old V4 aligned checkpoint when own equality key can be null") {
    withJoinConf(version = 4, logVersion = 2) {
      val (leftInput, rightInput, joined) = nullableAlignedJoinInputs()
      withTempDir { checkpointDir =>
        val checkpoint = checkpointDir.getCanonicalPath
        testStream(joined)(
          StartStream(checkpointLocation = checkpoint),
          MultiAddData(leftInput, -1, 10)(rightInput, 20),
          CheckNewAnswer(),
          StopStream)
        setMarker(checkpoint, None)
        val stateBefore = snapshot(new File(checkpoint, "state"))
        val commitsBefore = snapshot(new File(checkpoint, "commits"))
        testStream(joined)(
          StartStream(checkpointLocation = checkpoint),
          AddData(rightInput, 30),
          ExpectFailure[AnalysisException](assertFailure = error => {
            checkError(error.asInstanceOf[AnalysisException],
              condition = "STREAMING_JOIN_CHECKPOINT_INCOMPATIBLE",
              parameters = Map(
                "joinCondition" -> ".*leftTime.*rightTime.*",
                "leftWatermarkColumns" -> "leftTime",
                "rightWatermarkColumns" -> "rightTime"),
              matchPVals = true)
          }))
        assert(snapshot(new File(checkpoint, "state")) == stateBefore)
        assert(snapshot(new File(checkpoint, "commits")) == commitsBefore)
      }
    }
  }

  test("resume old V4 aligned checkpoint when own equality keys are non-null") {
    withJoinConf(version = 4, logVersion = 2) {
      val (leftInput, rightInput, joined) = nonNullableAlignedJoinInputs()
      withTempDir { checkpointDir =>
        val checkpoint = checkpointDir.getCanonicalPath
        testStream(joined)(
          StartStream(checkpointLocation = checkpoint),
          MultiAddData(leftInput, 10L)(rightInput, 20L),
          CheckNewAnswer(),
          StopStream)
        setMarker(checkpoint, None)
        withSQLConf(markerKey -> "0") {
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint),
            MultiAddData(leftInput, 30L)(rightInput, 30L),
            CheckNewAnswer(Row(30L, 30L)),
            Execute { query =>
              val join = query.lastExecution.executedPlan.collect {
                case j: StreamingSymmetricHashJoinExec => j
              }.head
              val leftEventTime = join.left.output
                .filter(_.metadata.contains(EventTimeWatermark.delayKey)).head
              val rightEventTime = join.right.output
                .filter(_.metadata.contains(EventTimeWatermark.delayKey)).head
              assert(!leftEventTime.nullable, s"Expected non-null left event time: $join")
              assert(!rightEventTime.nullable, s"Expected non-null right event time: $join")
              assert(join.leftKeys.head.nullable, s"Expected nullable physical left key: $join")
              assert(join.rightKeys.head.nullable, s"Expected nullable physical right key: $join")
            },
            StopStream)
        }
        assert(pinnedMarker(checkpoint).contains("0"))
      }
    }
  }

  test("resume old V4 window equality checkpoint") {
    withJoinConf(version = 4, logVersion = 2) {
      val leftInput = MemoryStream[Int]
      val rightInput = MemoryStream[Int]
      def windowed(input: MemoryStream[Int]): DataFrame = input.toDF()
        .select($"value".as("key"), timestamp_seconds($"value").as("time"))
        .withWatermark("time", "10 seconds")
        .select($"key", window($"time", "10 seconds"))
      // TimeWindow declares the window end nullable, but window() computes it from a non-null
      // event time, so old V4 state cannot contain a null selected key timestamp.
      val joined = windowed(leftInput).join(windowed(rightInput), Seq("key", "window"))
        .select($"key", $"window.end".cast("long"))
      withTempDir { checkpointDir =>
        val checkpoint = checkpointDir.getCanonicalPath
        testStream(joined)(
          StartStream(checkpointLocation = checkpoint),
          MultiAddData(leftInput, 1)(rightInput, 2),
          CheckNewAnswer(),
          StopStream)
        setMarker(checkpoint, None)
        withSQLConf(markerKey -> "0") {
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint),
            MultiAddData(leftInput, 30)(rightInput, 30),
            CheckNewAnswer(Row(30, 40L)),
            StopStream)
        }
        assert(pinnedMarker(checkpoint).contains("0"))
      }
    }
  }

  Seq(2, 4).foreach { version =>
    test(s"reject an incompatible checkpoint retrying batch zero, V$version") {
      withJoinConf(version, logVersion = 1) {
        val (leftInput, rightInput, joined) = joinInputs("range")
        withTempDir { checkpointDir =>
          val checkpoint = checkpointDir.getCanonicalPath
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint),
            MultiAddData(leftInput, (1, 10))(rightInput, (1, 10)),
            CheckNewAnswer(Row(1, 1)),
            StopStream)
          val offsets = new OffsetSeqLog(spark, checkpoint + "/offsets")
          assert(offsets.getLatest().get._1 == 0L)
          setMarker(checkpoint, None)
          // Emulate a crash after state was written but before the first batch committed.
          val commit = new Path(checkpoint, "commits/0")
          assert(commit.getFileSystem(spark.sessionState.newHadoopConf()).delete(commit, false))
          val before = snapshot(new File(checkpoint, "state"))
          val commitsBefore = snapshot(new File(checkpoint, "commits"))
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint),
            ExpectFailure[AnalysisException](assertFailure = error => {
              checkError(error.asInstanceOf[AnalysisException],
                condition = "STREAMING_JOIN_CHECKPOINT_INCOMPATIBLE",
                parameters = Map(
                  "joinCondition" -> ".*leftKey.*rightKey.*leftTime.*rightTime.*",
                  "leftWatermarkColumns" -> "none",
                  "rightWatermarkColumns" -> "rightTime"),
                matchPVals = true)
            }))
          assert(snapshot(new File(checkpoint, "state")) == before)
          assert(snapshot(new File(checkpoint, "commits")) == commitsBefore)
        }
      }
    }
  }

  // Both bundled historical join checkpoints are range joins, which are rejected, so they no
  // longer cover resuming from old state. No historical equality-join checkpoint is bundled.
  Seq("2.4.0", "3.5.1").foreach { sparkVersion =>
    test(s"reject the historical Spark $sparkVersion range checkpoint") {
      val input = MemoryStream[(Int, Long)]
      val data = input.toDF().select($"_1".as("value"), timestamp_seconds($"_2").as("timestamp"))
      val left = data.select($"value".as("leftId"), $"timestamp".as("leftTime"))
        .withWatermark("leftTime", "5 seconds")
      val right = data.where($"value" % 2 === 0)
        .select($"value".as("rightId"), $"timestamp".as("rightTime"))
        .withWatermark("rightTime", "5 seconds")
      val joined = left.join(right, expr(
        "rightId = leftId AND rightTime >= leftTime AND " +
          "rightTime <= leftTime + INTERVAL 5 SECONDS"))
      withTempDir { checkpointDir =>
        val resource = getClass.getResource(
          s"/structured-streaming/checkpoint-version-$sparkVersion-streaming-join/").toURI
        Utils.copyDirectory(new File(resource), checkpointDir)
        val before = snapshot(new File(checkpointDir, "state"))
        val commitsBefore = snapshot(new File(checkpointDir, "commits"))
        input.addData((1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L))
        testStream(joined)(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(input, (6, 6L), (7, 7L), (8, 8L), (9, 9L), (10, 10L)),
          ExpectFailure[AnalysisException](assertFailure = error => {
            checkError(error.asInstanceOf[AnalysisException],
              condition = "STREAMING_JOIN_CHECKPOINT_INCOMPATIBLE",
              parameters = Map(
                "joinCondition" -> "left\\.leftId = right\\.rightId AND .*",
                "leftWatermarkColumns" -> "leftTime",
                "rightWatermarkColumns" -> "rightTime"),
              matchPVals = true)
          }))
        assert(snapshot(new File(checkpointDir, "state")) == before)
        assert(snapshot(new File(checkpointDir, "commits")) == commitsBefore)
      }
    }
  }

}
