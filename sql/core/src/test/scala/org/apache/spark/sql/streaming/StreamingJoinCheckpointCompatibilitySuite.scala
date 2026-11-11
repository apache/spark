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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, Expression, GreaterThan}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeq, OffsetSeqLog}
import org.apache.spark.sql.execution.streaming.operators.stateful.join.{StreamingSymmetricHashJoinExec, StreamingSymmetricHashJoinHelper}
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper.{JoinStateKeyWatermarkPredicate, JoinStateValueWatermarkPredicate, LeftSide, RightSide}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, MetadataBuilder, StructField,
  StructType, TimestampType}
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
      // A stateful operator after this key-only join reads its output watermark.
      joined.select($"leftKey", $"rightKey", $"leftTime".cast("long"),
        $"rightTime".cast("long")).dropDuplicates()
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
      _: Seq[Attribute], _: Seq[Attribute], _: Seq[Expression], _: Seq[Expression], None,
      consumedSides = Set(LeftSide, RightSide))

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

    // A key-only join is accepted only when no stateful operator after it reads its output
    // watermark. Another condition, or a key that uses an event time, makes it not key-only.
    val notConsumed = StreamingSymmetricHashJoinHelper.isWatermarkCheckpointCompatible(
      _: Seq[Attribute], _: Seq[Attribute], _: Seq[Expression], _: Seq[Expression],
      _: Option[Expression], consumedSides = Set.empty)
    val leftInput = Seq(leftId, leftTime)
    val rightInput = Seq(rightId, rightTime)
    assert(notConsumed(leftInput, rightInput, Seq(leftId), Seq(rightId), None))
    assert(!notConsumed(leftInput, rightInput, Seq(leftId), Seq(rightId),
      Some(GreaterThan(leftId, rightId))))
    assert(!notConsumed(leftInput, rightInput, Seq(leftId, Cast(leftTime, LongType)),
      Seq(rightId, Cast(rightTime, LongType)), None))
    assert(!notConsumed(leftInput, rightInput, Seq(leftId, computed), Seq(rightId, rightTime),
      None))

    // Ordering time windows is excluded even when nothing reads the output watermark: older
    // versions derived an ordering bound for whole windows that this version removes.
    val windowType = StructType(Seq(
      StructField("start", TimestampType), StructField("end", TimestampType)))
    val leftWindow = AttributeReference("leftWindow", windowType,
      metadata = watermarkMetadata)()
    val rightWindow = AttributeReference("rightWindow", windowType,
      metadata = watermarkMetadata)()
    assert(!notConsumed(Seq(leftId, leftWindow), Seq(rightId, rightWindow), Seq(leftId),
      Seq(rightId), Some(GreaterThan(leftWindow, rightWindow))))
  }

  test("checkpoint compatibility does not depend on selected key nullability") {
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
        leftKey: Option[Attribute] = None,
        rightKey: Option[Attribute] = None): Boolean = {
      StreamingSymmetricHashJoinHelper.isWatermarkCheckpointCompatible(
        Seq(leftAttribute), Seq(rightAttribute),
        Seq(leftKey.getOrElse(leftAttribute)), Seq(rightKey.getOrElse(rightAttribute)), None,
        consumedSides = Set(LeftSide, RightSide))
    }

    // Old V4 state indexed a NULL selected key at timestamp 0 under its own key bytes, so it
    // never shares a list with a real row. Such a row cannot match, so evicting it is correct.
    Seq(nullableLeftTime, nonNullLeftTime).foreach { leftTime =>
      Seq(nullableRightTime, nonNullRightTime).foreach { rightTime =>
        assert(compatible(leftTime, rightTime))
      }
    }
    Seq(nullableEndWindow, nullableStructWindow, nonNullWindow).foreach { window =>
      assert(compatible(window, nonNullWindow))
    }
    assert(compatible(nonNullLeftTime, nonNullRightTime,
      leftKey = Some(nonNullKeyNullableInput), rightKey = Some(nonNullRightTime)))
    assert(compatible(nullableLeftTime, nonNullRightTime,
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

  test("resume old V4 aligned checkpoint when own equality key can be null") {
    // A run restarted from an old checkpoint produces the same rows as an uninterrupted run.
    Seq(false, true).foreach { restart =>
      withJoinConf(version = 4, logVersion = 2) {
        val (leftInput, rightInput, joined) = nullableAlignedJoinInputs()
        withTempDir { checkpointDir =>
          val checkpoint = checkpointDir.getCanonicalPath
          val firstBatch: Seq[StreamAction] = Seq(
            StartStream(checkpointLocation = checkpoint),
            MultiAddData(leftInput, -1, 10)(rightInput, 20),
            // A NULL leftTime can never match, so it is emitted at once.
            CheckNewAnswer(Row(null, null)))
          val laterBatches: Seq[StreamAction] = Seq(
            MultiAddData(leftInput, 30)(rightInput, 30),
            CheckNewAnswer(Row(30L, 30L)),
            MultiAddData(leftInput, 200)(rightInput, 200),
            CheckNewAnswer(Row(200L, 200L)),
            // The eviction watermark 100 emits the unmatched rows of the first batch.
            MultiAddData(leftInput, 300)(rightInput, 300),
            CheckNewAnswer(Row(300L, 300L), Row(10L, null), Row(null, 20L)),
            StopStream)
          if (restart) {
            testStream(joined)(firstBatch :+ StopStream: _*)
            setMarker(checkpoint, None)
            withSQLConf(markerKey -> "0") {
              testStream(joined)(StartStream(checkpointLocation = checkpoint) +: laterBatches: _*)
            }
            assert(pinnedMarker(checkpoint).contains("0"))
          } else {
            testStream(joined)(firstBatch ++ laterBatches: _*)
          }
        }
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

  private val intervalCondition =
    "leftKey = rightKey AND rightTime >= leftTime AND rightTime <= leftTime + INTERVAL 5 SECONDS"

  // Old state of a two-sided time-interval join on the inputs' own event times is reusable;
  // shapes that only resemble it are not.
  Seq(
    // (shape, join condition, state format versions, session time zone, join type, accepted)
    ("two-sided interval", intervalCondition, Seq(1, 2, 3, 4), "America/Los_Angeles",
      "inner", true),
    ("two-sided interval with the shift on the stored side",
      "leftKey = rightKey AND leftTime + INTERVAL 5 SECONDS >= rightTime AND " +
        "leftTime <= rightTime", Seq(2), "America/Los_Angeles", "inner", true),
    ("BETWEEN interval",
      "leftKey = rightKey AND rightTime BETWEEN leftTime AND leftTime + INTERVAL 5 SECONDS",
      Seq(2, 4), "America/Los_Angeles", "left_outer", true),
    // Both versions count a day as 24 hours in every time zone, so whole days keep their cutoffs.
    ("whole-day interval in a fixed-offset time zone",
      "leftKey = rightKey AND rightTime >= leftTime AND rightTime <= leftTime + INTERVAL 1 DAY",
      Seq(2, 4), "UTC", "inner", true),
    ("whole-day interval in a daylight saving time zone",
      "leftKey = rightKey AND rightTime >= leftTime AND rightTime <= leftTime + INTERVAL 1 DAY",
      Seq(2, 4), "America/Los_Angeles", "inner", true),
    ("24-hour interval in a daylight saving time zone",
      "leftKey = rightKey AND rightTime >= leftTime AND rightTime <= leftTime + INTERVAL 24 HOURS",
      Seq(2), "America/Los_Angeles", "left_outer", true),
    // An inner join evaluates a one-input conjunct below the join, so it is not in the condition.
    ("interval with a constant conjunct below an inner join",
      intervalCondition + " AND leftTime > timestamp_seconds(1)", Seq(2),
      "America/Los_Angeles", "inner", true),
    // Neither version bounds the right input; nothing after this join reads its event time.
    ("one-directional interval",
      "leftKey = rightKey AND rightTime <= leftTime + INTERVAL 5 SECONDS", Seq(2, 4),
      "America/Los_Angeles", "inner", true),
    // A month has no fixed length, so the solver gives it no cutoff.
    ("month interval",
      "leftKey = rightKey AND rightTime >= leftTime AND rightTime <= leftTime + INTERVAL 1 MONTH",
      Seq(2), "UTC", "inner", false),
    // Both versions read a cast of an event time with its own watermark as the event time.
    ("interval with a cast",
      "leftKey = rightKey AND CAST(rightTime AS BIGINT) >= CAST(leftTime AS BIGINT) AND " +
        "rightTime <= leftTime + INTERVAL 5 SECONDS", Seq(2, 4), "America/Los_Angeles", "inner",
      true),
    ("interval with casts and numeric constants",
      "leftKey = rightKey AND CAST(rightTime AS BIGINT) >= CAST(leftTime AS BIGINT) - 1 AND " +
        "CAST(rightTime AS BIGINT) <= CAST(leftTime AS BIGINT) + 5", Seq(2), "UTC", "inner",
      true),
    ("interval with a negated event time",
      "leftKey = rightKey AND CAST(rightTime AS BIGINT) >= 20 - CAST(leftTime AS BIGINT) AND " +
        "rightTime <= leftTime + INTERVAL 5 SECONDS", Seq(2), "UTC", "inner", false),
    ("interval with a constant conjunct in a left outer join",
      intervalCondition + " AND leftTime > timestamp_seconds(1)", Seq(2),
      "America/Los_Angeles", "left_outer", false),
    ("interval with a shifted event-time key",
      "leftKey = rightKey AND leftTime = rightTime + INTERVAL 1 SECOND AND " +
        "rightTime >= leftTime - INTERVAL 5 SECONDS", Seq(2), "America/Los_Angeles", "inner",
      false),
    // Neither version evicts a key-only join; nothing after this one reads its output watermark.
    ("key-only join keeping both event times", "leftKey = rightKey", Seq(2, 4),
      "America/Los_Angeles", "inner", true)
  ).foreach { case (shape, condition, versions, timeZone, joinType, accepted) =>
    versions.foreach { version =>
      val action = if (accepted) "resume" else "reject"
      test(s"$action old $shape checkpoint, V$version") {
        withJoinConf(version, logVersion = 1) {
          withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timeZone) {
            val leftInput = MemoryStream[(Int, Int)]
            val rightInput = MemoryStream[(Int, Int)]
            // A negative time is a null event time, so the event times are nullable.
            val left = leftInput.toDF().toDF("leftKey", "time")
              .select($"leftKey", when($"time" < 0, lit(null).cast(TimestampType))
                .otherwise(timestamp_seconds($"time")).as("leftTime"))
              .withWatermark("leftTime", "5 seconds")
            val right = rightInput.toDF().toDF("rightKey", "time")
              .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
              .withWatermark("rightTime", "5 seconds")
            val joined = left.join(right, expr(condition), joinType)
              .select($"leftKey", $"rightKey", $"leftTime".cast("long"),
                $"rightTime".cast("long"))
            withTempDir { checkpointDir =>
              val checkpoint = checkpointDir.getCanonicalPath
              testStream(joined)(
                StartStream(checkpointLocation = checkpoint),
                MultiAddData(leftInput, (1, 10), (2, -1))(rightInput, (3, 12)),
                ProcessAllAvailable(),
                StopStream)
              setMarker(checkpoint, None)
              if (accepted) {
                testStream(joined)(
                  StartStream(checkpointLocation = checkpoint),
                  AddData(rightInput, (1, 14)),
                  CheckNewAnswer(Row(1, 1, 10L, 14L)),
                  StopStream)
                assert(pinnedMarker(checkpoint).contains("0"))
              } else {
                val stateBefore = snapshot(new File(checkpoint, "state"))
                testStream(joined)(
                  StartStream(checkpointLocation = checkpoint),
                  AddData(rightInput, (1, 14)),
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
    }
  }

  // The first batch after restoring an older-generation checkpoint re-simulates the previous
  // batch with this version's output watermarks, which the older version may not have evicted
  // with, so that batch scans V4 join state from the start. Later batches use scan starts again.
  Seq(true, false).foreach { olderGeneration =>
    test(s"V4 scan start in the first batch after a restart, older generation=$olderGeneration") {
      withJoinConf(version = 4, logVersion = 2) {
        val leftInput = MemoryStream[(Int, Int)]
        val rightInput = MemoryStream[(Int, Int)]
        val left = leftInput.toDF().toDF("leftKey", "time")
          .select($"leftKey", timestamp_seconds($"time").as("leftTime"))
          .withWatermark("leftTime", "5 seconds")
        val right = rightInput.toDF().toDF("rightKey", "time")
          .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
          .withWatermark("rightTime", "5 seconds")
        val joined = left.join(right, expr(intervalCondition), "left_outer")
          .select($"leftKey", $"rightKey", $"leftTime".cast("long"), $"rightTime".cast("long"))
        def assertScanStarts(defined: Boolean): AssertOnQuery = Execute { query =>
          val join = query.lastExecution.executedPlan.collect {
            case j: StreamingSymmetricHashJoinExec => j
          }.head
          val scanStarts = Seq(join.stateWatermarkPredicates.left,
            join.stateWatermarkPredicates.right).map {
            case Some(p: JoinStateKeyWatermarkPredicate) => p.prevStateWatermark
            case Some(p: JoinStateValueWatermarkPredicate) => p.prevStateWatermark
            case _ => None
          }
          assert(scanStarts.forall(_.isDefined == defined), s"Unexpected scan starts: $join")
        }
        withTempDir { checkpointDir =>
          val checkpoint = checkpointDir.getCanonicalPath
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint),
            MultiAddData(leftInput, (1, 10), (2, 20))(rightInput, (3, 30)),
            ProcessAllAvailable(),
            MultiAddData(leftInput, (4, 40))(rightInput, (5, 40)),
            ProcessAllAvailable(),
            assertScanStarts(defined = true),
            StopStream)
          if (olderGeneration) {
            setMarker(checkpoint, None)
          }
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint),
            AddData(rightInput, (6, 50)),
            ProcessAllAvailable(),
            assertScanStarts(defined = !olderGeneration),
            AddData(rightInput, (7, 60)),
            ProcessAllAvailable(),
            assertScanStarts(defined = true),
            StopStream)
        }
      }
    }
  }

  // A one-directional interval join leaves the right input unbounded in both versions, so the
  // checkpoint is reusable only when no stateful operator after the join reads rightTime.
  Seq("leftTime" -> true, "rightTime" -> false).foreach { case (timeColumn, accepted) =>
    val action = if (accepted) "resume" else "reject"
    test(s"$action old one-directional interval checkpoint deduplicated on $timeColumn") {
      withJoinConf(version = 4, logVersion = 1) {
        val leftInput = MemoryStream[(Int, Int)]
        val rightInput = MemoryStream[(Int, Int)]
        val left = leftInput.toDF().toDF("leftKey", "time")
          .select($"leftKey", timestamp_seconds($"time").as("leftTime"))
          .withWatermark("leftTime", "5 seconds")
        val right = rightInput.toDF().toDF("rightKey", "time")
          .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
          .withWatermark("rightTime", "5 seconds")
        val deduplicated = left.join(right,
            expr("leftKey = rightKey AND rightTime <= leftTime + INTERVAL 5 SECONDS"))
          .select($"leftKey", col(timeColumn).as("t"))
          .dropDuplicates("leftKey", "t")
          .select($"leftKey", $"t".cast("long"))
        withTempDir { checkpointDir =>
          val checkpoint = checkpointDir.getCanonicalPath
          testStream(deduplicated)(
            StartStream(checkpointLocation = checkpoint),
            MultiAddData(leftInput, (1, 10))(rightInput, (3, 12)),
            ProcessAllAvailable(),
            StopStream)
          setMarker(checkpoint, None)
          if (accepted) {
            testStream(deduplicated)(
              StartStream(checkpointLocation = checkpoint),
              AddData(rightInput, (1, 14)),
              CheckNewAnswer(Row(1, 10L)),
              StopStream)
            assert(pinnedMarker(checkpoint).contains("0"))
          } else {
            val stateBefore = snapshot(new File(checkpoint, "state"))
            testStream(deduplicated)(
              StartStream(checkpointLocation = checkpoint),
              AddData(rightInput, (1, 14)),
              ExpectFailure[AnalysisException](assertFailure = error => {
                assert(error.asInstanceOf[AnalysisException].getCondition ==
                  "STREAMING_JOIN_CHECKPOINT_INCOMPATIBLE")
              }))
            assert(snapshot(new File(checkpoint, "state")) == stateBefore)
          }
        }
      }
    }
  }

  // Both bundled historical join checkpoints are two-sided time-interval joins on the inputs'
  // own event times, so they resume. No historical equality-join checkpoint is bundled.
  Seq("2.4.0", "3.5.1").foreach { sparkVersion =>
    test(s"resume the historical Spark $sparkVersion range checkpoint") {
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
        .select($"leftId", $"leftTime".cast("int"), $"rightId", $"rightTime".cast("int"))
      withTempDir { checkpointDir =>
        val resource = getClass.getResource(
          s"/structured-streaming/checkpoint-version-$sparkVersion-streaming-join/").toURI
        Utils.copyDirectory(new File(resource), checkpointDir)
        input.addData((1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L))
        testStream(joined)(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(input, (6, 6L), (7, 7L), (8, 8L), (9, 9L), (10, 10L)),
          CheckNewAnswer((6, 6, 6, 6), (8, 8, 8, 8), (10, 10, 10, 10)),
          AddData(input, (20, 20L), (30, 30L)),
          CheckNewAnswer((20, 20, 20, 20), (30, 30, 30, 30)),
          StopStream)
        assert(pinnedMarker(checkpointDir.getCanonicalPath).contains("0"))
      }
    }
  }

}
