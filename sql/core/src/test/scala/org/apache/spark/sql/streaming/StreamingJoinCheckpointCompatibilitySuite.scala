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
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, And, Attribute, AttributeReference, Between, Cast, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, TimestampAddInterval}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeq, OffsetSeqLog}
import org.apache.spark.sql.execution.streaming.operators.stateful.join.{StreamingSymmetricHashJoinExec, StreamingSymmetricHashJoinHelper}
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper.{JoinSide, JoinStateKeyWatermarkPredicate, JoinStateValueWatermarkPredicate, LeftSide, RightSide}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamExecution}
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DayTimeIntervalType, IntegerType, LongType, MetadataBuilder,
  StructField, StructType, TimestampType}
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.unsafe.types.CalendarInterval
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
      // A stateful operator after this key-only join reads the left input's event time.
      joined.select($"leftKey", $"rightKey", $"leftTime", $"rightTime".cast("long"))
        .dropDuplicates()
        .select($"leftKey", $"rightKey", $"leftTime".cast("long"), $"rightTime")
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
    // watermark. A key that uses an event time makes it not key-only; another condition that
    // gives no bound in either version keeps both inputs unbounded.
    val notConsumed = StreamingSymmetricHashJoinHelper.isWatermarkCheckpointCompatible(
      _: Seq[Attribute], _: Seq[Attribute], _: Seq[Expression], _: Seq[Expression],
      _: Option[Expression], consumedSides = Set.empty)
    val leftInput = Seq(leftId, leftTime)
    val rightInput = Seq(rightId, rightTime)
    assert(notConsumed(leftInput, rightInput, Seq(leftId), Seq(rightId), None))
    assert(notConsumed(leftInput, rightInput, Seq(leftId), Seq(rightId),
      Some(GreaterThan(leftId, rightId))))
    assert(!notConsumed(leftInput, rightInput, Seq(leftId), Seq(rightId),
      Some(GreaterThan(leftId, Literal(1)))))
    assert(!notConsumed(leftInput, rightInput, Seq(leftId, Cast(leftTime, LongType)),
      Seq(rightId, Cast(rightTime, LongType)), None))
    assert(!notConsumed(leftInput, rightInput, Seq(leftId, computed), Seq(rightId, rightTime),
      None))

    // Ordering time windows is excluded even when nothing reads the output watermark: this
    // version still derives the older bound for whole windows, but SPARK-61348 later removes it
    // for windows shorter than the other input's.
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

  test("checkpoint compatibility ignores conjuncts that no version derives a bound from") {
    val watermarkMetadata = new MetadataBuilder()
      .putLong(EventTimeWatermark.delayKey, 0L).build()
    val leftKey = AttributeReference("leftKey", IntegerType)()
    val rightKey = AttributeReference("rightKey", IntegerType)()
    val leftPrice = AttributeReference("leftPrice", IntegerType)()
    val rightPrice = AttributeReference("rightPrice", IntegerType)()
    val leftOther = AttributeReference("leftOther", TimestampType)()
    val leftTime = AttributeReference("leftTime", TimestampType,
      metadata = watermarkMetadata)()
    val rightTime = AttributeReference("rightTime", TimestampType,
      metadata = watermarkMetadata)()
    def plus(time: Expression, micros: Long): Expression =
      TimestampAddInterval(time, Literal(micros, DayTimeIntervalType()))
    val interval = And(GreaterThanOrEqual(rightTime, leftTime),
      LessThanOrEqual(rightTime, plus(leftTime, 5000000L)))
    def compatible(condition: Expression, consumedSides: Set[JoinSide]): Boolean =
      StreamingSymmetricHashJoinHelper.isWatermarkCheckpointCompatible(
        Seq(leftKey, leftPrice, leftOther, leftTime), Seq(rightKey, rightPrice, rightTime),
        Seq(leftKey), Seq(rightKey), Some(condition), consumedSides)
    val both = Set[JoinSide](LeftSide, RightSide)

    // A comparison of columns of both inputs that have no watermark gives no bound.
    assert(compatible(And(interval, GreaterThanOrEqual(leftPrice, rightPrice)), both))
    assert(compatible(And(interval, LessThan(Add(leftPrice, Literal(1)), rightPrice)), both))
    // Each half of a BETWEEN is judged on its own: a month interval gives no bound, so the left
    // input has none, which is accepted only when nothing after the join reads its event time.
    val monthBetween = Between(rightTime, leftTime,
      TimestampAddInterval(leftTime, Literal(new CalendarInterval(1, 0, 0L))))
    assert(compatible(monthBetween, Set(RightSide)))
    assert(!compatible(monthBetween, Set(LeftSide)))
    // A comparison of one input's column with a constant gives a bound in older versions, and so
    // does a BETWEEN half that bounds a column other than the event time.
    assert(!compatible(And(interval, GreaterThan(leftPrice, Literal(10))), both))
    assert(!compatible(And(interval, GreaterThan(leftTime, Literal(1000000L, TimestampType))),
      both))
    assert(!compatible(Between(rightTime, leftTime, leftOther), both))
    assert(!compatible(And(interval, LessThanOrEqual(rightTime, leftOther)), both))
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
    // A run restarted from an emulated old checkpoint produces the same rows as an uninterrupted
    // run. The emulated checkpoint holds no NULL-key row, because this version emits such a row
    // on arrival; a real older checkpoint can hold one at timestamp 0, which the first batch after
    // the restart evicts. The Spark 4.3.0 checkpoint test below covers that stored row.
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

  // Written by Spark 4.3.0 in state format 4 for the full outer join of nullableAlignedJoinInputs.
  // Batch 0 added 150 on both inputs, batch 1 added NULL and 160 on the left and 170 on the
  // right, and batch 2 added 220 on both inputs. Spark 4.3.0 stored the NULL key at timestamp 0
  // in batch 1, after the scan start had passed it, and never evicted that row.
  test("resume a Spark 4.3.0 V4 checkpoint that holds a NULL equality key") {
    withJoinConf(version = 4, logVersion = 1) {
      val (leftInput, rightInput, joined) = nullableAlignedJoinInputs()
      withTempDir { checkpointDir =>
        val resource = getClass.getResource(
          "/structured-streaming/checkpoint-version-4.3.0-streaming-join-null-equality-key/").toURI
        Utils.copyDirectory(new File(resource), checkpointDir)
        // One call per batch of the fixture, because a memory stream's offset counts the calls.
        Seq(Seq(150), Seq(-1, 160), Seq(220)).foreach(leftInput.addData(_: _*))
        Seq(Seq(150), Seq(170), Seq(220)).foreach(rightInput.addData(_: _*))
        testStream(joined)(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          // The watermark of the last batch is 120 seconds, past the timestamp 0 of the NULL key,
          // and the first batch of this version scans the index from the start.
          MultiAddData(leftInput, 400)(rightInput, 400),
          CheckNewAnswer(Row(400L, 400L), Row(null, null)),
          // The eviction watermark 300 emits the unmatched rows of batch 1, and not the NULL row
          // again.
          MultiAddData(leftInput, 600)(rightInput, 600),
          CheckNewAnswer(Row(600L, 600L), Row(160L, null), Row(null, 170L)),
          StopStream)
        assert(pinnedMarker(checkpointDir.getCanonicalPath).contains("0"))
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

  // Old state of a two-sided time-interval join on the inputs' own event times is reusable, and
  // so is that of a join whose unbounded input's event time no later stateful operator reads,
  // such as a one-directional interval join or a key-only join; shapes that only resemble them
  // are not.
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
    // A month has no fixed length, so neither version reads that conjunct, and neither bounds
    // the left input. Nothing after this join reads its event time.
    ("month interval",
      "leftKey = rightKey AND rightTime >= leftTime AND rightTime <= leftTime + INTERVAL 1 MONTH",
      Seq(2), "UTC", "inner", true),
    // Neither version derives a bound from a comparison of columns without a watermark.
    ("interval with a comparison of other columns of both inputs",
      intervalCondition + " AND leftKey <= rightKey + 1", Seq(2, 4), "America/Los_Angeles",
      "left_outer", true),
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
    // Older versions evicted the left input at the unshifted watermark, too early for a key
    // shifted backward on the right.
    ("interval with a backward-shifted event-time key",
      "leftKey = rightKey AND leftTime = rightTime - INTERVAL 4 SECONDS AND " +
        "rightTime >= leftTime - INTERVAL 5 SECONDS", Seq(2), "America/Los_Angeles", "inner",
      false),
    // A forward shift keeps old state reusable; nothing after this join reads leftTime.
    ("interval with a forward-shifted event-time key",
      "leftKey = rightKey AND leftTime + INTERVAL 4 SECONDS = rightTime AND " +
        "rightTime >= leftTime", Seq(2, 4), "America/Los_Angeles", "inner", true),
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

  // The scan start of each side of a V4 join, in left, right order.
  private def scanStarts(join: StreamingSymmetricHashJoinExec): Seq[Option[Long]] = {
    Seq(join.stateWatermarkPredicates.left, join.stateWatermarkPredicates.right).map {
      case Some(p: JoinStateKeyWatermarkPredicate) => p.prevStateWatermark
      case Some(p: JoinStateValueWatermarkPredicate) => p.prevStateWatermark
      case _ => None
    }
  }

  private def joinsOf(query: StreamExecution): Seq[StreamingSymmetricHashJoinExec] = {
    query.lastExecution.executedPlan.collect { case j: StreamingSymmetricHashJoinExec => j }
  }

  // The first batch of every run of an older-generation checkpoint re-simulates the previous
  // batch with this version's output watermarks, which the older version may not have evicted
  // with, so that batch scans V4 join state from the start. The pinned marker does not advance,
  // so a second run does the same. Later batches use scan starts again.
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
          val join = joinsOf(query).head
          assert(scanStarts(join).forall(_.isDefined == defined), s"Unexpected scan starts: $join")
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
          Seq(50, 70).foreach { time =>
            testStream(joined)(
              StartStream(checkpointLocation = checkpoint),
              AddData(rightInput, (6, time)),
              ProcessAllAvailable(),
              assertScanStarts(defined = !olderGeneration),
              AddData(rightInput, (7, time + 10)),
              ProcessAllAvailable(),
              assertScanStarts(defined = true),
              StopStream)
            assert(pinnedMarker(checkpoint).contains(if (olderGeneration) "0" else currentMarker))
          }
        }
      }
    }
  }

  // An upstream join's output watermark depends on the plan above it. Here a stream-static left
  // outer join between the two joins hides the downstream join from the tracer, so the upstream
  // join holds back its output watermark for its unbounded click input, at 0, and the downstream
  // left outer join evicts nothing. Removing the stream-static join between runs lets the tracer
  // reach the impression time, which raises the re-simulated watermark of the previous batch.
  // The downstream join must then scan from the start in the first batch, or it would never emit
  // impressions buffered below that watermark. The upstream join has no join below it and keeps
  // its scan start.
  test("V4 chained joins scan from the start after the plan between them changes") {
    withJoinConf(version = 4, logVersion = 2) {
      val impressionInput = MemoryStream[(Int, Int)]
      val clickInput = MemoryStream[(Int, Int)]
      val conversionInput = MemoryStream[(Int, Int)]
      val impressions = impressionInput.toDF().toDF("impressionKey", "time")
        .select($"impressionKey", timestamp_seconds($"time").as("impressionTime"))
        .withWatermark("impressionTime", "0 seconds")
      val clicks = clickInput.toDF().toDF("clickKey", "time")
        .select($"clickKey", timestamp_seconds($"time").as("clickTime"))
        .withWatermark("clickTime", "0 seconds")
      val conversions = conversionInput.toDF().toDF("conversionKey", "time")
        .select($"conversionKey", timestamp_seconds($"time").as("conversionTime"))
        .withWatermark("conversionTime", "0 seconds")
      val clicked = impressions.join(clicks, expr("impressionKey = clickKey AND " +
          "clickTime <= impressionTime + INTERVAL 10 SECONDS"))
        .select($"impressionKey", $"impressionTime")
      // A left anti join here would be pushed below the upstream join; an outer join is not.
      val campaigns = Seq(-1).toDF("campaignKey")
      def converted(withCampaigns: Boolean): DataFrame = {
        val upstream = if (withCampaigns) {
          clicked.join(campaigns, $"impressionKey" === $"campaignKey", "left_outer")
            .drop("campaignKey")
        } else {
          clicked
        }
        upstream.join(conversions, expr("impressionKey = conversionKey AND " +
            "conversionTime BETWEEN impressionTime AND impressionTime + INTERVAL 5 SECONDS"),
            "left_outer")
          .select($"impressionKey", $"impressionTime".cast("long"), $"conversionKey",
            $"conversionTime".cast("long"))
      }
      def addAll(
          impression: (Int, Int),
          click: (Int, Int),
          conversion: (Int, Int)): StreamAction =
        MultiAddData((impressionInput, Seq(impression)), (clickInput, Seq(click)),
          (conversionInput, Seq(conversion)))
      def assertScanStarts(upstream: Boolean, downstream: Boolean): AssertOnQuery =
        Execute { query =>
          val Seq(downstreamJoin, upstreamJoin) = joinsOf(query)
          // The click side of the upstream join has no bound and no scan start.
          assert(scanStarts(upstreamJoin).head.isDefined == upstream, s"$upstreamJoin")
          assert(scanStarts(downstreamJoin).forall(_.isDefined == downstream), s"$downstreamJoin")
        }
      withTempDir { checkpointDir =>
        val checkpoint = checkpointDir.getCanonicalPath
        testStream(converted(withCampaigns = true))(
          StartStream(checkpointLocation = checkpoint),
          addAll((1, 10), (1, 10), (5, 10)),
          CheckNewAnswer(),
          addAll((2, 100), (2, 100), (6, 100)),
          CheckNewAnswer(),
          AddData(conversionInput, (7, 101)),
          // The upstream join holds the downstream input watermark at 0, so nothing is evicted.
          CheckNewAnswer(),
          StopStream)
        testStream(converted(withCampaigns = false))(
          StartStream(checkpointLocation = checkpoint),
          addAll((3, 200), (3, 200), (8, 200)),
          // The re-simulated watermark of the previous batch is 100 seconds less 10 seconds and
          // 1 ms. The impression at 10 seconds lies below the scan start it would give.
          CheckNewAnswer(Row(1, 10L, null, null)),
          assertScanStarts(upstream = true, downstream = false),
          AddData(conversionInput, (9, 201)),
          CheckNewAnswer(Row(2, 100L, null, null)),
          assertScanStarts(upstream = true, downstream = true),
          StopStream)
      }
    }
  }

  // Written by Spark 4.3.0 in state format 4: the left outer join below on a left input whose
  // event time can be NULL. Batch 0 added (key 1, 100 s) on the left and (key 1, 102 s) on the
  // right, batch 1 added (key 3, 150 s) and (key 3, 152 s), and batch 2 added (key 2, NULL) and
  // (key 4, 300 s) on the left and (key 4, 302 s) on the right. Spark 4.3.0 stored the NULL event
  // time at timestamp 0 in batch 2, after its scan start had passed 0, and never evicted that row.
  // The first batch of this version scans the index from the start and emits it.
  test("resume a Spark 4.3.0 V4 checkpoint that holds a row with a NULL event time") {
    withJoinConf(version = 4, logVersion = 1) {
      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]
      val left = leftInput.toDF().toDF("leftKey", "time")
        .select($"leftKey", when($"time" >= 0, timestamp_seconds($"time")).as("leftTime"))
        .withWatermark("leftTime", "5 seconds")
      val right = rightInput.toDF().toDF("rightKey", "time")
        .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
        .withWatermark("rightTime", "5 seconds")
      val joined = left.join(right, expr(intervalCondition), "left_outer")
        .select($"leftKey", $"rightKey", $"leftTime".cast("long"), $"rightTime".cast("long"))
      withTempDir { checkpointDir =>
        val resource = getClass.getResource(
          "/structured-streaming/checkpoint-version-4.3.0-streaming-join-null-event-time/").toURI
        Utils.copyDirectory(new File(resource), checkpointDir)
        // One call per batch of the fixture, because a memory stream's offset counts the calls.
        Seq(Seq((1, 100)), Seq((3, 150)), Seq((2, -1), (4, 300)))
          .foreach(leftInput.addData(_: _*))
        Seq(Seq((1, 102)), Seq((3, 152)), Seq((4, 302))).foreach(rightInput.addData(_: _*))
        testStream(joined)(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          // The watermark of the last batch is 295 seconds, past the timestamp 0 of the NULL row.
          AddData(rightInput, (5, 400)),
          CheckNewAnswer(Row(2, null, null, null)),
          AddData(rightInput, (6, 500)),
          CheckNewAnswer(),
          StopStream)
        assert(pinnedMarker(checkpointDir.getCanonicalPath).contains("0"))
      }
    }
  }

  // A one-directional interval join leaves the right input unbounded in both versions, and a
  // month interval leaves the left input unbounded, so the checkpoint is reusable only when no
  // stateful operator after the join reads the unbounded input's event time.
  Seq(
    ("one-directional interval", "rightTime <= leftTime + INTERVAL 5 SECONDS", "leftTime", true),
    ("one-directional interval", "rightTime <= leftTime + INTERVAL 5 SECONDS", "rightTime",
      false),
    ("month interval", "rightTime BETWEEN leftTime AND leftTime + INTERVAL 1 MONTH",
      "rightTime", true),
    ("month interval", "rightTime BETWEEN leftTime AND leftTime + INTERVAL 1 MONTH",
      "leftTime", false)
  ).foreach { case (shape, timeCondition, timeColumn, accepted) =>
    val action = if (accepted) "resume" else "reject"
    test(s"$action old $shape checkpoint deduplicated on $timeColumn") {
      withJoinConf(version = 4, logVersion = 1) {
        val leftInput = MemoryStream[(Int, Int)]
        val rightInput = MemoryStream[(Int, Int)]
        val left = leftInput.toDF().toDF("leftKey", "time")
          .select($"leftKey", timestamp_seconds($"time").as("leftTime"))
          .withWatermark("leftTime", "5 seconds")
        val right = rightInput.toDF().toDF("rightKey", "time")
          .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
          .withWatermark("rightTime", "5 seconds")
        val deduplicated = left.join(right, expr(s"leftKey = rightKey AND $timeCondition"))
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
              CheckNewAnswer(Row(1, if (timeColumn == "leftTime") 10L else 14L)),
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

  // A join that neither version bounds on one input keeps its checkpoint when no stateful
  // operator after it uses its watermark: one with no event-time column in its input, no
  // event-time timeouts and no user code that can read the watermark.
  Seq(
    ("key-only join", "leftKey = rightKey", "count", true),
    ("one-directional interval",
      "leftKey = rightKey AND rightTime <= leftTime + INTERVAL 5 SECONDS", "count", true),
    ("key-only join", "leftKey = rightKey", "window count", false),
    ("key-only join", "leftKey = rightKey", "processing-time transformWithState", false)
  ).foreach { case (shape, condition, downstream, accepted) =>
    val action = if (accepted) "resume" else "reject"
    test(s"$action old $shape checkpoint followed by an update-mode $downstream") {
      withJoinConf(version = 4, logVersion = 1) {
        val leftInput = MemoryStream[(Int, Int)]
        val rightInput = MemoryStream[(Int, Int)]
        val left = leftInput.toDF().toDF("leftKey", "time")
          .select($"leftKey", timestamp_seconds($"time").as("leftTime"))
          .withWatermark("leftTime", "5 seconds")
        val right = rightInput.toDF().toDF("rightKey", "time")
          .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
          .withWatermark("rightTime", "5 seconds")
        val joined = left.join(right, expr(condition))
        // Each operator reads leftTime, so the join keeps its event-time columns, but only the
        // window count reads it as an event time.
        val counted = downstream match {
          case "count" =>
            joined.groupBy($"leftKey").agg(count($"leftTime".cast("long")).as("count"))
          case "window count" =>
            joined.groupBy(window($"leftTime", "10 seconds"), $"leftKey").count()
              .select($"leftKey", $"count")
          case _ =>
            joined.select(concat_ws(":", $"leftKey", $"leftTime".cast("long"))).as[String]
              .groupByKey(identity)
              .transformWithState(new RunningCountStatefulProcessor(), TimeMode.ProcessingTime(),
                OutputMode.Update())
              .toDF("key", "count")
              .select(split($"key", ":")(0).cast("int"), $"count".cast("long"))
        }
        withTempDir { checkpointDir =>
          val checkpoint = checkpointDir.getCanonicalPath
          testStream(counted, OutputMode.Update())(
            StartStream(checkpointLocation = checkpoint),
            MultiAddData(leftInput, (1, 10))(rightInput, (1, 10)),
            CheckNewAnswer(Row(1, 1L)),
            StopStream)
          setMarker(checkpoint, None)
          if (accepted) {
            testStream(counted, OutputMode.Update())(
              StartStream(checkpointLocation = checkpoint),
              AddData(rightInput, (1, 14)),
              CheckNewAnswer(Row(1, 2L)),
              StopStream)
            assert(pinnedMarker(checkpoint).contains("0"))
          } else {
            val stateBefore = snapshot(new File(checkpoint, "state"))
            testStream(counted, OutputMode.Update())(
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

  // For an equality key that shifts one input's event time forward, older versions evicted both
  // sides at the unshifted watermark: the cutoff this version uses for the shifted side, and at
  // or below the one it uses for the other side. Old state is reusable when no stateful operator
  // after the join reads the shifted input's event time, whose output watermark bound changed.
  Seq(
    // (shape, join condition, left time, right time, deduplicated column, versions, accepted)
    ("forward shift", "leftTime = rightTime + INTERVAL 10 SECONDS", 20, 10, None,
      Seq(1, 2, 3, 4), true),
    ("forward timestampadd shift", "leftTime = timestampadd(SECOND, 10, rightTime)", 20, 10,
      None, Seq(2, 4), true),
    ("forward whole-day shift", "leftTime = rightTime + INTERVAL 1 DAY", 86410, 10, None,
      Seq(2, 4), true),
    ("forward shift on the left key", "leftTime + INTERVAL 10 SECONDS = rightTime", 10, 20,
      None, Seq(2, 4), true),
    ("forward shift deduplicated on the unshifted time",
      "leftTime = rightTime + INTERVAL 10 SECONDS", 20, 10, Some("leftTime"), Seq(2, 4), true),
    ("forward shift deduplicated on the shifted time",
      "leftTime = rightTime + INTERVAL 10 SECONDS", 20, 10, Some("rightTime"), Seq(2, 4), false),
    ("backward shift", "leftTime = rightTime - INTERVAL 10 SECONDS", 10, 20, None, Seq(2, 4),
      false),
    ("shift on both keys", "leftTime + INTERVAL 1 SECOND = rightTime + INTERVAL 11 SECONDS", 20,
      10, None, Seq(2, 4), false)
  ).foreach { case (shape, timeCondition, leftTime, rightTime, deduplicated, versions,
      accepted) =>
    for {
      version <- versions
      joinType <- if (version == 1) Seq("inner") else Seq("inner", "left_outer")
    } {
      val action = if (accepted) "resume" else "reject"
      test(s"$action old $shape equality checkpoint, $joinType, V$version") {
        withJoinConf(version, logVersion = 1) {
          val leftInput = MemoryStream[(Int, Int)]
          val rightInput = MemoryStream[(Int, Int)]
          val left = leftInput.toDF().toDF("leftKey", "time")
            .select($"leftKey", timestamp_seconds($"time").as("leftTime"))
            .withWatermark("leftTime", "5 seconds")
          val right = rightInput.toDF().toDF("rightKey", "time")
            .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
            .withWatermark("rightTime", "5 seconds")
          val joined = left.join(right, expr(s"leftKey = rightKey AND $timeCondition"), joinType)
          val result = deduplicated match {
            case Some(column) =>
              joined.select($"leftKey", col(column).as("t")).dropDuplicates("leftKey", "t")
                .select($"leftKey", $"t".cast("long"))
            case None =>
              joined.select($"leftKey", $"rightKey", $"leftTime".cast("long"),
                $"rightTime".cast("long"))
          }
          def expected(key: Int): Row = deduplicated match {
            case Some("leftTime") => Row(key, leftTime.toLong)
            case Some(_) => Row(key, rightTime.toLong)
            case None => Row(key, key, leftTime.toLong, rightTime.toLong)
          }
          withTempDir { checkpointDir =>
            val checkpoint = checkpointDir.getCanonicalPath
            // Each row waits in state for its match, which arrives after the restart.
            testStream(result)(
              StartStream(checkpointLocation = checkpoint),
              MultiAddData(leftInput, (1, leftTime))(rightInput, (2, rightTime)),
              CheckNewAnswer(),
              StopStream)
            setMarker(checkpoint, None)
            if (accepted) {
              testStream(result)(
                StartStream(checkpointLocation = checkpoint),
                MultiAddData(leftInput, (2, leftTime))(rightInput, (1, rightTime)),
                CheckNewAnswer(expected(1), expected(2)),
                StopStream)
              assert(pinnedMarker(checkpoint).contains("0"))
            } else {
              val stateBefore = snapshot(new File(checkpoint, "state"))
              testStream(result)(
                StartStream(checkpointLocation = checkpoint),
                MultiAddData(leftInput, (2, leftTime))(rightInput, (1, rightTime)),
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
  }

  // Earlier versions stored a row whose eviction column is NULL and never evicted it from V1 to
  // V3 state. Here the first run has no time condition and no watermark, so nothing bounds the
  // state and this version stores the NULL-time left row the same way. After the restart with
  // the time condition and the watermarks, V1 to V3 eviction must remove that row and emit it
  // padded with nulls. A watermark on the right input alone would not do: since SPARK-61347 it
  // already bounds the left state, so the first run would emit the row on arrival.
  Seq(2, 3).foreach { version =>
    test(s"evict a restored left row with a NULL event time, V$version") {
      withJoinConf(version, logVersion = 1) {
        val leftInput = MemoryStream[(Int, Int)]
        val rightInput = MemoryStream[(Int, Int)]
        def joined(firstRun: Boolean): DataFrame = {
          // A negative time is a null event time.
          val left = leftInput.toDF().toDF("leftKey", "time")
            .select($"leftKey", when($"time" < 0, lit(null).cast(TimestampType))
              .otherwise(timestamp_seconds($"time")).as("leftTime"))
          val right = rightInput.toDF().toDF("rightKey", "time")
            .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
          val result = if (firstRun) {
            left.join(right, expr("leftKey = rightKey"))
          } else {
            left.withWatermark("leftTime", "5 seconds")
              .join(right.withWatermark("rightTime", "5 seconds"), expr(intervalCondition),
                "left_outer")
          }
          result.select($"leftKey", $"rightKey", $"leftTime".cast("long"),
            $"rightTime".cast("long"))
        }
        withTempDir { checkpointDir =>
          val checkpoint = checkpointDir.getCanonicalPath
          testStream(joined(firstRun = true))(
            StartStream(checkpointLocation = checkpoint),
            AddData(leftInput, (1, -1), (2, 10)),
            CheckNewAnswer(),
            StopStream)
          testStream(joined(firstRun = false))(
            StartStream(checkpointLocation = checkpoint),
            AddData(rightInput, (2, 12), (3, 30)),
            CheckNewAnswer(Row(2, 2, 10L, 12L), Row(1, null, null, null)),
            StopStream)
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
