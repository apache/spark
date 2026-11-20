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
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.Level

import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.catalyst.analysis.StreamingJoinHelper
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeq, OffsetSeqLog}
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinExec
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper.JoinStateValueWatermarkPredicate
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.util.Utils

@SlowSQLTest
class StreamingJoinRangeEvictionSuite extends StateStoreMetricsTest {
  import testImplicits._

  private def withJoinConf(version: Int)(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false")(f)
  }

  // The left input has the event time leftTime and another TIMESTAMP column expiry. A negative
  // start or end gives a NULL leftTime or expiry.
  private def joinWithExpiry(range: String, joinType: String)
      : (MemoryStream[(Int, Long, Long)], MemoryStream[(Int, Long)], DataFrame) = {
    val leftInput = MemoryStream[(Int, Long, Long)]
    val rightInput = MemoryStream[(Int, Long)]
    val left = leftInput.toDF().toDF("leftKey", "start", "end")
      .select($"leftKey", when($"start" >= 0, timestamp_seconds($"start")).as("leftTime"),
        when($"end" >= 0, timestamp_seconds($"end")).as("expiry"))
      .withWatermark("leftTime", "60 seconds")
    val right = rightInput.toDF().toDF("rightKey", "time")
      .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
      .withWatermark("rightTime", "60 seconds")
    val condition = expr(s"leftKey = rightKey AND $range")
    val joined = if (joinType == "right_outer") {
      right.join(left, condition, joinType)
    } else {
      left.join(right, condition, joinType)
    }
    val result = joined.select($"leftKey", $"rightTime".cast("long").as("matchTime"),
      $"leftTime".cast("long"))
    (leftInput, rightInput, result)
  }

  // State format 3 shares the eviction code of format 2.
  for {
    version <- Seq(2, 4)
    joinType <- Seq("inner", "left_outer", "right_outer")
    shape <- Seq("between", "conjunction", "upper", "interval")
  } {
    test(s"range eviction uses the bounded column, V$version $joinType $shape") {
      withJoinConf(version) {
        val range = shape match {
          case "between" => "rightTime BETWEEN leftTime AND expiry"
          case "conjunction" => "rightTime >= leftTime AND rightTime <= expiry"
          case "interval" => "rightTime BETWEEN leftTime AND leftTime + INTERVAL 10 MINUTES"
          case _ => "rightTime <= expiry"
        }
        val (leftInput, rightInput, result) = joinWithExpiry(range, joinType)
        testStream(result)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 100L, 700L), (2, 100L, 700L)), AddData(rightInput, (9, 100L)))),
          CheckNewAnswer(),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 400L, 1000L)), AddData(rightInput, (9, 400L)))),
          CheckNewAnswer(),
          AddData(rightInput, (9, 400L)),
          // The watermark has passed the start time, but the range has not expired.
          CheckNewAnswer(),
          Execute { query =>
            val join = query.lastExecution.executedPlan.collectFirst {
              case j: StreamingSymmetricHashJoinExec => j
            }.get
            assert(join.eventTimeWatermarkForEviction.contains(340000L))
          },
          AddData(rightInput, (1, 460L)),
          CheckNewAnswer(Row(1, 460L, 100L)),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 1000L, 1600L)), AddData(rightInput, (9, 1000L)))),
          CheckNewAnswer(),
          AddData(rightInput, (9, 1100L)),
          CheckNewAnswer((if (joinType != "inner") Seq(Row(2, null, 100L)) else Nil): _*)
        )
      }
    }
  }

  for {
    version <- Seq(2, 3, 4)
    joinType <- Seq("left_outer", "right_outer")
  } {
    test(s"range eviction includes newly admitted expired values, V$version $joinType") {
      withJoinConf(version) {
        val (leftInput, rightInput, result) = joinWithExpiry("rightTime <= expiry", joinType)
        testStream(result)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 100L, 700L)), AddData(rightInput, (9, 100L)))),
          CheckNewAnswer(),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 400L, 1000L)), AddData(rightInput, (9, 400L)))),
          CheckNewAnswer(),
          AddData(rightInput, (9, 400L)), CheckNewAnswer(),
          // The start passes late filtering, but expiry is below the previous eviction cutoff.
          AddData(leftInput, (3, 460L, 200L)), CheckNewAnswer(Row(3, null, 460L)),
          AddData(rightInput, (9, 500L)), CheckNewAnswer())
      }
    }
  }

  for {
    version <- Seq(2, 4)
    joinType <- Seq("left_outer", "right_outer")
  } {
    test(s"a row whose range-eviction column is NULL is emitted on arrival, V$version $joinType") {
      withJoinConf(version) {
        val (leftInput, rightInput, result) =
          joinWithExpiry("rightTime BETWEEN leftTime AND expiry", joinType)
        testStream(result)(
          // The row can never match, so it is not stored, although the next eviction would remove
          // it.
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 100L, -1L), (2, 100L, 700L)), AddData(rightInput, (2, 150L)))),
          CheckNewAnswer(Row(1, null, 100L), Row(2, 150L, 100L)),
          assertNumStateRows(total = 2, updated = 2))
      }
    }
  }

  for (version <- Seq(2, 4)) {
    test(s"a row whose event time is NULL is emitted on arrival, V$version") {
      withJoinConf(version) {
        val (leftInput, rightInput, result) = joinWithExpiry(
          "rightTime BETWEEN leftTime AND leftTime + INTERVAL 10 MINUTES", "left_outer")
        testStream(result)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, -1L, 700L), (2, 100L, 700L)), AddData(rightInput, (2, 150L)))),
          CheckNewAnswer(Row(1, null, null), Row(2, 150L, 100L)),
          assertNumStateRows(total = 2, updated = 2))
      }
    }
  }

  // The condition bounds the left input only through leftDate cast to TIMESTAMP, so the left
  // input is evicted by leftTime with the cutoff from leftDate, as in earlier versions. A negative
  // start gives a NULL leftTime, which the condition does not reference.
  private def joinWithDate(joinType: String)
      : (MemoryStream[(Int, Long, Int)], MemoryStream[(Int, Long)], DataFrame) = {
    val leftInput = MemoryStream[(Int, Long, Int)]
    val rightInput = MemoryStream[(Int, Long)]
    val left = leftInput.toDF().toDF("leftKey", "start", "day")
      .select($"leftKey", when($"start" >= 0, timestamp_seconds($"start")).as("leftTime"),
        date_add(lit("1970-01-01").cast("date"), $"day").as("leftDate"))
      .withWatermark("leftTime", "60 seconds")
    val right = rightInput.toDF().toDF("rightKey", "time")
      .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
      .withWatermark("rightTime", "60 seconds")
    val result = left.join(right,
      expr("leftKey = rightKey AND CAST(leftDate AS TIMESTAMP) >= rightTime"), joinType)
      .select($"leftKey", $"rightTime".cast("long").as("matchTime"), $"leftTime".cast("long"))
    (leftInput, rightInput, result)
  }

  for {
    version <- Seq(2, 4)
    joinType <- Seq("inner", "left_outer")
  } {
    test(s"an input bounded only through a cast column keeps the earlier eviction, " +
        s"V$version $joinType") {
      withJoinConf(version) {
        val (leftInput, rightInput, result) = joinWithDate(joinType)
        testStream(result)(
          // The left row with a NULL leftTime and a leftDate of one day matches.
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, -1L, 1), (2, 100L, 0)), AddData(rightInput, (1, 100L)))),
          CheckNewAnswer(Row(1, 100L, null)),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 400L, 0)), AddData(rightInput, (1, 400L)))),
          CheckNewAnswer(Row(1, 400L, null)),
          // The cutoff on leftTime is 340 s. State format 4 indexed the NULL leftTime as 0 and
          // evicted it in the last batch.
          AddData(rightInput, (1, 500L)),
          CheckNewAnswer(
            (if (version == 2) Seq(Row(1, 500L, null)) else Nil) ++
              (if (joinType == "left_outer") Seq(Row(2, null, 100L)) else Nil): _*))
      }
    }
  }

  // A left semi join processes its right input first. The right row and the left row of the
  // second batch match, and lie between the late-events watermark (0) and the eviction watermark
  // (10 h). The cutoff over the whole right input is 7 h, from rx, so the right row is stored.
  for (version <- Seq(2, 4)) {
    test(s"left semi join keeps a right row that the whole-input cutoff keeps, V$version") {
      withJoinConf(version) {
        val leftInput = MemoryStream[(Int, Long)]
        val rightInput = MemoryStream[(Int, Long, Long)]
        val left = leftInput.toDF().toDF("k", "t")
          .select($"k", timestamp_seconds($"t").as("lt"))
          .withWatermark("lt", "0 seconds")
        val right = rightInput.toDF().toDF("rk", "e", "x")
          .select($"rk", timestamp_seconds($"e").as("re"), timestamp_seconds($"x").as("rx"))
          .withWatermark("re", "0 seconds")
        val joined = left.join(right, expr("k = rk AND lt BETWEEN re - INTERVAL 1 HOUR AND re " +
          "AND rx >= lt - INTERVAL 3 HOURS"), "left_semi")
          .select($"k", $"lt".cast("long"))
        testStream(joined)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (9, 36000L)), AddData(rightInput, (9, 36000L, 36000L)))),
          CheckNewAnswer(Row(9, 36000L)),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 27000L)), AddData(rightInput, (1, 28800L, 28800L)))),
          CheckNewAnswer(Row(1, 27000L)))
      }
    }
  }

  // The legacy mode stores rows by the earlier rule: it evicts by the event time, with the cutoff
  // from expiry, and keeps a row whose event time is NULL.
  for (version <- Seq(2, 4)) {
    test(s"range eviction in the legacy mode keeps the earlier behavior, V$version") {
      withJoinConf(version) {
        withSQLConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> "false") {
          val (leftInput, rightInput, result) =
            joinWithExpiry("rightTime BETWEEN leftTime AND expiry", "left_outer")
          testStream(result)(
            StreamProgressLockedActions(Seq(
              AddData(leftInput, (1, 100L, 700L), (2, 100L, 700L), (3, -1L, 700L)),
              AddData(rightInput, (9, 100L)))),
            CheckNewAnswer(),
            assertNumStateRows(total = 4, updated = 4),
            StreamProgressLockedActions(Seq(
              AddData(leftInput, (8, 400L, 1000L)), AddData(rightInput, (9, 400L)))),
            // State format 4 indexes a NULL event time as 0.
            CheckNewAnswer((if (version == 4) Seq(Row(3, null, null)) else Nil): _*),
            AddData(rightInput, (9, 400L)),
            CheckNewAnswer(Row(1, null, 100L), Row(2, null, 100L)),
            AddData(rightInput, (1, 460L)),
            CheckNewAnswer())
        }
      }
    }
  }

  // An earlier version, like the legacy mode, stored a row whose expiry is NULL and evicted it by
  // leftTime.
  for (version <- Seq(2, 3)) {
    test(s"a stored row whose range-eviction column is NULL is evicted, V$version") {
      withJoinConf(version) {
        withTempDir { checkpoint =>
          val (leftInput, rightInput, result) =
            joinWithExpiry("rightTime BETWEEN leftTime AND expiry", "left_outer")
          withSQLConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> "false") {
            testStream(result)(
              StartStream(checkpointLocation = checkpoint.getCanonicalPath),
              StreamProgressLockedActions(Seq(
                AddData(leftInput, (1, 100L, -1L)), AddData(rightInput, (9, 100L)))),
              CheckNewAnswer(),
              StreamProgressLockedActions(Seq(
                AddData(leftInput, (8, 400L, 1000L)), AddData(rightInput, (9, 400L)))),
              CheckNewAnswer(),
              StopStream)
          }
          testStream(result)(
            StartStream(checkpointLocation = checkpoint.getCanonicalPath),
            AddData(rightInput, (9, 400L)),
            CheckNewAnswer(Row(1, null, 100L)))
        }
      }
    }
  }

  // In state format 4, the extra conjunct gives the right side a value predicate on rightTime, so
  // that the range scan of the right joiner solves the bound that shifts by a column.
  for {
    bound <- Seq("leftTime > rightTime + rightDelay",
      "CAST(leftTime AS BIGINT) + leftOffset > CAST(rightTime AS BIGINT)")
    version <- Seq(2, 4)
  } {
    test(s"range eviction does not log solver errors for other columns: $bound, V$version") {
      val logAppender = new LogAppender("stream-stream join state constraints")
      withLogAppender(logAppender,
          loggerNames = Seq(StreamingJoinHelper.getClass.getName.stripSuffix("$")),
          level = Some(Level.WARN)) {
        withJoinConf(version) {
          val leftInput = MemoryStream[(Int, Long, Long)]
          val rightInput = MemoryStream[(Int, Long, Int)]
          val left = leftInput.toDF().toDF("leftKey", "time", "leftOffset")
            .select($"leftKey", timestamp_seconds($"time").as("leftTime"), $"leftOffset")
            .withWatermark("leftTime", "0 seconds")
          val right = rightInput.toDF().toDF("rightKey", "time", "delay")
            .select($"rightKey", timestamp_seconds($"time").as("rightTime"),
              ($"delay" * expr("INTERVAL 1 SECOND")).as("rightDelay"))
            .withWatermark("rightTime", "0 seconds")
          val extra = if (version == 4) " AND leftTime < rightTime + INTERVAL 1 HOUR" else ""
          val joined = left.join(right, expr(s"leftKey = rightKey AND $bound$extra"))
            .select($"leftKey", $"leftTime".cast("long"), $"rightTime".cast("long"))
          testStream(joined)(
            StreamProgressLockedActions(Seq(
              AddData(leftInput, (1, 20L, 0L)), AddData(rightInput, (1, 10L, 5)))),
            CheckNewAnswer(Row(1, 20L, 10L)),
            AddData(rightInput, (2, 30L, 1)),
            CheckNewAnswer())
        }
      }
      // A per-row column leaves the bound unknown, which is not an error.
      assert(!logAppender.loggingEvents.exists(
        _.getMessage.getFormattedMessage.contains("Error trying to extract state constraint")))
    }
  }

  private val indexVersionKey = SQLConf.STREAMING_JOIN_STATE_V4_INDEX_VERSION.key

  private def indexVersion(checkpoint: File): Option[String] = {
    new OffsetSeqLog(spark, new File(checkpoint, "offsets").getCanonicalPath).getLatest().get._2
      .metadataOpt.get.conf.get(indexVersionKey)
  }

  // Removes the V4 index version from the latest offset log entry, as older versions wrote it.
  private def removeIndexVersion(checkpoint: File): Unit = {
    val offsetsPath = new File(checkpoint, "offsets").getCanonicalPath
    val log = new OffsetSeqLog(spark, offsetsPath)
    val (batchId, offsets) = log.getLatest().get
    val updated = offsets match {
      case o: OffsetSeq =>
        o.copy(metadataOpt = o.metadataOpt.map(m => m.copy(conf = m.conf - indexVersionKey)))
      case o: OffsetMap =>
        o.copy(metadata = o.metadata.copy(conf = o.metadata.conf - indexVersionKey))
    }
    val path = new Path(offsetsPath, batchId.toString)
    assert(path.getFileSystem(spark.sessionState.newHadoopConf()).delete(path, false))
    assert(log.add(batchId, updated))
  }

  // Copies a bundled checkpoint and points the copied operator metadata at the state schema files
  // of the copy. The bundled metadata names them under the placeholder directory /checkpoint.
  private def copyCheckpointFixture(name: String, checkpoint: File): Unit = {
    Utils.copyDirectory(
      new File(getClass.getResource(s"/structured-streaming/$name/").toURI), checkpoint)
    val metadataDir = new File(checkpoint, "state/0/_metadata/v2")
    metadataDir.listFiles().filter(f => f.isFile && !f.getName.startsWith(".")).foreach { file =>
      val metadata = new String(Files.readAllBytes(file.toPath), UTF_8)
      val updated = metadata.replace("file:/checkpoint/", new Path(checkpoint.toURI).toString + "/")
      assert(updated != metadata, s"No state schema file is named in $file")
      Files.write(file.toPath, updated.getBytes(UTF_8))
    }
  }

  test("V4 restart from an older checkpoint keeps evicting by the event time") {
    withJoinConf(4) {
      withTempDir { checkpoint =>
        val (leftInput, rightInput, result) = joinWithExpiry(
          "rightTime BETWEEN leftTime AND leftTime + INTERVAL 10 MINUTES", "left_outer")
        testStream(result)(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 100L, 700L), (2, 100L, 700L)), AddData(rightInput, (9, 100L)))),
          CheckNewAnswer(),
          StopStream)
        assert(indexVersion(checkpoint) === Some("1"))
        removeIndexVersion(checkpoint)
        testStream(result)(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          AddData(rightInput, (1, 160L)),
          CheckNewAnswer(Row(1, 160L, 100L)),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 1000L, 1600L)), AddData(rightInput, (9, 1000L)))),
          CheckNewAnswer(),
          // The cutoff on leftTime is 340 s.
          AddData(rightInput, (9, 1000L)),
          CheckNewAnswer(Row(2, null, 100L)),
          StopStream)
        assert(indexVersion(checkpoint) === Some("0"))
      }
    }
  }

  // The legacy mode orders the state by the event time.
  for (legacyFirst <- Seq(true, false)) {
    test("V4 restart fails when the legacy mode changes and the condition bounds another " +
        s"column, legacy first=$legacyFirst") {
      withJoinConf(4) {
        withTempDir { checkpoint =>
          val (leftInput, rightInput, result) =
            joinWithExpiry("rightTime BETWEEN leftTime AND expiry", "left_outer")
          withSQLConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> (!legacyFirst).toString) {
            testStream(result)(
              StartStream(checkpointLocation = checkpoint.getCanonicalPath),
              StreamProgressLockedActions(Seq(
                AddData(leftInput, (1, 100L, 700L)), AddData(rightInput, (9, 100L)))),
              CheckNewAnswer(),
              StopStream)
          }
          assert(indexVersion(checkpoint) === Some(if (legacyFirst) "0" else "1"))
          withSQLConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> legacyFirst.toString) {
            testStream(result)(
              StartStream(checkpointLocation = checkpoint.getCanonicalPath),
              AddData(rightInput, (9, 400L)),
              ExpectFailure[AnalysisException] { e =>
                checkError(
                  e.asInstanceOf[AnalysisException],
                  condition = "STREAMING_JOIN_STATE_V4_CHECKPOINT_INCOMPATIBLE",
                  parameters = Map("side" -> "left", "column" -> "`expiry`"))
              })
          }
          // The failed restart keeps the recorded version.
          assert(indexVersion(checkpoint) === Some(if (legacyFirst) "0" else "1"))
        }
      }
    }
  }

  // The legacy mode, like earlier versions, evicted left rows only up to the lower cutoff from
  // expiry, so the V4 scan after the restart must start at or below that cutoff.
  test("V4 restart evicts rows between the earlier and the new cutoff") {
    withJoinConf(4) {
      withTempDir { checkpoint =>
        val (leftInput, rightInput, result) = joinWithExpiry("leftTime >= " +
          "rightTime - INTERVAL 10 SECONDS AND expiry >= rightTime - INTERVAL 100 SECONDS",
          "left_outer")
        withSQLConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> "false") {
          testStream(result)(
            StartStream(checkpointLocation = checkpoint.getCanonicalPath),
            StreamProgressLockedActions(Seq(
              AddData(leftInput, (1, 900L, 5000L), (8, 1000L, 5000L)),
              AddData(rightInput, (9, 1000L)))),
            CheckNewAnswer(),
            // The watermark is 940 s, and the cutoff from expiry keeps leftTime 900 s.
            AddData(rightInput, (9, 1000L)),
            CheckNewAnswer(),
            StopStream)
        }
        testStream(result)(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          AddData(rightInput, (9, 1300L)),
          CheckNewAnswer(Row(1, null, 900L)))
      }
    }
  }

  // Within a run, the V4 scan of the left input starts at the event time's own cutoff, which is
  // above the cutoff from expiry.
  test("V4 range eviction by the event time over several batches of a run") {
    withJoinConf(4) {
      val (leftInput, rightInput, result) = joinWithExpiry("leftTime >= " +
        "rightTime - INTERVAL 10 SECONDS AND expiry >= rightTime - INTERVAL 100 SECONDS",
        "left_outer")
      testStream(result)(
        StreamProgressLockedActions(Seq(
          AddData(leftInput, (1, 900L, 5000L), (8, 1000L, 5000L)),
          AddData(rightInput, (9, 1000L)))),
        CheckNewAnswer(),
        // The watermark is 940 s, so the cutoff on leftTime is 930 s.
        AddData(rightInput, (9, 1000L)),
        CheckNewAnswer(Row(1, null, 900L)),
        StreamProgressLockedActions(Seq(
          AddData(leftInput, (2, 1200L, 5000L)), AddData(rightInput, (9, 1300L)))),
        CheckNewAnswer(),
        // The late-events watermark is 940 s. This batch scans the left input from the cutoff on
        // leftTime at it, not from the lower cutoff from expiry.
        Execute { q =>
          val join = q.lastExecution.executedPlan.collectFirst {
            case j: StreamingSymmetricHashJoinExec => j
          }.get
          assert(join.stateWatermarkPredicates.left.get
            .asInstanceOf[JoinStateValueWatermarkPredicate].prevStateWatermark === Some(929999L))
        },
        // The watermark is 1140 s, so the cutoff on leftTime is 1130 s.
        AddData(rightInput, (2, 1300L)),
        CheckNewAnswer(Row(8, null, 1000L)),
        AddData(rightInput, (9, 1300L)),
        CheckNewAnswer())
    }
  }

  // A legacy-mode restart of a checkpoint that orders the state by the event time works.
  test("V4 restart in the legacy mode of a join evicted by the event time") {
    withJoinConf(4) {
      withTempDir { checkpoint =>
        val (leftInput, rightInput, result) = joinWithExpiry(
          "rightTime BETWEEN leftTime AND leftTime + INTERVAL 10 MINUTES", "left_outer")
        testStream(result)(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 100L, 700L), (2, 100L, 700L)), AddData(rightInput, (9, 100L)))),
          CheckNewAnswer(),
          StopStream)
        assert(indexVersion(checkpoint) === Some("1"))
        withSQLConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> "false") {
          testStream(result)(
            StartStream(checkpointLocation = checkpoint.getCanonicalPath),
            AddData(rightInput, (1, 160L)),
            CheckNewAnswer(Row(1, 160L, 100L)),
            StreamProgressLockedActions(Seq(
              AddData(leftInput, (8, 1000L, 1600L)), AddData(rightInput, (9, 1000L)))),
            CheckNewAnswer(),
            AddData(rightInput, (9, 1000L)),
            CheckNewAnswer(Row(2, null, 100L)))
        }
      }
    }
  }

  test("V4 restart in the legacy mode fails for the right input that is evicted by another " +
      "column") {
    withJoinConf(4) {
      withTempDir { checkpoint =>
        val (leftInput, rightInput, result) =
          joinWithExpiry("rightTime BETWEEN leftTime AND expiry", "right_outer")
        testStream(result)(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 100L, 700L)), AddData(rightInput, (9, 100L)))),
          CheckNewAnswer(),
          StopStream)
        withSQLConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> "false") {
          testStream(result)(
            StartStream(checkpointLocation = checkpoint.getCanonicalPath),
            AddData(rightInput, (9, 400L)),
            ExpectFailure[AnalysisException] { e =>
              checkError(
                e.asInstanceOf[AnalysisException],
                condition = "STREAMING_JOIN_STATE_V4_CHECKPOINT_INCOMPATIBLE",
                parameters = Map("side" -> "right", "column" -> "`expiry`"))
            })
        }
      }
    }
  }

  // Only state format 4 orders the state by the column, and the check covers both inputs.
  for ((version, joinType) <- Seq((2, "left_outer"), (4, "right_outer"))) {
    test(s"restart from a checkpoint without the index version, V$version $joinType") {
      withJoinConf(version) {
        withTempDir { checkpoint =>
          val (leftInput, rightInput, result) =
            joinWithExpiry("rightTime BETWEEN leftTime AND expiry", joinType)
          testStream(result)(
            StartStream(checkpointLocation = checkpoint.getCanonicalPath),
            StreamProgressLockedActions(Seq(
              AddData(leftInput, (1, 100L, 700L)), AddData(rightInput, (9, 100L)))),
            CheckNewAnswer(),
            StopStream)
          removeIndexVersion(checkpoint)
          testStream(result)(
            StartStream(checkpointLocation = checkpoint.getCanonicalPath),
            AddData(rightInput, (1, 460L)),
            if (version == 4) {
              ExpectFailure[AnalysisException] { e =>
                checkError(
                  e.asInstanceOf[AnalysisException],
                  condition = "STREAMING_JOIN_STATE_V4_CHECKPOINT_INCOMPATIBLE",
                  parameters = Map("side" -> "right", "column" -> "`expiry`"))
              }
            } else {
              CheckNewAnswer(Row(1, 460L, 100L))
            })
        }
      }
    }
  }

  test("V4 restart from an older checkpoint fails when the condition bounds another column") {
    withJoinConf(4) {
      withTempDir { checkpoint =>
        // A version before this change, with the stream-stream join code of Spark 4.3, wrote this
        // checkpoint from the batches below.
        copyCheckpointFixture("checkpoint-streaming-join-v4-range-on-expiry", checkpoint)
        val (leftInput, rightInput, result) =
          joinWithExpiry("rightTime BETWEEN leftTime AND expiry", "left_outer")
        leftInput.addData((1, 100L, 700L), (2, 100L, 700L))
        rightInput.addData((9, 100L))
        leftInput.addData((8, 400L, 1000L))
        rightInput.addData((9, 400L))
        testStream(result)(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          AddData(rightInput, (9, 400L)),
          ExpectFailure[AnalysisException] { e =>
            checkError(
              e.asInstanceOf[AnalysisException],
              condition = "STREAMING_JOIN_STATE_V4_CHECKPOINT_INCOMPATIBLE",
              parameters = Map("side" -> "left", "column" -> "`expiry`"))
          })
      }
    }
  }
}
