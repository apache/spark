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

import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeq, OffsetSeqLog}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

/**
 * Range conditions of stream-stream joins only bound the state of one input when they can be
 * rearranged to `stateTime > otherTime + c`, where otherTime is the other input's watermark.
 */
class StreamingJoinRangeCutoffSuite extends StreamTest {
  import testImplicits._

  private def withJoinStateVersion(version: Int)(body: => Unit): Unit = {
    withSQLConf(
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
        SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
      body
    }
  }

  private def inputs(): (MemoryStream[(Int, Long)], MemoryStream[(Int, Long)], DataFrame,
      DataFrame) = {
    val leftInput = MemoryStream[(Int, Long)]
    val rightInput = MemoryStream[(Int, Long)]
    val left = leftInput.toDF().toDF("leftKey", "time")
      .select($"leftKey", timestamp_seconds($"time").as("lt"))
      .withWatermark("lt", "1 second")
    val right = rightInput.toDF().toDF("rightKey", "time")
      .select($"rightKey", timestamp_seconds($"time").as("rt"))
      .withWatermark("rt", "1 second")
    (leftInput, rightInput, left, right)
  }

  // In America/Los_Angeles, the day after 2026-10-31 12:00 PDT lasts 25 hours, but the solver reads
  // it as 24, so the constant-only conjunct keeps capping the cutoff. State format 4 probes the
  // same 24 hours, as in earlier versions, so the test covers formats 2 and 3.
  Seq(2, 3).foreach { version =>
    test(s"constant-only conjunct caps a cutoff through a day across a daylight-saving change, " +
        s"V$version") {
      withJoinStateVersion(version) {
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
          val (leftInput, rightInput, left, right) = inputs()
          val joined = left.join(right, expr("leftKey = rightKey AND rt >= lt AND " +
              "rt <= lt + INTERVAL 1 DAY AND CAST(lt AS LONG) > 1700000000"), "left_outer")
            .select($"leftKey", $"lt".cast("long"), $"rt".cast("long"))
          // 2026-10-31 12:00 PDT, 2026-11-01 11:30:01 PST and 2026-11-01 11:45 PST.
          val (start, later, end) = (1793473200L, 1793561401L, 1793562300L)
          testStream(joined)(
            StreamProgressLockedActions(Seq(
              AddData(leftInput, (1, start)), AddData(rightInput, (9, start)))),
            CheckNewAnswer(),
            StreamProgressLockedActions(Seq(
              AddData(leftInput, (8, later)), AddData(rightInput, (9, later)))),
            CheckNewAnswer(),
            // The watermark is now 11:30 PST. Read as 24 hours, the day after the left row ends
            // at 11:00 PST, but it ends at 12:00 PST.
            AddData(rightInput, (9, later)),
            CheckNewAnswer(),
            AddData(rightInput, (1, end)),
            CheckNewAnswer(Row(1, start, end)))
        }
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

  private val negativeSign = "leftKey = rightKey AND CAST(lt AS LONG) + CAST(rt AS LONG) > 100"

  // A checkpoint written before this change keeps the rule of earlier versions: the outer join is
  // accepted at the restart, and the negative-sign conjunct gives the left input the cutoff
  // 100 s minus the watermark. It evicts the left row (8, 21) at the watermark 4 s, so the later
  // right row (8, 80) finds no match, as in earlier versions.
  for (version <- Seq(2, 4); joinType <- Seq("inner", "left_outer")) {
    test(s"a restart from a checkpoint of an earlier version keeps the earlier cutoffs, " +
        s"V$version $joinType") {
      withJoinStateVersion(version) {
        withTempDir { checkpoint =>
          val (leftInput, rightInput, left, right) = inputs()
          val joined = left.join(right, expr(negativeSign), joinType)
            .select($"leftKey", $"lt".cast("long"), $"rt".cast("long"))
          val start = StartStream(checkpointLocation = checkpoint.getCanonicalPath)
          // The rule of earlier versions writes the checkpoint, as an earlier version would.
          def outer(rows: Row*): Seq[Row] = if (joinType == "inner") Nil else rows
          withSQLConf(SQLConf.STATEFUL_OPERATOR_CHECK_JOIN_OUTPUT_WATERMARK.key -> "false") {
            testStream(joined)(start,
              StreamProgressLockedActions(Seq(
                AddData(leftInput, (1, 5L)), AddData(rightInput, (9, 5L)))),
              CheckNewAnswer(outer(Row(1, 5L, null)): _*),
              StopStream)
          }
          removeCheckJoinOutputWatermark(checkpoint)
          testStream(joined)(start,
            StreamProgressLockedActions(Seq(
              AddData(leftInput, (8, 21L)), AddData(rightInput, (9, 21L)))),
            CheckNewAnswer(outer(Row(8, 21L, null)): _*),
            AddData(rightInput, (8, 80L)),
            CheckNewAnswer())
        }
      }
    }
  }

  for (version <- Seq(2, 4)) {
    test(s"a new outer join whose only range condition has a negative sign fails analysis, " +
        s"V$version") {
      withJoinStateVersion(version) {
        val (_, _, left, right) = inputs()
        intercept[AnalysisException] {
          left.join(right, expr(negativeSign), "left_outer").writeStream.format("noop").start()
        }
      }
    }
  }

  Seq(2, 3, 4).foreach { version =>
    test(s"negative watermark term does not evict state that can still match, V$version") {
      withJoinStateVersion(version) {
        val (leftInput, rightInput, left, right) = inputs()
        val joined = left
          .join(right, expr("leftKey = rightKey AND CAST(lt AS LONG) + CAST(rt AS LONG) > 100"))
          .select($"leftKey", $"lt".cast("long"), $"rt".cast("long"))

        testStream(joined)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 5L)), AddData(rightInput, (9, 5L)))),
          CheckNewAnswer(),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 21L)), AddData(rightInput, (9, 21L)))),
          CheckNewAnswer(),
          AddData(rightInput, (9, 22L)),
          CheckNewAnswer(),
          // The watermark is past 5, but a later rt still matches lt = 5.
          AddData(rightInput, (1, 100L)),
          CheckNewAnswer(Row(1, 5L, 100L)))
      }
    }

    test(s"left semi join whose only range condition is constant-only, V$version") {
      withJoinStateVersion(version) {
        val (leftInput, rightInput, left, right) = inputs()
        val joined = left
          .join(right, expr("leftKey = rightKey AND CAST(lt AS LONG) > 10"), "left_semi")
          .select($"leftKey", $"lt".cast("long"))

        testStream(joined)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 20L), (2, 20L)), AddData(rightInput, (1, 30L)))),
          CheckNewAnswer(Row(1, 20L)),
          StopStream,
          StartStream(),
          // The condition gives no cutoff, so the left row (2, 20) is still in state.
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 60L)), AddData(rightInput, (2, 60L), (1, 60L)))),
          CheckNewAnswer(Row(2, 20L)))
      }
    }

    // The left input is evicted by its event time with a cutoff solved for another column: through
    // a TIMESTAMP_NTZ column in the default mode, or through expiry in the legacy mode. The
    // constant-only conjunct still caps that cutoff, as in earlier versions.
    Seq(false, true).foreach { legacyMode =>
      test(s"constant-only conjunct caps a cutoff solved for another column, V$version, " +
          s"legacy mode $legacyMode") {
        withJoinStateVersion(version) {
          withSQLConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> (!legacyMode).toString) {
            val (leftInput, rightInput, input, right) = inputs()
            val left = input.select($"leftKey", $"lt",
              (expr("lt + INTERVAL 1000 SECONDS")).cast("timestamp_ntz").as("lntz"),
              expr("lt + INTERVAL 10 MINUTES").as("lexp"))
            val (bound, start, later) = if (legacyMode) {
              ("rt BETWEEN lt AND lexp", 100L, 460L)
            } else {
              ("CAST(lntz AS TIMESTAMP) > rt - INTERVAL 5 SECONDS", 20L, 420L)
            }
            val joined = left
              .join(right, expr(s"leftKey = rightKey AND $bound AND CAST(lt AS LONG) > 10"),
                "left_outer")
              .select($"leftKey", $"lt".cast("long"), $"rt".cast("long"))
            testStream(joined)(
              StreamProgressLockedActions(Seq(
                AddData(leftInput, (1, start), (8, 400L)), AddData(rightInput, (9, 400L)))),
              CheckNewAnswer(),
              // The watermark is 399 s. The constant keeps the left row (1, start) in state.
              AddData(rightInput, (9, 400L)),
              CheckNewAnswer(),
              AddData(rightInput, (1, later)),
              CheckNewAnswer(Row(1, start, later)))
          }
        }
      }
    }

    test(s"constant-only conjunct caps a cutoff through a truncating cast, V$version") {
      withJoinStateVersion(version) {
        val leftInput = MemoryStream[(Int, Double)]
        val rightInput = MemoryStream[(Int, Double)]
        val left = leftInput.toDF().toDF("leftKey", "time")
          .select($"leftKey", timestamp_seconds($"time").as("lt"))
          .withWatermark("lt", "1 second")
        val right = rightInput.toDF().toDF("rightKey", "time")
          .select($"rightKey", timestamp_seconds($"time").as("rt"))
          .withWatermark("rt", "1 second")
        val condition = expr("leftKey = rightKey AND " +
          "CAST(lt AS LONG) >= CAST(rt AS LONG) - 5 AND CAST(lt AS LONG) > 10")
        val joined = left.join(right, condition, "left_outer")
          .select($"leftKey", $"lt".cast("double"), $"rt".cast("double"))

        testStream(joined)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 14.2)), AddData(rightInput, (9, 14.2)))),
          CheckNewAnswer(),
          // The watermark becomes 19.5 s. The cast bound alone would give a cutoff of about
          // 14.5 s, but CAST(14.2) = 14 >= CAST(19.6) - 5 = 14 still matches.
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 20.5)), AddData(rightInput, (9, 20.5)))),
          CheckNewAnswer(),
          AddData(rightInput, (9, 20.6)),
          CheckNewAnswer(),
          AddData(rightInput, (1, 19.6)),
          CheckNewAnswer(Row(1, 14.2, 19.6)))
      }
    }

    // The default mode would evict the left input by expiry, whose cutoff goes through a cast to a
    // whole number and so lacks the cap of the conjunct on the event time. The input keeps the
    // eviction by the event time with the earlier minimum.
    Seq(("left_outer", "CAST(lt AS LONG) > 10", 0.0),
        ("inner", "CAST(lt AS LONG) + CAST(rt AS LONG) > 100", 100.0)).foreach {
      case (joinType, cap, shift) =>
        test(s"$cap caps a cutoff through a truncating cast on another column, V$version " +
            s"$joinType") {
          withJoinStateVersion(version) {
            val leftInput = MemoryStream[(Int, Double, Double)]
            val rightInput = MemoryStream[(Int, Double)]
            val left = leftInput.toDF().toDF("leftKey", "time", "end")
              .select($"leftKey", timestamp_seconds($"time").as("lt"),
                timestamp_seconds($"end").as("expiry"))
              .withWatermark("lt", "1 second")
            val right = rightInput.toDF().toDF("rightKey", "time")
              .select($"rightKey", timestamp_seconds($"time").as("rt"))
              .withWatermark("rt", "1 second")
            val condition = expr(
              s"leftKey = rightKey AND CAST(expiry AS LONG) >= CAST(rt AS LONG) AND $cap")
            val joined = left.join(right, condition, joinType)
              .select($"leftKey", $"lt".cast("double"), $"rt".cast("double"))
            testStream(joined)(
              StreamProgressLockedActions(Seq(
                AddData(leftInput, (1, 14.2 + shift, 19.2 + shift)),
                AddData(rightInput, (9, 14.2 + shift)))),
              CheckNewAnswer(),
              // The watermark becomes 19.5 s after the shift. The cutoff from expiry alone would
              // be about the same, but CAST(19.2) = 19 >= CAST(19.6) = 19 still matches.
              StreamProgressLockedActions(Seq(
                AddData(leftInput, (8, 20.5 + shift, 20.5 + shift)),
                AddData(rightInput, (9, 20.5 + shift)))),
              CheckNewAnswer(),
              AddData(rightInput, (9, 20.6 + shift)),
              CheckNewAnswer(),
              AddData(rightInput, (1, 19.6 + shift)),
              CheckNewAnswer(Row(1, 14.2 + shift, 19.6 + shift)))
          }
        }
    }

    // DECIMAL(10, 0) rounds half up, so 39.3 and 39.45 both become 39.
    test(s"constant-only conjunct caps a cutoff through a rounding cast, V$version") {
      withJoinStateVersion(version) {
        val leftInput = MemoryStream[(Int, Double)]
        val rightInput = MemoryStream[(Int, Double)]
        val left = leftInput.toDF().toDF("leftKey", "time")
          .select($"leftKey", timestamp_seconds($"time").as("lt"))
          .withWatermark("lt", "1 second")
        val right = rightInput.toDF().toDF("rightKey", "time")
          .select($"rightKey", timestamp_seconds($"time").as("rt"))
          .withWatermark("rt", "1 second")
        val condition = expr("leftKey = rightKey AND " +
          "CAST(lt AS DECIMAL(10, 0)) >= CAST(rt AS DECIMAL(10, 0)) AND CAST(lt AS LONG) > 10")
        val joined = left.join(right, condition, "left_outer")
          .select($"leftKey", $"lt".cast("double"), $"rt".cast("double"))

        testStream(joined)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 39.3)), AddData(rightInput, (9, 39.3)))),
          CheckNewAnswer(),
          // The watermark becomes 39.4 s, above lt = 39.3 s.
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 40.4)), AddData(rightInput, (9, 40.4)))),
          CheckNewAnswer(),
          AddData(rightInput, (9, 40.5)),
          CheckNewAnswer(),
          AddData(rightInput, (1, 39.45)),
          CheckNewAnswer(Row(1, 39.3, 39.45)))
      }
    }

    // A left semi join processes its right input first. The negative-sign conjunct kept the
    // earlier cutoff low, so the right row (1, 1200) was stored for the left row of its batch.
    test(s"left semi join keeps a right row that a negative-sign conjunct kept, V$version") {
      withJoinStateVersion(version) {
        val (leftInput, rightInput, left, right) = inputs()
        val joined = left.join(right, expr("leftKey = rightKey AND rt BETWEEN " +
            "lt - INTERVAL 10 MINUTES AND lt + INTERVAL 10 MINUTES AND " +
            "CAST(lt AS LONG) + CAST(rt AS LONG) > 100"), "left_semi")
          .select($"leftKey", $"lt".cast("long"))

        testStream(joined)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (9, 1000L)), AddData(rightInput, (9, 1000L)))),
          CheckNewAnswer(Row(9, 1000L)),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 2000L)), AddData(rightInput, (8, 2000L)))),
          CheckNewAnswer(Row(8, 2000L)),
          StreamProgressLockedActions(Seq(
            AddData(rightInput, (1, 1200L)), AddData(leftInput, (1, 1300L)))),
          CheckNewAnswer(Row(1, 1300L)))
      }
    }

    // Neither input gets a cutoff, so the left state keeps (1, 200). Its state watermark of
    // earlier versions, from the negative-sign conjunct, still holds back the join's output
    // watermark for the deduplication.
    test(s"output watermark keeps the cutoff of earlier versions for an input without one, " +
        s"V$version") {
      withJoinStateVersion(version) {
        val (leftInput, rightInput, left, right) = inputs()
        val joined = left.join(right, expr(
            "leftKey = rightKey AND CAST(lt AS LONG) + CAST(rt AS LONG) > 100"))
          .select($"leftKey", $"lt").dropDuplicatesWithinWatermark()
          .select($"leftKey", $"lt".cast("long"))

        testStream(joined)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 200L)), AddData(rightInput, (9, 200L)))),
          CheckNewAnswer(),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 300L)), AddData(rightInput, (8, 300L)))),
          CheckNewAnswer(Row(8, 300L)),
          AddData(rightInput, (9, 400L)),
          CheckNewAnswer(),
          // The watermark is 299 s, but the joined row's lt of 200 s is not late.
          AddData(rightInput, (1, 500L)),
          CheckNewAnswer(Row(1, 200L)))
      }
    }

    test(s"constant-only conjunct does not cap the state cutoff, V$version") {
      withJoinStateVersion(version) {
        val (leftInput, rightInput, left, right) = inputs()
        val condition = expr(
          "leftKey = rightKey AND lt > rt - INTERVAL 5 SECONDS AND CAST(lt AS LONG) > 10")
        val joined = left.join(right, condition, "left_outer")
          .select($"leftKey", $"lt".cast("long"), $"rt".cast("long"))

        testStream(joined)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 20L)), AddData(rightInput, (9, 20L)))),
          CheckNewAnswer(),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 40L)), AddData(rightInput, (9, 40L)))),
          CheckNewAnswer(),
          // The watermark is 39, so no right row can match lt = 20 any more.
          AddData(rightInput, (9, 41L)),
          CheckNewAnswer(Row(1, 20L, null)))
      }
    }
  }
}
