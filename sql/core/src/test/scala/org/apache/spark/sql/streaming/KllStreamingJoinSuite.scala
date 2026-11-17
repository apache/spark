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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeq, OffsetSeqLog}
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinExec
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{HDFSBackedStateStoreProvider, RocksDBStateStoreProvider}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.BinaryType

class KllStreamingJoinSuite extends StreamTest {
  import testImplicits._

  test("KLL legacy join restoration retains provenance across static cache substitution") {
    withTempView("old_kll_constant", "current_kll_constant") {
      val rollback = "spark.sql.legacy.kllAggregateDeterministic.enabled"
      val sharing = "spark.sql.streaming.aggregation.legacyKllSharing.enabled"
      def constant(name: String, legacy: Boolean): DataFrame =
        withSQLConf(rollback -> legacy.toString) {
          val data = spark.sql("""SELECT
              |kll_sketch_get_n_bigint(kll_sketch_agg_bigint(v)) AS n
              |FROM VALUES (0), (1), (2) AS t(v)""".stripMargin).cache()
          data.collect()
          data.createOrReplaceTempView(name)
          data
        }
      val oldConstant = constant("old_kll_constant", legacy = true)
      val currentConstant = constant("current_kll_constant", legacy = false)
      try {
        withTempDir { checkpoint =>
          val left = MemoryStream[Int]
          val right = MemoryStream[Int]
          def query(table: String, legacy: Boolean): DataFrame =
            withSQLConf(rollback -> legacy.toString) {
              left.toDF().as("l").join(right.toDF().as("r"),
                expr(s"l.value = r.value + (SELECT n FROM $table)"))
                .selectExpr("l.value", "r.value")
            }
          val historical = query("old_kll_constant", legacy = true)
          val current = query("current_kll_constant", legacy = false)
          testStream(historical)(
            StartStream(checkpointLocation = checkpoint.getAbsolutePath,
              additionalConfs = Map(sharing -> "true")),
            AddData(left, 3), AddData(right, 0), CheckLastBatch(Row(3, 0)), StopStream)
          testStream(current)(
            StartStream(checkpointLocation = checkpoint.getAbsolutePath,
              additionalConfs = Map(sharing -> "false")),
            AddData(right, 0), CheckLastBatch(Row(3, 0)), StopStream)
        }
      } finally {
        oldConstant.unpersist()
        currentConstant.unpersist()
      }
    }
  }

  test("KLL scalar comparison wrappers preserve streaming equi keys") {
    withUserDefinedFunction("stream_kll_equal" -> true) {
      spark.sql("""CREATE TEMPORARY FUNCTION stream_kll_equal(a INT, b INT, s BIGINT)
          |RETURNS BOOLEAN RETURN a + s = b + s""".stripMargin)
      for (legacy <- Seq(false, true); shared <- Seq(false, true)) {
        val left = MemoryStream[Int]
        val right = MemoryStream[Int]
        val scalar = """(SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(v))
            |FROM VALUES (0), (1), (2) AS t(v))""".stripMargin
        val condition = if (shared) s"stream_kll_equal(l.value, r.value, $scalar)"
          else s"(l.value = r.value + $scalar) OR false"
        val result = left.toDF().as("l").join(right.toDF().as("r"), expr(condition))
          .selectExpr("l.value", "r.value")
        val l = if (shared) 0 else 3
        testStream(result)(
          StartStream(additionalConfs = Map(
            "spark.sql.streaming.aggregation.legacyKllSharing.enabled" -> legacy.toString)),
          AddData(left, l), AddData(right, 0), CheckLastBatch(Row(l, 0)),
          AddData(left, l + 1), AddData(right, 1), CheckLastBatch(Row(l + 1, 1)),
          StopStream)
      }
    }
  }

  test("KLL join state retains only the consumed sketch getter value") {
    for (valueType <- Seq("bigint", "float", "double"); merge <- Seq(false, true)) {
      val left = MemoryStream[Int]
      val right = MemoryStream[Int]
      val sketch = s"kll_sketch_agg_$valueType(CAST(v AS $valueType), 1024)"
      val values = "VALUES (0), (1), (2) AS t(v)"
      val aggregate = if (merge) s"kll_merge_agg_$valueType(s)" else sketch
      val from = if (merge) s"(SELECT $sketch AS s FROM $values GROUP BY v)" else values
      val scalar = s"kll_sketch_get_n_$valueType((SELECT $aggregate FROM $from))"
      val result = left.toDF().as("l").join(right.toDF().as("r"),
        expr(s"l.value + $scalar = r.value")).selectExpr("l.value", "r.value")
      testStream(result)(
        StartStream(additionalConfs = Map(
          "spark.sql.streaming.aggregation.legacyKllSharing.enabled" -> "false")),
        AddData(left, 0), AddData(right, 3), CheckLastBatch(Row(0, 3)),
        AssertOnQuery { query =>
          val join = query.lastExecution.executedPlan.collectFirst {
            case j: StreamingSymmetricHashJoinExec => j
          }.get
          assert((join.left.output ++ join.right.output).forall(_.dataType != BinaryType),
            query.lastExecution.toString)
          true
        }, StopStream)
    }
  }

  for (stageLeft <- Seq(false, true)) {
    test(s"KLL nested historical join restoration: stageLeft=$stageLeft") {
      withTempDir { checkpoint =>
        val left = MemoryStream[(Int, Long, String)]
        val right = MemoryStream[(Int, Long, String)]
        val rollback = "spark.sql.legacy.kllAggregateDeterministic.enabled"
        val sharing = "spark.sql.streaming.aggregation.legacyKllSharing.enabled"
        val scalar = """(SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(v))
            |FROM VALUES (0), (1), (2) AS t(v))""".stripMargin
        def query(legacy: Boolean): DataFrame = withSQLConf(rollback -> legacy.toString) {
          val static = Seq(0, 1, 3, 4).toDF("static_key")
          val l = left.toDF().toDF("lk", "lp", "lt")
          val r = right.toDF().toDF("rk", "rp", "rt")
          val nestedLeft = if (stageLeft) l.join(static, expr("lk = static_key")) else l
          val nestedRight = if (!stageLeft) r.join(static, expr("rk = static_key")) else r
          val condition = if (stageLeft) s"lk + $scalar = rk" else s"lk = rk + $scalar"
          nestedLeft.join(nestedRight, expr(condition))
            .selectExpr("lk", "lp", "lt", "rk", "rp", "rt")
        }
        val l = if (stageLeft) 0 else 3
        val r = if (stageLeft) 3 else 0
        testStream(query(legacy = true))(
          StartStream(checkpointLocation = checkpoint.getAbsolutePath,
            additionalConfs = Map(sharing -> "true")),
          AddData(left, (l, 100L, "left-old")),
          AddData(right, (r, 200L, "right-old")),
          CheckLastBatch(Row(l, 100L, "left-old", r, 200L, "right-old")), StopStream)
        testStream(query(legacy = false))(
          StartStream(checkpointLocation = checkpoint.getAbsolutePath,
            additionalConfs = Map(sharing -> "false")),
          AddData(right, (r, 201L, "right-new")),
          CheckLastBatch(Row(l, 100L, "left-old", r, 201L, "right-new")),
          AddData(left, (l, 101L, "left-new")),
          CheckLastBatch(Row(l, 101L, "left-new", r, 200L, "right-old"),
            Row(l, 101L, "left-new", r, 201L, "right-new")), StopStream)
      }
    }
  }

  for (version <- Seq(1, 2); provider <- Seq(
      classOf[HDFSBackedStateStoreProvider].getName,
      classOf[RocksDBStateStoreProvider].getName)) {
    test(s"KLL streaming join checkpoint layouts: state=$version provider=$provider") {
      withSQLConf(
          SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> provider,
          SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
        for (recorded <- Seq(None, Some(true), Some(false));
            stageLeft <- Seq(false, true); userAlias <- Seq(false, true);
            analysisLegacy <- Seq(false, true)) {
          withTempDir { checkpoint =>
            val rollback = "spark.sql.legacy.kllAggregateDeterministic.enabled"
            val sharing = "spark.sql.streaming.aggregation.legacyKllSharing.enabled"
            val legacy = recorded.getOrElse(true)
            val left = MemoryStream[Int]
            val right = MemoryStream[Int]
            val scalar = """(SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(v))
                |FROM VALUES (0), (1), (2) AS t(v))""".stripMargin
            def query(analysisLegacy: Boolean): DataFrame =
              withSQLConf(rollback -> analysisLegacy.toString) {
                def input(stream: MemoryStream[Int], staged: Boolean): DataFrame = {
                  val data = stream.toDF()
                  if (userAlias && staged) data.selectExpr("value", s"$scalar AS s") else data
                }
                val value = if (userAlias) (if (stageLeft) "l.s" else "r.s") else scalar
                val condition = if (stageLeft) s"l.value + $value = r.value"
                  else s"l.value = r.value + $value"
                input(left, stageLeft).as("l").join(input(right, !stageLeft).as("r"),
                  expr(condition)).selectExpr("l.value", "r.value")
              }
            // Rollback construction independently recreates the historical unstaged layout.
            val initial = query(analysisLegacy = legacy)
            val current = query(analysisLegacy = analysisLegacy)
            val l = if (stageLeft) 0 else 3
            val r = if (stageLeft) 3 else 0
            def checkPayload: AssertOnQuery = AssertOnQuery { query =>
              val join = query.lastExecution.executedPlan.collectFirst {
                case j: StreamingSymmetricHashJoinExec => j
              }.get
              val staged = if (stageLeft) join.left else join.right
              staged.output.size == (if (userAlias || !legacy) 2 else 1)
            }
            testStream(initial)(
              StartStream(checkpointLocation = checkpoint.getAbsolutePath,
                additionalConfs = Map(sharing -> legacy.toString)),
              AddData(left, l), AddData(right, r), CheckLastBatch(Row(l, r)),
              checkPayload, StopStream)
            val logPath = new File(checkpoint, "offsets").getAbsolutePath
            if (recorded.isEmpty) {
              val (batchId, offsets) = new OffsetSeqLog(spark, logPath).getLatest().get
              val historical = offsets match {
                case o: OffsetSeq =>
                  o.copy(metadataOpt = o.metadataOpt.map(m => m.copy(conf = m.conf - sharing)))
                case o: OffsetMap =>
                  o.copy(metadata = o.metadata.copy(conf = o.metadata.conf - sharing))
              }
              val path = new Path(logPath, batchId.toString)
              assert(path.getFileSystem(spark.sessionState.newHadoopConf()).delete(path, false))
              assert(new OffsetSeqLog(spark, logPath).add(batchId, historical))
            }
            testStream(current)(
              StartStream(checkpointLocation = checkpoint.getAbsolutePath,
                additionalConfs = Map(sharing -> (!legacy).toString)),
              AddData(right, r), CheckLastBatch(Row(l, r)),
              AddData(left, l + 1), CheckLastBatch(),
              AddData(right, r + 1), CheckLastBatch(Row(l + 1, r + 1)),
              checkPayload, StopStream)
            assert(new OffsetSeqLog(spark, logPath).getLatest().get._2
              .metadataOpt.get.conf(sharing) == legacy.toString)
          }
        }
      }
    }
  }
}
