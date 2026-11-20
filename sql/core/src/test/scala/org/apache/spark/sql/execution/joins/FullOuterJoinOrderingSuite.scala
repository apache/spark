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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.execution.{LocalTableScanExec, ProjectExec, SortExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, AQEShuffleReadExec}
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.functions.{coalesce, col}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class FullOuterJoinOrderingSuite
  extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper {

  override protected def sparkConf = super.sparkConf
    .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
    .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    .set(SQLConf.PREFER_SORTMERGEJOIN.key, "true")
    .set(SQLConf.SHUFFLE_PARTITIONS.key, "5")
    .set(SQLConf.SHUFFLE_SPREAD_NULL_JOIN_KEYS_ENABLED.key, "false")
    .set(SQLConf.ENABLE_FULL_OUTER_SORT_MERGE_JOIN_CODEGEN.key, "true")
    .set(SQLConf.ADAPTIVE_MAX_SHUFFLE_HASH_JOIN_LOCAL_MAP_THRESHOLD.key, "0")

  testWithWholeStageCodegenOnAndOff("single nullable keys retain merged ordering") { codegen =>
    withSingleKeys(
        Seq(Row(null), Row(1), Row(3), Row(3)),
        Seq(Row(null), Row(2), Row(3), Row(4))) {
      val df = sql("SELECT /*+ MERGE(a, b) */ k FROM a FULL OUTER JOIN b USING (k)")
      checkOrdered(df, Seq(Row(null), Row(null), Row(1), Row(2), Row(3), Row(3), Row(4)), "k")
      val plan = df.queryExecution.executedPlan
      val generatedJoins = collect(plan) {
        case w @ WholeStageCodegenExec(ProjectExec(_, _: SortMergeJoinExec)) => w
      }
      assert(generatedJoins.nonEmpty == codegen.toBoolean, plan)
      assert(fullOuterJoin(df).leftKeys.head.nullable)
    }
  }

  testWithWholeStageCodegenOnAndOff("disjoint keys and exhausted inputs retain ordering") { _ =>
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      Seq(
        (Seq(Row(1), Row(2), Row(3)), Seq(Row(4), Row(5)), (1 to 5).map(Row(_))),
        (Seq.empty[Row], Seq(Row(2), Row(1)), Seq(Row(1), Row(2))),
        (Seq(Row(2), Row(1)), Seq.empty[Row], Seq(Row(1), Row(2)))).foreach {
        case (left, right, expected) =>
          withSingleKeys(left, right) {
            val df = sql("SELECT /*+ MERGE(a, b) */ k FROM a FULL OUTER JOIN b USING (k)")
            fullOuterJoin(df)
            checkOrdered(df, expected, "k")
          }
      }
    }
  }

  testWithWholeStageCodegenOnAndOff("residual predicates keep equal-key groups together") { _ =>
    withTempView("a", "b") {
      frame(Seq(Row(1, 0), Row(3, 10), Row(3, 20), Row(5, 0)),
        "k" -> false, "v" -> false).createOrReplaceTempView("a")
      frame(Seq(Row(2, 0), Row(3, 15), Row(3, 5), Row(4, 0)),
        "k" -> false, "v" -> false).createOrReplaceTempView("b")
      val df = sql(
        """SELECT /*+ MERGE(a, b) */ coalesce(a.k, b.k) AS k, a.v, b.v
          |FROM a FULL OUTER JOIN b ON a.k = b.k AND a.v < b.v""".stripMargin)
      assert(fullOuterJoin(df).condition.nonEmpty)
      checkOrdered(df, Seq(
        Row(1, 0, null), Row(2, null, 0), Row(3, 10, 15), Row(3, 20, null),
        Row(3, null, 5), Row(4, null, 0), Row(5, 0, null)), "k")
    }
  }

  testWithWholeStageCodegenOnAndOff("non-null composite keys support mixed COALESCE orders") { _ =>
    withTempView("a", "b") {
      frame(Seq(Row(1, 2), Row(2, 0), Row(2, 0), Row(5, 1)),
        "k1" -> false, "k2" -> false).createOrReplaceTempView("a")
      frame(Seq(Row(1, 1), Row(2, 0), Row(4, 3)),
        "k1" -> false, "k2" -> false).createOrReplaceTempView("b")
      val df = sql(
        """SELECT /*+ MERGE(a, b) */
          |coalesce(a.k1, b.k1) AS x, coalesce(b.k2, a.k2) AS y
          |FROM a FULL OUTER JOIN b ON a.k1 = b.k1 AND a.k2 = b.k2""".stripMargin)
      val join = fullOuterJoin(df)
      assert((join.leftKeys ++ join.rightKeys).forall(!_.nullable))
      checkOrdered(df, Seq(Row(1, 1), Row(1, 2), Row(2, 0), Row(2, 0), Row(4, 3), Row(5, 1)),
        "x", "y")
      val sorted = df.sortWithinPartitions("x", "y")
      assert(numSorts(sorted.queryExecution.executedPlan) == 2)
    }
  }

  testWithWholeStageCodegenOnAndOff("nullable composite keys keep their consumer sort") { _ =>
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val nullable = frame(Seq(Row(2, null)), "k1" -> false, "k2" -> true)
      val nonNullable = frame(Seq(Row(1, 0)), "k1" -> false, "k2" -> false)
      Seq((nullable, nonNullable), (nonNullable, nullable)).foreach { case (left, right) =>
        val df = left.as("l").hint("merge").join(right.as("r"),
          col("l.k1") === col("r.k1") && col("l.k2") === col("r.k2"), "full_outer")
          .select(coalesce(col("l.k1"), col("r.k1")).as("k1"),
            coalesce(col("l.k2"), col("r.k2")).as("k2"))
        assert(fullOuterJoin(df).outputOrdering.isEmpty)
        checkAnswer(df, Seq(Row(2, null), Row(1, 0)))
        val sorted = df.sortWithinPartitions("k1", "k2")
        assert(numSorts(sorted.queryExecution.executedPlan) == 3)
        assert(sorted.collect().toSeq == Seq(Row(1, 0), Row(2, null)))
        checkAnswer(df.orderBy("k1", "k2").limit(1), Seq(Row(1, 0)))
      }
    }
  }

  test("ordering alternatives recursively make key attributes nullable") {
    val l1 = AttributeReference("l1", IntegerType, nullable = false)()
    val l2 = AttributeReference("l2", IntegerType, nullable = false)()
    val r1 = AttributeReference("r1", IntegerType, nullable = false)()
    val r2 = AttributeReference("r2", IntegerType, nullable = false)()
    val join = metadataJoin(Seq(l1, Add(l2, Literal(1))), Seq(r1, Add(r2, Literal(1))))
    val leftFirst = Seq(
      Coalesce(Seq(l1.withNullability(true), r1.withNullability(true))),
      Coalesce(Seq(Add(l2.withNullability(true), Literal(1)),
        Add(r2.withNullability(true), Literal(1)))))
    val rightFirst = leftFirst.map(e => Coalesce(e.children.reverse))
    assert(join.outputOrdering == leftFirst.zip(rightFirst).map { case (l, r) =>
      SortOrder(l, Ascending, NullsFirst, Seq(r))
    })
  }

  test("null-padding checks cover both sides and every key pair") {
    val l = AttributeReference("l", IntegerType, nullable = false)()
    val r = AttributeReference("r", IntegerType, nullable = false)()
    val nullable = AttributeReference("n", IntegerType, nullable = true)()
    val unsafe = Coalesce(Seq(nullable, Literal(0)))
    assert(!unsafe.nullable)
    Seq(
      metadataJoin(Seq(unsafe), Seq(r)),
      metadataJoin(Seq(l), Seq(unsafe)),
      metadataJoin(Seq(l, unsafe), Seq(r, r)),
      metadataJoin(Seq(l, l), Seq(r, unsafe))).foreach { join =>
      // No nullable key can mask a missing padding check in these composite cases.
      assert((join.leftKeys ++ join.rightKeys).forall(!_.nullable))
      assert(join.outputOrdering.isEmpty)
    }
  }

  test("unsafe nullable expressions on either side do not claim ordering") {
    withSingleKeys(Seq(Row(1), Row(3)), Seq(Row(1), Row(2))) {
      def unsafe(key: String): String =
        s"CASE WHEN $key IS NULL THEN 0 WHEN $key = 99 THEN NULL ELSE $key END"
      Seq(
        (unsafe("a.k"), "b.k"),
        (unsafe("b.k"), "a.k")).foreach { case (first, second) =>
        val df = sql(s"SELECT /*+ MERGE(a, b) */ coalesce($first, $second) AS k " +
          s"FROM a FULL OUTER JOIN b ON $first = $second")
        assert(fullOuterJoin(df).outputOrdering.isEmpty)
        checkAnswer(df.orderBy("k").limit(1), Seq(Row(0)))
      }
    }
  }

  test("null-safe and normalized floating-point keys keep ordering unknown") {
    withSingleKeys(Seq(Row(null), Row(1)), Seq(Row(null), Row(2))) {
      val nullSafe = sql("SELECT /*+ MERGE(a, b) */ * FROM a FULL OUTER JOIN b ON a.k <=> b.k")
      assert(fullOuterJoin(nullSafe).outputOrdering.isEmpty)
      checkAnswer(nullSafe, Seq(Row(null, null), Row(1, null), Row(null, 2)))
      val floats = sql(
        """SELECT /*+ MERGE(x, y) */ coalesce(x.k, y.k) AS k
          |FROM (SELECT CAST(k AS DOUBLE) AS k FROM a) x
          |FULL OUTER JOIN (SELECT CAST(k AS DOUBLE) AS k FROM b) y ON x.k = y.k""".stripMargin)
      assert(fullOuterJoin(floats).outputOrdering.isEmpty)
      checkAnswer(floats, Seq(Row(null), Row(null), Row(1.0), Row(2.0)))
    }
  }

  test("USING and either explicit COALESCE order eliminate local sorts through aliases") {
    withSingleKeys(Seq(Row(1), Row(3)), Seq(Row(2), Row(3))) {
      Seq(
        "SELECT /*+ MERGE(a, b) */ k AS merged FROM a FULL OUTER JOIN b USING (k)",
        "SELECT /*+ MERGE(a, b) */ coalesce(a.k, b.k) AS merged " +
          "FROM a FULL OUTER JOIN b ON a.k = b.k",
        "SELECT /*+ MERGE(a, b) */ coalesce(b.k, a.k) AS merged " +
          "FROM a FULL OUTER JOIN b ON a.k = b.k").foreach { query =>
        val df = sql(query).selectExpr("merged AS renamed").sortWithinPartitions("renamed")
        checkOrdered(df, Seq(Row(1), Row(2), Row(3)), "renamed")
        assert(numSorts(df.queryExecution.executedPlan) == 2)
      }
      val joined = sql("SELECT /*+ MERGE(a, b) */ a.k AS ak, b.k AS bk " +
        "FROM a FULL OUTER JOIN b ON a.k = b.k")
      Seq(col("ak"), col("bk"), coalesce(col("ak"), col("bk")).desc,
        coalesce(col("ak"), col("bk")).asc_nulls_last).foreach { order =>
        assert(numSorts(joined.sortWithinPartitions(order).queryExecution.executedPlan) == 3)
      }
    }
  }

  test("single-key chains reuse the lower join ordering") {
    withThreeInputs {
      Seq(usingChain, explicitChain).foreach { query =>
        val df = sql(query)
        checkOrdered(df, chainAnswer, "k")
        assertChainSorts(df.queryExecution.executedPlan)
      }
    }
  }

  test("window and sort aggregate consumers reuse merged ordering") {
    withSingleKeys(Seq(Row(1), Row(3), Row(3)), Seq(Row(2), Row(3))) {
      val window = sql(
        """SELECT k, count(*) OVER (PARTITION BY k) AS cnt FROM
          |(SELECT /*+ MERGE(a, b) */ k FROM a FULL OUTER JOIN b USING (k))""".stripMargin)
      checkAnswer(window, Seq(Row(1, 1L), Row(2, 1L), Row(3, 2L), Row(3, 2L)))
      val windows = collect(window.queryExecution.executedPlan) { case w: WindowExec => w }
      assert(windows.size == 1)
      assert(numSorts(window.queryExecution.executedPlan) == 2)
      withSQLConf(SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED.key -> "true") {
        val grouped = sql("SELECT /*+ MERGE(a, b) */ k, count(*) " +
          "FROM a FULL OUTER JOIN b USING (k) GROUP BY k")
        checkAnswer(grouped, Seq(Row(1, 1L), Row(2, 1L), Row(3, 2L)))
        assert(collect(grouped.queryExecution.executedPlan) {
          case a: SortAggregateExec => a
        }.nonEmpty)
        assert(numSorts(grouped.queryExecution.executedPlan) == 2)
      }
    }
  }

  test("global ordering still requires a global distribution") {
    withThreeInputs {
      val df = sql(usingChain).orderBy("k")
      assert(df.collect().toSeq == chainAnswer)
      val plan = df.queryExecution.executedPlan
      assert(collect(plan) { case s: SortExec if s.global => s }.nonEmpty, plan)
      assert(collect(plan) { case s: ShuffleExchangeExec => s }.size == 4, plan)
    }
  }

  test("checkpointed plain and nested keys retain nullable ordering expressions") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val left = frame((0 until 3).map(Row(_)), "ak" -> false)
      val right = frame((2 until 5).map(Row(_)), "bk" -> false)
      Seq(
        (col("ak"), col("bk"), (0 until 5).map(Row(_))),
        (col("ak") + 1, col("bk") + 1, (1 to 5).map(Row(_)))).foreach {
        case (leftKey, rightKey, expected) =>
          val joined = left.hint("merge").join(right, leftKey === rightKey, "full_outer")
          fullOuterJoin(joined)
          val checkpointed = joined.localCheckpoint()
          try {
            val bySide = checkpointed.select(leftKey.as("k")).sortWithinPartitions("k")
            assert(numSorts(bySide.queryExecution.executedPlan) == 1)
            val byMerged = checkpointed.select(coalesce(leftKey, rightKey).as("k"))
              .sortWithinPartitions("k")
            assert(numSorts(byMerged.queryExecution.executedPlan) == 0)
            checkOrdered(byMerged, expected, "k")
          } finally {
            checkpointed.unpersist()
          }
      }
    }
  }

  test("AQE keeps ordering with several coalesced partitions") {
    withThreeInputs {
      withSQLConf(aqeCoalescing: _*) {
        val df = sql(usingChain)
        checkOrdered(df, chainAnswer, "k")
        val plan = df.queryExecution.executedPlan
        val reads = collect(plan) { case r: AQEShuffleReadExec => r }
        assert(reads.nonEmpty, plan)
        assert(reads.forall(r => (2 to 4).contains(r.outputPartitioning.numPartitions)), plan)
        assertChainSorts(plan)
      }
    }
  }

  test("changing null spreading after preparation keeps ordering and answers") {
    withThreeInputs {
      val spread = SQLConf.SHUFFLE_SPREAD_NULL_JOIN_KEYS_ENABLED.key
      Seq(Seq.empty[(String, String)],
        Seq(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false"), aqeCoalescing).foreach { confs =>
        withSQLConf(confs :+ (spread -> "true"): _*) {
          val df = sql(usingChain)
          df.queryExecution.executedPlan
          withSQLConf(spread -> "false") {
            checkOrdered(df, chainAnswer, "k")
          }
        }
      }
    }
  }

  test("AQE restores consumer ordering when a full outer SMJ becomes SHJ") {
    withSingleKeys(Seq(Row(1), Row(3)), Seq(Row(2), Row(3))) {
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
          SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "1MB",
          SQLConf.ADAPTIVE_MAX_SHUFFLE_HASH_JOIN_LOCAL_MAP_THRESHOLD.key -> "64MB") {
        val df = sql("SELECT k, count(*) OVER (PARTITION BY k) AS cnt " +
          "FROM a FULL OUTER JOIN b USING (k)")
        assert(collect(df.queryExecution.executedPlan) {
          case j: SortMergeJoinExec => j
        }.size == 1)
        checkOrdered(df, Seq(Row(1, 1L), Row(2, 1L), Row(3, 1L)), "k")
        val plan = df.queryExecution.executedPlan
        val hashes = collect(plan) { case j: ShuffledHashJoinExec => j }
        assert(hashes.size == 1, plan)
        assert(hashes.head.outputOrdering.isEmpty)
        assert(numSorts(plan) == 1, plan)
      }
    }
  }

  private val chainAnswer = Seq.fill(3)(Row(null)) ++ (0 until 30).map(Row(_))

  private val usingChain =
    "SELECT /*+ MERGE(a, b, c) */ k FROM a FULL OUTER JOIN b USING (k) " +
      "FULL OUTER JOIN c USING (k)"

  private val explicitChain =
    "SELECT /*+ MERGE(a, b, c) */ coalesce(coalesce(a.k, b.k), c.k) AS k " +
      "FROM a FULL OUTER JOIN b ON a.k = b.k " +
      "FULL OUTER JOIN c ON coalesce(a.k, b.k) = c.k"

  private val aqeCoalescing = Seq(
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
    SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
    SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST.key -> "false",
    SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "64MB",
    SQLConf.COALESCE_PARTITIONS_MAX_REDUCER_PARTITIONS_PER_TASK.key -> "3",
    SQLConf.SHUFFLE_PARTITIONS.key -> "10")

  private def frame(rows: Seq[Row], fields: (String, Boolean)*): DataFrame = {
    val schema = StructType(fields.map { case (name, nullable) =>
      StructField(name, IntegerType, nullable)
    })
    spark.createDataFrame(spark.sparkContext.parallelize(rows, 2), schema)
  }

  private def withSingleKeys(left: Seq[Row], right: Seq[Row])(f: => Unit): Unit = {
    withTempView("a", "b") {
      frame(left, "k" -> true).createOrReplaceTempView("a")
      frame(right, "k" -> true).createOrReplaceTempView("b")
      f
    }
  }

  private def withThreeInputs(f: => Unit): Unit = {
    withTempView("a", "b", "c") {
      frame((0 until 20).map(Row(_)) :+ Row(null), "k" -> true).createOrReplaceTempView("a")
      frame((10 until 25).map(Row(_)) :+ Row(null), "k" -> true).createOrReplaceTempView("b")
      frame((15 until 30).map(Row(_)) :+ Row(null), "k" -> true).createOrReplaceTempView("c")
      f
    }
  }

  private def metadataJoin(left: Seq[Expression], right: Seq[Expression]): SortMergeJoinExec = {
    SortMergeJoinExec(left, right, FullOuter, None,
      LocalTableScanExec(left.flatMap(_.references).distinct, Nil, None),
      LocalTableScanExec(right.flatMap(_.references).distinct, Nil, None))
  }

  private def fullOuterJoin(df: DataFrame): SortMergeJoinExec = {
    val joins = collect(df.queryExecution.executedPlan) {
      case j: SortMergeJoinExec if j.joinType == FullOuter => j
    }
    assert(joins.size == 1, df.queryExecution.executedPlan)
    joins.head
  }

  private def numSorts(plan: SparkPlan): Int = collect(plan) { case s: SortExec => s }.size

  private def checkOrdered(df: DataFrame, expected: Seq[Row], keys: String*): Unit = {
    // checkAnswer treats even SORT BY as globally ordered; compare the rows independently.
    QueryTest.sameRows(expected, df.collect().toSeq).foreach { error =>
      fail(s"$error\n${df.queryExecution}")
    }
    val plan = stripAQEPlan(df.queryExecution.executedPlan)
    val required = keys.map(k => SortOrder(plan.output.find(_.name == k).get, Ascending))
    assert(SortOrder.orderingSatisfies(plan.outputOrdering, required), plan)
    assertLocallyOrdered(plan, required)
  }

  private def assertLocallyOrdered(plan: SparkPlan, required: Seq[SortOrder]): Unit = {
    val bound = BindReferences.bindReferences(required, plan.output)
    val violations = plan.execute().mapPartitions { rows =>
      val ordering = RowOrdering.create(bound, Nil)
      Iterator(rows.map(_.copy()).sliding(2).exists {
        case Seq(previous, current) => ordering.compare(previous, current) > 0
        case _ => false
      })
    }.collect()
    assert(!violations.contains(true), plan)
  }

  private def assertChainSorts(plan: SparkPlan): Unit = {
    val joins = collect(plan) { case j: SortMergeJoinExec => j }
    assert(joins.size == 2, plan)
    assert(numSorts(plan) == 3, plan)
    assert(collect(plan) { case s: ShuffleExchangeExec => s }.size == 3, plan)
    val upper = joins.head
    assert(SortOrder.orderingSatisfies(upper.left.outputOrdering,
      upper.requiredChildOrdering.head), plan)
    assert(!upper.left.isInstanceOf[SortExec], plan)
  }
}
