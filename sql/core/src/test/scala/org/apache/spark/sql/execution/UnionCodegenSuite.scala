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

package org.apache.spark.sql.execution

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioningLike, UnknownPartitioning}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar.InMemoryTableScanLike
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, REPARTITION_BY_NUM, ShuffleExchangeExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Tests for `UnionExec` whole-stage codegen fusion: plan-shape assertions,
 * correctness, type widening, metrics, and fallbacks.
 */
class UnionCodegenSuite extends SharedSparkSession with AdaptiveSparkPlanHelper {

  // Union codegen fusion is off by default; turn it on for this suite.
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key, "true")

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Convenience: spark.range returning a DataFrame (not Dataset[Long]). */
  protected def rangeDF(end: Long): DataFrame = spark.range(end).toDF("id")
  protected def rangeDF(start: Long, end: Long): DataFrame =
    spark.range(start, end).toDF("id")
  protected def rangeDF(start: Long, end: Long, step: Long,
      numPartitions: Int): DataFrame =
    spark.range(start, end, step, numPartitions).toDF("id")

  private def wscgCount(df: DataFrame): Int =
    df.queryExecution.executedPlan.collect {
      case s: WholeStageCodegenExec => s
    }.size

  /**
   * `AdaptiveSparkPlanHelper.collect` descends through AQE wrappers and query stages;
   * `SparkPlan.collect` stops at them, since both are `LeafExecNode`s.
   *
   * Stricter than `codegenUnions` on purpose: this matches only a union that is the root of its own
   * codegen stage, which is the node the callers here reach for its tags and metrics.
   */
  private def fusedUnions(df: DataFrame): Seq[UnionExec] =
    collect(df.queryExecution.executedPlan) {
      case w: WholeStageCodegenExec if w.child.isInstanceOf[UnionExec] =>
        w.child.asInstanceOf[UnionExec]
    }

  /**
   * The unions that take part in a codegen stage: inside a `WholeStageCodegenExec` with no
   * `InputAdapter` between them and the stage root. That is what fusion means. Asking only whether
   * a stage holds a `UnionExec` somewhere does not answer it, since `CollapseCodegenStages` leaves
   * a non-participating union inside the stage too, under an `InputAdapter`; `fusedUnions` asks for
   * the stronger property of rooting the stage.
   *
   * Ask this of a plan that has run. `CollapseCodegenStages` is a post-stage-creation rule under
   * AQE, so a wrapped plan that never executed holds no `WholeStageCodegenExec` at all and the
   * answer is empty whatever the confs say. `AdaptiveSparkPlanHelper.collect` is what descends
   * through AQE wrappers and query stages; `SparkPlan.collect` stops at them, since both are
   * `LeafExecNode`s.
   */
  private def codegenUnions(df: DataFrame): Seq[UnionExec] = {
    def participating(plan: SparkPlan): Seq[UnionExec] = plan match {
      case _: InputAdapter => Nil
      case u: UnionExec => u +: u.children.flatMap(participating)
      case other => other.children.flatMap(participating)
    }
    collect(df.queryExecution.executedPlan) { case w: WholeStageCodegenExec => w }
      .flatMap(w => participating(w.child))
  }

  /**
   * A cached aggregate, so the union's children read an `InMemoryTableScanExec`. The caller needs
   * the cache unmaterialized; `withTempView` drops the view on the way out, and `dropTempView`
   * uncaches this view's plan.
   */
  private def cacheAggregateView(view: String): Unit = {
    spark.range(0, 200, 1, 4)
      .selectExpr("id % 10 AS k", "id AS v")
      .groupBy("k")
      .agg(sum("v").as("s"))
      .createOrReplaceTempView(view)
    spark.catalog.cacheTable(view)
  }

  /**
   * Run `buildDf()` with union codegen on, then again with it off, and assert the two agree.
   *
   * A fresh DataFrame per flag value, and not one built outside: `queryExecution` is memoized and
   * the decision is stamped at preparation, so collecting the same DataFrame twice replays the plan
   * the first value prepared and compares its output against itself. The off half also has to show
   * no union took part in codegen, which is only observable once the plan has executed, hence
   * after `checkAnswer`. Whether the on half fused anything is the caller's to assert, since a
   * fallback case is unfused either way: both DataFrames are returned, collected, for that.
   */
  protected def assertFlagParity(buildDf: () => DataFrame): (DataFrame, DataFrame) = {
    val fused = buildDf()
    val onRows = fused.collect().toSeq
    withSQLConf(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "false") {
      val plain = buildDf()
      checkAnswer(plain, onRows)
      assert(codegenUnions(plain).isEmpty,
        s"expected no union taking part in codegen with the flag off:\n${plain.queryExecution}")
      (fused, plain)
    }
  }

  // ---------------------------------------------------------------------------
  // Configuration smoke
  // ---------------------------------------------------------------------------

  test("SPARK-56482: SQLConf keys are pinned under wholeStage namespace") {
    // Pins the user-visible config keys so a future symbol rename does not
    // silently change the published key string.
    assert(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key ==
      "spark.sql.codegen.wholeStage.union.enabled")
    assert(SQLConf.WHOLESTAGE_UNION_MAX_CHILDREN.key ==
      "spark.sql.codegen.wholeStage.union.maxChildren")
  }

  // ---------------------------------------------------------------------------
  // Plan-shape tests
  // ---------------------------------------------------------------------------

  test("SPARK-56482: plain union with filter fuses into one WSCG stage") {
    val df = rangeDF(100).union(rangeDF(100)).filter(col("id") > 0)
    assert(wscgCount(df) == 1)
    assert(codegenUnions(df).nonEmpty)
  }

  test("SPARK-56482: flag off restores pre-patch plan shape") {
    withSQLConf(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "false") {
      val df = rangeDF(100).union(rangeDF(100)).filter(col("id") > 0)
      assert(wscgCount(df) >= 2)
      assert(codegenUnions(df).isEmpty)
    }
  }

  test("SPARK-56482: maxChildren exceeded falls back") {
    withSQLConf(
      SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "true",
      SQLConf.WHOLESTAGE_UNION_MAX_CHILDREN.key -> "2") {
      val df = rangeDF(10).union(rangeDF(10)).union(rangeDF(10))
      assert(codegenUnions(df).isEmpty)
    }
  }

  test("SPARK-56482: nested UnionExec - outer non-codegen, inner codegen") {
    val inner = rangeDF(10).union(rangeDF(10)).filter(col("id") > 0)
    val outer = inner.union(rangeDF(10)).filter(col("id") > 0)
    val plan = outer.queryExecution.executedPlan
    val fusedOuterUnions = plan.collect {
      case w: WholeStageCodegenExec
          if w.find {
            case u: UnionExec => u.exists {
              case inner: UnionExec => inner ne u
              case _ => false
            }
            case _ => false
          }.isDefined => w
    }
    assert(fusedOuterUnions.isEmpty,
      "UnionExec with any descendant UnionExec must not be inside a WSCG stage")
    assertFlagParity(() => inner.union(rangeDF(10)).filter(col("id") > 0).orderBy("id"))
  }

  test("SPARK-56482: indirect nested UnionExec behind Project is not fused") {
    val inner1 = rangeDF(4).union(rangeDF(4))
    val inner2 = rangeDF(4).union(rangeDF(4))
    val outer = inner1.select(col("id") + 1 as "id")
      .union(inner2.select(col("id") + 1 as "id"))
    val plan = outer.queryExecution.executedPlan
    val fused = plan.collect {
      case w: WholeStageCodegenExec if w.find {
        case u: UnionExec => u.exists {
          case d: UnionExec => d ne u
          case _ => false
        }
        case _ => false
      }.isDefined => w
    }
    assert(fused.isEmpty,
      "UnionExec with a non-direct descendant UnionExec must not be fused")
    assertFlagParity(() =>
      inner1.select(col("id") + 1 as "id")
        .union(inner2.select(col("id") + 1 as "id")).orderBy("id"))
  }

  test("SPARK-56482: non-CodegenSupport child union produces correct results") {
    // LocalTableScanExec may or may not be fused via InputAdapter wrapping
    // depending on the planner. Just verify correctness.
    val schema = StructType(Seq(StructField("id", LongType)))
    val local = spark.createDataFrame(
      java.util.Arrays.asList(Row(1L), Row(2L)), schema)
    val df = local.union(rangeDF(10))
    assert(df.count() == 12L)
    assertFlagParity(() => local.union(rangeDF(10)).orderBy("id"))
  }

  test("SPARK-56482: WSCG count drops from N+1 to 1 (N=4)") {
    def buildDf(): DataFrame = {
      val dfs = (0 until 4).map(i => rangeDF(i * 10L, i * 10L + 10L))
      dfs.reduce((a, b) => a.union(b)).filter(col("id") > 0)
    }
    assert(wscgCount(buildDf()) == 1)
    withSQLConf(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "false") {
      assert(wscgCount(buildDf()) >= 2)
    }
  }

  // ---------------------------------------------------------------------------
  // Correctness: type widening
  // ---------------------------------------------------------------------------

  test("SPARK-56482: type widening int -> long") {
    assertFlagParity { () =>
      val a = rangeDF(3).select(col("id").cast(IntegerType).as("v"))
      val b = rangeDF(3).select(col("id").as("v"))
      a.union(b).orderBy("v")
    }
  }

  test("SPARK-56482: type widening decimal precision (equal scale)") {
    assertFlagParity { () =>
      val a = rangeDF(3).select(col("id").cast(DecimalType(5, 0)).as("v"))
      val b = rangeDF(3).select(col("id").cast(DecimalType(10, 0)).as("v"))
      a.union(b).orderBy("v")
    }
  }

  test("SPARK-56482: type widening decimal precision (different scale)") {
    // decimal(5,0) union decimal(10,2) -> decimal(10,2) per
    // DecimalPrecisionTypeCoercion.widerDecimalType (scale=max(0,2)=2,
    // precision=scale+max(p1-s1,p2-s2)=2+max(5,8)=10). WidenSetOperationTypes
    // aligns both precision and scale, so the physical UnionExec sees
    // matching child output dataTypes.
    val build = () => {
      val a = rangeDF(3).select(col("id").cast(DecimalType(5, 0)).as("v"))
      val b = rangeDF(3).select(col("id").cast(DecimalType(10, 2)).as("v"))
      a.union(b)
    }
    assert(codegenUnions(build().filter(col("v") >= 0)).nonEmpty,
      "decimal precision/scale widening should still fuse into one WSCG stage")
    assertFlagParity(() => build().orderBy("v"))
  }

  test("SPARK-56482: nullability widening top-level") {
    assertFlagParity { () =>
      val a = rangeDF(3).select(col("id").as("v"))
      val b = spark.createDataFrame(
        java.util.Arrays.asList(Row(null), Row(1L)),
        StructType(Seq(StructField("v", LongType, nullable = true))))
      a.union(b).orderBy("v")
    }
  }

  test("SPARK-56482: widening union with filter fuses into one WSCG stage") {
    // Plan-shape check that fusion is actually taken when types differ at the
    // user level (forcing `WidenSetOperationTypes` to insert Project(Cast)
    // above each child). Uses `.filter` rather than `.orderBy` so the plan
    // has no Exchange and AQE does not wrap it.
    val a = rangeDF(3).select(col("id").cast(IntegerType).as("v"))
    val b = rangeDF(3).select(col("id").as("v"))
    val df = a.union(b).filter(col("v") >= 0)
    assert(codegenUnions(df).nonEmpty,
      "widened-children Union should fuse with filter into a single WSCG stage")
    checkAnswer(df, Seq(Row(0L), Row(1L), Row(2L), Row(0L), Row(1L), Row(2L)))
  }

  test("SPARK-56482: nested-nullability mismatch falls back to non-codegen") {
    // Children differ only in nested struct nullability, which
    // `WidenSetOperationTypes` does not align (see `allChildOutputDataTypesMatch`
    // in `UnionExec`). The codegen path must fall back to `doExecute` rather
    // than crash on the resulting type mismatch.
    val structInner = StructType(Seq(StructField("f", IntegerType, nullable = false)))
    val structOuterNotNull = StructType(Seq(StructField("s", structInner, nullable = false)))
    val structInnerNullable =
      StructType(Seq(StructField("f", IntegerType, nullable = true)))
    val structOuterNullable =
      StructType(Seq(StructField("s", structInnerNullable, nullable = false)))
    val a = spark.createDataFrame(
      java.util.Arrays.asList(Row(Row(1)), Row(Row(2))), structOuterNotNull)
    val b = spark.createDataFrame(
      java.util.Arrays.asList(Row(Row(3)), Row(Row(4))), structOuterNullable)
    val df = a.union(b)
    assert(codegenUnions(df).isEmpty,
      "Nested-nullability mismatch must fall back to non-codegen")
    val unionExec = df.queryExecution.executedPlan.collectFirst {
      case u: UnionExec => u
    }.get
    assert(!unionExec.metrics.contains("numOutputRows"),
      "numOutputRows metric must not be registered when fusion is denied")
    checkAnswer(df,
      Seq(Row(Row(1)), Row(Row(2)), Row(Row(3)), Row(Row(4))))
  }

  test("SPARK-56482: array containsNull mismatch falls back to non-codegen") {
    // ArrayType.containsNull is the array analog of struct field nullability:
    // skipped by `WidenSetOperationTypes`, so the codegen path must fall back.
    val schemaNotNull =
      StructType(Seq(StructField("a", ArrayType(IntegerType, containsNull = false))))
    val schemaNullable =
      StructType(Seq(StructField("a", ArrayType(IntegerType, containsNull = true))))
    val a = spark.createDataFrame(
      java.util.Arrays.asList(Row(java.util.Arrays.asList(1, 2))), schemaNotNull)
    val b = spark.createDataFrame(
      java.util.Arrays.asList(Row(java.util.Arrays.asList(3, 4))), schemaNullable)
    val df = a.union(b)
    assert(codegenUnions(df).isEmpty,
      "Array containsNull mismatch must fall back to non-codegen")
    val unionExec = df.queryExecution.executedPlan.collectFirst {
      case u: UnionExec => u
    }.get
    assert(!unionExec.metrics.contains("numOutputRows"),
      "numOutputRows metric must not be registered when fusion is denied")
    val collectedArrays = df.collect()
      .map(_.getList[Int](0).toArray.toSeq)
      .toSet
    assert(collectedArrays == Set(Seq(1, 2), Seq(3, 4)),
      s"Expected the union of both array rows, got $collectedArrays")
  }

  // ---------------------------------------------------------------------------
  // Correctness: N children, empty partitions, mixed partition counts
  // ---------------------------------------------------------------------------

  test("SPARK-56482: N = 3 children") {
    assertFlagParity { () =>
      val a = rangeDF(3)
      val b = rangeDF(3, 6)
      val c = rangeDF(6, 9)
      a.union(b).union(c).orderBy("id")
    }
  }

  test("SPARK-56482: N = 8 children") {
    assertFlagParity { () =>
      val dfs = (0 until 8).map(i => rangeDF(i * 5L, i * 5L + 5L))
      dfs.reduce((a, b) => a.union(b)).orderBy("id")
    }
  }

  test("SPARK-56482: empty-partition child") {
    assertFlagParity { () =>
      val a = rangeDF(0, 0, 1, numPartitions = 4)
      val b = rangeDF(3)
      a.union(b).orderBy("id")
    }
  }

  test("SPARK-56482: mixed partition counts") {
    assertFlagParity { () =>
      val a = rangeDF(0, 10, 1, numPartitions = 2)
      val b = rangeDF(10, 30, 1, numPartitions = 5)
      a.union(b).orderBy("id")
    }
  }

  // ---------------------------------------------------------------------------
  // Correctness: RangeExec under fusion (the partitionIndex fix)
  // ---------------------------------------------------------------------------

  test("SPARK-56482: range union fuses correctly (childPartitionIndex)") {
    assertFlagParity { () =>
      rangeDF(0, 10, 1, numPartitions = 2)
        .union(rangeDF(10, 20, 1, numPartitions = 2))
        .orderBy("id")
    }
  }

  test("SPARK-56482: range(2).union(range(2)) returns 4 rows") {
    val df = rangeDF(2).union(rangeDF(2))
    assert(df.count() == 4)
    assert(df.collect().map(_.getLong(0)).sorted.toSeq == Seq(0, 0, 1, 1))
  }

  test("SPARK-56482: three RangeExec children fuse correctly") {
    assertFlagParity { () =>
      val a = rangeDF(0, 5, 1, numPartitions = 2)
      val b = rangeDF(5, 10, 1, numPartitions = 3)
      val c = rangeDF(10, 15, 1, numPartitions = 2)
      a.union(b).union(c).orderBy("id")
    }
  }

  // ---------------------------------------------------------------------------
  // Metrics
  // ---------------------------------------------------------------------------

  test("SPARK-56482: numOutputRows metric equals total child rows") {
    val df = rangeDF(3).union(rangeDF(5))
    df.collect()
    val unionExec = df.queryExecution.executedPlan.collectFirst {
      case u: UnionExec => u
    }.get
    assert(unionExec.metrics("numOutputRows").value == 8L)
  }

  test("SPARK-56482: numOutputRows with mixed partition counts") {
    val a = rangeDF(0, 40, 1, numPartitions = 4)
    val b = rangeDF(0, 200, 1, numPartitions = 2)
    val df = a.union(b)
    df.collect()
    val unionExec = df.queryExecution.executedPlan.collectFirst {
      case u: UnionExec => u
    }.get
    assert(unionExec.metrics("numOutputRows").value == 240L)
  }

  // ---------------------------------------------------------------------------
  // LIMIT
  // ---------------------------------------------------------------------------

  test("SPARK-56482: LIMIT above Union returns exactly K rows") {
    assertFlagParity { () =>
      val a = rangeDF(100)
      val b = rangeDF(100, 200)
      a.union(b).limit(5)
    }
  }

  // ---------------------------------------------------------------------------
  // needCopyResult
  // ---------------------------------------------------------------------------

  test("SPARK-56482: needCopyResult all-scan children is false") {
    val df = rangeDF(10).union(rangeDF(10))
    df.collect()
    val unionExec = df.queryExecution.executedPlan.collectFirst {
      case u: UnionExec => u
    }.get
    assert(!unionExec.needCopyResult,
      "UnionExec with scan-only children should not need row copy")
  }

  test("SPARK-56482: BHJ child union correctness") {
    // Verify that a union with a BHJ child produces correct results
    // under both flag states (the needCopyResult override ensures
    // UnsafeRow buffers aren't aliased across multi-row BHJ output).
    withSQLConf(
      SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760") {
      val (fused, _) = assertFlagParity { () =>
        val left = rangeDF(100).select(col("id").as("lk"), col("id").as("lv"))
        val right = rangeDF(100).select(col("id").as("rk"))
        val bhj = left.join(broadcast(right), col("lk") === col("rk"))
          .select("lk", "lv")
        bhj.union(rangeDF(100).select(col("id").as("lk"), col("id").as("lv")))
      }
      assert(codegenUnions(fused).size == 1,
        s"the aliasing this case is about needs the fused path:\n${fused.queryExecution}")
    }
  }

  test("SPARK-56482: BHJ multi-row child feeds downstream agg correctly") {
    withSQLConf(
      SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760") {
      val (fused, _) = assertFlagParity { () =>
        val probe = rangeDF(10).select(col("id").as("k"))
        val build = rangeDF(20)
          .select((col("id") % 5).as("k"), col("id").as("v"))
        val bhj = probe.join(broadcast(build), "k")
        val df = bhj.union(
          rangeDF(0).select(col("id").as("k"), col("id").as("v")))
        df.groupBy("k").count().orderBy("k")
      }
      // The partial aggregate roots the stage here and the union is fused into it, so this is the
      // case `fusedUnions` would miss.
      assert(codegenUnions(fused).size == 1,
        s"the aliasing this case is about needs the fused path:\n${fused.queryExecution}")
    }
  }

  // ---------------------------------------------------------------------------
  // Structural denylist: SortMergeJoin child
  // ---------------------------------------------------------------------------

  test("SPARK-56482: SMJ child union correctness") {
    // SMJ is in the structural denylist (multi-RDD codegen), so
    // UnionExec should fall back for that child. Verify correctness.
    withSQLConf(
      SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val (fused, _) = assertFlagParity { () =>
        val left = rangeDF(100).select(col("id").as("k"))
        val right = rangeDF(100).select(col("id").as("k"))
        val smj = left.join(right, "k")
        smj.union(rangeDF(100).select(col("id").as("k")))
      }
      // Not `fusedUnions`: this union would not root a stage anyway, so that would hold with or
      // without the denylist. What the denylist owes is that it takes no part in codegen.
      assert(codegenUnions(fused).isEmpty,
        s"the denylist has to keep this union out of codegen:\n${fused.queryExecution}")
    }
  }

  // ---------------------------------------------------------------------------
  // Columnar fallback
  // ---------------------------------------------------------------------------

  test("SPARK-56482: parquet union correctness") {
    // Verify unions of Parquet-backed DataFrames produce correct results
    // regardless of columnar/row mode and codegen flag state.
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      rangeDF(100).write.parquet(path)
      val df = spark.read.parquet(path).union(spark.read.parquet(path))
      assert(df.count() == 200L)
      val flagOn = df.orderBy("id").collect().toSeq
      withSQLConf(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "false") {
        checkAnswer(df.orderBy("id"), flagOn)
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Cached DataFrame child
  // ---------------------------------------------------------------------------

  test("SPARK-56482: cached DataFrame child correctness across flag states") {
    val cached = rangeDF(100).cache()
    try {
      cached.count()
      val (fused, _) = assertFlagParity(() => cached.union(rangeDF(100, 200)))
      assert(codegenUnions(fused).size == 1, s"expected a fused union:\n${fused.queryExecution}")
      // The cached child is the point of the case: without it this is a plain range union.
      assert(collect(fused.queryExecution.executedPlan) {
        case s: InMemoryTableScanLike => s
      }.nonEmpty, s"expected a cached child:\n${fused.queryExecution}")
    } finally {
      cached.unpersist()
    }
  }

  // ---------------------------------------------------------------------------
  // Reused subquery
  // ---------------------------------------------------------------------------

  test("SPARK-56482: reused subquery across Union children") {
    val t = "union_codegen_sub_test"
    rangeDF(100).createOrReplaceTempView(t)
    try {
      val q =
        s"""
           |SELECT id FROM $t WHERE id IN (SELECT MAX(id) FROM $t)
           |UNION ALL
           |SELECT id FROM $t WHERE id IN (SELECT MAX(id) FROM $t)
         """.stripMargin
      val flagOn = spark.sql(q).collect().toSet
      withSQLConf(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "false") {
        assert(spark.sql(q).collect().toSet == flagOn)
      }
    } finally {
      spark.catalog.dropTempView(t)
    }
  }

  // ---------------------------------------------------------------------------
  // storeAssignmentPolicy regression guard
  // ---------------------------------------------------------------------------

  test("SPARK-56482: storeAssignmentPolicy regression guard") {
    Seq("LEGACY", "STRICT", "ANSI").foreach { policy =>
      withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> policy) {
        val df = rangeDF(3).union(rangeDF(3, 6))
        assert(df.collect().map(_.get(0).asInstanceOf[Long]).toSet == (0L to 5L).toSet,
          s"policy=$policy")
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Very large N (fallback via hugeMethodLimit)
  // ---------------------------------------------------------------------------

  test("SPARK-56482: over-cap falls back to per-child stages") {
    // Explicit cap so the assertion is robust to future default changes.
    withSQLConf(SQLConf.WHOLESTAGE_UNION_MAX_CHILDREN.key -> "16") {
      val n = 32
      val dfs = (0 until n).map(i => rangeDF(i.toLong, i.toLong + 1L))
      val unioned = dfs.reduce((x, y) => x.union(y))
      assert(unioned.count() == n.toLong)
      assert(codegenUnions(unioned).isEmpty)
    }
  }

  // ---------------------------------------------------------------------------
  // Runtime toggle
  // ---------------------------------------------------------------------------

  test("SPARK-56482: flag flip takes effect across QueryExecutions") {
    def buildDf(): DataFrame =
      rangeDF(100).union(rangeDF(100)).filter(col("id") > 0)

    assert(wscgCount(buildDf()) == 1)
    withSQLConf(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "false") {
      assert(wscgCount(buildDf()) >= 2)
    }
  }

  // ---------------------------------------------------------------------------
  // supportCodegenFailureReason branch coverage
  // ---------------------------------------------------------------------------

  test("SPARK-56482: Nondeterministic child causes codegen fallback") {
    // rand() is Nondeterministic; union fusion should be denied
    val a = rangeDF(10).select(col("id"), rand(42).as("r"))
    val b = rangeDF(10).select(col("id"), rand(43).as("r"))
    val df = a.union(b)
    assert(codegenUnions(df).isEmpty,
      "Union with Nondeterministic child must not be inside WSCG")
    // Verify correctness despite fallback
    assertFlagParity(() => a.union(b).orderBy("id"))
  }

  test("SPARK-56482: monotonically_increasing_id child causes codegen fallback") {
    val a = rangeDF(10).select(col("id"), monotonically_increasing_id().as("mid"))
    val b = rangeDF(10).select(col("id"), monotonically_increasing_id().as("mid"))
    val df = a.union(b)
    assert(codegenUnions(df).isEmpty,
      "Union with monotonically_increasing_id child must not be inside WSCG")
  }

  test("SPARK-56482: column pruning works under union codegen (usedInputs=empty)") {
    // Union of 2-column children, parent selects only 1 column
    val (fused, _) = assertFlagParity { () =>
      val a = rangeDF(10).select(col("id"), (col("id") * 2).as("v"))
      val b = rangeDF(10, 20).select(col("id"), (col("id") * 3).as("v"))
      a.union(b).select("id").orderBy("id")
    }
    assert(codegenUnions(fused).size == 1,
      s"pruning is what this case is about, so the union has to be fused:\n${fused.queryExecution}")
  }

  test("SPARK-56482: numOutputRows with empty union children") {
    val a = rangeDF(0, 0, 1, numPartitions = 2)
    val b = rangeDF(0, 0, 1, numPartitions = 3)
    val df = a.union(b)
    df.collect()
    val unionExec = df.queryExecution.executedPlan.collectFirst {
      case u: UnionExec => u
    }
    // UnionExec may or may not exist depending on optimizer elimination
    unionExec.foreach { u =>
      assert(u.metrics("numOutputRows").value == 0L,
        "numOutputRows should be 0 for all-empty union")
    }
  }

  test("SPARK-56482: partitioning-aware union falls back to non-codegen") {
    // After repartition, both children expose a `HashPartitioning` on the same key,
    // so `UnionExec.outputPartitioning` is non-Unknown and the codegen path is denied.
    // AQE is disabled here for the `collectFirst` below: `SparkPlan.collect` stops at
    // `AdaptiveSparkPlanExec`, and before execution there is no final plan to reach anyway.
    withSQLConf(
      SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val a = rangeDF(100).repartition(4, col("id"))
      val b = rangeDF(100, 200).repartition(4, col("id"))
      val df = a.union(b)
      assert(codegenUnions(df).isEmpty,
        "Partitioning-aware union must not fuse into WSCG")
      val unionExec = df.queryExecution.executedPlan.collectFirst {
        case u: UnionExec => u
      }.get
      assert(!unionExec.metrics.contains("numOutputRows"),
        "numOutputRows metric must not be registered on the partitioning-aware path")
      assertFlagParity(() => a.union(b).orderBy("id"))
    }
  }

  test("SPARK-59122: a fused union keeps numOutputRows and reports UnknownPartitioning") {
    // The children's partitioning is not stable while the plan is being prepared:
    // `InMemoryTableScanExec.cachedPlan` unwraps the inner `AdaptiveSparkPlanExec` only once
    // `isFinalPlan` is true, and reports `UnknownPartitioning(0)` until then, so the union looks
    // plain and is fused. The projection is what makes that reachable: `supportsColumnar` is
    // `children.forall`, so one row-based `ProjectExec` over the columnar scan is enough to make
    // it false, and without one `supportCodegenFailureReason` reports `columnar` and nothing
    // fuses. `SELECT *` or a plain alias collapses the projection away and does not reproduce
    // this. Once the cache stages finalise, both children report the same concrete layout, and
    // re-deriving the decision at that point left `metrics` empty while `doProduce` asked
    // `metricTerm` for `numOutputRows`.
    //
    // Both halves of the decision are asserted here. Registering the metric unconditionally would
    // fix the crash and leave the other half broken: a fused union concatenates its children's
    // partitions, so claiming their partitioning would let a parent satisfy a clustered
    // distribution from an RDD that does not have it.
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "true",
        SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true") {
      withTempView("v") {
        cacheAggregateView("v")
        val df = spark.sql("SELECT k, abs(s) AS s FROM v UNION ALL SELECT k, s FROM v")
        // Execute this DataFrame rather than a count over it: the plan being inspected has to be
        // the one that ran, and an AQE plan that never ran has no final plan to inspect.
        assert(df.collect().length == 20)
        val fused = fusedUnions(df)
        assert(fused.nonEmpty,
          "this shape must actually fuse, or the test is not exercising the defect")
        fused.foreach { u =>
          // Part of the premise, not the whole of it: the children expose a concrete layout by now,
          // so this node is not reporting `UnknownPartitioning` merely for want of anything to
          // derive from. `rawPartitioning` also falls back when the children's remapped
          // partitionings do not compare equal, and that cannot be asserted here: each side carries
          // its own exprIds, and they line up only after the private `prepareOutputPartitioning`.
          val childPartitionings = u.children.map(_.outputPartitioning)
          assert(childPartitionings.forall(_.isInstanceOf[HashPartitioningLike]),
            s"premise: got $childPartitionings")
          assert(childPartitionings.map(_.numPartitions).distinct.size == 1,
            s"premise: got $childPartitionings")
          assert(u.metrics.contains("numOutputRows"),
            "a fused union must register the metric its generated code increments")
          assert(u.outputPartitioning.isInstanceOf[UnknownPartitioning],
            s"a fused union must not claim a concrete partitioning, got ${u.outputPartitioning}")
        }
      }
    }
  }

  test("SPARK-59122: a partitioning-aware union keeps its layout when the conf changes between " +
    "planning and execution") {
    // `spark.sql.unionOutputPartitioning` is read once during preparation, ahead of
    // `EnsureRequirements`, not on every `outputPartitioning` call, so a plan executes by the
    // partitioning it was planned against. Reading it per call let the parent aggregate lose its
    // exchange at planning and get a plain concatenation at execution, reporting each group twice.
    // The `checkAnswer` below stays outside the block that planned the DataFrame on purpose: the
    // plan is forced inside that block and `executedPlan` is memoized, so the two phases see
    // different confs. Asserting inside it, or dropping the second `withSQLConf`, makes the test
    // pass without testing this.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val left = spark.range(0, 20, 1, 2).selectExpr("id % 5 AS k")
      val right = spark.range(20, 40, 1, 2).selectExpr("id % 5 AS k")

      val planned = withSQLConf(SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true") {
        val df = left.repartition(4, col("k"))
          .union(right.repartition(4, col("k"))).groupBy("k").count()
        val plan = df.queryExecution.executedPlan
        val unions = plan.collect { case u: UnionExec => u }
        assert(unions.size == 1)
        // Asserted through the exchanges rather than through `isPlainUnion`, so that the check
        // does not depend on how the decision is stored: only the two repartitions may shuffle, so
        // the aggregate's exchange was elided, which it could only be if the union reported a
        // concrete partitioning.
        val shuffles = plan.collect { case s: ShuffleExchangeExec => s }
        assert(shuffles.size == 2)
        assert(shuffles.forall(_.shuffleOrigin == REPARTITION_BY_NUM),
          s"expected only the two repartitions, got ${shuffles.map(_.shuffleOrigin)}")
        df
      }

      withSQLConf(SQLConf.UNION_OUTPUT_PARTITIONING.key -> "false") {
        // Each side contributes four ids per `k`, so the answer is fixed. Comparing against the
        // same query run with the conf off would also pass if both paths regressed to ten rows.
        checkAnswer(planned, (0L until 5L).map(k => Row(k, 8L)))
      }
    }
  }

  test("SPARK-59122: a fused union keeps numOutputRows when the codegen conf changes between " +
    "planning and execution") {
    // `supportCodegenFailureReason` used to read `WHOLESTAGE_UNION_CODEGEN_ENABLED` live, and the
    // copy that `insertInputAdapter` puts inside the codegen shell evaluated it for the first time
    // at execution. Planned with the conf on the union is fused, so the generated code increments
    // `numOutputRows`; if the copy re-derives the reason with the conf off, `metrics` comes back
    // empty and `doProduce` throws `key not found: numOutputRows`.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val planned = withSQLConf(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "true") {
        // Each child is an exchange, which is not `CodegenSupport`, so `insertInputAdapter` wraps
        // it and the union is rebuilt through `withNewChildren`, the copy this test needs. Children
        // that do support codegen can still produce one, since `insertInputAdapter` recurses into
        // their descendants; exchanges just make it certain.
        val df = rangeDF(100).repartition(2).union(rangeDF(100).repartition(2))
        // `fusedUnions` requires the union to be the stage root, which is what this test needs: it
        // reaches for that node itself below.
        assert(fusedUnions(df).size == 1, "this shape must fuse, or the test exercises nothing")
        df
      }
      withSQLConf(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "false") {
        assert(planned.collect().length == 200)
        // The row count alone does not discriminate, since the shell was installed at planning and
        // keeps emitting; registering `numOutputRows` unconditionally and reading the conf per call
        // passes it. The `supportCodegen` assertion below is what fails there.
        val copy = fusedUnions(planned)
        assert(copy.size == 1)
        // The copy this test needs: `insertInputAdapter` wrapped both children, so the shell holds
        // a copy rather than the instance the gate answered on. This copy's reason is first forced
        // by the `SparkPlanInfo` that `collect()` above builds, with the conf already off, so what
        // it answers can only come from the stamp.
        assert(copy.head.children.forall(_.isInstanceOf[InputAdapter]))
        assert(copy.head.supportCodegen,
          "the copy in the shell must keep the decision it was planned with")
      }
    }
  }

  test("SPARK-59122: a fused union keeps numOutputRows when the child cap drops between " +
    "planning and execution") {
    // `WHOLESTAGE_UNION_MAX_CHILDREN` is on the same snapshot as the enable flag, so the same shape
    // has to hold for it: prepared under a cap this union meets, it stays fused even if the cap is
    // lowered under it. Reading the cap live would give the shell's copy `max-children-exceeded`,
    // empty `metrics`, and `doProduce` failing at `metricTerm`. Three children against a cap of
    // two, since the conf refuses anything below two.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val planned = withSQLConf(
          SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "true",
          SQLConf.WHOLESTAGE_UNION_MAX_CHILDREN.key -> "3") {
        // Exchange children again, so the shell really holds a `withNewChildren` copy.
        val df = rangeDF(100).repartition(2)
          .union(rangeDF(100).repartition(2))
          .union(rangeDF(100).repartition(2))
        val fused = fusedUnions(df)
        assert(fused.size == 1 && fused.head.children.size == 3,
          s"this shape must fuse as one three-child union, got ${fused.map(_.children.size)}")
        df
      }
      withSQLConf(SQLConf.WHOLESTAGE_UNION_MAX_CHILDREN.key -> "2") {
        assert(planned.collect().length == 300)
        val copy = fusedUnions(planned)
        assert(copy.size == 1)
        assert(copy.head.children.forall(_.isInstanceOf[InputAdapter]))
        assert(copy.head.supportCodegen,
          "the copy in the shell must keep the cap it was planned with")
        // Not `metrics.contains`, which `collect()` above already proves: an empty `metrics` would
        // have thrown at `metricTerm`. The count is what says the fused code ran and counted.
        assert(copy.head.metrics("numOutputRows").value == 300)
      }
    }
  }

  test("SPARK-59122: the codegen gate re-derives when a rule replaces the children") {
    // The gate's children-dependent terms must not outlive the children they were taken from.
    // `SQLExecution` builds a `SparkPlanInfo` before execution, which reads `metrics` on every
    // node; a decision carried from there onto a node whose children a rule then replaced would
    // fuse a topology that the gate rejects.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "true") {
      val df = rangeDF(100).union(rangeDF(100))
      val unions = fusedUnions(df)
      assert(unions.size == 1, "this shape must fuse, or the test exercises nothing")
      val union = unions.head
      // What the plan update does, and what decides the gate for this instance.
      assert(union.metrics.contains("numOutputRows"))
      assert(union.supportCodegen)

      // A nested union is one of the topologies the gate rejects, and `withNewChildren` is the path
      // a rule takes when it rewrites children in place. A rule returning an arbitrary replacement
      // node is a different path, and one `copyTagsFrom` need not carry the tags along.
      val nested = UnionExec(Seq(union.children.head, union.children.head))
      val rebuilt = union.withNewChildren(Seq(nested, union.children.last)).asInstanceOf[UnionExec]
      assert(!rebuilt.supportCodegen, "the rebuilt union must answer against its own children")
      // Implied by the line above as the code stands, and kept as the pin on that: registering the
      // metric unconditionally would leave the line above green, and only this one would fail.
      assert(rebuilt.metrics.isEmpty)
    }
  }

  test("SPARK-59122: reading the unprepared plan does not decide the prepared one") {
    // `QueryExecution.executedPlan` is `prepareForExecution(sparkPlan.clone())`, and `clone` ends
    // in `makeCopy`, which calls `copyTagsFrom`. A decision written while answering a read on
    // `sparkPlan` would therefore ride into the prepared plan. Here the two answers differ: each
    // child is an aggregate whose exchange `EnsureRequirements` has yet to insert, so the union
    // passes nothing through before preparation and both children's `HashPartitioning` after it.
    // Reads before `StampUnionDecisions` answer without writing, so only preparation decides.
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true") {
      val left = spark.range(0, 20, 1, 2).selectExpr("id % 5 AS k").groupBy("k").count()
      val right = spark.range(20, 40, 1, 2).selectExpr("id % 5 AS k").groupBy("k").count()
      val df = left.union(right)

      val unprepared = df.queryExecution.sparkPlan.collect { case u: UnionExec => u }
      assert(unprepared.size == 1)
      assert(unprepared.head.outputPartitioning.isInstanceOf[UnknownPartitioning],
        "the aggregates have no exchange under them yet, so there is nothing to pass through")

      val prepared = df.queryExecution.executedPlan.collect { case u: UnionExec => u }
      assert(prepared.size == 1)
      assert(!prepared.head.outputPartitioning.isInstanceOf[UnknownPartitioning],
        "the read above must not have decided for the prepared plan, got " +
          s"${prepared.head.outputPartitioning}")
      checkAnswer(df, (0L until 5L).flatMap(k => Seq(Row(k, 4L), Row(k, 4L))))
    }
  }

  test("SPARK-59122: a partitioning-aware union follows its children's coalesced partition count") {
    // Only the decision is stamped, never the `Partitioning`. AQE coalescing changes the children's
    // `numPartitions` after the stamp, and `unionRDDs` hands whatever it reports to
    // `SQLPartitioningAwareUnionRDD`, which builds exactly that many partitions from each child: a
    // count frozen at stamping time asks for partitions the coalesced children no longer have.
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
        SQLConf.SHUFFLE_PARTITIONS.key -> "20",
        SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true") {
      val left = spark.range(0, 100, 1, 4).selectExpr("id % 10 AS k").groupBy("k").count()
      val right = spark.range(100, 200, 1, 4).selectExpr("id % 10 AS k").groupBy("k").count()
      val df = left.union(right).groupBy("k").agg(sum("count").as("c"))
      checkAnswer(df, (0L until 10L).map(k => Row(k, 20L)))

      val unions = collect(df.queryExecution.executedPlan) { case u: UnionExec => u }
      assert(unions.size == 1)
      val children = unions.head.children.map(_.outputPartitioning.numPartitions)
      assert(children.distinct.size == 1 && children.head < 20,
        s"the children must have been coalesced as one group, got $children")
      assert(unions.head.outputPartitioning.numPartitions == children.head,
        "the union must report what its children report now, got " +
          s"${unions.head.outputPartitioning}")
    }
  }

  test("SPARK-59122: a later stamping pass fills in a fresh union and keeps stamped ones") {
    // `StampUnionDecisions` is listed again after the phases that can add a `UnionExec`, so one an
    // injected columnar or query-stage rule created does not answer from whatever the conf says
    // wherever it is first asked. A later pass must also not move a decision already taken, which
    // is the second half here. The rule is driven directly, since what this case is about is its
    // contract; that the pipelines still list it after each phase that can add a union is pinned
    // from the outside by the extension-driven cases in `SparkSessionExtensionSuite`, which need a
    // session of their own.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      // Pins the property the standard pipeline has to keep: a stamping pass runs after
      // `EnsureRequirements`, so the decision is taken from the plan the exchanges were placed in.
      // A count would break on a sixth legitimate pass and say nothing about the order. The AQE
      // lists are private to `AdaptiveSparkPlanExec`, so their first pass has no counterpart here.
      val rules = QueryExecution.preparations(spark, subquery = false)
      val firstStamp = rules.indexWhere(_ eq StampUnionDecisions)
      val ensureRequirements = rules.indexWhere(_.isInstanceOf[EnsureRequirements])
      val columnarRules =
        rules.indexWhere(_.isInstanceOf[ApplyColumnarRulesAndInsertTransitions])
      assert(ensureRequirements >= 0 && firstStamp > ensureRequirements &&
          firstStamp < columnarRules,
        "expected a stamping pass between EnsureRequirements and the columnar rules, got " +
          s"$ensureRequirements/$firstStamp/$columnarRules")

      val stamped = withSQLConf(SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true") {
        val df = spark.range(0, 20, 1, 2).selectExpr("id % 5 AS k").repartition(4, col("k"))
          .union(spark.range(20, 40, 1, 2).selectExpr("id % 5 AS k").repartition(4, col("k")))
        val union = df.queryExecution.executedPlan.collect { case u: UnionExec => u }
        assert(union.size == 1)
        union.head
      }

      // A fresh node standing in for one an extension made after the first pass: no decision yet.
      val fresh = UnionExec(stamped.children)
      withSQLConf(SQLConf.UNION_OUTPUT_PARTITIONING.key -> "false") {
        StampUnionDecisions(fresh)
      }
      // Read back with the conf the other way round, so the answer can only come from the stamp:
      // deriving here would make it non-plain, these children being co-partitioned.
      withSQLConf(SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true") {
        assert(fresh.isPlainUnion, "the barrier must have decided the fresh node")
      }

      // The other half. The conf a decision was stamped with is the part a second pass could move,
      // so the node to watch is one whose gate the conf still answers: plain, and with its reason
      // not yet forced. `fusedUnions` returns the copy inside the codegen shell, whose reason no
      // preparation rule has asked for, so what it answers below comes from the stamp alone.
      val fused = withSQLConf(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "true") {
        val df = rangeDF(100).repartition(2).union(rangeDF(100).repartition(2))
        val union = fusedUnions(df)
        assert(union.size == 1, "this shape must fuse, or the test exercises nothing")
        union.head
      }
      withSQLConf(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "false") {
        StampUnionDecisions(fused)
        assert(fused.supportCodegen, "a second pass must not restamp the conf it was decided with")
      }
    }
  }

  test("SPARK-59122: the stamp uses the conf the exchanges were planned against") {
    // `EnsureRequirements` asks the union what it reports, and the barrier behind it freezes that
    // answer one rule later. `conf` is live, so another thread turning `UNION_OUTPUT_PARTITIONING`
    // off in between would leave the parent's elided exchange standing over a union that then
    // concatenates. `SnapshotUnionOutputPartitioningConf` records the value ahead of
    // `EnsureRequirements` for both to use. Driven rule by rule, because the two sit next to each
    // other in the pipeline and no injected rule can run in the window.
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true") {
      // The three have to stay contiguous, which nothing at the list itself says: an injected rule
      // cannot land between them, but an edit to the list can. AQE builds its own list, and
      // `AdaptiveQueryExecSuite` asserts the same order there.
      val rules = QueryExecution.preparations(spark, subquery = false)
      val snapshot = rules.indexWhere(_ eq SnapshotUnionOutputPartitioningConf)
      val ensureRequirements = rules.indexWhere(_.isInstanceOf[EnsureRequirements])
      assert(snapshot >= 0 && snapshot == ensureRequirements - 1,
        s"expected the conf snapshot right before EnsureRequirements at $ensureRequirements, " +
          s"got $snapshot")

      val df = spark.range(0, 20, 1, 2).selectExpr("id % 5 AS k").repartition(4, col("k"))
        .union(spark.range(20, 40, 1, 2).selectExpr("id % 5 AS k").repartition(4, col("k")))
        .groupBy("k").count()
      val required = EnsureRequirements()(
        SnapshotUnionOutputPartitioningConf(df.queryExecution.sparkPlan.clone()))
      assert(required.collect { case s: ShuffleExchangeExec => s.shuffleOrigin } ==
        Seq(REPARTITION_BY_NUM, REPARTITION_BY_NUM),
        "the aggregate's exchange must have been elided, or the window has nothing at stake")

      val union = required.collect { case u: UnionExec => u }
      assert(union.size == 1)
      withSQLConf(SQLConf.UNION_OUTPUT_PARTITIONING.key -> "false") {
        StampUnionDecisions(required)
        assert(!union.head.outputPartitioning.isInstanceOf[UnknownPartitioning],
          "the answer must come from the conf snapshot, not from the value read now, got " +
            s"${union.head.outputPartitioning}")
      }
    }
  }

  test("SPARK-56482: input_file_name child fuses (Nondeterministic but partition-index-free)") {
    // `InputFileName` is `Nondeterministic` but reads from `InputFileBlockHolder`
    // (a per-task thread-local) and does not embed `partitionIndex`. The gate's
    // narrow check should let this fuse.
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      rangeDF(20).write.parquet(path)
      val a = spark.read.parquet(path).select(col("id"), input_file_name().as("f"))
      val b = spark.read.parquet(path).select(col("id"), input_file_name().as("f"))
      val df = a.union(b).filter(col("id") > 0)
      assert(codegenUnions(df).nonEmpty,
        "Union with input_file_name child should fuse into WSCG")
      assertFlagParity(() => a.union(b).orderBy("id", "f"))
    }
  }

  test("SPARK-56482: union with sample children fuses (or falls back) without crashing") {
    // `SampleExec.doConsume` reads `currentPartitionIndexVar` from inside an
    // `addMutableState` initializer, which is emitted into the state-init
    // function rather than the per-child helper. The bound expression must
    // therefore resolve in any emission scope, not just inside the helper.
    val a = rangeDF(20).sample(false, 0.5, 1L)
    val b = rangeDF(20).sample(false, 0.5, 1L)
    val df = a.union(b).filter(col("id") > 0)
    df.collect()
    assertFlagParity(() => a.union(b).orderBy("id"))
  }

  test("SPARK-57196: concurrent codegen of a shared UnionExec stage is thread-safe") {
    // A single `UnionExec` instance can have its whole-stage codegen driven by
    // more than one thread at a time: a reused exchange/subquery is generated
    // concurrently with the main plan, and async subquery/DPP execution can
    // overlap a driver-side `doCodeGen`. The fusion path kept per-emission state
    // (`currentEmittingChild`) in a mutable field on the shared instance, so a
    // racing `doProduce` could reset it to -1 while another thread was still in
    // `doConsume`, tripping the "UnionExec.doConsume invoked outside doProduce
    // emission window" requirement. Generating the same fused stage from many
    // threads reproduces the race.
    val df = rangeDF(100).union(rangeDF(100)).filter(col("id") > 0)
    assert(codegenUnions(df).nonEmpty)
    val wscg = df.queryExecution.executedPlan.collectFirst {
      case w: WholeStageCodegenExec if w.find(_.isInstanceOf[UnionExec]).isDefined => w
    }.getOrElse(fail("expected a fused UnionExec stage"))

    val numThreads = 8
    val iterations = 200
    val pool = Executors.newFixedThreadPool(numThreads)
    val errors = java.util.Collections.synchronizedList(new java.util.ArrayList[Throwable]())
    try {
      val startLatch = new CountDownLatch(1)
      val futures = (0 until numThreads).map { _ =>
        pool.submit(new Runnable {
          override def run(): Unit = {
            startLatch.await()
            var n = 0
            while (n < iterations) {
              try {
                wscg.doCodeGen()
              } catch {
                case t: Throwable => errors.add(t)
              }
              n += 1
            }
          }
        })
      }
      startLatch.countDown()
      futures.foreach(_.get(60, TimeUnit.SECONDS))
    } finally {
      pool.shutdownNow()
    }
    assert(errors.isEmpty,
      "concurrent doCodeGen on a shared UnionExec stage raced:\n" +
        errors.toArray.map(_.toString).mkString("\n"))
  }
}

/** Runs [[UnionCodegenSuite]] with ANSI mode enabled. */
class UnionCodegenAnsiSuite extends UnionCodegenSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")
}

/** Runs [[UnionCodegenSuite]] with adaptive query execution enabled. */
class UnionCodegenAqeSuite extends UnionCodegenSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
}
