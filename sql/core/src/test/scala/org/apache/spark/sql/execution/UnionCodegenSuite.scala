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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Tests for `UnionExec` whole-stage codegen fusion: plan-shape assertions,
 * correctness, type widening, metrics, and fallbacks.
 */
class UnionCodegenSuite extends QueryTest with SharedSparkSession {

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

  private def unionInsideWSCG(df: DataFrame): Boolean =
    df.queryExecution.executedPlan.collect {
      case w: WholeStageCodegenExec if w.find(_.isInstanceOf[UnionExec]).isDefined => w
    }.nonEmpty

  /** Run query with flag on, then flag off, assert results match. */
  protected def assertFlagParity(buildDf: () => DataFrame): Unit = {
    val onRows = buildDf().collect().toSeq
    withSQLConf(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "false") {
      checkAnswer(buildDf(), onRows)
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
    assert(unionInsideWSCG(df))
  }

  test("SPARK-56482: flag off restores pre-patch plan shape") {
    withSQLConf(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "false") {
      val df = rangeDF(100).union(rangeDF(100)).filter(col("id") > 0)
      assert(wscgCount(df) >= 2)
      assert(!unionInsideWSCG(df))
    }
  }

  test("SPARK-56482: maxChildren exceeded falls back") {
    withSQLConf(
      SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "true",
      SQLConf.WHOLESTAGE_UNION_MAX_CHILDREN.key -> "2") {
      val df = rangeDF(10).union(rangeDF(10)).union(rangeDF(10))
      assert(!unionInsideWSCG(df))
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
    assert(unionInsideWSCG(build().filter(col("v") >= 0)),
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
    assert(unionInsideWSCG(df),
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
    assert(!unionInsideWSCG(df),
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
    assert(!unionInsideWSCG(df),
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
      val left = rangeDF(100).select(col("id").as("lk"), col("id").as("lv"))
      val right = rangeDF(100).select(col("id").as("rk"))
      val bhj = left.join(broadcast(right), col("lk") === col("rk"))
        .select("lk", "lv")
      val df = bhj.union(
        rangeDF(100).select(col("id").as("lk"), col("id").as("lv")))
      val flagOn = df.collect().toSeq
      withSQLConf(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "false") {
        checkAnswer(df, flagOn)
      }
    }
  }

  test("SPARK-56482: BHJ multi-row child feeds downstream agg correctly") {
    withSQLConf(
      SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760") {
      val probe = rangeDF(10).select(col("id").as("k"))
      val build = rangeDF(20)
        .select((col("id") % 5).as("k"), col("id").as("v"))
      val bhj = probe.join(broadcast(build), "k")
      val df = bhj.union(
        rangeDF(0).select(col("id").as("k"), col("id").as("v")))
      val agg = df.groupBy("k").count().orderBy("k")
      val flagOn = agg.collect()
      withSQLConf(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "false") {
        checkAnswer(agg, flagOn.toSeq)
      }
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
      val left = rangeDF(100).select(col("id").as("k"))
      val right = rangeDF(100).select(col("id").as("k"))
      val smj = left.join(right, "k")
      val df = smj.union(rangeDF(100).select(col("id").as("k")))
      val flagOn = df.collect().toSeq
      withSQLConf(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "false") {
        checkAnswer(df, flagOn)
      }
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
      val df = cached.union(rangeDF(100, 200))
      val flagOn = df.collect().toSet
      withSQLConf(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "false") {
        assert(df.collect().toSet == flagOn)
      }
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
      assert(!unionInsideWSCG(unioned))
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
    assert(!unionInsideWSCG(df),
      "Union with Nondeterministic child must not be inside WSCG")
    // Verify correctness despite fallback
    assertFlagParity(() => a.union(b).orderBy("id"))
  }

  test("SPARK-56482: monotonically_increasing_id child causes codegen fallback") {
    val a = rangeDF(10).select(col("id"), monotonically_increasing_id().as("mid"))
    val b = rangeDF(10).select(col("id"), monotonically_increasing_id().as("mid"))
    val df = a.union(b)
    assert(!unionInsideWSCG(df),
      "Union with monotonically_increasing_id child must not be inside WSCG")
  }

  test("SPARK-56482: column pruning works under union codegen (usedInputs=empty)") {
    // Union of 2-column children, parent selects only 1 column
    val a = rangeDF(10).select(col("id"), (col("id") * 2).as("v"))
    val b = rangeDF(10, 20).select(col("id"), (col("id") * 3).as("v"))
    val df = a.union(b).select("id").orderBy("id")
    val flagOn = df.collect()
    withSQLConf(SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "false") {
      checkAnswer(df, flagOn.toSeq)
    }
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
