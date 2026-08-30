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

package org.apache.spark.sql.connector

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.expressions.{DynamicPruning, DynamicPruningExpression, DynamicPruningSubquery, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for SPARK-30628: subquery partition pruning and DPP for V2 file sources.
 *
 * These tests exercise V2 file scans (e.g. ParquetScan) -- distinct from the in-memory V2
 * catalog used by [[org.apache.spark.sql.DynamicPartitionPruningV2Suite]] and from the V2
 * iterative-pushdown work covered by [[DataSourceV2EnhancedRuntimePartitionFilterSuite]].
 */
class DataSourceV2FileSourceDPPSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  // Standard V2-only conf for DPP tests on file sources.
  private def withDppV2Conf[T](thunk: => T): T = {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "2")(thunk)
  }

  // Writes a partitioned `fact` and an unpartitioned `dim` table in `format` and registers them
  // as temp views. `fact` has 100 rows across 10 partitions (10 rows each); `dim` has 10 rows
  // with `dim_id` and `dim_val` both in [0, 10).
  private def writeFactAndDim(dir: java.io.File, format: String = "parquet"): Unit = {
    val factPath = new java.io.File(dir, "fact").getCanonicalPath
    val dimPath = new java.io.File(dir, "dim").getCanonicalPath
    spark.range(100)
      .selectExpr("id", "id % 10 AS part")
      .write.format(format).partitionBy("part").save(factPath)
    spark.read.format(format).load(factPath).createOrReplaceTempView("fact")
    spark.range(10)
      .selectExpr("id AS dim_id", "id AS dim_val")
      .write.format(format).save(dimPath)
    spark.read.format(format).load(dimPath).createOrReplaceTempView("dim")
  }

  // The BatchScanExec reading `fact`, the only partitioned table of the two. The lookup descends
  // into AQE query stages, so it finds the scan whichever stage it ended up in.
  private def factScanOf(df: DataFrame): BatchScanExec = {
    val executedPlan = df.queryExecution.executedPlan
    collectFirst(executedPlan) {
      case b: BatchScanExec if b.scan.isInstanceOf[FileScan] &&
        b.scan.asInstanceOf[FileScan].readPartitionSchema.nonEmpty => b
    }.getOrElse(fail("no fact BatchScanExec found in:\n" + executedPlan.treeString))
  }

  private def collectDppFilters(plan: LogicalPlan): Seq[Expression] = {
    plan.collect {
      case f: Filter if f.condition.exists(_.isInstanceOf[DynamicPruningSubquery]) => f.condition
    }
  }

  test("DPP inserts DynamicPruningSubquery for V2 parquet partitioned table") {
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        val df = sql(
          """SELECT f.id FROM fact f JOIN dim d
            |ON f.part = d.dim_id WHERE d.dim_val = 7""".stripMargin)
        val optimized = df.queryExecution.optimizedPlan
        assert(collectDppFilters(optimized).nonEmpty,
          "expected DynamicPruningSubquery in optimized plan over V2 parquet, got plan:\n" +
            optimized.treeString)
      }
    }
  }

  test("DPP does not fire when join key is a data column (non-partition)") {
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        // Join on f.id (data column), not f.part (partition column).
        val df = sql(
          """SELECT f.id FROM fact f JOIN dim d
            |ON f.id = d.dim_id WHERE d.dim_val = 7""".stripMargin)
        val optimized = df.queryExecution.optimizedPlan
        assert(collectDppFilters(optimized).isEmpty,
          "DPP should not fire on non-partition join keys, got plan:\n" +
            optimized.treeString)
      }
    }
  }

  test("DPP fires when partitioned fact is on the right side of the join") {
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        // dim on left, fact on right; join on partition column
        val df = sql(
          """SELECT f.id FROM dim d JOIN fact f
            |ON d.dim_id = f.part WHERE d.dim_val = 7""".stripMargin)
        val optimized = df.queryExecution.optimizedPlan
        assert(collectDppFilters(optimized).nonEmpty,
          "expected DPP on right-side partitioned fact, got plan:\n" +
            optimized.treeString)
      }
    }
  }

  test("DPP filter reaches BatchScanExec.runtimeFilters as DynamicPruningExpression") {
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        val df = sql(
          """SELECT f.id FROM fact f JOIN dim d
            |ON f.part = d.dim_id WHERE d.dim_val = 7""".stripMargin)
        df.collect()
        val executedPlan = stripAQEPlan(df.queryExecution.executedPlan)
        val fileBatchScans = executedPlan.collect {
          case b: BatchScanExec if b.scan.isInstanceOf[FileScan] => b
        }
        assert(fileBatchScans.nonEmpty,
          "expected at least one FileScan-backed BatchScanExec, got plan:\n" +
            executedPlan.treeString)
        val dppFiltersByScan = fileBatchScans.map { b =>
          b.runtimeFilters.collect { case d: DynamicPruningExpression => d }
        }
        assert(dppFiltersByScan.exists(_.nonEmpty),
          "expected DynamicPruningExpression in BatchScanExec.runtimeFilters of fact, got:\n" +
            executedPlan.treeString)
      }
    }
  }

  test("DPP prunes input partitions at runtime for v2 parquet") {
    // Fact has 100 rows across 10 partitions (10 rows each). DPP filter d.dim_val = 7 selects
    // exactly partition 7 -> 10 rows. Without runtime pruning, all 100 rows are read.
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        val df = sql(
          """SELECT f.id FROM fact f JOIN dim d
            |ON f.part = d.dim_id WHERE d.dim_val = 7""".stripMargin)
        val rows = df.collect()
        // Correctness: 10 fact rows match (partition 7), each multiplied by 1 dim row.
        assert(rows.length == 10,
          s"expected 10 rows after join+filter, got ${rows.length}")
        // Pruning: fact scan should have read only the 10 rows in partition 7.
        val numOutputRows = factScanOf(df).metrics("numOutputRows").value
        assert(numOutputRows == 10,
          s"expected fact scan to read 10 rows after DPP, got $numOutputRows. plan:\n" +
            df.queryExecution.executedPlan.treeString)
      }
    }
  }

  test("scalar subquery on partition column of v2 parquet prunes partitions") {
    // Scalar subquery (SELECT max(dim_id) FROM dim WHERE dim_val = 7) evaluates to 7.
    // WHERE f.part = 7 should select exactly partition 7 -> 10 rows. Without partition
    // pruning, the scan reads all 100 rows and the post-filter keeps 10.
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        val df = sql(
          """SELECT f.id FROM fact f
            |WHERE f.part = (SELECT max(dim_id) FROM dim WHERE dim_val = 7)""".stripMargin)
        val rows = df.collect()
        assert(rows.length == 10,
          s"expected 10 rows after scalar-subquery filter, got ${rows.length}")
        val executedPlan = stripAQEPlan(df.queryExecution.executedPlan)
        val numOutputRows = factScanOf(df).metrics("numOutputRows").value
        assert(numOutputRows == 10,
          s"expected fact scan to read 10 rows after scalar-subquery pruning, got " +
            s"$numOutputRows. plan:\n" + executedPlan.treeString)
      }
    }
  }

  test("scalar subquery filter on partition column does not wrap in DynamicPruning") {
    // Scalar-subquery filters on partition columns must NOT be wrapped in DynamicPruning.
    // They reach the scan as raw ScalarSubquery / ExecScalarSubquery expressions; partition
    // pruning happens at runtime via FileScan.planInputPartitionsWithRuntimeFilters, not via
    // the DPP rule. Wrapping in DynamicPruning would route them through a path that assumes
    // a DPP-style broadcast subquery and breaks the scalar-subquery semantics.
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        val df = sql(
          """SELECT f.id FROM fact f
            |WHERE f.part = (SELECT max(dim_id) FROM dim WHERE dim_val < 5)""".stripMargin)
        val optimized = df.queryExecution.optimizedPlan
        val dynamicPruningInstances = optimized.flatMap(_.expressions).flatMap { e =>
          e.collect { case d: DynamicPruning => d }
        }
        assert(dynamicPruningInstances.isEmpty,
          "scalar subquery filters must not be wrapped in DynamicPruning at this commit, " +
            "got plan:\n" + optimized.treeString)
      }
    }
  }

  test("scalar subquery on a partition column is not re-evaluated after the scan") {
    // FileScan declares its partition columns in fullyPushedFilterAttributes: applying such a
    // filter selects partition directories, so every row the scan returns satisfies it and Spark
    // drops the post-scan FilterExec, the same treatment a compile-time partition filter gets.
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        val df = sql(
          """SELECT f.id FROM fact f
            |WHERE f.part = (SELECT max(dim_id) FROM dim WHERE dim_val = 7)""".stripMargin)
        val rows = df.collect()
        assert(rows.length == 10, s"expected 10 rows, got ${rows.length}")
        val executedPlan = df.queryExecution.executedPlan
        assert(collect(executedPlan) { case f: FilterExec => f }.isEmpty,
          "a fully pushed partition filter should leave no FilterExec above the scan, got:\n" +
            executedPlan.treeString)
      }
    }
  }

  test("runtime partition pruning resolves a case-insensitive partition column reference") {
    // The filter Spark routes to the scan carries the relation output's attribute name, and
    // PartitioningAwareFileIndex matches that name against its partition columns by exact string.
    // A case-insensitive reference must still resolve to the schema's `part`, or the predicate
    // would be silently skipped -- and since the filter is fully pushed, evaluated nowhere.
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        val df = sql(
          """SELECT f.id FROM fact f
            |WHERE f.PART = (SELECT max(dim_id) FROM dim WHERE dim_val = 7)""".stripMargin)
        val rows = df.collect()
        assert(rows.length == 10, s"expected 10 rows, got ${rows.length}")
        val numOutputRows = factScanOf(df).metrics("numOutputRows").value
        assert(numOutputRows == 10,
          s"expected the fact scan to read 10 rows, got $numOutputRows")
      }
    }
  }

  test("a DPP filter degraded to true leaves the v2 file scan unpruned") {
    // With reuse-only DPP and no broadcast to reuse, the subquery is replaced by true before
    // execution. Such a filter matches every row, so the scan must read every partition.
    withDppV2Conf {
      withSQLConf(
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        withTempDir { dir =>
          writeFactAndDim(dir)
          val df = sql(
            """SELECT f.id FROM fact f JOIN dim d
              |ON f.part = d.dim_id WHERE d.dim_val = 7""".stripMargin)
          val rows = df.collect()
          assert(rows.length == 10, s"expected 10 rows after join+filter, got ${rows.length}")
          val factScan = factScanOf(df)
          assert(factScan.runtimeFilters.exists {
            case DynamicPruningExpression(l: Literal) => l == Literal.TrueLiteral
            case _ => false
          }, s"expected a degraded DPP filter, got ${factScan.runtimeFilters}")
          val numOutputRows = factScan.metrics("numOutputRows").value
          assert(numOutputRows == 100,
            s"expected the fact scan to read all 100 rows, got $numOutputRows")
        }
      }
    }
  }

  Seq("orc", "json").foreach { format =>
    test(s"DPP prunes input partitions at runtime for v2 $format") {
      withDppV2Conf {
        withTempDir { dir =>
          writeFactAndDim(dir, format)
          val df = sql(
            """SELECT f.id FROM fact f JOIN dim d
              |ON f.part = d.dim_id WHERE d.dim_val = 7""".stripMargin)
          val rows = df.collect()
          assert(rows.length == 10, s"expected 10 rows after join+filter, got ${rows.length}")
          val numOutputRows = factScanOf(df).metrics("numOutputRows").value
          assert(numOutputRows == 10,
            s"expected the fact scan to read 10 rows after DPP, got $numOutputRows")
        }
      }
    }
  }
}
