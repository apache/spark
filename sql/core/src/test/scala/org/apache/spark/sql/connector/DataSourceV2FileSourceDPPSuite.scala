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

import java.io.File

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, DynamicPruningSubquery, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
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
abstract class DataSourceV2FileSourceDPPSuiteBase extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  // Standard V2-only conf for DPP tests on file sources. AQE is set by the concrete subclasses,
  // which mix in DisableAdaptiveExecutionSuite / EnableAdaptiveExecutionSuite: the DPP subquery is
  // planned by PlanDynamicPruningFilters without AQE and by PlanAdaptiveDynamicPruningFilters with
  // it, and those are two separate rules.
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
  private def writeFactAndDim(dir: File, format: String = "parquet"): Unit = {
    val factPath = new File(dir, "fact").getCanonicalPath
    val dimPath = new File(dir, "dim").getCanonicalPath
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

  private def fileScanOf(df: DataFrame): FileScan = factScanOf(df).scan.asInstanceOf[FileScan]

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
        val factScan = factScanOf(df)
        // A DPP filter that degraded to true is also a DynamicPruningExpression, so match on the
        // payload: only a subquery child means the filter can actually prune anything.
        val dppFilters = factScan.runtimeFilters.collect {
          case d @ DynamicPruningExpression(child) if !child.isInstanceOf[Literal] => d
        }
        assert(dppFilters.nonEmpty,
          "expected the fact scan's runtimeFilters to carry a DynamicPruningExpression over a " +
            s"subquery, got ${factScan.runtimeFilters}")
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
        val numOutputRows = factScanOf(df).metrics("numOutputRows").value
        assert(numOutputRows == 10,
          s"expected fact scan to read 10 rows after scalar-subquery pruning, got " +
            s"$numOutputRows. plan:\n" + df.queryExecution.executedPlan.treeString)
      }
    }
  }

  test("filterAttributes reports the read partition columns") {
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        val df = sql("SELECT f.id, f.part FROM fact f")
        df.collect()
        val declared = fileScanOf(df).filterAttributes().flatMap(_.fieldNames).toSeq
        assert(declared == Seq("part"),
          s"expected the partition column to be declared runtime-filterable, got $declared")
      }
    }
  }

  test("filterAttributes reports nothing once a pushed aggregate drops the partition columns") {
    // With an aggregate pushed down, ParquetScan.readSchema() returns only the aggregate schema, so
    // the partition columns are not in the scan relation output. Spark resolves every declared
    // runtime-filter attribute against that output and fails the query if one is missing, so
    // filterAttributes() must not report them.
    withDppV2Conf {
      withSQLConf(SQLConf.PARQUET_AGGREGATE_PUSHDOWN_ENABLED.key -> "true") {
        withTempDir { dir =>
          writeFactAndDim(dir)
          val df = sql("SELECT count(part) FROM fact")
          checkAnswer(df, Row(100))
          val declared = fileScanOf(df).filterAttributes().flatMap(_.fieldNames).toSeq
          assert(declared.isEmpty,
            s"expected no runtime-filterable attribute under a pushed aggregate, got $declared")
        }
      }
    }
  }

  test("filterAttributes keeps a partition column a pushed aggregate groups by") {
    // Aggregate pushdown only happens when the GROUP BY set covers every partition column, and
    // those columns go into the pushed-down schema, so they stay in the scan relation output and
    // stay runtime-filterable. This is the other side of the intersection from the test above.
    withDppV2Conf {
      withSQLConf(SQLConf.PARQUET_AGGREGATE_PUSHDOWN_ENABLED.key -> "true") {
        withTempDir { dir =>
          writeFactAndDim(dir)
          val df = sql("SELECT part, count(*) FROM fact GROUP BY part")
          checkAnswer(df, (0 until 10).map(p => Row(p, 10)))
          val declared = fileScanOf(df).filterAttributes().flatMap(_.fieldNames).toSeq
          assert(declared == Seq("part"),
            s"expected the group-by partition column to stay declared, got $declared")
        }
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

  test("a runtime filter is ANDed with the compile-time partition filters, not substituted") {
    // `buildPartitions` gets `partitionFilters ++ expressions`. Both halves are the scan's only
    // evaluator: FileScanBuilder.pushFilters returns just the data filters as post-scan filters,
    // and a partition-column runtime filter is declared fully pushed, so dropping either half
    // returns rows that nothing filters out.
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        // min(dim_id) over dim_val > 6 is 7, so this keeps partitions 7 and 8: 7 from the runtime
        // filter's lower bound, 8 from the compile-time upper bound.
        val df = sql(
          """SELECT f.id FROM fact f
            |WHERE f.part <= 8
            |  AND f.part >= (SELECT min(dim_id) FROM dim WHERE dim_val > 6)""".stripMargin)
        checkAnswer(df, (0 until 10).flatMap(i => Seq(Row(i * 10 + 7), Row(i * 10 + 8))))
        val numOutputRows = factScanOf(df).metrics("numOutputRows").value
        assert(numOutputRows == 20,
          s"expected the fact scan to read only partitions 7 and 8, got $numOutputRows rows")
      }
    }
  }

  test("runtime partition pruning resolves a case-insensitive partition column reference") {
    // `f.PART` resolves to the schema's `part`, so the filter Spark routes to the scan carries the
    // schema spelling -- which is what PartitioningAwareFileIndex matches against its partition
    // columns by exact string. The query has to keep pruning, and with the filter fully pushed a
    // silently skipped predicate would also return wrong rows, not just read too much.
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

  test("a fully pushed filter stays exact when one FilePartition spans partition directories") {
    // The fully-pushed contract used to ask for the attribute's value to be fixed within every
    // InputPartition. A file scan does not satisfy that: FilePartition packs by size and can hold
    // files from several partition directories. What makes the declaration sound is that the
    // predicate chose those directories, so every row in the partition satisfies it even though the
    // value varies. Pack the surviving directories into one FilePartition to exercise that.
    withDppV2Conf {
      withSQLConf(
        SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1g",
        SQLConf.FILES_MIN_PARTITION_NUM.key -> "1",
        SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "0") {
        withTempDir { dir =>
          writeFactAndDim(dir)
          // min(dim_id) over dim_val > 7 is 8, so this keeps partitions 8 and 9.
          val df = sql(
            """SELECT f.id FROM fact f
              |WHERE f.part >= (SELECT min(dim_id) FROM dim WHERE dim_val > 7)""".stripMargin)
          checkAnswer(df, (0 until 10).flatMap(i => Seq(Row(i * 10 + 8), Row(i * 10 + 9))))
          val factScan = factScanOf(df)
          assert(factScan.filteredPartitions.flatten.size == 1,
            "expected both surviving partition directories to pack into one FilePartition, got " +
              s"${factScan.filteredPartitions.flatten.size}")
          val numOutputRows = factScan.metrics("numOutputRows").value
          assert(numOutputRows == 20,
            s"expected that one FilePartition to carry both directories' 20 rows, got " +
              s"$numOutputRows")
          assert(collect(df.queryExecution.executedPlan) { case f: FilterExec => f }.isEmpty,
            "expected the partition filter to be fully pushed, leaving no FilterExec")
        }
      }
    }
  }

  test("a DPP filter degraded to true leaves the v2 file scan unpruned") {
    // With reuse-only DPP and no broadcast to reuse, the subquery is replaced by true before
    // execution. What this pins is that such a filter still reaches the scan and the query stays
    // correct; the row count only records that nothing was pruned, which is what a true predicate
    // does whether or not `catalystRuntimeFilters` drops it.
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

class DataSourceV2FileSourceDPPSuiteAEOff extends DataSourceV2FileSourceDPPSuiteBase
  with DisableAdaptiveExecutionSuite

class DataSourceV2FileSourceDPPSuiteAEOn extends DataSourceV2FileSourceDPPSuiteBase
  with EnableAdaptiveExecutionSuite
