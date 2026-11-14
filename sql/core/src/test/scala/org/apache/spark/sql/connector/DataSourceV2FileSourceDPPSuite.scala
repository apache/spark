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

import org.apache.logging.log4j.Level

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, DynamicPruningExpression, DynamicPruningSubquery, EqualTo, EvalMode, Expression, GreaterThanOrEqual, In, Literal, Not}
import org.apache.spark.sql.catalyst.planning.{DeltaBasedRowLevelOperation, GroupBasedRowLevelOperation}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, ReplaceData, WriteDelta}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{ReplaceDataProjections, WriteDeltaProjections}
import org.apache.spark.sql.connector.catalog.{Column, InMemoryTableCatalog, SupportsRowLevelOperations, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference}
import org.apache.spark.sql.connector.read.{InputPartition, ScanBuilder}
import org.apache.spark.sql.connector.write.{DeltaWriteBuilder, LogicalWriteInfo, RowLevelOperation, RowLevelOperationBuilder, RowLevelOperationInfo, RowLevelOperationTable, SupportsDelta, WriteBuilder}
import org.apache.spark.sql.execution.{FilterExec, ScalarSubquery}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.execution.datasources.{FilePartition, InMemoryFileIndex, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation, FileScan, PushDownUtils, RuntimePartitionPruningFileScan}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.datasources.v2.text.TextScan
import org.apache.spark.sql.execution.dynamicpruning.{PartitionPruning, RowLevelOperationRuntimeGroupFiltering}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.tags.DataSourceV2Test

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

  // The tests register their temp views over temp directories deleted when the test ends.
  override def afterEach(): Unit = {
    try {
      spark.sessionState.catalog.clearTempTables()
    } finally {
      super.afterEach()
    }
  }

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
  // with `dim_id` and `dim_val` both in [0, 10). The read schema is given explicitly because csv
  // is written without a header; it also keeps the join key `cast(part as bigint)` in every format.
  private def writeFactAndDim(dir: File, format: String = "parquet"): Unit = {
    val factPath = new File(dir, "fact").getCanonicalPath
    val dimPath = new File(dir, "dim").getCanonicalPath
    spark.range(100)
      .selectExpr("id", "id % 10 AS part")
      .write.format(format).partitionBy("part").save(factPath)
    spark.read.format(format).schema("id long, part int").load(factPath)
      .createOrReplaceTempView("fact")
    spark.range(10)
      .selectExpr("id AS dim_id", "id AS dim_val")
      .write.format(format).save(dimPath)
    spark.read.format(format).schema("dim_id long, dim_val long").load(dimPath)
      .createOrReplaceTempView("dim")
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

  private def fileScanOf(df: DataFrame): RuntimePartitionPruningFileScan =
    factScanOf(df).scan.asInstanceOf[RuntimePartitionPruningFileScan]

  // A copy of `scan` whose file index runs `onList` every time it lists files.
  private def withListingHook(scan: ParquetScan)(onList: => Unit): ParquetScan = {
    val index = scan.fileIndex
    scan.copy(fileIndex = new InMemoryFileIndex(spark, index.rootPaths, Map.empty, None,
        userSpecifiedPartitionSpec = Some(index.partitionSpec())) {
      override def listFiles(
          partitionFilters: Seq[Expression],
          dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
        onList
        super.listFiles(partitionFilters, dataFilters)
      }
    })
  }

  // The splits of each partition, as (path, start, length).
  private def layoutOf(parts: Array[InputPartition]): Seq[Seq[(String, Long, Long)]] =
    parts.toSeq.map(_.asInstanceOf[FilePartition].files.toSeq.map { f =>
      (f.filePath.toString, f.start, f.length)
    })

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
        // Join on f.id (data column), not f.part (partition column). `f.part` stays in the
        // projection so the fact scan still reads a partition column: without it the scan declares
        // nothing filterable for a reason that has nothing to do with the join key.
        val df = sql(
          """SELECT f.id, f.part FROM fact f JOIN dim d
            |ON f.id = d.dim_id WHERE d.dim_val = 7""".stripMargin)
        val optimized = df.queryExecution.optimizedPlan
        assert(collectDppFilters(optimized).isEmpty,
          "DPP should not fire on non-partition join keys, got plan:\n" +
            optimized.treeString)
        assert(df.collect().length === 1)
        // And the scan is one that could have been pruned, had the join key been its partition
        // column: a filterAttributes() reporting every read column would make DPP fire above, and
        // the fact scan would read fewer than all 100 rows.
        assert(fileScanOf(df).filterAttributes().flatMap(_.fieldNames).toSeq === Seq("part"))
        val numOutputRows = factScanOf(df).metrics("numOutputRows").value
        assert(numOutputRows === 100,
          s"expected the fact scan to read every row, got $numOutputRows")
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
        checkAnswer(df, (0 until 10).map(i => Row(i * 10 + 7)))
        val numOutputRows = factScanOf(df).metrics("numOutputRows").value
        assert(numOutputRows === 10,
          s"expected the fact scan to read only partition 7, got $numOutputRows rows")
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

  test("DPP that prunes every partition leaves the scan with no input partition") {
    // `fact5` has partitions 0..4 while the dim row selects 7, so the build side is not empty and
    // the surviving partition set is. That combination is what reaches the zero-partition
    // DataSourceRDD: BatchScanExec's EmptyRDDWithPartitions branch keys off SinglePartition, which
    // a file scan never reports.
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        val factPath = new File(dir, "fact5").getCanonicalPath
        spark.range(50).selectExpr("id", "id % 5 AS part")
          .write.format("parquet").partitionBy("part").save(factPath)
        spark.read.format("parquet").schema("id long, part int").load(factPath)
          .createOrReplaceTempView("fact5")
        val df = sql(
          """SELECT f.id FROM fact5 f JOIN dim d
            |ON f.part = d.dim_id WHERE d.dim_val = 7""".stripMargin)
        assert(df.collect().isEmpty)
        val factScan = factScanOf(df)
        assert(factScan.filteredPartitions.flatten.isEmpty,
          s"expected no input partition, got ${factScan.filteredPartitions.flatten.size}")
        // What the assertion above does not see: BatchScanExec turning an empty partition set into
        // an RDD with one empty partition still satisfies it.
        assert(factScan.inputRDD.partitions.isEmpty,
          s"expected an RDD with no partition, got ${factScan.inputRDD.partitions.length}")
        val numOutputRows = factScan.metrics("numOutputRows").value
        assert(numOutputRows == 0,
          s"expected the fact scan to read nothing, got $numOutputRows rows")
      }
    }
  }

  test("a DPP filter a union branch cannot apply is dropped at planning") {
    // PartitionPruning resolves a union key through the first branch only, and the filter it
    // inserts is then pushed positionally into every branch. So the unpartitioned branch's scan
    // is offered a filter over a column it never declared filterable. Dropping it costs nothing,
    // since that branch cannot prune on it and the join re-applies it; keeping it would plan its
    // subquery for nothing and leave a dead filter in EXPLAIN.
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        val otherPath = new File(dir, "fact_other").getCanonicalPath
        spark.range(100).selectExpr("id", "id % 10 AS other")
          .write.format("parquet").save(otherPath)
        spark.read.format("parquet").schema("id long, other long").load(otherPath)
          .createOrReplaceTempView("fact_other")
        val df = sql(
          """SELECT u.id FROM (
            |  SELECT id, part FROM fact UNION ALL SELECT id, other FROM fact_other
            |) u JOIN dim d ON u.part = d.dim_id WHERE d.dim_val = 7""".stripMargin)
        val expected = (0 until 10).map(i => Row(i * 10 + 7))
        checkAnswer(df, expected ++ expected)
        // The filter did reach the branch that cannot use it in the optimized plan, which is what
        // makes this a regression test rather than a query that happens to work.
        val optimizedBranchFilters = df.queryExecution.optimizedPlan.collect {
          case f: Filter if f.condition.exists(_.isInstanceOf[DynamicPruningSubquery]) &&
            f.child.output.exists(_.name == "other") => f
        }
        assert(optimizedBranchFilters.size == 1,
          "expected the DPP filter over the branch that cannot apply it, got plan:\n" +
            df.queryExecution.optimizedPlan.treeString)
        // The strategy dropped it at planning, so the scan of that branch has no runtime filter.
        val otherScans = collect(df.queryExecution.executedPlan) {
          case b: BatchScanExec if b.output.exists(_.name == "other") => b
        }
        assert(otherScans.size == 1, s"expected one scan of fact_other, got $otherScans")
        assert(otherScans.head.runtimeFilters.isEmpty,
          s"expected no runtime filter on fact_other, got ${otherScans.head.runtimeFilters}")
        // And the branch that can use it still pruned.
        val numOutputRows = factScanOf(df).metrics("numOutputRows").value
        assert(numOutputRows == 10,
          s"expected the partitioned branch to read 10 rows, got $numOutputRows")
      }
    }
  }

  test("a runtime filter keeps a partition directory it cannot evaluate") {
    // The join applies a DPP filter again, and the FilterExec above the scan a scalar subquery
    // filter, so the scan keeps a directory whose value fails the filter's cast under ANSI.
    withDppV2Conf {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        withTempDir { dir =>
          writeFactAndDim(dir)
          def readDirs(df: DataFrame): Seq[String] = factScanOf(df).filteredPartitions.flatten
            .flatMap(_.asInstanceOf[FilePartition].files.map(_.partitionValues.getString(0)))
            .distinct.sorted
          val strPath = new File(dir, "fact_str").getCanonicalPath
          spark.range(10)
            .selectExpr("id", "CASE WHEN id = 3 THEN 'abc' ELSE CAST(id AS STRING) END AS part")
            .write.format("parquet").partitionBy("part").save(strPath)
          spark.read.format("parquet").schema("id long, part string").load(strPath)
            .createOrReplaceTempView("fact_str")
          // `f.id <> 3` drops the row of directory 'abc' before the join casts its key.
          val dpp = sql(
            """SELECT f.id FROM fact_str f JOIN dim d
              |ON f.part = d.dim_id WHERE d.dim_val = 7 AND f.id <> 3""".stripMargin)
          val logAppender = new LogAppender("runtime filter evaluation failure")
          withLogAppender(logAppender, Seq(classOf[ParquetScan].getName), Some(Level.WARN)) {
            checkAnswer(dpp, Row(7))
          }
          assert(logAppender.loggingEvents.exists { e =>
            e.getLevel == Level.WARN && e.getMessage.getFormattedMessage.contains(
              "on which a runtime filter could not be evaluated: 1.")
          }, s"expected a warning, got ${logAppender.loggingEvents.map(_.getMessage)}")
          assert(factScanOf(dpp).runtimeFilters.exists(_.isInstanceOf[DynamicPruningExpression]),
            s"expected a DPP filter, got ${factScanOf(dpp).runtimeFilters}")
          assert(readDirs(dpp) === Seq("7", "abc"))
          // Directory 'abc' holds a file with no row, so only the scan evaluates the filter on it.
          val emptyPath = new File(dir, "fact_empty").getCanonicalPath
          spark.range(10).selectExpr("id", "CAST(id AS STRING) AS part")
            .write.format("parquet").partitionBy("part").save(emptyPath)
          spark.range(0).write.parquet(new File(emptyPath, "part=abc").getCanonicalPath)
          spark.read.format("parquet").schema("id long, part string").load(emptyPath)
            .createOrReplaceTempView("fact_empty")
          val scalar = sql(
            """SELECT f.id FROM fact_empty f
              |WHERE f.part = (SELECT max(dim_id) FROM dim WHERE dim_val = 7)""".stripMargin)
          checkAnswer(scalar, Row(7))
          val scalarFilters = factScanOf(scalar).runtimeFilters
          assert(scalarFilters.exists(_.exists(_.isInstanceOf[ScalarSubquery])),
            s"expected the scalar subquery filter on the scan, got $scalarFilters")
          assert(readDirs(scalar) === Seq("7", "abc"))
        }
      }
    }
  }

  test("several runtime filters on one file scan prune it together") {
    // A DPP filter on p2 and a scalar subquery filter on p3 reach the same scan, which keeps only
    // the directories that satisfy both.
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        val tPath = new File(dir, "t").getCanonicalPath
        spark.range(100).selectExpr("id", "id % 5 AS p2", "id % 3 AS p3")
          .write.format("parquet").partitionBy("p2", "p3").save(tPath)
        spark.read.format("parquet").schema("id long, p2 int, p3 int").load(tPath)
          .createOrReplaceTempView("t")
        val df = sql(
          """SELECT t.id FROM t JOIN dim d ON t.p2 = d.dim_id
            |WHERE d.dim_val = 2 AND t.p3 = (SELECT max(dim_id) FROM dim WHERE dim_val = 1)
            |""".stripMargin)
        checkAnswer(df, (0 until 100).filter(i => i % 5 == 2 && i % 3 == 1).map(i => Row(i)))
        val scan = factScanOf(df)
        assert(scan.runtimeFilters.exists(_.isInstanceOf[DynamicPruningExpression]) &&
          scan.runtimeFilters.exists(_.exists(_.isInstanceOf[ScalarSubquery])),
          s"expected a DPP and a scalar subquery filter, got ${scan.runtimeFilters}")
        val dirs = scan.filteredPartitions.flatten.flatMap(_.asInstanceOf[FilePartition].files
          .map(f => (f.partitionValues.getInt(0), f.partitionValues.getInt(1)))).distinct
        assert(dirs === Seq((2, 1)))
      }
      // A directory that one filter fails on and another filter prunes is dropped and not counted
      // as a failure.
      withTempDir { dir =>
        val strPath = new File(dir, "fact_str").getCanonicalPath
        spark.range(10).selectExpr("id",
          "CASE id WHEN 3 THEN 'abc' WHEN 4 THEN 'xyz' ELSE CAST(id AS STRING) END AS part")
          .write.format("parquet").partitionBy("part").save(strPath)
        spark.read.format("parquet").schema("id long, part string").load(strPath)
          .createOrReplaceTempView("fact_str")
        val df = sql("SELECT f.id, f.part FROM fact_str f")
        df.collect()
        val scan = fileScanOf(df)
        val part = AttributeReference("part", StringType)()
        val failsOnLetters = EqualTo(Cast(part, LongType, None, EvalMode.ANSI), Literal(7L))
        val prunesXyz = Not(EqualTo(part, Literal("xyz")))
        var planned = Array.empty[InputPartition]
        val logAppender = new LogAppender("runtime filter evaluation failure")
        withLogAppender(logAppender, Seq(classOf[ParquetScan].getName), Some(Level.WARN)) {
          // failsOnLetters goes first, so it fails on 'xyz' before prunesXyz prunes it; in the
          // other order 'xyz' would be pruned without a failure.
          planned = scan.planInputPartitionsWithRuntimeFilters(Array(failsOnLetters, prunesXyz))
        }
        assert(planned.toSeq.flatMap(_.asInstanceOf[FilePartition].files
          .map(_.partitionValues.getString(0))).distinct.sorted === Seq("7", "abc"))
        assert(logAppender.loggingEvents.exists(_.getMessage.getFormattedMessage.contains(
          "on which a runtime filter could not be evaluated: 1.")),
          s"expected a warning, got ${logAppender.loggingEvents.map(_.getMessage)}")
      }
    }
  }

  test("a runtime filter that is true for a null partition value keeps that directory") {
    // A directory's null partition value binds as null, not as its type's default.
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        val nullPath = new File(dir, "fact_null").getCanonicalPath
        spark.range(10)
          .selectExpr("id", "CASE WHEN id = 3 THEN NULL ELSE CAST(id AS INT) END AS part")
          .write.format("parquet").partitionBy("part").save(nullPath)
        spark.read.format("parquet").schema("id long, part int").load(nullPath)
          .createOrReplaceTempView("fact_null")
        // The subquery returns null, so only the null directory satisfies the filter.
        val df = sql(
          """SELECT f.id FROM fact_null f
            |WHERE f.part <=> (SELECT max(dim_id) FROM dim WHERE dim_val > 100)""".stripMargin)
        checkAnswer(df, Row(3))
        val scan = factScanOf(df)
        assert(scan.runtimeFilters.exists(_.exists(_.isInstanceOf[ScalarSubquery])),
          s"expected the scalar subquery filter on the scan, got ${scan.runtimeFilters}")
        val values = scan.filteredPartitions.flatten.flatMap(_.asInstanceOf[FilePartition].files
          .map(f => Option.when(!f.partitionValues.isNullAt(0))(f.partitionValues.getInt(0))))
        assert(values.distinct === Seq(None))
      }
    }
  }

  test("a runtime filter that prunes nothing returns the partitions planInputPartitions " +
    "planned") {
    // Spark passes an empty array when every runtime filter of a scan node is screened out, e.g. a
    // DPP filter degraded to true, and a filter can keep every directory. Each packing builds new
    // partitions, so getting the planned ones back means the files were not packed again.
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        val df = sql("SELECT f.id, f.part FROM fact f")
        df.collect()
        val scan = fileScanOf(df)
        val planned = scan.planInputPartitions()
        val keepAll = GreaterThanOrEqual(AttributeReference("part", IntegerType)(), Literal(0))
        Seq(Array.empty[Expression], Array[Expression](keepAll)).foreach { filters =>
          assert(planned.nonEmpty && scan.planInputPartitionsWithRuntimeFilters(filters).toSeq
            .corresponds(planned.toSeq)(_ eq _),
            s"expected the planned partitions for [${filters.mkString(", ")}]")
        }
      }
    }
  }

  test("the plain and the runtime-filtered read paths share one listing") {
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        val df = sql("SELECT f.id, f.part FROM fact f")
        df.collect()
        var listings = 0
        val scan = withListingHook(fileScanOf(df).asInstanceOf[ParquetScan]) { listings += 1 }
        def partValues(parts: Array[InputPartition]): Seq[Int] =
          parts.flatMap(_.asInstanceOf[FilePartition].files.map(_.partitionValues.getInt(0)))
            .distinct.sorted.toSeq
        val partAttr = AttributeReference("part", IntegerType)()
        assert(partValues(scan.planInputPartitions()) === (0 until 10))
        assert(partValues(scan.planInputPartitionsWithRuntimeFilters(
          Array(EqualTo(partAttr, Literal(3))))) === Seq(3))
        assert(partValues(scan.planInputPartitions()) === (0 until 10))
        assert(listings === 1, s"expected one listing, got $listings")
      }
    }
  }

  test("only a scan with runtime-filterable columns keeps its file listing") {
    // Nothing reads the listing of another scan again, so planning packs it without keeping it.
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        val df = sql("SELECT f.id FROM fact f")
        df.collect()
        val base = collectFirst(df.queryExecution.executedPlan) {
          case b: BatchScanExec => b.scan.asInstanceOf[ParquetScan]
        }.getOrElse(fail(s"no scan in:\n${df.queryExecution.executedPlan}"))
        assert(base.filterAttributes().isEmpty)
        var listings = 0
        val scan = withListingHook(base) { listings += 1 }
        val planned = scan.planInputPartitions()
        scan.planInputPartitionsWithRuntimeFilters(
          Array(EqualTo(AttributeReference("part", IntegerType)(), Literal(3))))
        assert(listings === 2, s"expected the runtime-filtered call to list again, got $listings")
        // A filter that prunes nothing gets the packing of the listing it filtered, not of the one
        // planning packed.
        val keepAll = scan.planInputPartitionsWithRuntimeFilters(
          Array(GreaterThanOrEqual(AttributeReference("part", IntegerType)(), Literal(0))))
        assert(keepAll.nonEmpty && !keepAll.toSeq.corresponds(planned.toSeq)(_ eq _),
          "expected a packing of the runtime-filtered listing")
      }
    }
  }

  test("another thread can read the scan while it lists its files") {
    // The thread forces a lazy val of the scan through `equals`. Listing under the scan's
    // monitor would block it.
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        val df = sql("SELECT f.id, f.part FROM fact f")
        df.collect()
        val scan = fileScanOf(df).asInstanceOf[ParquetScan]
        val partAttr = AttributeReference("part", IntegerType)()
        Seq[ParquetScan => Array[InputPartition]](
          _.planInputPartitions(),
          _.planInputPartitionsWithRuntimeFilters(Array(EqualTo(partAttr, Literal(3))))
        ).foreach { firstCall =>
          var probe: ParquetScan = null
          probe = withListingHook(scan) {
            val reader = new Thread(() => probe.equals(probe))
            reader.start()
            reader.join(30000)
            assert(!reader.isAlive, "the other thread is still blocked on the scan")
          }
          firstCall(probe)
        }
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
    // With a GROUP BY, aggregate pushdown only happens when it groups by exactly the partition
    // columns, and those columns go into the pushed-down schema, so they stay in the scan relation
    // output and stay runtime-filterable. This is the other side of the intersection from the
    // test above.
    withDppV2Conf {
      withSQLConf(SQLConf.PARQUET_AGGREGATE_PUSHDOWN_ENABLED.key -> "true") {
        withTempDir { dir =>
          writeFactAndDim(dir)
          val df = sql("SELECT part, count(*) FROM fact GROUP BY part")
          checkAnswer(df, (0 until 10).map(p => Row(p, 10)))
          val scan = fileScanOf(df)
          // Without the pushdown the assertion below says nothing: `part` is in the read partition
          // schema either way. Pin that the aggregate really went into the scan.
          assert(scan.asInstanceOf[ParquetScan].pushedAggregate.isDefined,
            s"expected the aggregate to be pushed into the scan, got ${scan.description()}")
          val declared = scan.filterAttributes().flatMap(_.fieldNames).toSeq
          assert(declared == Seq("part"),
            s"expected the group-by partition column to stay declared, got $declared")
        }
      }
    }
  }

  test("a scalar subquery filter on a partition column is evaluated again after the scan") {
    // The scan does not declare its partition columns fully pushed, so a plugin node that reads
    // the scan's planned partitions instead of pruning them still returns the right rows, and one
    // that goes through PushDownUtils.pushRuntimeFilters gets false back rather than an error.
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        val df = sql(
          """SELECT f.id FROM fact f
            |WHERE f.part = (SELECT max(dim_id) FROM dim WHERE dim_val = 7)""".stripMargin)
        checkAnswer(df, (0 until 10).map(i => Row(i * 10 + 7)))
        assert(fileScanOf(df).fullyPushedFilterAttributes().isEmpty)
        val executedPlan = df.queryExecution.executedPlan
        assert(collect(executedPlan) {
          case f: FilterExec if f.condition.exists(_.isInstanceOf[ScalarSubquery]) => f
        }.nonEmpty, s"expected a FilterExec applying the scalar subquery, got:\n" +
          executedPlan.treeString)
        val factScan = factScanOf(df)
        assert(factScan.runtimeFilters.exists(_.exists(_.isInstanceOf[ScalarSubquery])),
          s"expected the scalar subquery filter on the scan, got ${factScan.runtimeFilters}")
        assert(!PushDownUtils.pushRuntimeFilters(
          factScan.scan, factScan.runtimeFilters, factScan.table, factScan.output))
      }
    }
  }

  test("a runtime filter is ANDed with the compile-time partition filters, not substituted") {
    // The compile-time filter goes through `fileIndex.listFiles`, and the runtime filter prunes
    // that listing, so the scan reads only what both keep.
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

  test("a runtime filter the scan cannot apply is rejected rather than silently dropped") {
    // Unreachable through SQL: PushDownUtils screens a filter against the attributes the scan
    // declared before handing it over, so the scan is called directly. A mixed-case reference is
    // matched the way the scan matches partition column names, so it binds.
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        val df = sql("SELECT f.id, f.part FROM fact f")
        df.collect()
        val scan = fileScanOf(df)
        val e = intercept[SparkException] {
          scan.planInputPartitionsWithRuntimeFilters(
            Array(EqualTo(AttributeReference("id", LongType)(), Literal(1L))))
        }
        assert(e.getCondition === "INTERNAL_ERROR")
        assert(e.getMessage.contains("can only apply a runtime filter over its partition"),
          s"unexpected message: ${e.getMessage}")

        val mixedCase = scan.planInputPartitionsWithRuntimeFilters(
          Array(EqualTo(AttributeReference("PART", IntegerType)(), Literal(7))))
        assert(mixedCase.flatMap(
          _.asInstanceOf[FilePartition].files.map(_.partitionValues.getInt(0))).distinct.toSeq
          === Seq(7))
      }
    }
  }

  test("runtime pruning splits the surviving files for their own size") {
    // As in V1, the split size follows the files that survive, so a runtime-pruned scan is split
    // like the same partitions selected at compile time.
    withDppV2Conf {
      withSQLConf(
        SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1g",
        SQLConf.FILES_MIN_PARTITION_NUM.key -> "20",
        SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "0") {
        withTempDir { dir =>
          writeFactAndDim(dir)
          def layout(df: DataFrame): Seq[Seq[(String, Long, Long)]] =
            layoutOf(factScanOf(df).filteredPartitions.flatten.toArray)
          val expected = (0 until 10).flatMap(i => Seq(Row(i * 10 + 8, 8), Row(i * 10 + 9, 9)))
          val compileTime = sql("SELECT f.id, f.part FROM fact f WHERE f.part >= 8")
          checkAnswer(compileTime, expected)
          val unpruned = sql("SELECT f.id, f.part FROM fact f")
          unpruned.collect()
          // Sized for the whole table, the files of partitions 8 and 9 are cut into fewer splits.
          val unprunedSplits = factScanOf(unpruned).filteredPartitions.flatten
            .flatMap(_.asInstanceOf[FilePartition].files)
            .count(_.partitionValues.getInt(0) >= 8)
          assert(layout(compileTime).flatten.size > unprunedSplits,
            s"expected more than $unprunedSplits splits at compile time, got " +
              s"${layout(compileTime).flatten.size}")
          Seq(
            // min(dim_id) over dim_val > 7 is 8.
            """SELECT f.id, f.part FROM fact f
              |WHERE f.part >= (SELECT min(dim_id) FROM dim WHERE dim_val > 7)""".stripMargin,
            """SELECT f.id, f.part FROM fact f JOIN dim d
              |ON f.part = d.dim_id WHERE d.dim_val > 7""".stripMargin).foreach { query =>
            val runtime = sql(query)
            checkAnswer(runtime, expected)
            val sameLayout = layout(runtime) == layout(compileTime)
            assert(sameLayout, s"expected the ${layout(compileTime).flatten.size} splits of " +
              s"the compile-time scan, got ${layout(runtime).flatten.size}")
          }
        }
      }
    }
  }

  test("a DPP filter degraded to true leaves the v2 file scan unpruned") {
    // With reuse-only DPP and no broadcast to reuse, the subquery is replaced by true before
    // execution.
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

  test("a runtime filter binds to the read partition columns when only some of them are read") {
    // The filter is evaluated on directory values, which follow the table's partition schema
    // (p1, p2, p3), while the scan reads only p2 and p3.
    withDppV2Conf {
      withTempDir { dir =>
        val tPath = new File(dir, "t").getCanonicalPath
        spark.range(100).selectExpr("id", "id % 2 AS p1", "id % 5 AS p2", "id % 3 AS p3")
          .write.format("parquet").partitionBy("p1", "p2", "p3").save(tPath)
        spark.read.format("parquet").schema("id long, p1 int, p2 int, p3 int").load(tPath)
          .createOrReplaceTempView("t")
        writeFactAndDim(dir)
        val expected = (0 until 100).filter(_ % 5 == 2).map(i => Row(i.toLong, i % 3))
        Seq(
          """SELECT id, p3 FROM t
            |WHERE p2 = (SELECT max(dim_id) FROM dim WHERE dim_val = 2)""".stripMargin,
          """SELECT t.id, t.p3 FROM t JOIN dim d
            |ON t.p2 = d.dim_id WHERE d.dim_val = 2""".stripMargin).foreach { query =>
          val df = sql(query)
          checkAnswer(df, expected)
          val scan = factScanOf(df)
          assert(scan.scan.asInstanceOf[FileScan].readPartitionSchema.fieldNames.toSeq ===
            Seq("p2", "p3"), "the scan must read a strict subset of the partition columns")
          val numOutputRows = scan.metrics("numOutputRows").value
          assert(numOutputRows == expected.size,
            s"expected the scan to read ${expected.size} rows, got $numOutputRows")
        }
      }
    }
  }

  test("no DPP over a partition column a static equality already fixes, as in V1") {
    // `FileScanBuilder` pushes the static partition filter fully into the scan, so the cleanup
    // reads it from the scan relation's pushed filters, as it reads it from the `Filter` over a V1
    // relation. V1 is the reference.
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        // An int join key, so the pruning key is `part` itself rather than a cast of it.
        val dimIntPath = new File(dir, "dim_int").getCanonicalPath
        spark.range(10).selectExpr("CAST(id AS INT) AS dim_id", "CAST(id AS INT) AS dim_val")
          .write.format("parquet").save(dimIntPath)
        // (condition, the rows it keeps by (id, part), whether DPP stays, whether V1 is checked).
        // The first six are the SPARK-38148 cases. The rest do not fix the key, so V2 keeps the
        // DPP; they are checked under V2 only, because V1 reads an equality anywhere in its filter
        // and drops the DPP. The last two reference a data column, so they stay in the `Filter`
        // above the scan rather than in its pushed filters.
        Seq[(String, (Int, Int) => Boolean, Boolean, Boolean)](
          ("f.part = 3", (_, p) => p == 3, false, true),
          ("3 = f.part", (_, p) => p == 3, false, true),
          ("f.part <=> 3", (_, p) => p == 3, false, true),
          ("3 <=> f.part", (_, p) => p == 3, false, true),
          ("f.part > 3", (_, p) => p > 3, true, true),
          ("5 > f.part", (_, p) => p < 5, true, true),
          ("f.part <> 3", (_, p) => p != 3, true, false),
          ("(f.part = 3 OR f.part = 4)", (_, p) => p == 3 || p == 4, true, false),
          ("(f.part = 3 OR f.id > 50)", (i, p) => p == 3 || i > 50, true, false),
          ("(f.part <> 3 OR f.id > 50)", (i, p) => p != 3 || i > 50, true, false)
        ).foreach { case (condition, keeps, hasDpp, checkV1) =>
          val query =
            s"""SELECT /*+ broadcast(d) */ f.id FROM fact f JOIN dim_int d
               |ON f.part = d.dim_id WHERE $condition AND d.dim_val > 2""".stripMargin
          val expected = (0 until 100).filter { i =>
            val part = i % 10
            part > 2 && keeps(i, part)
          }.map(i => Row(i.toLong))
          (if (checkV1) Seq("parquet", "") else Seq("")).foreach { v1Sources =>
            withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> v1Sources) {
              // A temp view keeps the relation it was created with, so create both under this conf.
              spark.read.format("parquet").schema("id long, part int")
                .load(new File(dir, "fact").getCanonicalPath).createOrReplaceTempView("fact")
              spark.read.format("parquet").load(dimIntPath).createOrReplaceTempView("dim_int")
              val df = sql(query)
              val optimized = df.queryExecution.optimizedPlan
              assert(optimized.exists(_.isInstanceOf[DataSourceV2ScanRelation]) ===
                v1Sources.isEmpty, s"wrong source version for '$v1Sources':\n$optimized")
              assert(collectDppFilters(optimized).nonEmpty === hasDpp,
                s"`$condition` with v1 sources '$v1Sources': expected DPP $hasDpp, got\n$optimized")
              checkAnswer(df, expected)
            }
          }
        }
      }
    }
  }

  test("DPP for a dimension filtered only on its partition column, as in V1") {
    // `FileScanBuilder` takes the dimension's partition filter in without leaving a `Filter` above
    // the scan, so PartitionPruning finds the selective predicate in the scan relation's pushed
    // filters. V1 keeps the `Filter`. V1 is the reference.
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        val dimPartPath = new File(dir, "dim_part").getCanonicalPath
        spark.range(10).selectExpr("id AS k", "CAST(id % 2 AS INT) AS dp")
          .write.format("parquet").partitionBy("dp").save(dimPartPath)
        Seq("parquet", "").foreach { v1Sources =>
          withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> v1Sources) {
            // A temp view keeps the relation it was created with, so create both under this conf.
            spark.read.format("parquet").schema("id long, part int")
              .load(new File(dir, "fact").getCanonicalPath).createOrReplaceTempView("fact")
            spark.read.format("parquet").schema("k long, dp int").load(dimPartPath)
              .createOrReplaceTempView("dim_part")
            val df = sql(
              """SELECT f.id FROM fact f JOIN dim_part d
                |ON f.part = d.k WHERE d.dp = 1""".stripMargin)
            val optimized = df.queryExecution.optimizedPlan
            assert(optimized.exists(_.isInstanceOf[DataSourceV2ScanRelation]) ===
              v1Sources.isEmpty, s"wrong source version for '$v1Sources':\n$optimized")
            assert(collectDppFilters(optimized).nonEmpty,
              s"expected DPP with v1 sources '$v1Sources', got\n$optimized")
            checkAnswer(df, (0 until 100).filter(_ % 2 == 1).map(i => Row(i.toLong)))
            if (v1Sources.isEmpty) {
              val factScan = collectFirst(df.queryExecution.executedPlan) {
                case b: BatchScanExec if b.output.exists(_.name == "id") => b
              }.getOrElse(fail(s"no fact scan in:\n${df.queryExecution.executedPlan}"))
              val numOutputRows = factScan.metrics("numOutputRows").value
              assert(numOutputRows == 50,
                s"expected the fact scan to read the 5 odd partitions, got $numOutputRows rows")
            }
          }
        }
      }
    }
  }

  test("DPP for a dimension whose filter a connector scan took in fully") {
    // The in-memory scan takes `dp IN (1, 3)` on its single identity partition column in fully,
    // so no `Filter` above it keeps a selective predicate.
    // The catalog manager keeps `dpp_mem` loaded after the conf is restored.
    try {
      withDppV2Conf {
        withSQLConf("spark.sql.catalog.dpp_mem" -> classOf[InMemoryTableCatalog].getName) {
          withTempDir { dir =>
            writeFactAndDim(dir)
            withTable("dpp_mem.dim_mem") {
              sql("CREATE TABLE dpp_mem.dim_mem (k BIGINT, dp INT) PARTITIONED BY (dp)")
              sql("INSERT INTO dpp_mem.dim_mem VALUES " +
                (0 until 10).map(k => s"($k, ${k % 4})").mkString(", "))
              val query =
                """SELECT f.id FROM fact f JOIN dpp_mem.dim_mem d
                  |ON f.part = d.k WHERE d.dp IN (1, 3)""".stripMargin
              val df = sql(query)
              val optimized = df.queryExecution.optimizedPlan
              val dimRelation = optimized.collectFirst {
                case r: DataSourceV2ScanRelation if r.output.exists(_.name == "k") => r
              }.getOrElse(fail(s"no dimension scan in:\n$optimized"))
              assert(dimRelation.pushedFilters.exists(_.isInstanceOf[In]),
                s"expected the pushed IN filter:\n$optimized")
              assert(collectDppFilters(optimized).nonEmpty, s"expected DPP, got\n$optimized")
              checkAnswer(df, (0 until 100).filter(_ % 2 == 1).map(i => Row(i.toLong)))
              val numOutputRows = factScanOf(df).metrics("numOutputRows").value
              assert(numOutputRows == 50,
                s"expected the fact scan to read the 5 odd partitions, got $numOutputRows rows")
              // A dimension scan with a row-selecting pushdown, such as a limit, may return other
              // rows when a DPP subquery reads it again, so its pushed filters do not count.
              val withoutDpp =
                withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false") {
                  sql(query).queryExecution.optimizedPlan
                }
              Seq(true, false).foreach { rowWise =>
                val plan = PartitionPruning(withoutDpp.transform {
                  case r: DataSourceV2ScanRelation if r.output.exists(_.name == "k") =>
                    r.copy(onlyRowWisePushdowns = rowWise)
                })
                assert(collectDppFilters(plan).nonEmpty === rowWise,
                  s"onlyRowWisePushdowns = $rowWise: expected DPP $rowWise, got\n$plan")
              }
            }
          }
        }
      }
    } finally {
      spark.sessionState.catalogManager.reset()
    }
  }

  test("DPP and a scalar subquery prune input partitions at runtime for v2 text") {
    // The text writer takes a single string column, so `fact_text` has its own fixture. Its files
    // are smaller than `dim`'s, so DPP has no estimated benefit and has to reuse the broadcast.
    withDppV2Conf {
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
        withTempDir { dir =>
          writeFactAndDim(dir)
          val textPath = new File(dir, "fact_text").getCanonicalPath
          spark.range(100).selectExpr("CAST(id AS STRING) AS value", "id % 10 AS part")
            .write.format("text").partitionBy("part").save(textPath)
          spark.read.format("text").schema("value string, part int").load(textPath)
            .createOrReplaceTempView("fact_text")
          Seq(
            """SELECT /*+ broadcast(d) */ f.value FROM fact_text f JOIN dim d
              |ON f.part = d.dim_id WHERE d.dim_val = 7""".stripMargin,
            """SELECT f.value FROM fact_text f
              |WHERE f.part = (SELECT max(dim_id) FROM dim WHERE dim_val = 7)""".stripMargin
          ).foreach { query =>
            val df = sql(query)
            checkAnswer(df, (0 until 10).map(i => Row((i * 10 + 7).toString)))
            val factScan = factScanOf(df)
            assert(factScan.scan.isInstanceOf[TextScan],
              s"expected a text scan, got ${factScan.scan}")
            val numOutputRows = factScan.metrics("numOutputRows").value
            assert(numOutputRows == 10, s"expected the scan to read partition 7 only, got " +
              s"$numOutputRows rows for:\n$query")
          }
        }
      }
    }
  }

  test("no group filter for a row-level operation over a built-in file scan") {
    // A built-in file scan does not record which groups its runtime-filtered read returned, which
    // a group-based write needs in order to replace only those groups. The group filter's IN is
    // never true on a directory with a null partition value, so the scan's pruning would drop it,
    // while a delta-based write needs the matching rows there.
    withDppV2Conf {
      withTempDir { dir =>
        writeFactAndDim(dir)
        val read = sql("SELECT * FROM fact").queryExecution.optimizedPlan.collectFirst {
          case r: DataSourceV2ScanRelation => r
        }.getOrElse(fail("expected a DataSourceV2ScanRelation"))
        val noOptimization = new Rule[LogicalPlan] {
          override def apply(plan: LogicalPlan): LogicalPlan = plan
        }
        Seq[RowLevelOperation](new DeleteOnlyOperation, new DeleteOnlyDeltaOperation).foreach {
          operation =>
            val rowLevelTable = RowLevelOperationTable(
              new RowLevelOperationOnlyTable(read.relation.table), operation)
            val rowLevelRead = read.copy(relation = read.relation.copy(table = rowLevelTable))
            assert(rowLevelRead.runtimeFilterAttrs.nonEmpty)
            val idAttr = rowLevelRead.output.find(_.name == "id").getOrElse(fail("expected id"))
            val cond = EqualTo(idAttr, Literal(5L))
            val query = Filter(Not(cond), rowLevelRead)
            val projection = ProjectingInternalRow(read.schema, read.output.indices)
            val write = operation match {
              case _: SupportsDelta =>
                WriteDelta(rowLevelRead.relation, cond, query, read.relation,
                  WriteDeltaProjections(None, projection, None), groupFilterCondition = Some(cond))
              case _ =>
                ReplaceData(rowLevelRead.relation, cond, query, read.relation,
                  ReplaceDataProjections(projection, None), groupFilterCondition = Some(cond))
            }
            // The rule's patterns match this plan, so only the guard for a built-in file scan
            // keeps it from injecting a group filter.
            assert(GroupBasedRowLevelOperation.unapply(write).exists(_._4 eq rowLevelRead) ||
              DeltaBasedRowLevelOperation.unapply(write).exists(_._4 eq rowLevelRead))
            val filtered = new RowLevelOperationRuntimeGroupFiltering(noOptimization)(write)
            assert(filtered === write, s"expected no group filter, got:\n$filtered")
        }
      }
    }
  }

  Seq("parquet", "orc", "json", "csv").foreach { format =>
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
            s"expected the fact scan to read 10 rows after DPP, got $numOutputRows. plan:\n" +
              df.queryExecution.executedPlan.treeString)
        }
      }
    }
  }
}

// Wraps a built-in file table as the `SupportsRowLevelOperations` table that
// `RowLevelOperationTable` requires; nothing reads or writes through it.
private class RowLevelOperationOnlyTable(table: Table)
  extends Table with SupportsRowLevelOperations {
  override def name(): String = table.name()
  override def columns(): Array[Column] = table.columns()
  override def capabilities(): java.util.Set[TableCapability] = table.capabilities()
  override def newRowLevelOperationBuilder(
      info: RowLevelOperationInfo): RowLevelOperationBuilder =
    throw new UnsupportedOperationException("not built in this test")
}

private class DeleteOnlyOperation extends RowLevelOperation {
  override def command(): RowLevelOperation.Command = RowLevelOperation.Command.DELETE
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    throw new UnsupportedOperationException("not read in this test")
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    throw new UnsupportedOperationException("not written in this test")
}

private class DeleteOnlyDeltaOperation extends SupportsDelta {
  override def command(): RowLevelOperation.Command = RowLevelOperation.Command.DELETE
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    throw new UnsupportedOperationException("not read in this test")
  override def newWriteBuilder(info: LogicalWriteInfo): DeltaWriteBuilder =
    throw new UnsupportedOperationException("not written in this test")
  override def rowId(): Array[NamedReference] = Array(FieldReference.column("id"))
}

@DataSourceV2Test
class DataSourceV2FileSourceDPPSuiteAEOff extends DataSourceV2FileSourceDPPSuiteBase
  with DisableAdaptiveExecutionSuite

@DataSourceV2Test
class DataSourceV2FileSourceDPPSuiteAEOn extends DataSourceV2FileSourceDPPSuiteBase
  with EnableAdaptiveExecutionSuite
