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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DynamicPartitionPruningV2CatalystFilterSuiteAEOff, DynamicPartitionPruningV2CatalystFilterSuiteAEOn, DynamicPartitionPruningV2FilterSuiteAEOff, DynamicPartitionPruningV2FilterSuiteAEOn, DynamicPartitionPruningV2SuiteAEOff, DynamicPartitionPruningV2SuiteAEOn, Row}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.plans.physical.{KeyedPartitioning, UnknownPartitioning}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.Expressions.identity
import org.apache.spark.sql.execution.{CoalesceExec, ProjectExec}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.datasources.v2.PackV2ScanPartitions
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ShuffleExchangeExec, ValidateRequirements}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.ExtendedSQLTest

class PackV2ScanPartitionsSuite extends KeyGroupedPartitioningSuiteBase {
  private val enabled = SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED.key
  private val minPartitionNum = SQLConf.V2_SCAN_PARTITION_PACKING_MIN_PARTITION_NUM.key
  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, -1L)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_OPEN_COST_IN_BYTES, 0L)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_BYTES, 2400L)

  after { catalog.clearTables() }

  private def createSplits(keys: Int = 2, splits: Int = 24, keyed: Boolean = true): Unit = {
    createTable(items, itemsColumns, if (keyed) Array(identity("id")) else Array.empty)
    sql(s"INSERT INTO testcat.ns.$items VALUES " + (1 to keys).flatMap { id =>
      (1 to splits).map(i => s"($id, 'split$i', 1.0, NULL)")
    }.mkString(", "))
  }

  test("SPARK-60265: scan packing is disabled by default") {
    createSplits(splits = 3)
    assert(!spark.sessionState.conf.getConf(SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED))
    val df = sql(s"SELECT name, count(*) FROM testcat.ns.$items GROUP BY name")
    checkAnswer(df, (1 to 3).map(i => Row(s"split$i", 2L)))
    assert(collectScans(df.queryExecution.executedPlan).forall(_.partitionPacking.isEmpty))
  }

  test("SPARK-60265: size-pack keyed and unkeyed scans before partial aggregation") {
    for (keyed <- Seq(false, true)) {
      createSplits(keyed = keyed)
      for (adaptive <- Seq(false, true); packing <- Seq(false, true);
          bucketing <- Seq(false, true)) {
        withSQLConf(enabled -> packing.toString, minPartitionNum -> "1",
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
            SQLConf.V2_BUCKETING_ENABLED.key -> bucketing.toString) {
          val df = sql(s"SELECT price, count(*), max(id) FROM testcat.ns.$items GROUP BY price")
          checkAnswer(df, Seq(Row(1.0, 48L, 2L)))
          val plan = df.queryExecution.executedPlan
          val scan = collectScans(plan).head
          assert(scan.inputRDD.getNumPartitions === (if (packing) 2 else 48))
          assert(scan.partitionPacking.isDefined === packing)
          val exchange = collect(plan) { case s: ShuffleExchangeExec => s }.head
          val aggregate = collect(exchange.child) { case a: BaseAggregateExec => a }.head
          // Partial aggregation runs once per packed task, not once per input.
          val partialRows = aggregate.execute().count()
          assert(partialRows === (if (packing) 2L else 48L))
          assert(ValidateRequirements.validate(stripAQEPlan(plan)))
        }
      }
      catalog.dropTable(Identifier.of(Array("ns"), items))
    }
  }

  test("SPARK-60265: packing depends on bytes rather than key count") {
    createSplits(keys = 1)
    withSQLConf(enabled -> "true", minPartitionNum -> "1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_BYTES.key -> "600") {
      val df = sql(s"SELECT name, count(*) FROM testcat.ns.$items GROUP BY name")
      checkAnswer(df, (1 to 24).map(i => Row(s"split$i", 1L)))
      val scan = collectScans(df.queryExecution.executedPlan).head
      assert(scan.inputRDD.getNumPartitions === 4)
      // A packed scan that outputs its key claims no layout, while the same scan unpacked would.
      val keyed = collectScans(spark.table(s"testcat.ns.$items").queryExecution.executedPlan).head
      assert(keyed.partitionPacking.isDefined)
      assert(keyed.outputPartitioning === UnknownPartitioning(0))
      assert(keyed.outputOrdering.isEmpty)
      val unpacked = keyed.copy(partitionPacking = None)
      assert(unpacked.outputPartitioning.isInstanceOf[KeyedPartitioning])
      assert(unpacked.outputOrdering.nonEmpty)
    }
  }

  test("SPARK-60265: row conversion survives direct collection, shuffle, filtering and codegen") {
    createSplits(splits = 3)
    for (adaptive <- Seq(false, true); codegen <- Seq(false, true); filter <- Seq(false, true)) {
      withSQLConf(enabled -> "true", minPartitionNum -> "1",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString) {
        val where = if (filter) " WHERE id > 0" else ""
        val direct = sql(s"SELECT id, name FROM testcat.ns.$items$where")
        checkAnswer(direct, for (id <- 1 to 2; i <- 1 to 3) yield Row(id, s"split$i"))
        val plan = stripAQEPlan(direct.queryExecution.executedPlan)
        assert(collectScans(plan).head.inputRDD.getNumPartitions === 1)
        assert(collect(plan) { case p: ProjectExec => p }.nonEmpty)
        assert(plan.execute().map(_.isInstanceOf[UnsafeRow]).collect().forall(value => value))
        val shuffled = sql(s"SELECT name, count(*), max(id) FROM testcat.ns.$items$where " +
          "GROUP BY name")
        checkAnswer(shuffled, (1 to 3).map(i => Row(s"split$i", 2L, 2L)))
        assert(ValidateRequirements.validate(stripAQEPlan(shuffled.queryExecution.executedPlan)))
      }
    }
  }

  test("SPARK-60265: repeated requirement planning keeps packing stable") {
    createSplits(splits = 3)
    withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = sql(s"SELECT name, count(*) FROM testcat.ns.$items GROUP BY name")
      // Packing is its own rule after EnsureRequirements; re-running both must not repack.
      val once = PackV2ScanPartitions()(EnsureRequirements()(df.queryExecution.sparkPlan))
      val twice = PackV2ScanPartitions()(EnsureRequirements()(once))
      assert(PackV2ScanPartitions()(twice) === twice)
      assert(collectScans(twice).head.partitionPacking.isDefined)
      assert(ValidateRequirements.validate(twice))
      checkAnswer(df, (1 to 3).map(i => Row(s"split$i", 2L)))
    }
  }

  test("SPARK-60265: the minimum packed task count defaults to the leaf node parallelism") {
    createSplits()
    for (parallelism <- Seq(1, 4)) {
      withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> parallelism.toString) {
        val df = sql(s"SELECT name, count(*) FROM testcat.ns.$items GROUP BY name")
        checkAnswer(df, (1 to 24).map(i => Row(s"split$i", 2L)))
        // 4800 bytes: one task at most holds 2400 of them, or 1200 to make four tasks.
        assert(collectScans(df.queryExecution.executedPlan).head.inputRDD.getNumPartitions ===
          (if (parallelism == 1) 2 else 4))
      }
    }
  }

  test("SPARK-60265: equal scans packed at different default parallelism stay equal") {
    createSplits()
    withSQLConf(enabled -> "true") {
      val sparkPlan = sql(s"SELECT * FROM testcat.ns.$items").queryExecution.sparkPlan
      val packed = Seq(2, 4).map { parallelism =>
        withSQLConf(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> parallelism.toString) {
          collectScans(PackV2ScanPartitions()(sparkPlan)).head
        }
      }
      assert(packed.forall(_.partitionPacking.isDefined))
      // Stage and broadcast reuse compare scans, so equality must not depend on the cluster.
      assert(packed.head.sameResult(packed.last))
    }
  }

  test("SPARK-60265: an empty scan runs no task, and an explicit coalesce keeps its scan") {
    createTable(items, itemsColumns, Array(identity("id")))
    withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val empty = spark.table(s"testcat.ns.$items")
      checkAnswer(empty, Seq.empty)
      val emptyScan = collectScans(empty.queryExecution.executedPlan).head
      assert(emptyScan.partitionPacking.isDefined)
      assert(emptyScan.inputRDD.getNumPartitions === 0)
      sql(s"INSERT INTO testcat.ns.$items VALUES (1, 'a', 1.0, NULL), (2, 'b', 1.0, NULL)")
      val input = spark.table(s"testcat.ns.$items")
      // A shuffle above the coalesce, so nothing above it uses the coalesce's input layout.
      val df = input.coalesce(2).groupBy("name").count()
      checkAnswer(df, Seq(Row("a", 1L), Row("b", 1L)))
      val scan = collectScans(df.queryExecution.executedPlan).head
      assert(scan.partitionPacking.isEmpty)
      assert(scan.inputRDD.getNumPartitions === 2)
      assert(collect(df.queryExecution.executedPlan) { case c: CoalesceExec => c }.size === 1)
    }
  }
}

object PackV2ScanPartitionsSuite {
  /**
   * Scan packing for the re-run suites below. In-memory splits report 100 bytes each, so a packed
   * scan reads three of them per task: a scan packed where its layout is used gets several tasks
   * and loses rows, rather than one task that happens to hold every row.
   */
  def withPacking(conf: SparkConf): SparkConf = conf
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED, true)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_BYTES, 300L)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_OPEN_COST_IN_BYTES, 0L)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_MIN_PARTITION_NUM, 1)
}

/** Re-run existing SPJ, ordering and reuse regressions with scan packing enabled. */
@ExtendedSQLTest
class KeyGroupedPartitioningPackScanPartitionsSuite extends KeyGroupedPartitioningSuite {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

@ExtendedSQLTest
class KeyGroupedPartitioningPackRuntimeFilterSuite
  extends KeyGroupedPartitioningRuntimeFilterSuite with PackV2ScanPartitionsRuntimeFilterTests {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

@ExtendedSQLTest
class KeyGroupedPartitioningPackCatalystRuntimeFilterSuite
  extends KeyGroupedPartitioningCatalystRuntimeFilterSuite
  with PackV2ScanPartitionsRuntimeFilterTests {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

/** Re-run row-level operations, whose target scans can be packed, with scan packing enabled. */
class GroupBasedDeleteFromTablePackScanPartitionsSuite extends GroupBasedDeleteFromTableSuite {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

class GroupBasedNoMetadataDeleteFromTablePackScanPartitionsSuite
  extends GroupBasedNoMetadataDeleteFromTableSuite {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

class DeltaBasedMergeIntoTablePackScanPartitionsSuite extends DeltaBasedMergeIntoTableSuite {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

/** Re-run DSv2 dynamic partition pruning, whose pruned scans and broadcasts can be packed. */
@ExtendedSQLTest
class DynamicPartitionPruningV2PackScanPartitionsSuiteAEOff
  extends DynamicPartitionPruningV2SuiteAEOff {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

@ExtendedSQLTest
class DynamicPartitionPruningV2PackScanPartitionsSuiteAEOn
  extends DynamicPartitionPruningV2SuiteAEOn {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

@ExtendedSQLTest
class DynamicPartitionPruningV2FilterPackScanPartitionsSuiteAEOff
  extends DynamicPartitionPruningV2FilterSuiteAEOff {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

@ExtendedSQLTest
class DynamicPartitionPruningV2FilterPackScanPartitionsSuiteAEOn
  extends DynamicPartitionPruningV2FilterSuiteAEOn {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

@ExtendedSQLTest
class DynamicPartitionPruningV2CatalystFilterPackScanPartitionsSuiteAEOff
  extends DynamicPartitionPruningV2CatalystFilterSuiteAEOff {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

@ExtendedSQLTest
class DynamicPartitionPruningV2CatalystFilterPackScanPartitionsSuiteAEOn
  extends DynamicPartitionPruningV2CatalystFilterSuiteAEOn {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

trait PackV2ScanPartitionsRuntimeFilterTests extends KeyGroupedPartitioningSuiteBase {
  test("SPARK-60265: runtime filtering retains key metadata without executing empty slots") {
    createTable(items, itemsColumns, Array(identity("id")))
    createTable(purchases, purchasesColumns, Array.empty)
    val values = (1 to 2).flatMap { id =>
      (1 to 24).map(split => s"($id, 'split$split', 1.0, NULL)")
    }.mkString(", ")
    sql(s"INSERT INTO testcat.ns.$items VALUES $values")
    sql(s"INSERT INTO testcat.ns.$purchases VALUES (1, 42.0, NULL), (2, 10.0, NULL)")
    for (adaptive <- Seq(false, true); (predicate, keptKey) <-
        Seq("> 40" -> Some(1), "< 40" -> Some(2), "> 100" -> None)) {
      withSQLConf(SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED.key -> "true",
          SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_BYTES.key -> "2400",
          SQLConf.V2_SCAN_PARTITION_PACKING_OPEN_COST_IN_BYTES.key -> "0",
          SQLConf.V2_SCAN_PARTITION_PACKING_MIN_PARTITION_NUM.key -> "1",
          SQLConf.V2_BUCKETING_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
          SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
          SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "10",
          // Keep the fully pruned scan visible; adaptive runtime-filter planning stays enabled.
          SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key ->
            "org.apache.spark.sql.execution.adaptive.AQEPropagateEmptyRelation") {
        val df = sql(s"SELECT i.id, i.name, p.price FROM testcat.ns.$items i " +
          s"JOIN testcat.ns.$purchases p ON i.id = p.item_id WHERE p.price $predicate")
        val expected = keptKey.toSeq.flatMap { key =>
          (1 to 24).map(i => Row(key, s"split$i", if (key == 1) 42.0 else 10.0))
        }
        checkAnswer(df, expected)
        val plan = df.queryExecution.executedPlan
        assert(collectShuffles(plan).size === 2)
        val scan = collectScans(plan).find(_.table.name().endsWith(items)).get
        assert(scan.runtimeFilters.nonEmpty)
        assert(scan.filteredPartitions.size === 48)
        assert(scan.filteredPartitions.count(_.isEmpty) === 48 - expected.size)
        if (keptKey.contains(2)) assert(scan.filteredPartitions.take(24).forall(_.isEmpty))
        assert(scan.partitionPacking.isDefined)
        val rowsPerTask = scan.inputRDD.mapPartitions(rows => Iterator.single(rows.size)).collect()
        assert(rowsPerTask.length === (if (expected.isEmpty) 0 else 1))
        assert(scan.metrics("packingNumInputPartitions").value === expected.size)
        assert(rowsPerTask.sum === expected.size)
        if (expected.nonEmpty) {
          // The scan keeps padding for runtime-filter validation, but packed tasks must not
          // serialize or compute the empty slots, including a completely pruned first key.
          assert(scan.inputRDD.dependencies.head.rdd.getNumPartitions === expected.size)
        }
        assert(scan.outputOrdering.isEmpty)
        assert(ValidateRequirements.validate(stripAQEPlan(plan)))
      }
    }
  }
}
