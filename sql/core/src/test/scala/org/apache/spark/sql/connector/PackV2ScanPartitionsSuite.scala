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

import java.util.OptionalLong

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.Expressions.identity
import org.apache.spark.sql.connector.read.{HasPartitionStatistics, InputPartition}
import org.apache.spark.sql.execution.{CoalesceExec, ProjectExec}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.datasources.v2.V2ScanPartitionPacking
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, PackV2ScanPartitions, ShuffleExchangeExec, ValidateRequirements}
import org.apache.spark.sql.internal.SQLConf

class V2ScanPartitionPackingSuite extends org.apache.spark.SparkFunSuite {
  private case class Sized(bytes: Option[Long]) extends HasPartitionStatistics {
    override def sizeInBytes(): OptionalLong = bytes.map(OptionalLong.of)
      .getOrElse(OptionalLong.empty())
    override def numRows(): OptionalLong = OptionalLong.empty()
    override def filesCount(): OptionalLong = OptionalLong.empty()
  }
  private def inputs(sizes: Long*): Seq[Option[InputPartition]] =
    sizes.map(size => Some(Sized(Some(size))))

  test("pack actual input sizes, keeping oversized splits separate and covering each input once") {
    val policy = V2ScanPartitionPacking(100, 0, 1, Some(1))
    val parts = inputs(30, 40, 200, 20, 30, 90)
    val groups = policy.pack(parts)
    assert(groups === Seq(Seq(0, 1), Seq(2), Seq(3, 4), Seq(5)))
    assert(groups.flatten === parts.indices)
    assert(policy.pack(Seq.empty).isEmpty)
    assert(policy.pack(inputs(0, 0, 0)) === Seq(Seq(0, 1, 2)))
  }

  test("opening cost and suggested parallelism constrain small inputs") {
    assert(V2ScanPartitionPacking(100, 10, 1, None).pack(inputs(40, 40, 0)) ===
      Seq(Seq(0, 1), Seq(2)))
    assert(V2ScanPartitionPacking(1000, 0, 3, None).pack(inputs(100, 100, 100, 100)) ===
      Seq(Seq(0), Seq(1), Seq(2), Seq(3)))
    assert(V2ScanPartitionPacking(5, 10, 1, None).pack(inputs(0, 0)) ===
      Seq(Seq(0), Seq(1)))
  }

  test("missing, mixed and negative sizes require an explicit count fallback") {
    val unknown = Some(new InputPartition {})
    Seq(unknown, Some(Sized(None)), Some(Sized(Some(-1)))).foreach { p =>
      val parts = inputs(30, 40) :+ p
      assert(V2ScanPartitionPacking(100, 0, 1, None).pack(parts).size === 3)
      assert(V2ScanPartitionPacking(100, 0, 1, Some(2)).pack(parts) ===
        Seq(Seq(0), Seq(1, 2)))
      assert(V2ScanPartitionPacking(100, 0, 1, Some(10)).pack(parts).size === 3)
    }
  }

  test("overflow, exact limits and runtime-pruned empty slots") {
    val policy = V2ScanPartitionPacking(Long.MaxValue, 10, 1, None)
    assert(policy.pack(inputs(Long.MaxValue, 1)) === Seq(Seq(0), Seq(1)))
    assert(V2ScanPartitionPacking(100, 0, 1, None).pack(inputs(100, 1)) ===
      Seq(Seq(0), Seq(1)))
    assert(V2ScanPartitionPacking(100, 0, 1, None).pack(
      Seq(None, Some(Sized(Some(30))), None, Some(Sized(Some(40))))) ===
      Seq(Seq(0, 1, 2, 3)))
  }
}

class PackV2ScanPartitionsSuite extends KeyGroupedPartitioningSuiteBase {
  private val enabled = SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED.key
  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, -1L)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_OPEN_COST_IN_BYTES, 0L)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_BYTES, 2400L)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_MIN_PARTITION_NUM, 1)

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
        withSQLConf(enabled -> packing.toString,
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
          // Once per packed task, rather than once per split inside a CoalesceExec.
          val partialRows = aggregate.execute().count()
          assert(partialRows === (if (packing) 2L else 48L))
          assert(collect(plan) { case c: CoalesceExec => c }.isEmpty)
          assert(ValidateRequirements.validate(stripAQEPlan(plan)))
        }
      }
      catalog.dropTable(Identifier.of(Array("ns"), items))
    }
  }

  test("SPARK-60265: packing depends on bytes rather than key count") {
    createSplits(keys = 1)
    withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_BYTES.key -> "600") {
      val df = sql(s"SELECT name, count(*) FROM testcat.ns.$items GROUP BY name")
      checkAnswer(df, (1 to 24).map(i => Row(s"split$i", 1L)))
      val scan = collectScans(df.queryExecution.executedPlan).head
      assert(scan.inputRDD.getNumPartitions === 4)
      assert(scan.outputPartitioning === UnknownPartitioning(0))
      assert(scan.outputOrdering.isEmpty)
    }
  }

  test("SPARK-60265: row conversion survives direct collection, shuffle, filtering and codegen") {
    createSplits(splits = 3)
    for (adaptive <- Seq(false, true); codegen <- Seq(false, true); filter <- Seq(false, true)) {
      withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString) {
        val where = if (filter) " WHERE id > 0" else ""
        val direct = sql(s"SELECT id, name FROM testcat.ns.$items$where")
        checkAnswer(direct, for (id <- 1 to 2; i <- 1 to 3) yield Row(id, s"split$i"))
        val plan = stripAQEPlan(direct.queryExecution.executedPlan)
        assert(collectScans(plan).head.partitionPacking.isDefined)
        assert(collect(plan) { case p: ProjectExec => p }.nonEmpty || codegen)
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
      val once = EnsureRequirements()(df.queryExecution.sparkPlan)
      val twice = EnsureRequirements()(once)
      assert(PackV2ScanPartitions(twice) === twice)
      assert(collectScans(twice).head.partitionPacking.isDefined)
      assert(ValidateRequirements.validate(twice))
      checkAnswer(df, (1 to 3).map(i => Row(s"split$i", 2L)))
    }
  }

  test("SPARK-60265: empty scans and explicit coalesces remain unchanged") {
    createTable(items, itemsColumns, Array(identity("id")))
    withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val empty = spark.table(s"testcat.ns.$items")
      checkAnswer(empty, Seq.empty)
      assert(collectScans(empty.queryExecution.executedPlan).head.partitionPacking.isEmpty)
      sql(s"INSERT INTO testcat.ns.$items VALUES (1, 'a', 1.0, NULL), (2, 'b', 1.0, NULL)")
      val input = spark.table(s"testcat.ns.$items")
      val df = input.coalesce(1).groupBy("name").count()
      checkAnswer(df, Seq(Row("a", 1L), Row("b", 1L)))
      assert(collectScans(df.queryExecution.executedPlan).head.partitionPacking.isEmpty)
      assert(collect(df.queryExecution.executedPlan) { case c: CoalesceExec => c.numPartitions } ===
        Seq(1))
    }
  }
}

/** Re-run existing SPJ, ordering and reuse regressions with scan packing enabled. */
class KeyGroupedPartitioningPackScanPartitionsSuite extends KeyGroupedPartitioningSuite {
  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED, true)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_MIN_PARTITION_NUM, 1)
}

class KeyGroupedPartitioningPackRuntimeFilterSuite
  extends KeyGroupedPartitioningRuntimeFilterSuite with PackV2ScanPartitionsRuntimeFilterTests {
  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED, true)
}

class KeyGroupedPartitioningPackCatalystRuntimeFilterSuite
  extends KeyGroupedPartitioningCatalystRuntimeFilterSuite
  with PackV2ScanPartitionsRuntimeFilterTests {
  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED, true)
}
