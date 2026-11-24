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

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{DataFrame, DynamicPartitionPruningV2CatalystFilterSuiteAEOff, DynamicPartitionPruningV2CatalystFilterSuiteAEOn, DynamicPartitionPruningV2FilterSuiteAEOff, DynamicPartitionPruningV2FilterSuiteAEOn, DynamicPartitionPruningV2SuiteAEOff, DynamicPartitionPruningV2SuiteAEOn, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.physical.{KeyedPartitioning, PartitioningCollection}
import org.apache.spark.sql.connector.catalog.Column
import org.apache.spark.sql.connector.expressions.Expressions.identity
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.HasPartitionStatistics
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, PackScanPartitionsExec, PackV2ScanPartitions, V2ScanPartitionPacking}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, EnsureRequirements, ShuffleExchangeExec, ValidateRequirements}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.functions.{broadcast, max}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.tags.DataSourceV2Test

@DataSourceV2Test
class PackV2ScanPartitionsSuite extends KeyGroupedPartitioningSuiteBase {
  import testImplicits._
  import PackV2ScanPartitionsSuite.meetsRequirements

  private val enabled = SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED.key
  private val minPartitionNum = SQLConf.V2_SCAN_PARTITION_PACKING_MIN_PARTITION_NUM.key
  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, -1L)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_OPEN_COST_IN_BYTES, 0L)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_BYTES, 2400L)

  after { catalog.clearTables() }

  // In-memory splits report 100 bytes each.
  private def createSplits(keys: Int = 2, splits: Int = 24, keyed: Boolean = true): Unit = {
    createTable(items, itemsColumns, if (keyed) Array(identity("id")) else Array.empty)
    sql(s"INSERT INTO testcat.ns.$items VALUES " + (1 to keys).flatMap { id =>
      (1 to splits).map(i => s"($id, 'split$i', 1.0, NULL)")
    }.mkString(", "))
  }

  private def packNodes(plan: SparkPlan): Seq[PackScanPartitionsExec] =
    collect(plan) { case p: PackScanPartitionsExec => p }

  // The tables whose scans a packing node packs: those below it but not below a broadcast.
  private def packedTables(plan: SparkPlan): Seq[String] = {
    def scans(p: SparkPlan): Seq[String] = p match {
      case _: BroadcastExchangeLike => Nil
      case s: BatchScanExec => Seq(s.table.name().split('.').last)
      case other => other.children.flatMap(scans)
    }
    packNodes(plan).flatMap(node => scans(node.child))
  }

  test("SPARK-60265: scan packing is disabled by default") {
    createSplits(splits = 3)
    assert(!spark.sessionState.conf.getConf(SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED))
    val df = sql(s"SELECT name, count(*) FROM testcat.ns.$items GROUP BY name")
    checkAnswer(df, (1 to 3).map(i => Row(s"split$i", 2L)))
    assert(packNodes(df.queryExecution.executedPlan).isEmpty)
  }

  test("SPARK-60265: sized inputs pack as spark.sql.files.* packs files of the same sizes") {
    case class Sized(bytes: Long) extends HasPartitionStatistics {
      override def sizeInBytes(): OptionalLong = OptionalLong.of(bytes)
      override def numRows(): OptionalLong = OptionalLong.empty()
      override def filesCount(): OptionalLong = OptionalLong.empty()
    }
    val mb = 1024L * 1024
    // (sizes, maxPartitionBytes, openCostInBytes, minPartitionNum, maxPartitionNum)
    val fixed = Seq(
      (Seq.fill(40)(62 * mb), 128 * mb, 4 * mb, 1, None),
      (Seq(50 * mb, 44 * mb), 100 * mb, 4 * mb, 1, None),
      (Seq.fill(1000)(0L), 128 * mb, 4 * mb, 1, None),
      // The minimum-count target rounds down, as maxSplitBytes does: 39 / 5 = 7 keeps each apart.
      (Seq(12L, 0L, 4L, 7L), 128L, 4L, 5, None),
      // Above the cap, whose target rounds up: 11 / 2 = 6.
      (Seq(1L, 3L, 1L), 5L, 2L, 1, Some(2)),
      // Above the cap, and still 3 groups after packing again, as for files.
      (Seq(120 * mb, 10 * mb, 124 * mb), 128 * mb, 4 * mb, 1, Some(2)),
      (Seq.fill(8)(100L), 100L, 0L, 1, Some(2)))
    val random = new Random(60265)
    val generated = Seq.fill(300) {
      val sizes = Seq.fill(1 + random.nextInt(40))(random.nextLong(200 * mb))
      (sizes, Seq(64 * mb, 128 * mb)(random.nextInt(2)), Seq(0L, 4 * mb)(random.nextInt(2)),
        1 + random.nextInt(8), Option.when(random.nextBoolean())(1 + random.nextInt(sizes.size)))
    }
    (fixed ++ generated).foreach { case (sizes, maxBytes, openCost, minNum, maxNum) =>
      val conf = Seq(SQLConf.FILES_MAX_PARTITION_BYTES.key -> maxBytes.toString,
        SQLConf.FILES_OPEN_COST_IN_BYTES.key -> openCost.toString,
        SQLConf.FILES_MIN_PARTITION_NUM.key -> minNum.toString) ++
        maxNum.map(SQLConf.FILES_MAX_PARTITION_NUM.key -> _.toString)
      withSQLConf(conf: _*) {
        // Each file records its position as its start offset, which packing does not read.
        val files = sizes.zipWithIndex.map { case (size, index) =>
          PartitionedFile(InternalRow.empty, SparkPath.fromPathString(s"/f$index"), index, size)
        }
        val splitBytes = FilePartition.maxSplitBytes(spark, sizes.map(_ + openCost).sum)
        val expected = FilePartition.getFilePartitions(spark, files, splitBytes)
          .map(_.files.map(_.start.toInt).toSeq)
        val packed = V2ScanPartitionPacking(maxBytes, openCost, Some(minNum), maxNum)
          .pack(sizes.map(Sized), 1).getOrElse(sizes.indices.map(Seq(_)))
        assert(packed === expected, s"sizes=$sizes maxBytes=$maxBytes openCost=$openCost " +
          s"minPartitionNum=$minNum maxPartitionNum=$maxNum")
      }
    }
  }

  test("SPARK-60265: a shuffle reads keyed and unkeyed scans packed directly above the scan") {
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
          val exchange = collect(plan) { case s: ShuffleExchangeExec => s }.head
          assert(packNodes(plan).size === (if (packing) 1 else 0))
          // The shuffle's map stage runs one task per packed group.
          assert(exchange.child.execute().getNumPartitions === (if (packing) 2 else 48))
          // The scan runs as without the node, which sits directly above it. The partial
          // aggregate above the node emits its rows once per packed group.
          val scan = collectScans(plan).head
          assert(scan.inputRDD.getNumPartitions === 48)
          assert(scan.outputPartitioning.isInstanceOf[KeyedPartitioning] === (keyed && bucketing))
          assert(packNodes(plan).forall(_.child eq scan))
          val aggregate = collect(exchange.child) { case a: BaseAggregateExec => a }.head
          assert(aggregate.execute().count() === (if (packing) 2L else 48L))
          assert(meetsRequirements(plan))
        }
      }
      catalog.clearTables()
    }
  }

  test("SPARK-60265: the node goes directly above a scan that a shuffle reads through the " +
      "listed operators") {
    createSplits(splits = 3)
    createTable(purchases, purchasesColumns, Array(identity("item_id")))
    sql(s"INSERT INTO testcat.ns.$purchases VALUES (1, 42.0, NULL), (2, 10.0, NULL)")
    for (adaptive <- Seq(false, true)) {
      withSQLConf(enabled -> "true", minPartitionNum -> "1",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.V2_BUCKETING_ENABLED.key -> "true") {
        val itemsDf = spark.table(s"testcat.ns.$items")
        val purchasesDf = spark.table(s"testcat.ns.$purchases")
        def check(df: DataFrame, expected: Seq[String]): Unit = {
          df.collect()
          val plan = df.queryExecution.executedPlan
          withClue(s"adaptive=$adaptive\n${plan.treeString}") {
            assert(packedTables(plan).sorted === expected.sorted)
            // Nothing on these paths relies on the scan's ordering.
            assert(packNodes(plan).forall(_.child.isInstanceOf[BatchScanExec]))
            assert(meetsRequirements(plan))
          }
        }
        val itemsName = items
        val purchasesName = purchases
        // Projects, filters and partial aggregates below a shuffle.
        check(itemsDf.where("price > 0").groupBy("name").count(), Seq(itemsName))
        // A sort that a partial sort aggregate needs, since max of a string has no hash aggregate.
        // The sort goes above the node.
        val sorted = itemsDf.groupBy("price").agg(max("name"))
        check(sorted, Seq(itemsName))
        val sortedExchange = collect(sorted.queryExecution.executedPlan) {
          case s: ShuffleExchangeExec => s
        }.head
        assert(collect(sortedExchange.child) { case s: SortExec => s }
          .exists(_.exists(_.isInstanceOf[PackScanPartitionsExec])))
        // A broadcast hash join's streamed side, but not its build side.
        check(itemsDf.join(broadcast(purchasesDf), $"id" === $"item_id").groupBy("name").count(),
          Seq(itemsName))
        // Both sides of a sort-merge join that shuffles them.
        check(itemsDf.join(purchasesDf, itemsDf("price") === purchasesDf("price")),
          Seq(itemsName, purchasesName))
        // A global sort's range shuffle.
        check(itemsDf.orderBy("name"), Seq(itemsName))
        // An explicit repartition keeps its output count.
        val repartitioned = itemsDf.repartition(5)
        check(repartitioned, Seq(itemsName))
        assert(repartitioned.rdd.getNumPartitions === 5)
        // No shuffle reads the scans: at the root, under a storage-partitioned join, and under an
        // aggregate on the key.
        check(itemsDf, Nil)
        check(itemsDf.join(purchasesDf, $"id" === $"item_id"), Nil)
        check(itemsDf.groupBy("id").count(), Nil)
        // A union or a coalesce sits between the shuffle and the scans.
        check(itemsDf.union(itemsDf).groupBy("name").count(), Nil)
        check(itemsDf.coalesce(2).groupBy("name").count(), Nil)
      }
    }
  }

  test("SPARK-60265: a local sort that a shuffle reads directly is still removed with packing") {
    createSplits(splits = 3)
    for (adaptive <- Seq(false, true); packing <- Seq(false, true)) {
      withSQLConf(enabled -> packing.toString,
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = spark.table(s"testcat.ns.$items").sortWithinPartitions("name")
          .repartition(3, $"price")
        checkAnswer(df, spark.table(s"testcat.ns.$items"))
        val plan = df.queryExecution.executedPlan
        assert(collect(plan) { case s: SortExec => s }.isEmpty, plan.treeString)
        assert(packNodes(plan).size === (if (packing) 1 else 0))
      }
    }
  }

  test("SPARK-60265: planning again does not pack again") {
    createSplits(splits = 3)
    withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = sql(s"SELECT name, count(*) FROM testcat.ns.$items GROUP BY name")
      val once = PackV2ScanPartitions(EnsureRequirements()(df.queryExecution.sparkPlan))
      assert(packNodes(once).size === 1)
      assert(PackV2ScanPartitions(EnsureRequirements()(once)) === once)
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
        // 4800 bytes: one task holds at most 2400 of them, or 1200 to make four tasks.
        assert(packNodes(df.queryExecution.executedPlan).head.execute().getNumPartitions ===
          (if (parallelism == 1) 2 else 4))
      }
    }
  }

  test("SPARK-60265: the opening cost is charged for each file an input reports") {
    // In-memory splits report 100 bytes in 100 files each. A cost of 1 per file gives 12 splits of
    // 200 bytes per task, where a cost of 1 per split would give 23 splits of 101 bytes.
    createSplits()
    withSQLConf(enabled -> "true", minPartitionNum -> "1",
        SQLConf.V2_SCAN_PARTITION_PACKING_OPEN_COST_IN_BYTES.key -> "1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = sql(s"SELECT name, count(*) FROM testcat.ns.$items GROUP BY name")
      checkAnswer(df, (1 to 24).map(i => Row(s"split$i", 2L)))
      assert(packNodes(df.queryExecution.executedPlan).head.execute().getNumPartitions === 4)
    }
  }

  test("SPARK-60265: plans packed at different default parallelism stay equal") {
    createSplits()
    withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val sparkPlan = EnsureRequirements()(
        sql(s"SELECT name, count(*) FROM testcat.ns.$items GROUP BY name").queryExecution.sparkPlan)
      val packed = Seq(2, 4).map { parallelism =>
        withSQLConf(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> parallelism.toString) {
          PackV2ScanPartitions(sparkPlan)
        }
      }
      assert(packed.forall(packNodes(_).size == 1))
      // Stage and broadcast reuse compare plans, so equality must not depend on the cluster.
      assert(packed.head.sameResult(packed.last))
    }
  }

  test("SPARK-60265: the node leaves an RDD whose partitions are not a scan's as it is") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val child = spark.range(0, 6, 1, 3).queryExecution.executedPlan
      val node = PackScanPartitionsExec(child, V2ScanPartitionPacking(300L, 0L, Some(1), Some(1)))
      val rdd = node.execute()
      assert(rdd.getNumPartitions === 3)
      assert(rdd.map(_.getLong(0)).collect().toSeq === (0L until 6L))
      assert(node.metrics("numPartitions").value === 3)
    }
  }

  test("SPARK-60265: a broadcast hash join's streamed side is packed on either build side") {
    createSplits()
    createTable(purchases, purchasesColumns, Array(identity("item_id")))
    sql(s"INSERT INTO testcat.ns.$purchases VALUES (1, 42.0, NULL), (2, 10.0, NULL)")
    for (adaptive <- Seq(false, true); buildLeft <- Seq(false, true)) {
      withSQLConf(enabled -> "true", minPartitionNum -> "1",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val itemsDf = spark.table(s"testcat.ns.$items")
        val purchasesDf = spark.table(s"testcat.ns.$purchases")
        val joined = if (buildLeft) {
          broadcast(purchasesDf).join(itemsDf, $"item_id" === $"id")
        } else {
          itemsDf.join(broadcast(purchasesDf), $"id" === $"item_id")
        }
        val df = joined.groupBy("name").count()
        checkAnswer(df, (1 to 24).map(i => Row(s"split$i", 2L)))
        val plan = df.queryExecution.executedPlan
        withClue(s"adaptive=$adaptive buildLeft=$buildLeft\n${plan.treeString}") {
          val joins = collect(plan) { case j: BroadcastHashJoinExec => j }
          assert(joins.map(_.buildSide) === Seq(if (buildLeft) BuildLeft else BuildRight))
          assert(packedTables(plan) === Seq(items))
          assert(packNodes(plan).forall(_.child.isInstanceOf[BatchScanExec]))
          // The 48 splits of 100 bytes run in two map tasks of 2,400 bytes.
          assert(packNodes(plan).map(_.execute().getNumPartitions) === Seq(2))
        }
      }
    }
  }

  test("SPARK-60265: AQE reads the shuffle of a packed keyed scan in full where a join needs it") {
    // The query of "SPARK-59671: an AQE local read over a split scan does not lose matches",
    // which the packing re-run of KeyGroupedPartitioningSuite excludes: its setup expects the
    // scan's keyed layout to reach a local read, and over a packed shuffle it does not. The
    // matches across the splits of key 1 must still all be found.
    val idCols = Array(Column.create("id", IntegerType), Column.create("data", StringType))
    createTable("lrt", idCols, Array(identity("id")))
    createTable("lrs1", idCols, Array.empty[Transform])
    createTable("lrs2", idCols, Array.empty[Transform])
    val pad = "x" * 4096
    sql(s"INSERT INTO testcat.ns.lrt VALUES (1, 't1a$pad')")
    sql(s"INSERT INTO testcat.ns.lrt VALUES (1, 't1b$pad')")
    sql("INSERT INTO testcat.ns.lrt VALUES (2, 't2a')")
    sql("INSERT INTO testcat.ns.lrs1 VALUES (1, 's1a'), (2, 's1b')")
    sql("INSERT INTO testcat.ns.lrs2 VALUES (1, 's2a'), (2, 's2b')")
    withSQLConf(enabled -> "true", minPartitionNum -> "1",
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "1k",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "4") {
      val df = sql(
        """
          |SELECT substring(a.data, 1, 3), substring(b.data, 1, 3)
          |FROM (SELECT t1.id AS id, t1.data AS data FROM testcat.ns.lrt t1
          |      JOIN testcat.ns.lrs1 s1 ON t1.id = s1.id) a
          |JOIN (SELECT t2.id AS id, t2.data AS data FROM testcat.ns.lrt t2
          |      JOIN testcat.ns.lrs2 s2 ON t2.id = s2.id) b
          |ON a.id = b.id
          |""".stripMargin)
      checkAnswer(df, Seq(Row("t1a", "t1a"), Row("t1a", "t1b"), Row("t1b", "t1a"),
        Row("t1b", "t1b"), Row("t2a", "t2a")))
      val plan = df.queryExecution.executedPlan
      withClue(plan.treeString) {
        assert(packedTables(plan).contains("lrt"))
        assert(collect(plan) { case j: BroadcastHashJoinExec => j }.size === 2)
        // Both narrow sides broadcast at runtime, and their shuffles are read locally to build
        // the broadcasts. A local read over the wide side's packed shuffle would report no
        // partitioning, while the sort merge join above needs it hash partitioned, so AQE reads
        // that shuffle in full: no read sits over it. The local reads, over the unkeyed narrow
        // sides, report no keyed layout either.
        assert(collect(plan) { case j: SortMergeJoinExec => j }.size === 1)
        val reads = collect(plan) { case r: AQEShuffleReadExec => r }
        assert(reads.forall(_.isLocalRead))
        assert(reads.flatMap(collectScans).map(_.table.name().split('.').last).sorted ===
          Seq("lrs1", "lrs2"))
        assert(reads.forall { r =>
          PartitioningCollection.flatten(r.outputPartitioning)
            .forall(!_.isInstanceOf[KeyedPartitioning])
        })
      }
    }
  }

  test("SPARK-60265: an empty scan stays as it is") {
    createTable(items, itemsColumns, Array(identity("id")))
    withSQLConf(enabled -> "true", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = sql(s"SELECT name, count(*) FROM testcat.ns.$items GROUP BY name")
      checkAnswer(df, Seq.empty)
      assert(packNodes(df.queryExecution.executedPlan).head.execute().getNumPartitions === 0)
      val global = sql(s"SELECT count(*) FROM testcat.ns.$items")
      checkAnswer(global, Seq(Row(0L)))
    }
  }
}

object PackV2ScanPartitionsSuite extends AdaptiveSparkPlanHelper {
  /**
   * Whether `plan` meets every operator's requirements, also inside the query stages of an AQE
   * plan, which `stripAQEPlan` leaves as leaves.
   */
  def meetsRequirements(plan: SparkPlan): Boolean =
    ValidateRequirements.validate(stripAQEPlan(plan)) &&
      collect(plan) { case s: QueryStageExec => s.plan }.forall(ValidateRequirements.validate)

  /**
   * Scan packing for the re-run suites below. In-memory splits report 100 bytes each, so a packed
   * shuffle input reads three of them per task.
   */
  def withPacking(conf: SparkConf): SparkConf = conf
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_ENABLED, true)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_BYTES, 300L)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_OPEN_COST_IN_BYTES, 0L)
    .set(SQLConf.V2_SCAN_PARTITION_PACKING_MIN_PARTITION_NUM, 1)
}

/** Re-run existing SPJ, ordering and reuse regressions with scan packing enabled. */
@DataSourceV2Test
class KeyGroupedPartitioningPackScanPartitionsSuite extends KeyGroupedPartitioningSuite {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)

  // With packing, the node sits directly above a scan that a shuffle reads, and the node reports
  // no keyed layout. So the split keyed layout that the first test sets up never reaches a local
  // read. The second test fails only its setup check where a side is shuffled: the project it
  // inspects for the collapsed layout reads the node. What it checks there, no grouping and the
  // shuffle counts, is decided before packing, and its plans without a shuffle are not packed.
  override protected def excluded: Seq[String] = super.excluded ++ Seq(
    "SPARK-59671: an AQE local read over a split scan does not lose matches",
    "SPARK-58974: the collapse skew guard applies regardless of requireAllClusterKeys")
}

@DataSourceV2Test
class KeyGroupedPartitioningPackRuntimeFilterSuite
  extends KeyGroupedPartitioningRuntimeFilterSuite with PackV2ScanPartitionsRuntimeFilterTests {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

@DataSourceV2Test
class KeyGroupedPartitioningPackCatalystRuntimeFilterSuite
  extends KeyGroupedPartitioningCatalystRuntimeFilterSuite
  with PackV2ScanPartitionsRuntimeFilterTests {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

/** Re-run row-level operations, whose shuffled target scans can be packed. */
@DataSourceV2Test
class GroupBasedDeleteFromTablePackScanPartitionsSuite extends GroupBasedDeleteFromTableSuite {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

@DataSourceV2Test
class GroupBasedNoMetadataDeleteFromTablePackScanPartitionsSuite
  extends GroupBasedNoMetadataDeleteFromTableSuite {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

@DataSourceV2Test
class DeltaBasedMergeIntoTablePackScanPartitionsSuite extends DeltaBasedMergeIntoTableSuite {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

/** Re-run DSv2 dynamic partition pruning, whose pruned scans can feed packed shuffles. */
@DataSourceV2Test
class DynamicPartitionPruningV2PackScanPartitionsSuiteAEOff
  extends DynamicPartitionPruningV2SuiteAEOff {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

@DataSourceV2Test
class DynamicPartitionPruningV2PackScanPartitionsSuiteAEOn
  extends DynamicPartitionPruningV2SuiteAEOn {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

@DataSourceV2Test
class DynamicPartitionPruningV2FilterPackScanPartitionsSuiteAEOff
  extends DynamicPartitionPruningV2FilterSuiteAEOff {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

@DataSourceV2Test
class DynamicPartitionPruningV2FilterPackScanPartitionsSuiteAEOn
  extends DynamicPartitionPruningV2FilterSuiteAEOn {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

@DataSourceV2Test
class DynamicPartitionPruningV2CatalystFilterPackScanPartitionsSuiteAEOff
  extends DynamicPartitionPruningV2CatalystFilterSuiteAEOff {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

@DataSourceV2Test
class DynamicPartitionPruningV2CatalystFilterPackScanPartitionsSuiteAEOn
  extends DynamicPartitionPruningV2CatalystFilterSuiteAEOn {
  override def sparkConf: SparkConf = PackV2ScanPartitionsSuite.withPacking(super.sparkConf)
}

trait PackV2ScanPartitionsRuntimeFilterTests extends KeyGroupedPartitioningSuiteBase {
  import PackV2ScanPartitionsSuite.meetsRequirements

  test("SPARK-60265: inputs emptied by runtime filtering stay in the packed groups") {
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
        val node = collect(plan) { case p: PackScanPartitionsExec => p }
          .find(p => collect(p) { case s: BatchScanExec => s }.contains(scan)).get
        // Every slot is computed once, in one task here: the kept inputs fit in one, and the
        // empty slots join it.
        val rowsPerTask = node.execute().mapPartitions(rows => Iterator.single(rows.size))
          .collect()
        assert(rowsPerTask.toSeq === Seq(expected.size))
        assert(node.metrics("numInputPartitions").value === 48)
        assert(node.metrics("numPartitions").value === 1)
        // A global aggregate over the same scan counts its rows, however many slots were emptied.
        checkAnswer(sql(s"SELECT count(*) FROM testcat.ns.$items i " +
          s"JOIN testcat.ns.$purchases p ON i.id = p.item_id WHERE p.price $predicate"),
          Seq(Row(expected.size.toLong)))
        assert(meetsRequirements(plan))
      }
    }
  }
}
