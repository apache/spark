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

import java.sql.Timestamp
import java.util.Collections

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.rdd.SortedMergeCoalescedRDD
import org.apache.spark.sql.{DataFrame, ExplainSuiteHelper, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, ExprId, Literal, TransformExpression}
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.connector.catalog.{Column, Identifier, InMemoryCatalystRuntimeFilterCatalog, InMemoryTableCatalog}
import org.apache.spark.sql.connector.catalog.functions._
import org.apache.spark.sql.connector.distributions.Distributions
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.expressions.Expressions._
import org.apache.spark.sql.execution.{
  ExtendedMode,
  FormattedMode,
  LocalTableScanExec,
  ProjectExec,
  RDDScanExec,
  SimpleMode,
  SortExec,
  SparkPlan,
  UnionExec}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation, GroupPartitionsExec}
import org.apache.spark.sql.execution.exchange.{ShuffleExchangeExec, ShuffleExchangeLike, ValidateRequirements}
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.types._
import org.apache.spark.tags.ExtendedSQLTest

abstract class KeyGroupedPartitioningSuiteBase extends DistributionAndOrderingSuiteBase {

  protected val emptyProps: java.util.Map[String, String] = {
    Collections.emptyMap[String, String]
  }

  protected val items: String = "items"
  protected val itemsColumns: Array[Column] = Array(
    Column.create("id", LongType),
    Column.create("name", StringType),
    Column.create("price", FloatType),
    Column.create("arrive_time", TimestampType))

  protected val purchases: String = "purchases"
  protected val purchasesColumns: Array[Column] = Array(
    Column.create("item_id", LongType),
    Column.create("price", FloatType),
    Column.create("time", TimestampType))

  protected def createTable(
      table: String,
      columns: Array[Column],
      partitions: Array[Transform],
      ordering: Array[SortOrder] = Array.empty,
      catalog: InMemoryTableCatalog = catalog): Unit = {
    catalog.createTable(Identifier.of(Array("ns"), table),
      columns, partitions, emptyProps, Distributions.unspecified(), ordering, None, None,
      numRowsPerSplit = 1)
  }

  protected def collectShuffles(plan: SparkPlan): Seq[ShuffleExchangeLike] = {
    // here we skip collecting shuffle operators that are not associated with SMJ
    collect(plan) {
      case s: SortMergeJoinExec => s
    }.flatMap(smj =>
      collect(smj) {
        case s: ShuffleExchangeExec => s
      })
  }.toSet.toSeq

  protected def collectGroupPartitions(plan: SparkPlan): Seq[GroupPartitionsExec] = {
    // here we skip collecting group-partition operators that are not associated with SMJ
    collect(plan) {
      case s: SortMergeJoinExec => s
    }.flatMap(smj =>
      collect(smj) {
        case g: GroupPartitionsExec => g
      })
  }.toSet.toSeq

  protected def collectScans(plan: SparkPlan): Seq[BatchScanExec] = {
    collect(plan) { case s: BatchScanExec => s }
  }

}

/**
 * Tests for runtime filtering under a storage-partitioned join, whose outcome depends on how the
 * scan takes runtime filters.
 */
trait KeyGroupedPartitioningRuntimeFilterTests extends KeyGroupedPartitioningSuiteBase {

  /**
   * Helper method to verify that filteredPartitions contains the expected number of
   * Some and None values. This is used to verify that dynamic partition filtering
   * properly fills filtered-out partitions with None.
   */
  private def assertFilteredPartitions(
      scans: Seq[BatchScanExec],
      expectedTotalPartitions: Seq[Int],
      expectedFilteredOutPartitions: Seq[Int]): Unit = {
    assert(scans.size === expectedTotalPartitions.size,
      s"Expected ${expectedTotalPartitions.size} scans but got ${scans.size}")

    scans.zip(expectedTotalPartitions).zip(expectedFilteredOutPartitions).foreach {
      case ((scan, expectedTotal), expectedFiltered) =>
        val filtered = scan.filteredPartitions
        assert(filtered.size === expectedTotal,
          s"Expected $expectedTotal total partitions but got ${filtered.size}")

        val noneCount = filtered.count(_.isEmpty)
        assert(noneCount === expectedFiltered,
          s"Expected $expectedFiltered None values but got $noneCount")

        val someCount = filtered.count(_.isDefined)
        assert(someCount === (expectedTotal - expectedFiltered),
          s"Expected ${expectedTotal - expectedFiltered} Some values but got $someCount")
    }
  }

  test("data source partitioning + dynamic partition filtering") {
    withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "10") {
      val items_partitions = Array(identity("id"))
      createTable(items, itemsColumns, items_partitions)
      sql(s"INSERT INTO testcat.ns.$items VALUES " +
          s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
          s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
          s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
          s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp)), " +
          s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

      val purchases_partitions = Array(identity("item_id"))
      createTable(purchases, purchasesColumns, purchases_partitions)
      sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
          s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
          s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
          s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
          s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
          s"(3, 19.5, cast('2020-02-01' as timestamp))")

      Seq(true, false).foreach { pushDownValues =>
        withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString) {
          // number of unique partitions changed after dynamic filtering - the gap should be filled
          // with empty partitions and the job should still succeed
          var df = sql(s"SELECT sum(p.price) from testcat.ns.$items i, testcat.ns.$purchases p " +
              "WHERE i.id = p.item_id AND i.price > 40.0")

          var shuffles = collectShuffles(df.queryExecution.executedPlan)
          assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
          var scans = collectScans(df.queryExecution.executedPlan)
          assert(scans.forall(_.outputPartitioning.numPartitions === 5))
          var groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
          assert(groupPartitions.forall(_.outputPartitioning.numPartitions === 3))

          checkAnswer(df, Seq(Row(131)))

          // Verify that filteredPartitions contains None for filtered-out partitions.
          // After DPF with filter i.price > 40.0, only id=1 survives on items side.
          // The purchases side should be pruned to only item_id=1.
          // purchases: 5 total partitions (3 for id=1, 1 for id=2, 1 for id=3)
          // After DPF: 3 Some (id=1), 2 None (id=2, id=3)
          assertFilteredPartitions(scans, Seq(5, 5), Seq(0, 2))

          // dynamic filtering doesn't change partitioning so storage-partitioned join should kick
          // in
          df = sql(s"SELECT sum(p.price) from testcat.ns.$items i, testcat.ns.$purchases p " +
              "WHERE i.id = p.item_id AND i.price >= 10.0")

          shuffles = collectShuffles(df.queryExecution.executedPlan)
          assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
          scans = collectScans(df.queryExecution.executedPlan)
          assert(scans.forall(_.outputPartitioning.numPartitions === 5))
          groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
          assert(groupPartitions.forall(_.outputPartitioning.numPartitions === 3))

          checkAnswer(df, Seq(Row(303.5)))

          // With filter i.price >= 10.0, all ids (1, 2, 3) survive,
          // so no partitions should be filtered out
          assertFilteredPartitions(scans, Seq(5, 5), Seq(0, 0))
        }
      }
    }
  }

  test("SPARK-42038: partially clustered: with dynamic partition filtering") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp)), " +
        s"(4, 'dd', 18.0, cast('2023-01-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 50.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 55.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 60.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 65.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 19.5, cast('2020-02-01' as timestamp)), " +
        s"(5, 25.0, cast('2023-01-01' as timestamp)), " +
        s"(5, 26.0, cast('2023-01-01' as timestamp)), " +
        s"(5, 28.0, cast('2023-01-01' as timestamp)), " +
        s"(6, 50.0, cast('2023-02-01' as timestamp)), " +
        s"(6, 50.0, cast('2023-02-01' as timestamp))")

    Seq(true, false).foreach { pushDownValues =>
      Seq(("true", 15), ("false", 6)).foreach {
        case (enable, expected) =>
          withSQLConf(
              SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
              SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
              SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
              SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
              SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "10",
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable) {

            // When partition values are pushed down, storage-partitioned join fills the missing
            // partitions & splits after dynamic filtering with empty partitions & splits.
            val df = sql(s"SELECT sum(p.price) from " +
                s"testcat.ns.$purchases p, testcat.ns.$items i WHERE " +
                s"p.item_id = i.id AND p.price < 45.0")

            checkAnswer(df, Seq(Row(213.5)))
            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            val scans = collectScans(df.queryExecution.executedPlan)
            val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
            assert(scans.map(_.outputPartitioning.numPartitions) === Seq(14, 6))
            if (pushDownValues) {
              assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
              assert(groupPartitions.forall(_.outputPartitioning.numPartitions === expected))
            } else {
              assert(shuffles.nonEmpty,
                "should contain shuffle when not pushing down partition values")
              assert(groupPartitions.isEmpty)
            }

            // Verify filteredPartitions for DPF.
            // After filter p.price < 45.0, purchases has item_ids {1, 2, 3, 5}.
            // Items side should be pruned to these ids. Since items has {1, 2, 3, 4},
            // id=4 should be filtered out.
            // purchases: 14 total, all kept (0 None) - no DPF on probe side
            // items: 6 total, id=4 filtered (1 None)
            assertFilteredPartitions(scans, Seq(14, 6), Seq(0, 1))
          }
      }
    }
  }

  test("SPARK-45652: SPJ should handle empty partition after dynamic filtering") {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "10") {
      val items_partitions = Array(identity("id"))
      createTable(items, itemsColumns, items_partitions)
      sql(s"INSERT INTO testcat.ns.$items VALUES " +
          s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
          s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
          s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
          s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp)), " +
          s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

      val purchases_partitions = Array(identity("item_id"))
      createTable(purchases, purchasesColumns, purchases_partitions)
      sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
          s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
          s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
          s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
          s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
          s"(3, 19.5, cast('2020-02-01' as timestamp))")

      Seq(true, false).foreach { pushDownValues =>
        Seq(true, false).foreach { partiallyClustered => {
          withSQLConf(
            SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
                partiallyClustered.toString,
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString) {
            // The dynamic filtering effectively filtered out all the partitions
            val df = sql(s"SELECT p.price from testcat.ns.$items i, testcat.ns.$purchases p " +
                "WHERE i.id = p.item_id AND i.price > 50.0")
            checkAnswer(df, Seq.empty)
          }
        }
        }
      }
    }
  }
}

@ExtendedSQLTest
class KeyGroupedPartitioningSuite
  extends KeyGroupedPartitioningSuiteBase with ExplainSuiteHelper {
  private val functions = Seq(
    UnboundYearsFunction,
    UnboundDaysFunction,
    UnboundBucketFunction,
    UnboundTruncateFunction)

  override def sparkConf: SparkConf = super.sparkConf
    .set(V2_BUCKETING_ENABLED, true)
    .set(AUTO_BROADCASTJOIN_THRESHOLD, -1L)

  before {
    functions.foreach { f =>
      catalog.createFunction(Identifier.of(Array.empty, f.name()), f)
    }
  }

  after {
    catalog.clearTables()
    catalog.clearFunctions()
  }

  private val table: String = "tbl"

  private val columns: Array[Column] = Array(
    Column.create("id", IntegerType),
    Column.create("data", StringType),
    Column.create("ts", TimestampType))

  private val columns2: Array[Column] = Array(
      Column.create("store_id", IntegerType),
      Column.create("dept_id", IntegerType),
      Column.create("data", StringType))

  def withFunction[T](fns: UnboundFunction*)(f: => T): T = {
    val fnIds = catalog.listFunctions(Array.empty)
    val oldFns = fns.map { fn =>
      val id = Identifier.of(Array.empty, fn.name())
      val oldFn = Option.when(fnIds.contains(id)) {
        val fn = catalog.loadFunction(id)
        catalog.dropFunction(id)
        fn
      }
      catalog.createFunction(id, fn)
      (id, oldFn)
    }
    try f finally {
      oldFns.foreach { case (id, oldFn) =>
        catalog.dropFunction(id)
        oldFn.foreach(catalog.createFunction(id, _))
      }
    }
  }

  test("clustered distribution: output partitioning should be KeyedPartitioning") {
    val partitions: Array[Transform] = Array(Expressions.years("ts"))

    // create a table with 3 partitions, partitioned by `years` transform
    createTable(table, columns, partitions)
    sql(s"INSERT INTO testcat.ns.$table VALUES " +
        s"(0, 'aaa', CAST('2022-01-01' AS timestamp)), " +
        s"(1, 'bbb', CAST('2021-01-01' AS timestamp)), " +
        s"(2, 'ccc', CAST('2020-01-01' AS timestamp))")

    var df = sql(s"SELECT count(*) FROM testcat.ns.$table GROUP BY ts")
    val catalystDistribution = physical.ClusteredDistribution(
      Seq(TransformExpression(YearsFunction, Seq(attr("ts")))))
    val partitionKeys = Seq(50, 51, 52).map(v => InternalRow.fromSeq(Seq(v)))

    checkQueryPlan(df, catalystDistribution,
      physical.KeyedPartitioning(catalystDistribution.clustering, partitionKeys))

    // multiple group keys should work too as long as partition keys are subset of them
    df = sql(s"SELECT count(*) FROM testcat.ns.$table GROUP BY id, ts")
    checkQueryPlan(df, catalystDistribution,
      physical.KeyedPartitioning(catalystDistribution.clustering, partitionKeys))
  }

  test("non-clustered distribution: no partition") {
    val partitions: Array[Transform] = Array(bucket(32, "ts"))
    createTable(table, columns, partitions)

    val df = sql(s"SELECT * FROM testcat.ns.$table")
    val distribution = physical.ClusteredDistribution(
      Seq(TransformExpression(BucketFunction, Seq(attr("ts")), Some(32))))

    checkQueryPlan(df, distribution, physical.UnknownPartitioning(0))
  }

  test("non-clustered distribution: single partition") {
    val partitions: Array[Transform] = Array(bucket(32, "ts"))
    createTable(table, columns, partitions)
    sql(s"INSERT INTO testcat.ns.$table VALUES (0, 'aaa', CAST('2020-01-01' AS timestamp))")

    val df = sql(s"SELECT * FROM testcat.ns.$table")
    val distribution = physical.ClusteredDistribution(
      Seq(TransformExpression(BucketFunction, Seq(attr("ts")), Some(32))))

    // Has exactly one partition.
    val partitionKeys = Seq(0).map(v => InternalRow.fromSeq(Seq(v)))
    checkQueryPlan(df, distribution,
      physical.KeyedPartitioning(distribution.clustering, partitionKeys))
  }

  test("non-clustered distribution: no V2 catalog") {
    spark.conf.set("spark.sql.catalog.testcat2", classOf[InMemoryTableCatalog].getName)
    val nonFunctionCatalog = spark.sessionState.catalogManager.catalog("testcat2")
        .asInstanceOf[InMemoryTableCatalog]
    val partitions: Array[Transform] = Array(bucket(32, "ts"))
    createTable(table, columns, partitions, catalog = nonFunctionCatalog)
    sql(s"INSERT INTO testcat2.ns.$table VALUES " +
        s"(0, 'aaa', CAST('2022-01-01' AS timestamp)), " +
        s"(1, 'bbb', CAST('2021-01-01' AS timestamp)), " +
        s"(2, 'ccc', CAST('2020-01-01' AS timestamp))")

    val df = sql(s"SELECT * FROM testcat2.ns.$table")
    val distribution = physical.UnspecifiedDistribution

    try {
      checkQueryPlan(df, distribution, physical.UnknownPartitioning(0))
    } finally {
      spark.conf.unset("spark.sql.catalog.testcat2")
    }
  }

  test("non-clustered distribution: no V2 function provided") {
    catalog.clearFunctions()

    val partitions: Array[Transform] = Array(bucket(32, "ts"))
    createTable(table, columns, partitions)
    sql(s"INSERT INTO testcat.ns.$table VALUES " +
        s"(0, 'aaa', CAST('2022-01-01' AS timestamp)), " +
        s"(1, 'bbb', CAST('2021-01-01' AS timestamp)), " +
        s"(2, 'ccc', CAST('2020-01-01' AS timestamp))")

    val df = sql(s"SELECT * FROM testcat.ns.$table")
    val distribution = physical.UnspecifiedDistribution

    checkQueryPlan(df, distribution, physical.UnknownPartitioning(0))
  }

  test("non-clustered distribution: V2 bucketing disabled") {
    withSQLConf(SQLConf.V2_BUCKETING_ENABLED.key -> "false") {
      val partitions: Array[Transform] = Array(bucket(32, "ts"))
      createTable(table, columns, partitions)
      sql(s"INSERT INTO testcat.ns.$table VALUES " +
          s"(0, 'aaa', CAST('2022-01-01' AS timestamp)), " +
          s"(1, 'bbb', CAST('2021-01-01' AS timestamp)), " +
          s"(2, 'ccc', CAST('2020-01-01' AS timestamp))")

      val df = sql(s"SELECT * FROM testcat.ns.$table")
      val distribution = physical.ClusteredDistribution(
        Seq(TransformExpression(BucketFunction, Seq(attr("ts")), Some(32))))

      checkQueryPlan(df, distribution, physical.UnknownPartitioning(0))
    }
  }

  test("non-clustered distribution: V2 function with multiple args") {
    val partitions: Array[Transform] = Array(
      Expressions.apply("truncate", Expressions.column("data"), Expressions.literal(2))
    )

    // create a table with 3 partitions, partitioned by `truncate` transform
    createTable(table, columns, partitions)
    sql(s"INSERT INTO testcat.ns.$table VALUES " +
      s"(0, 'aaa', CAST('2022-01-01' AS timestamp)), " +
      s"(1, 'bbb', CAST('2021-01-01' AS timestamp)), " +
      s"(2, 'ccc', CAST('2020-01-01' AS timestamp))")

    val df = sql(s"SELECT * FROM testcat.ns.$table")
    val distribution = physical.ClusteredDistribution(
      Seq(TransformExpression(TruncateFunction, Seq(attr("data"), Literal(2)))))

    checkQueryPlan(df, distribution, physical.UnknownPartitioning(0))
  }

  /**
   * Check whether the query plan from `df` has the expected `distribution`, `ordering` and
   * `partitioning`.
   */
  private def checkQueryPlan(
      df: DataFrame,
      distribution: physical.Distribution,
      partitioning: physical.Partitioning): Unit = {
    // check distribution & ordering are correctly populated in logical plan
    val relation = df.queryExecution.optimizedPlan.collect {
      case r: DataSourceV2ScanRelation => r
    }.head

    resolveDistribution(distribution, relation) match {
      case physical.ClusteredDistribution(clustering, _, _, _) =>
        assert(relation.keyGroupedPartitioning.isDefined &&
          relation.keyGroupedPartitioning.get == clustering)
      case _ =>
        assert(relation.keyGroupedPartitioning.isEmpty)
    }

    // check distribution, ordering and output partitioning are correctly populated in physical plan
    val scan = collect(df.queryExecution.executedPlan) {
      case s: BatchScanExec => s
    }.head

    val expectedPartitioning = resolvePartitioning(partitioning, scan)
    assert(expectedPartitioning == scan.outputPartitioning)
  }

  private val customers: String = "customers"
  private val customersColumns: Array[Column] = Array(
    Column.create("customer_name", StringType),
    Column.create("customer_age", IntegerType),
    Column.create("customer_id", LongType))

  private val orders: String = "orders"
  private val ordersColumns: Array[Column] = Array(
    Column.create("order_amount", DoubleType),
    Column.create("customer_id", LongType))

  private def selectWithMergeJoinHint(t1: String, t2: String): String = {
    s"SELECT /*+ MERGE($t1, $t2) */ "
  }

  private def createJoinTestDF(
      keys: Seq[(String, String)],
      extraColumns: Seq[String] = Nil,
      joinType: String = ""): DataFrame = {
    val extraColList = if (extraColumns.isEmpty) "" else extraColumns.mkString(", ", ", ", "")
    sql(
      s"""
         |${selectWithMergeJoinHint("i", "p")}
         |id, name, i.price as purchase_price, p.price as sale_price $extraColList
         |FROM testcat.ns.$items i $joinType JOIN testcat.ns.$purchases p
         |ON ${keys.map(k => s"i.${k._1} = p.${k._2}").mkString(" AND ")}
         |ORDER BY id, purchase_price, sale_price $extraColList
         |""".stripMargin)
  }

  /**
   * Creates a table partitioned by `bucket(numBuckets, id)` and holding the ids 0 until `numIds`.
   * Joining two such tables reduces both sides onto the greatest common divisor of their bucket
   * counts, unless that divisor is a side's own bucket count, in which case only the other side
   * reduces. `numIds` has to exceed the smaller of the two bucket counts for a reduce to happen at
   * all. Below that both sides report one key per id, so they are co-partitioned as they stand
   * and the join has nothing to reduce.
   */
  private def createBucketedIdTable(name: String, numBuckets: Int, numIds: Int = 12): Unit = {
    val bucketedColumns = Array(
      Column.create("id", LongType),
      Column.create("data", StringType))
    createTable(name, bucketedColumns, Array(bucket(numBuckets, "id")))
    sql(s"INSERT INTO testcat.ns.$name VALUES " +
      (0 until numIds).map(i => s"($i, 'v$i')").mkString(", "))
  }

  /** Creates a `bucket<n>` table for each of the given bucket counts. */
  private def createBucketedIdTables(bucketCounts: Int*): Unit =
    bucketCounts.foreach(n => createBucketedIdTable(s"bucket$n", n))

  /** Joins `bucket12`, `bucket8` and `bucket<third>` on `id`, in that order. */
  private def threeWayBucketJoinDF(third: Int): DataFrame =
    sql("SELECT b12.id FROM testcat.ns.bucket12 b12 " +
      "JOIN testcat.ns.bucket8 b8 ON b12.id = b8.id " +
      s"JOIN testcat.ns.bucket$third b ON b12.id = b.id")

  /** The `(id, ts)` rows the `withReducedTsJoinLegs` tables are filled from, one per year. */
  private val row2020 = "(0, cast('2020-01-01' as timestamp))"
  private val row2021 = "(1, cast('2021-01-03' as timestamp))"
  private val bothRows = s"$row2020, $row2021"

  /** The timestamps those rows hold, as a query over them reports them. */
  private val ts2020 = Row(Timestamp.valueOf("2020-01-01 00:00:00"))
  private val ts2021 = Row(Timestamp.valueOf("2021-01-03 00:00:00"))
  private val bothTimestamps = Seq(ts2020, ts2021)

  /**
   * Creates `days1` and `days2` partitioned by `days(ts)` and `years1` and `years2` by `years(ts)`,
   * all over `(id, ts)`, with the `toYears`-reducing `days` and `years` functions registered.
   * `leg1Values` goes into `days1` and `years1`, `leg2Values` into `days2` and `years2`, unless
   * `leg2YearsValues` puts something else into `years2`.
   */
  private def withReducedTsJoinLegs(
      leg1Values: String,
      leg2Values: String,
      leg2YearsValues: Option[String] = None)(body: => Unit): Unit = {
    withFunction(
      UnboundDaysFunctionWithToYearsReducerWithLongResult,
      UnboundYearsFunctionWithToYearsReducerWithLongResult) {
      val tsColumns = Array(
        Column.create("id", LongType),
        Column.create("ts", TimestampType))
      Seq(("days1", leg1Values, days("ts")), ("days2", leg2Values, days("ts")),
        ("years1", leg1Values, years("ts")),
        ("years2", leg2YearsValues.getOrElse(leg2Values), years("ts"))).foreach {
        case (table, values, partition) =>
          createTable(table, tsColumns, Array(partition))
          sql(s"INSERT INTO testcat.ns.$table VALUES $values")
      }

      body
    }
  }

  /**
   * Joins `days1` to `years1` and `days2` to `years2`, each reducing both of its sides onto the
   * year key space, then joins the two reduced legs to each other with `joinType`. `leg2First` puts
   * the second leg on the left of that join. The projection takes the timestamp from whichever side
   * has it, so an outer join reports the same rows in either order.
   */
  private def reducedTsLegJoin(leg2First: Boolean = false, joinType: String = "JOIN"): String = {
    val leg1 = "SELECT d.ts FROM testcat.ns.days1 d JOIN testcat.ns.years1 y ON y.ts = d.ts"
    val leg2 = "SELECT y.ts FROM testcat.ns.days2 d JOIN testcat.ns.years2 y ON y.ts = d.ts"
    val (left, right) = if (leg2First) (leg2, leg1) else (leg1, leg2)
    s"SELECT coalesce(l.ts, r.ts) AS ts FROM ($left) l $joinType ($right) r ON l.ts = r.ts"
  }

  private def testWithCustomersAndOrders(
      customers_partitions: Array[Transform],
      orders_partitions: Array[Transform],
      expectedNumOfShuffleExecs: Int,
      expectedGroupPartitionsExecs: Int): Unit = {
    createTable(customers, customersColumns, customers_partitions)
    sql(s"INSERT INTO testcat.ns.$customers VALUES " +
        s"('aaa', 10, 1), ('bbb', 20, 2), ('ccc', 30, 3)")

    createTable(orders, ordersColumns, orders_partitions)
    sql(s"INSERT INTO testcat.ns.$orders VALUES " +
        s"(100.0, 1), (200.0, 1), (150.0, 2), (250.0, 2), (350.0, 2), (400.50, 3)")

    val df = sql(
      s"""
        |${selectWithMergeJoinHint("c", "o")}
        |customer_name, customer_age, order_amount
        |FROM testcat.ns.$customers c JOIN testcat.ns.$orders o
        |ON c.customer_id = o.customer_id ORDER BY c.customer_id, order_amount
        |""".stripMargin)

    val shuffles = collectShuffles(df.queryExecution.executedPlan)
    assert(shuffles.length == expectedNumOfShuffleExecs)

    val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
    assert(groupPartitions.length == expectedGroupPartitionsExecs)

    checkAnswer(df,
      Seq(Row("aaa", 10, 100.0), Row("aaa", 10, 200.0), Row("bbb", 20, 150.0),
        Row("bbb", 20, 250.0), Row("bbb", 20, 350.0), Row("ccc", 30, 400.50)))
  }

  protected def collectAllShuffles(plan: SparkPlan): Seq[ShuffleExchangeLike] = {
    collect(plan) {
      case s: ShuffleExchangeExec => s
    }
  }

  protected def collectAllGroupPartitions(plan: SparkPlan): Seq[GroupPartitionsExec] = {
    collect(plan) {
      case g: GroupPartitionsExec => g
    }
  }

  /** Every `KeyedPartitioning` these nodes report, flattening partitioning collections. */
  protected def keyedPartitioningsOf(
      nodes: Seq[SparkPlan]): Seq[physical.KeyedPartitioning] = {
    nodes.map(_.outputPartitioning)
      .flatMap(physical.PartitioningCollection.flatten)
      .collect { case kp: physical.KeyedPartitioning => kp }
  }

  test("partitioned join: exact distribution (same number of buckets) from both sides") {
    val customers_partitions = Array(bucket(4, "customer_id"))
    val orders_partitions = Array(bucket(4, "customer_id"))

    testWithCustomersAndOrders(customers_partitions, orders_partitions, 0, 1)
  }

  test("partitioned join: number of buckets mismatch should trigger shuffle") {
    val customers_partitions = Array(bucket(4, "customer_id"))
    val orders_partitions = Array(bucket(2, "customer_id"))

    // should shuffle both sides when number of buckets are not the same
    testWithCustomersAndOrders(customers_partitions, orders_partitions, 2, 0)
  }

  test("partitioned join: only one side reports partitioning") {
    val customers_partitions = Array(bucket(4, "customer_id"))

    testWithCustomersAndOrders(customers_partitions, Array.empty, 2, 0)
  }

  private val details: String = "details"
  private val detailsColumns: Array[Column] = Array(
    Column.create("item_id", LongType),
    Column.create("description", StringType),
    Column.create("updated", TimestampType))

  test("SPARK-48655: group by on partition keys should not introduce additional shuffle") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val df = sql(s"SELECT MAX(price) AS res FROM testcat.ns.$items GROUP BY id")
    val shuffles = collectAllShuffles(df.queryExecution.executedPlan)
    assert(shuffles.isEmpty,
      "should not contain shuffle when grouping by partition values")
    val groupPartitions = collectAllGroupPartitions(df.queryExecution.executedPlan)
    assert(groupPartitions.size == 1,
      "should contain group partitions when grouping by partition values")

    checkAnswer(df.sort("res"), Seq(Row(10.0), Row(15.5), Row(41.0)))
  }

  test("SPARK-48655: order by on partition keys should not introduce additional shuffle") {
    val items_partitions = Array(identity("price"), identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
      s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp)), " +
      s"(null, 'cc', 15.5, cast('2020-02-01' as timestamp)), " +
      s"(3, 'cc', null, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach { sortingEnabled =>
      withSQLConf(SQLConf.V2_BUCKETING_SORTING_ENABLED.key -> sortingEnabled.toString) {

        def verifyShuffle(cmd: String, answer: Seq[Row], expectedGroupPartitions: Int): Unit = {
          val df = sql(cmd)
          if (sortingEnabled) {
            assert(collectAllShuffles(df.queryExecution.executedPlan).isEmpty,
              "should contain no shuffle when sorting by partition values")
            assert(collectAllGroupPartitions(df.queryExecution.executedPlan).size ==
              expectedGroupPartitions,
              "should contain partition grouping when sorting by partition values")
          } else {
            assert(collectAllShuffles(df.queryExecution.executedPlan).size == 1,
              "should contain one shuffle when optimization is disabled")
            assert(collectAllGroupPartitions(df.queryExecution.executedPlan).isEmpty,
              "should contain no partition grouping when optimization is disabled")
          }
          checkAnswer(df, answer)
        }: Unit

        verifyShuffle(
          s"SELECT price, id FROM testcat.ns.$items ORDER BY price ASC, id ASC",
          // Default ordering of partitions matches requested ordering so we don't expect any
          // shuffles or group partitions
          Seq(Row(null, 3), Row(10.0, 2), Row(15.5, null),
            Row(15.5, 3), Row(40.0, 1), Row(41.0, 1)), 0)

        verifyShuffle(
          s"SELECT price, id FROM testcat.ns.$items " +
            s"ORDER BY price ASC NULLS LAST, id ASC NULLS LAST",
          Seq(Row(10.0, 2), Row(15.5, 3), Row(15.5, null),
            Row(40.0, 1), Row(41.0, 1), Row(null, 3)), 1)

        verifyShuffle(
          s"SELECT price, id FROM testcat.ns.$items ORDER BY price DESC, id ASC",
          Seq(Row(41.0, 1), Row(40.0, 1), Row(15.5, null),
            Row(15.5, 3), Row(10.0, 2), Row(null, 3)), 1)

        verifyShuffle(
          s"SELECT price, id FROM testcat.ns.$items ORDER BY price DESC, id DESC",
          Seq(Row(41.0, 1), Row(40.0, 1), Row(15.5, 3),
            Row(15.5, null), Row(10.0, 2), Row(null, 3)), 1)

        verifyShuffle(
          s"SELECT price, id FROM testcat.ns.$items " +
            s"ORDER BY price DESC NULLS FIRST, id DESC NULLS FIRST",
          Seq(Row(null, 3), Row(41.0, 1), Row(40.0, 1),
            Row(15.5, null), Row(15.5, 3), Row(10.0, 2)), 1);
      }
    }
  }

  test("SPARK-49179: Fix v2 multi bucketed inner joins throw AssertionError") {
    val cols = Array(
      Column.create("id", LongType),
      Column.create("name", StringType))
    val buckets = Array(bucket(8, "id"))

    withTable("t1", "t2", "t3") {
      Seq("t1", "t2", "t3").foreach { t =>
        createTable(t, cols, buckets)
        sql(s"INSERT INTO testcat.ns.$t VALUES (1, 'aa'), (2, 'bb'), (3, 'cc')")
      }
      val df = sql(
        """
          |SELECT t1.id, t2.id, t3.name FROM testcat.ns.t1
          |JOIN testcat.ns.t2 ON t1.id = t2.id
          |JOIN testcat.ns.t3 ON t1.id = t3.id
          |""".stripMargin)
      checkAnswer(df, Seq(Row(1, 1, "aa"), Row(2, 2, "bb"), Row(3, 3, "cc")))
      assert(collectShuffles(df.queryExecution.executedPlan).isEmpty)
      assert(collectGroupPartitions(df.queryExecution.executedPlan).isEmpty)
    }
  }

  test("SPARK-59045: compatible identity and bucket transforms reduce data type") {
    // `identity(id)` reports a Long partition key while `bucket(4, id)` reports an Integer one.
    // The identity->bucket reducer maps the Long keys to Integer; the GroupPartitionsExec output
    // partitioning must report the reduced (Integer) expression, not the original Long identity,
    // so downstream consumers see the keys' real type: another join can reduce onto the reported
    // transform, and a reduced layout can serve as another child's shuffle target only when the
    // expressions describe the keys.
    val cols = Array(
      Column.create("id", LongType),
      Column.create("data", StringType))
    createTable("t1", cols, Array(identity("id")))
    sql("INSERT INTO testcat.ns.t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    createTable("t2", cols, Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.t2 VALUES (1, 'x'), (2, 'y'), (3, 'z')")

    val df = sql(
      "SELECT t1.id, t1.data, t2.data FROM testcat.ns.t1 JOIN testcat.ns.t2 ON t1.id = t2.id")

    withSQLConf(
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      checkAnswer(df, Seq(Row(1, "a", "x"), Row(2, "b", "y"), Row(3, "c", "z")))
      assert(collectShuffles(df.queryExecution.executedPlan).isEmpty,
        "storage-partitioned join should not shuffle")
    }
  }

  test("SPARK-59045: compatible transforms reduce multiple times") {
    // t1 is partitioned by identity(id) (Long), t2 by bucket(4, id), t3 by bucket(2, id). The
    // first join reduces t1 to bucket(4, id) (data type changes), and the second join reduces the
    // result to bucket(2, id). The reduced expression reported by the first join must remain a
    // ReducibleFunction so the second reduction can be computed.
    // This test deliberately leaves `V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS` off: both
    // joins use the whole partition key, and without the config the failure on base is exercised
    // through the second, independent trigger (`reduceKeys` at the second join) instead of
    // `createShuffleSpec` -> `toGrouped`.
    val cols = Array(Column.create("id", LongType), Column.create("data", StringType))
    createTable("t1", cols, Array(identity("id")))
    createTable("t2", cols, Array(bucket(4, "id")))
    createTable("t3", cols, Array(bucket(2, "id")))
    sql("INSERT INTO testcat.ns.t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    sql("INSERT INTO testcat.ns.t2 VALUES (1, 'x'), (2, 'y'), (3, 'z')")
    sql("INSERT INTO testcat.ns.t3 VALUES (1, 'p'), (2, 'q'), (3, 'r')")

    val df = sql(
      "SELECT t1.id, t1.data, t2.data, t3.data FROM testcat.ns.t1 " +
        "JOIN testcat.ns.t2 ON t1.id = t2.id JOIN testcat.ns.t3 ON t1.id = t3.id")

    withSQLConf(
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
      checkAnswer(df, Seq(
        Row(1, "a", "x", "p"), Row(2, "b", "y", "q"), Row(3, "c", "z", "r")))
      assert(collectShuffles(df.queryExecution.executedPlan).isEmpty,
        "storage-partitioned join should not shuffle")
    }
  }

  test("SPARK-59045: reduced expression is retargeted per KeyedPartitioning") {
    // A chained SPJ's output partitioning reports one `KeyedPartitioning` per join side, but the
    // reduced expression is derived from the single spec that `createKeyedShuffleSpec` picks
    // (`collectFirst`). Re-targeting it at each `KeyedPartitioning`'s own key attribute keeps the
    // other sides' partitionings intact - otherwise a GROUP BY on the other side's key no longer
    // sees a partitioning on it and the query shuffles (0 shuffles on base and here, 1 if the
    // use-site re-targeting is dropped).
    val cols = Array(Column.create("id", LongType), Column.create("data", StringType))
    createTable("b16", cols, Array(bucket(16, "id")))
    createTable("b8", cols, Array(bucket(8, "id")))
    createTable("b4", cols, Array(bucket(4, "id")))
    val values = (0 until 16).map(i => s"($i, 'v$i')").mkString(", ")
    Seq("b16", "b8", "b4").foreach(t => sql(s"INSERT INTO testcat.ns.$t VALUES $values"))

    val df = sql(
      "SELECT b8.id, count(*) FROM testcat.ns.b16 " +
        "JOIN testcat.ns.b8 ON b16.id = b8.id JOIN testcat.ns.b4 ON b16.id = b4.id " +
        "GROUP BY b8.id")

    withSQLConf(SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
      checkAnswer(df, (0 until 16).map(i => Row(i.toLong, 1L)))
      assert(collectShuffles(df.queryExecution.executedPlan).isEmpty,
        "storage-partitioned join should not shuffle")
    }
  }

  test("SPARK-59045: compatible transforms reduce data type with subset join keys") {
    // The join is on `id`, a subset of the partition keys `[identity(dt), identity(id)]` and
    // `[identity(dt), bucket(2, id)]`. The identity(id) side is reduced to bucket(2, id), whose
    // data type differs, while the dt partition key is projected away.
    val cols = Array(
      Column.create("id", LongType),
      Column.create("dt", StringType),
      Column.create("data", StringType))
    createTable("t1", cols, Array(identity("dt"), identity("id")))
    createTable("t2", cols, Array(identity("dt"), bucket(2, "id")))
    sql("INSERT INTO testcat.ns.t1 VALUES (1, '2020', 'a'), (2, '2020', 'b'), (3, '2021', 'c')")
    sql("INSERT INTO testcat.ns.t2 VALUES (1, '2020', 'x'), (2, '2020', 'y'), (3, '2021', 'z')")

    val df = sql(
      "SELECT t1.id, t1.dt, t1.data, t2.dt, t2.data FROM testcat.ns.t1 " +
        "JOIN testcat.ns.t2 ON t1.id = t2.id")

    withSQLConf(
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      checkAnswer(df, Seq(
        Row(1, "2020", "a", "2020", "x"), Row(2, "2020", "b", "2020", "y"),
        Row(3, "2021", "c", "2021", "z")))
      assert(collectShuffles(df.queryExecution.executedPlan).isEmpty,
        "storage-partitioned join should not shuffle")
    }
  }

  test("SPARK-59045: canonicalization normalizes the reduced expressions") {
    // `KeyReducer` is a plain case class, not an `Expression`, so plan canonicalization does not
    // normalize the exprIds inside it. Two structurally identical `GroupPartitionsExec`s with
    // value-equal reducers must still compare equal after canonicalization, or exchange/subquery
    // reuse silently stops deduplicating their subtrees. The reduced expression is the other join
    // side's transform, so it references an attribute this node's child does not output; the
    // identity reducer's transform references this side's own key.
    val a1 = AttributeReference("id", LongType, nullable = true)().withExprId(ExprId(1))
    val a2 = AttributeReference("id", LongType, nullable = true)().withExprId(ExprId(2))
    val o1 = AttributeReference("oid", LongType, nullable = true)().withExprId(ExprId(11))
    val o2 = AttributeReference("oid", LongType, nullable = true)().withExprId(ExprId(12))
    def groupPartitions(
        attr: AttributeReference,
        otherAttr: AttributeReference,
        reducer: Reducer[_, _]): GroupPartitionsExec = {
      val child = new LocalTableScanExec(Seq(attr), Nil, None, false)
      val reduced = TransformExpression(BucketFunction, Seq(otherAttr), Some(2))
      GroupPartitionsExec(child,
        reducers = Some(Seq(Some(physical.KeyReducer(reducer, reduced)))))
    }
    // A value-equal reducer, with the reduced expression over the other side's attribute.
    assert(groupPartitions(a1, o1, BucketReducer(2)).canonicalized ==
      groupPartitions(a2, o2, BucketReducer(2)).canonicalized)
    // The identity-derived reducer, whose transform is over this side's own attribute.
    assert(groupPartitions(a1, o1, physical.IdentityReducer(
        TransformExpression(BucketFunction, Seq(a1), Some(2)))).canonicalized ==
      groupPartitions(a2, o2, physical.IdentityReducer(
        TransformExpression(BucketFunction, Seq(a2), Some(2)))).canonicalized)
    // Structurally different reducers stay unequal after canonicalization.
    assert(groupPartitions(a1, o1, BucketReducer(2)).canonicalized !=
      groupPartitions(a2, o2, BucketReducer(3)).canonicalized)

    // A multi-key partitioning with a mixed reducer sequence (identity keys are not reducible):
    // the normalization is per position, and the None entries pass through untouched.
    val dt1 = AttributeReference("dt", StringType, nullable = true)().withExprId(ExprId(21))
    val dt2 = AttributeReference("dt", StringType, nullable = true)().withExprId(ExprId(22))
    def mixedKeyGroupPartitions(
        attr: AttributeReference,
        dt: AttributeReference,
        otherAttr: AttributeReference): GroupPartitionsExec = {
      val child = new LocalTableScanExec(Seq(attr, dt), Nil, None, false)
      val reduced = TransformExpression(BucketFunction, Seq(otherAttr), Some(2))
      GroupPartitionsExec(child,
        reducers = Some(Seq(None, Some(physical.KeyReducer(BucketReducer(2), reduced)))))
    }
    assert(mixedKeyGroupPartitions(a1, dt1, o1).canonicalized ==
      mixedKeyGroupPartitions(a2, dt2, o2).canonicalized)
  }

  test("SPARK-59121: two sides reduced together are not reduced a second time") {
    withReducedTsJoinLegs(bothRows, row2021) {
      // Both inner joins reduce onto the year key space, and the two legs hold different key sets,
      // so the outer join takes the path that pushes the common keys down and computes reducers.
      // The two legs are the same pairing, so they are compatible and there is nothing left to
      // reduce. Deriving a reducer from their expressions again would apply `toYears` to keys
      // that already hold years.
      withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
        checkAnswer(sql(reducedTsLegJoin()), Seq(ts2021))
      }
    }
  }

  test("SPARK-59121: a join does not reduce already reduced partition keys") {
    createBucketedIdTables(12, 8, 6)

    // The first join reduces both sides onto `id % 4` (the greatest common divisor of 12 and 8), so
    // `bucket(12, id)` no longer describes its keys. The second join must not derive a `bucket(6)`
    // reducer from that expression and apply it to keys that are already reduced. It has to
    // shuffle instead. `(id % 4) % 6` is `id % 4`, so the reduce would leave the left keys alone
    // while the right side moves to `id % 6`.
    withSQLConf(
      SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
      val df = threeWayBucketJoinDF(6)

      checkAnswer(df, (0 until 12).map(i => Row(i.toLong)))
      assert(collectShuffles(stripAQEPlan(df.queryExecution.executedPlan)).size == 2,
        "the second join cannot join on reduced keys, so both its sides are shuffled")
    }
  }

  test("SPARK-59121: a union does not merge an already reduced partitioning") {
    createBucketedIdTables(12, 8)

    // The union's children report the same `bucket(12, id)` expressions, but the join side's keys
    // were reduced to `id % 4` and its expressions no longer describe them. Merging the two into
    // one partitioning would claim `bucket(12, id)` for the concatenated keys, and the aggregate
    // above would then group a key of the reduced side with an unrelated key of the other side.
    withSQLConf(
      SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
      val df = sql("SELECT id, count(*) AS c FROM (" +
        "SELECT b12.id FROM testcat.ns.bucket12 b12 JOIN testcat.ns.bucket8 b8 ON b12.id = b8.id " +
        "UNION ALL SELECT id FROM testcat.ns.bucket12) GROUP BY id")

      checkAnswer(df, (0 until 12).map(i => Row(i.toLong, 2L)))
      val unions = collect(stripAQEPlan(df.queryExecution.executedPlan)) { case u: UnionExec => u }
      assert(unions.size == 1)
      assert(!unions.head.outputPartitioning.isInstanceOf[physical.KeyedPartitioning],
        "the union must not claim a key-grouped partitioning it cannot describe")
    }
  }

  test("SPARK-59121: another side is not shuffled onto reduced keys") {
    createBucketedIdTables(12, 8, 2)

    // The first join reduces both sides onto `id % 4`, and that partitioning has more partitions
    // than `bucket(2, id)`, so it is the one `EnsureRequirements` would pick to shuffle the third
    // table onto. It must not. Shuffling evaluates the reported `bucket(12, id)` per row, which
    // does not produce the reduced keys the partitions are laid out by.
    withSQLConf(
      SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
      val df = threeWayBucketJoinDF(2)

      checkAnswer(df, (0 until 12).map(i => Row(i.toLong)))
      // The reduced side is the one that gets shuffled, onto `bucket(2, id)`. Nothing is shuffled
      // onto the reduced keys, which is what the assertion below states directly.
      val shuffles = collectShuffles(stripAQEPlan(df.queryExecution.executedPlan))
      assert(shuffles.size == 1)
      assert(shuffles.forall(_.outputPartitioning match {
        case kp: physical.KeyedPartitioning => kp.expressionsDescribeKeys
        case _ => true
      }))
    }
  }

  test("SPARK-59121: two reduced partitionings are not compatible by their transforms") {
    // 24 ids, so that each leg's two sides really report different key sets and the join reduces
    // them. With 12 ids `bucket(18, id)` is the identity and the leg would be co-partitioned as it
    // stands, with nothing reduced and nothing to tell apart.
    Seq("left12" -> 12, "left8" -> 8, "right12" -> 12, "right18" -> 18).foreach {
      case (name, buckets) => createBucketedIdTable(name, buckets, numIds = 24)
    }

    // The two legs reduce onto two different key spaces: `bucket(12) JOIN bucket(8)` onto `id % 4`
    // and `bucket(12) JOIN bucket(18)` onto `id % 6`. Both legs keep reporting `bucket(12, id)`, so
    // comparing the transforms says the two sides are co-partitioned when they are not, and an id
    // sits in a different partition on each side. Only the pairing tells the two spaces apart, and
    // marking the keys without it is not enough.
    withSQLConf(
      SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
      val df = sql(
        """
          |SELECT l.id FROM
          |  (SELECT l12.id FROM testcat.ns.left12 l12
          |    JOIN testcat.ns.left8 l8 ON l12.id = l8.id) l
          |  JOIN
          |  (SELECT r12.id FROM testcat.ns.right12 r12
          |    JOIN testcat.ns.right18 r18 ON r12.id = r18.id) r
          |  ON l.id = r.id
          |""".stripMargin)

      checkAnswer(df, (0 until 24).map(i => Row(i.toLong)))
    }
  }

  test("SPARK-59121: two sides reduced onto the same keys still join without a shuffle") {
    withReducedTsJoinLegs(bothRows, bothRows) {
      // Each inner join reduces both of its sides onto the year key space, and the projections keep
      // one reduced partitioning per side. Refusing to compare reduced keys must not go so far as
      // to refuse these two. They came out of the same pairing, so they carry the same keys and
      // the outer join is co-partitioned as well.
      withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
        val df = sql(reducedTsLegJoin())

        checkAnswer(df, bothTimestamps)
        val plan = stripAQEPlan(df.queryExecution.executedPlan)
        assert(collectShuffles(plan).isEmpty, "should not add shuffle for any of the three joins")
      }
    }
  }

  test("SPARK-59176: a leg reduced onto no key at all still joins") {
    withReducedTsJoinLegs(bothRows, row2020, leg2YearsValues = Some(row2021)) {
      // The second leg's two sides hold disjoint years, so the partition filter intersects them to
      // nothing and the leg reports a reduced partitioning with no key. The reduced types then have
      // to come from the first leg. The marked expressions still name the un-reduced `days` and
      // `years` transforms, whose types are not the `LongType` the reduced keys hold.
      withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
        // Both orders, since the side that has no key is the one to leave out of the comparison.
        // And both join types, since the inner join intersects the two key sets to nothing and so
        // has nothing to sort, while the full outer join keeps the other side's keys and sorts them
        // by the reported types.
        Seq("JOIN" -> Nil, "FULL OUTER JOIN" -> bothTimestamps).foreach {
          case (joinType, expected) =>
            Seq(false, true).foreach { leg2First =>
              val df = sql(reducedTsLegJoin(leg2First, joinType))

              checkAnswer(df, expected)
              assert(collectShuffles(stripAQEPlan(df.queryExecution.executedPlan)).isEmpty,
                "the two legs are the same pairing, so all three joins are co-partitioned")
            }
        }
      }
    }
  }

  test("SPARK-59176: an empty side whose expressions describe its keys keeps the reducer check") {
    withFunction(UnboundDaysFunctionWithToYearsReducerWithDateResult) {
      createTable(items, itemsColumns, Array(days("arrive_time")))
      sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(0, 'aa', 39.0, cast('2020-01-01' as timestamp))")

      Seq(purchases -> "2020-01-01", "purchases2" -> "2022-01-01").foreach {
        case (table, day) =>
          createTable(table, purchasesColumns, Array(years("time")))
          sql(s"INSERT INTO testcat.ns.$table VALUES (1, 42.0, cast('$day' as timestamp))")
      }

      // The inner join intersects two disjoint year key sets, so its leg reports a `years(time)`
      // partitioning with no key. Nothing reduced it, so its expressions still describe the keys it
      // would have had, and the reduced-types comparison must still run. This `days` function
      // breaks the reducer contract, returning `DateType` where the target `years` transform is
      // `IntegerType`, and that is what the comparison is there to catch.
      withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
        val e = intercept[SparkException] {
          sql(
            s"""
               |${selectWithMergeJoinHint("i", "e")} i.id
               |FROM testcat.ns.$items i
               |JOIN (SELECT p.time FROM testcat.ns.$purchases p
               |  JOIN testcat.ns.purchases2 p2 ON p2.time = p.time) e
               |ON e.time = i.arrive_time
               |""".stripMargin).collect()
        }
        assert(e.getMessage.contains(
          "Storage-partition join partition transforms produced incompatible reduced types"))
      }
    }
  }

  test("partitioned join: join with two partition keys and matching & sorted partitions") {
    val items_partitions = Array(bucket(8, "id"), days("arrive_time"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"), days("time"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 19.5, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach { pushDownValues =>
      withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString) {
        val df = createJoinTestDF(Seq("id" -> "item_id", "arrive_time" -> "time"))
        val shuffles = collectShuffles(df.queryExecution.executedPlan)
        assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
        val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
        assert(groupPartitions.size === 2,
          "should contain group partitions on both sides of the join")
        checkAnswer(df,
          Seq(Row(1, "aa", 40.0, 42.0), Row(1, "aa", 41.0, 44.0), Row(1, "aa", 41.0, 45.0),
            Row(2, "bb", 10.0, 11.0), Row(2, "bb", 10.5, 11.0), Row(3, "cc", 15.5, 19.5))
        )
      }
    }
  }

  test("partitioned join: join with two partition keys and unsorted partitions") {
    val items_partitions = Array(bucket(8, "id"), days("arrive_time"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp)), " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"), days("time"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
        s"(3, 19.5, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach { pushDownValues =>
      withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString) {
        val df = createJoinTestDF(Seq("id" -> "item_id", "arrive_time" -> "time"))
        val shuffles = collectShuffles(df.queryExecution.executedPlan)
        assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
        val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
        assert(groupPartitions.size === 2,
          "should contain group partitions on both sides of the join")
        checkAnswer(df,
          Seq(Row(1, "aa", 40.0, 42.0), Row(1, "aa", 41.0, 44.0), Row(1, "aa", 41.0, 45.0),
            Row(2, "bb", 10.0, 11.0), Row(2, "bb", 10.5, 11.0), Row(3, "cc", 15.5, 19.5))
        )
      }
    }
  }

  test("partitioned join: join with two partition keys and different # of partition keys") {
    val items_partitions = Array(bucket(8, "id"), days("arrive_time"))
    createTable(items, itemsColumns, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"), days("time"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 11.0, cast('2020-01-01' as timestamp))")

    Seq(true, false).foreach { pushDownValues =>
      withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString) {
        val df = createJoinTestDF(Seq("id" -> "item_id", "arrive_time" -> "time"))
        val shuffles = collectShuffles(df.queryExecution.executedPlan)
        val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
        if (pushDownValues) {
          assert(shuffles.isEmpty, "should not add shuffle when partition values mismatch")
          assert(groupPartitions.size === 2,
            "should add group partitions when partition values mismatch")
        } else {
          assert(shuffles.nonEmpty, "should add shuffle when partition values mismatch, and " +
              "pushing down partition values is not enabled")
          assert(groupPartitions.isEmpty, "should not add group partition when partition values " +
            "mismatch, and pushing down partition values is not enabled")
        }

        checkAnswer(df,
          Seq(Row(1, "aa", 40.0, 42.0), Row(2, "bb", 10.0, 11.0)))
      }
    }
  }

  test("SPARK-41413: partitioned join: partition values from one side are subset of those from " +
      "the other side") {
    val items_partitions = Array(bucket(4, "id"))
    createTable(items, itemsColumns, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(bucket(4, "item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)

    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(3, 19.5, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach { pushDownValues =>
      withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString) {
        val df = createJoinTestDF(Seq("id" -> "item_id"))
        val shuffles = collectShuffles(df.queryExecution.executedPlan)
        val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
        if (pushDownValues) {
          assert(shuffles.isEmpty, "should not add shuffle when partition values mismatch")
          assert(groupPartitions.size === 2,
            "should add group partitions when partition values mismatch")
        } else {
          assert(shuffles.nonEmpty, "should add shuffle when partition values mismatch, and " +
              "pushing down partition values is not enabled")
          assert(groupPartitions.isEmpty, "should not add group partition when partition values " +
            "mismatch, and pushing down partition values is not enabled")
        }

        checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0), Row(3, "bb", 10.0, 19.5)))
      }
    }
  }

  test("SPARK-41413: partitioned join: partition values from both sides overlaps") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(2, 19.5, cast('2020-02-01' as timestamp)), " +
        "(4, 30.0, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach { pushDownValues =>
      withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString) {
        val df = createJoinTestDF(Seq("id" -> "item_id"))
        val shuffles = collectShuffles(df.queryExecution.executedPlan)
        val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
        if (pushDownValues) {
          assert(shuffles.isEmpty, "should not add shuffle when partition values mismatch")
          assert(groupPartitions.size === 2,
            "should add group partitions when partition values mismatch")
        } else {
          assert(shuffles.nonEmpty, "should add shuffle when partition values mismatch, and " +
              "pushing down partition values is not enabled")
          assert(groupPartitions.isEmpty, "should not add group partition when partition values " +
            "mismatch, and pushing down partition values is not enabled")
        }

        checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0), Row(2, "bb", 10.0, 19.5)))
      }
    }
  }

  test("SPARK-41413: partitioned join: non-overlapping partition values from both sides") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(4, 42.0, cast('2020-01-01' as timestamp)), " +
        "(5, 19.5, cast('2020-02-01' as timestamp)), " +
        "(6, 30.0, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach { pushDownValues =>
      withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString) {
        val df = createJoinTestDF(Seq("id" -> "item_id"))
        val shuffles = collectShuffles(df.queryExecution.executedPlan)
        val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
        if (pushDownValues) {
          assert(shuffles.isEmpty, "should not add shuffle when partition values mismatch")
          assert(groupPartitions.size === 2,
            "should add group partitions when partition values mismatch")
        } else {
          assert(shuffles.nonEmpty, "should add shuffle when partition values mismatch, and " +
              "pushing down partition values is not enabled")
          assert(groupPartitions.isEmpty, "should not add group partition when partition values " +
            "mismatch, and pushing down partition values is not enabled")
        }

        checkAnswer(df, Seq.empty)
      }
    }
  }

  test("SPARK-49205: KeyedPartitioning should be an Expression") {
    val items_partitions = Array(days("arrive_time"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
      "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      "(2, 'bb', 10.5, cast('2020-01-01' as timestamp)), " +
      "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(days("time"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 42.0, cast('2020-01-01' as timestamp)), " +
      "(1, 44.0, cast('2020-01-15' as timestamp)), " +
      "(1, 45.0, cast('2020-01-15' as timestamp)), " +
      "(2, 11.0, cast('2020-01-01' as timestamp)), " +
      "(3, 19.5, cast('2020-02-01' as timestamp))")

    val df = sql(
      s"""
        |SELECT x, count(*) FROM (
        | SELECT /*+ broadcast(t2) */ arrive_time as x, * FROM testcat.ns.$items t1
        | JOIN testcat.ns.$purchases t2 ON t1.arrive_time = t2.time
        |)
        |GROUP BY x
        |""".stripMargin)
    checkAnswer(df,
      Seq(Row(Timestamp.valueOf("2020-01-01 00:00:00"), 6),
        Row(Timestamp.valueOf("2020-01-15 00:00:00"), 2),
        Row(Timestamp.valueOf("2020-02-01 00:00:00"), 1)))
    assert(collectAllShuffles(df.queryExecution.executedPlan).isEmpty)

    val df2 = sql(
      s"""
        |WITH t1 (SELECT * FROM testcat.ns.$items)
        |SELECT x, count(*) FROM (
        | SELECT /*+ broadcast(t2) */ t2.time as x FROM t1
        | JOIN testcat.ns.$purchases t2 ON t1.arrive_time = t2.time
        | JOIN t1 t3 ON t1.arrive_time = t3.arrive_time
        |) GROUP BY x
        |""".stripMargin)
    checkAnswer(df2,
      Seq(Row(Timestamp.valueOf("2020-01-01 00:00:00"), 18),
        Row(Timestamp.valueOf("2020-01-15 00:00:00"), 2),
        Row(Timestamp.valueOf("2020-02-01 00:00:00"), 1)))
    assert(collectAllShuffles(df2.queryExecution.executedPlan).isEmpty)
  }

  test("SPARK-42038: partially clustered: with same partition keys and one side fully clustered") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 50.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach { pushDownValues =>
      Seq(("true", 5), ("false", 3)).foreach {
        case (enable, expected) =>
          withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable) {
            val df = createJoinTestDF(Seq("id" -> "item_id"))
            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            assert(shuffles.isEmpty, "should not contain any shuffle")
            if (pushDownValues) {
              val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
              assert(groupPartitions.forall(_.outputPartitioning.numPartitions == expected))
            }
            checkAnswer(df, Seq(Row(1, "aa", 40.0, 45.0), Row(1, "aa", 40.0, 50.0),
              Row(2, "bb", 10.0, 15.0), Row(2, "bb", 10.0, 20.0), Row(3, "cc", 15.5, 20.0)))
          }
      }
    }
  }

  test("SPARK-42038: partially clustered: with same partition keys and both sides partially " +
      "clustered") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 50.0, cast('2020-01-02' as timestamp)), " +
        s"(1, 55.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-03' as timestamp)), " +
        s"(2, 22.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach { pushDownValues =>
      Seq(("true", 7), ("false", 3)).foreach {
        case (enable, expected) =>
          withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable) {
            val df = createJoinTestDF(Seq("id" -> "item_id"))
            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            assert(shuffles.isEmpty, "should not contain any shuffle")
            if (pushDownValues) {
              val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
              assert(groupPartitions.forall(_.outputPartitioning.numPartitions === expected))
            }
            checkAnswer(df, Seq(
              Row(1, "aa", 40.0, 45.0), Row(1, "aa", 40.0, 50.0), Row(1, "aa", 40.0, 55.0),
              Row(1, "aa", 41.0, 45.0), Row(1, "aa", 41.0, 50.0), Row(1, "aa", 41.0, 55.0),
              Row(2, "bb", 10.0, 15.0), Row(2, "bb", 10.0, 20.0), Row(2, "bb", 10.0, 22.0),
              Row(3, "cc", 15.5, 20.0)))
          }
      }
    }
  }

  test("SPARK-42038: partially clustered: with different partition keys and both sides partially " +
      "clustered") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp)), " +
        s"(4, 'dd', 18.0, cast('2023-01-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 50.0, cast('2020-01-02' as timestamp)), " +
        s"(1, 55.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-03' as timestamp)), " +
        s"(2, 25.0, cast('2020-01-03' as timestamp)), " +
        s"(2, 30.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    Seq(true, false).foreach { pushDownValues =>
      Seq((true, true, 8), (false, true, 3), (true, false, 10), (false, false, 5)).foreach {
        case (partial, filter, expected) =>
          withSQLConf(
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
            SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> filter.toString,
            SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> partial.toString) {
            val df = createJoinTestDF(Seq("id" -> "item_id"))
            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            if (pushDownValues) {
              assert(shuffles.isEmpty, "should not contain any shuffle")
              val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
              assert(groupPartitions.forall(_.outputPartitioning.numPartitions === expected))
            } else {
              assert(shuffles.nonEmpty,
                "should contain shuffle when not pushing down partition values")
            }
            checkAnswer(df, Seq(
              Row(1, "aa", 40.0, 45.0), Row(1, "aa", 40.0, 50.0), Row(1, "aa", 40.0, 55.0),
              Row(1, "aa", 41.0, 45.0), Row(1, "aa", 41.0, 50.0), Row(1, "aa", 41.0, 55.0),
              Row(2, "bb", 10.0, 15.0), Row(2, "bb", 10.0, 20.0), Row(2, "bb", 10.0, 25.0),
              Row(2, "bb", 10.0, 30.0), Row(3, "cc", 15.5, 20.0)))
          }
      }
    }
  }


  test("SPARK-42038: partially clustered: with different partition keys and missing keys on " +
      "left-hand side") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp)), " +
        s"(4, 'dd', 18.0, cast('2023-01-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 50.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-03' as timestamp)), " +
        s"(2, 25.0, cast('2020-01-03' as timestamp)), " +
        s"(2, 30.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    Seq(true, false).foreach { pushDownValues =>
      Seq((true, true, 3), (false, true, 2), (true, false, 9), (false, false, 5)).foreach {
        case(partial, filter, expected) =>
          withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> filter.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
                partial.toString) {
            val df = createJoinTestDF(Seq("id" -> "item_id"))
            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            if (pushDownValues) {
              assert(shuffles.isEmpty, "should not contain any shuffle")
              val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
              assert(groupPartitions.forall(_.outputPartitioning.numPartitions === expected))
            } else {
              assert(shuffles.nonEmpty,
                "should contain shuffle when not pushing down partition values")
            }
            checkAnswer(df, Seq(
              Row(1, "aa", 40.0, 45.0), Row(1, "aa", 40.0, 50.0),
              Row(1, "aa", 41.0, 45.0), Row(1, "aa", 41.0, 50.0),
              Row(3, "cc", 15.5, 20.0)))
          }
      }
    }
  }

  test("SPARK-42038: partially clustered: with different partition keys and missing keys on " +
      "right-hand side") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(2, 15.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp)), " +
        s"(4, 25.0, cast('2020-02-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    Seq(true, false).foreach { pushDownValues =>
      Seq((true, true, 2), (false, true, 2), (true, false, 6), (false, false, 5)).foreach {
        case (partial, filter, expected) =>
          withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> filter.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
                partial.toString) {
            val df = createJoinTestDF(Seq("id" -> "item_id"))
            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            if (pushDownValues) {
              assert(shuffles.isEmpty, "should not contain any shuffle")
              val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
              assert(groupPartitions.forall(_.outputPartitioning.numPartitions === expected))
            } else {
              assert(shuffles.nonEmpty,
                "should contain shuffle when not pushing down partition values")
            }
            checkAnswer(df, Seq(
              Row(2, "bb", 10.0, 15.0), Row(2, "bb", 10.0, 20.0), Row(3, "cc", 15.5, 20.0)))
          }
      }
    }
  }

  test("SPARK-42038: partially clustered: left outer join") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 15.0, cast('2020-01-02' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(2, 20.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp)), " +
        s"(4, 25.0, cast('2020-02-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    // In a left-outer join, and when the left side has larger stats, partially clustered
    // distribution should kick in and pick the right hand side to replicate partitions.
    Seq(true, false).foreach { pushDownValues =>
      Seq((true, true, 5), (false, true, 3), (true, false, 7), (false, false, 5)).foreach {
        case (partial, filter, expected) =>
          withSQLConf(
            SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> false.toString,
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
            SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> filter.toString,
            SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
              partial.toString) {
            val df = createJoinTestDF(
              Seq("id" -> "item_id", "arrive_time" -> "time"), joinType = "LEFT")
            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            if (pushDownValues) {
              assert(shuffles.isEmpty, "should not contain any shuffle")
              val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
              assert(groupPartitions.forall(_.outputPartitioning.numPartitions === expected))
            } else {
              assert(shuffles.nonEmpty,
                "should contain shuffle when not pushing down partition values")
            }
            checkAnswer(df, Seq(
              Row(1, "aa", 40.0, null), Row(1, "aa", 41.0, null),
              Row(2, "bb", 10.0, 20.0), Row(2, "bb", 15.0, null), Row(3, "cc", 15.5, 20.0)))
          }
      }
    }
  }

  test("SPARK-42038: partially clustered: right outer join") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp)), " +
        s"(4, 25.0, cast('2020-02-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    // The left-hand side is picked as the side to replicate partitions based on stats, but since
    // this is right outer join, partially clustered distribution won't kick in, and Spark should
    // only push down partition values on both side.
    Seq(true, false).foreach { pushDownValues =>
      Seq(("true", 5), ("false", 5)).foreach {
        case (enable, expected) =>
          withSQLConf(
            SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> false.toString,
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
            SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable) {
            val df = createJoinTestDF(
              Seq("id" -> "item_id", "arrive_time" -> "time"), joinType = "RIGHT")
            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            if (pushDownValues) {
              assert(shuffles.isEmpty, "should not contain any shuffle")
              val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
              assert(groupPartitions.forall(_.outputPartitioning.numPartitions === expected))
            } else {
              assert(shuffles.nonEmpty,
                "should contain shuffle when not pushing down partition values")
            }
            checkAnswer(df, Seq(
              Row(null, null, null, 25.0), Row(null, null, null, 30.0),
              Row(1, "aa", 40.0, 45.0),
              Row(2, "bb", 10.0, 15.0), Row(2, "bb", 10.0, 20.0), Row(3, "cc", 15.5, 20.0)))
          }
      }
    }
  }

  test("SPARK-42038: partially clustered: full outer join is not applicable") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-01-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-02' as timestamp)), " +
        s"(3, 20.0, cast('2020-01-01' as timestamp)), " +
        s"(4, 25.0, cast('2020-01-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    Seq(true, false).foreach { pushDownValues =>
      Seq(("true", 5), ("false", 5)).foreach {
        case (enable, expected) =>
          withSQLConf(
            SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> false.toString,
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
            SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable) {
            val df = createJoinTestDF(
              Seq("id" -> "item_id", "arrive_time" -> "time"), joinType = "FULL OUTER")
            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            if (pushDownValues) {
              assert(shuffles.isEmpty, "should not contain any shuffle")
              val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
              assert(groupPartitions.forall(_.outputPartitioning.numPartitions === expected))
            } else {
              assert(shuffles.nonEmpty,
                "should contain shuffle when not pushing down partition values")
            }
            checkAnswer(df, Seq(
              Row(null, null, null, 20.0), Row(null, null, null, 25.0), Row(null, null, null, 30.0),
              Row(1, "aa", 40.0, 45.0), Row(1, "aa", 41.0, null),
              Row(2, "bb", 10.0, 15.0), Row(3, "cc", 15.5, 20.0)))
          }
      }
    }
  }

  test("[SPARK-53074] partial clustering avoided to meet a non-JOIN required distribution") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 45.0, cast('2020-01-01' as timestamp)), " +
      "(1, 50.0, cast('2020-01-02' as timestamp)), " +
      "(2, 15.0, cast('2020-01-02' as timestamp)), " +
      "(2, 20.0, cast('2020-01-03' as timestamp)), " +
      "(3, 20.0, cast('2020-02-01' as timestamp))")

    for {
      pushDownValues <- Seq(true, false)
      enable <- Seq("true", "false")
    } yield {
      withSQLConf(
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
          SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable) {
        // The left side uses a key-grouped partitioning to satisfy the WINDOW function's
        // required distribution. By default, the left side will be partially clustered (since
        // it's estimated to be larger), but this partial clustering won't be applied because the
        // left side needs to be key-grouped partitioned to satisfy the WINDOW's required
        // distribution.
        // The left side needs to project additional fields to ensure it's estimated to be
        // larger than the right side.
        val df = sql(
          s"""
             |WITH purchases_windowed AS (
             |  SELECT
             |    ROW_NUMBER() OVER (
             |      PARTITION BY item_id ORDER BY time DESC
             |    ) AS RN,
             |    item_id,
             |    price,
             |    STRUCT(item_id, price, time) AS purchases_struct
             |  FROM testcat.ns.$purchases
             |)
             |SELECT
             |  SUM(p.price),
             |  SUM(p.purchases_struct.item_id),
             |  SUM(p.purchases_struct.price),
             |  MAX(p.purchases_struct.time)
             |FROM
             |  purchases_windowed p JOIN testcat.ns.$items i
             |  ON i.id = p.item_id
             |WHERE p.RN = 1
             |""".stripMargin)
        checkAnswer(df, Seq(Row(140.0, 7, 140.0, Timestamp.valueOf("2020-02-01 00:00:00"))))
        val shuffles = collectShuffles(df.queryExecution.executedPlan)
        assert(shuffles.isEmpty, "should not contain any shuffle")
        if (pushDownValues) {
          val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
          assert(groupPartitions.forall(_.outputPartitioning.numPartitions === 3))
        }
      }
    }
  }

  test("SPARK-55848: dropDuplicates after SPJ with partial clustering") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    // Two rows for id=1 so partial clustering may split them across tasks
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
        "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(1, 50.0, cast('2020-01-02' as timestamp)), " +
        "(2, 11.0, cast('2020-01-01' as timestamp)), " +
        "(3, 19.5, cast('2020-02-01' as timestamp))")

    withSQLConf(
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> true.toString) {
      // dropDuplicates on the join key after a partially-clustered SPJ must still
      // produce the correct number of distinct ids.  Before the fix, the
      // partially-clustered partitioning was incorrectly treated as satisfying
      // ClusteredDistribution, so EnsureRequirements did not insert an Exchange
      // before the dedup, leading to duplicate rows.
      val df = sql(
        s"""
           |${selectWithMergeJoinHint("i", "p")} DISTINCT i.id
           |FROM testcat.ns.$items i
           |JOIN testcat.ns.$purchases p ON i.id = p.item_id
           |""".stripMargin)
      checkAnswer(df, Seq(Row(1), Row(2), Row(3)))

      // One GroupPartitionsExec per join child to align the partially-clustered
      // partitions, and one above the join to group for the aggregate.
      val joinGP = collectGroupPartitions(df.queryExecution.executedPlan)
      assert(joinGP.size === 2,
        "expected 2 GroupPartitionsExec under the join")
      val allGP = collectAllGroupPartitions(df.queryExecution.executedPlan)
      assert(allGP.size === 3,
        "expected 3 GroupPartitionsExec total (2 under join + 1 above for aggregate)")
    }
  }

  test("SPARK-55848: Window dedup after SPJ with partial clustering") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
        "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(1, 50.0, cast('2020-01-02' as timestamp)), " +
        "(2, 11.0, cast('2020-01-01' as timestamp)), " +
        "(3, 19.5, cast('2020-02-01' as timestamp))")

    withSQLConf(
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> true.toString) {
      // Use ROW_NUMBER() OVER to dedup joined rows per id after a partially-clustered
      // SPJ.  The WINDOW operator requires ClusteredDistribution on i.id; with partial
      // clustering the plan must insert the right exchange/group so that the window
      // produces exactly one row per id.
      val df = sql(
        s"""
           |SELECT id, price FROM (
           |  ${selectWithMergeJoinHint("i", "p")} i.id, i.price,
           |    ROW_NUMBER() OVER (PARTITION BY i.id ORDER BY i.price DESC) AS rn
           |  FROM testcat.ns.$items i
           |  JOIN testcat.ns.$purchases p ON i.id = p.item_id
           |) t WHERE rn = 1
           |""".stripMargin)
      checkAnswer(df, Seq(Row(1, 41.0f), Row(2, 10.0f), Row(3, 15.5f)))

      // One GroupPartitionsExec per join child to align the partially-clustered
      // partitions, and one above the join to group for the window.
      val joinGP = collectGroupPartitions(df.queryExecution.executedPlan)
      assert(joinGP.size === 2,
        "expected 2 GroupPartitionsExec under the join")
      val allGP = collectAllGroupPartitions(df.queryExecution.executedPlan)
      assert(allGP.size === 3,
        "expected 3 GroupPartitionsExec total (2 under join + 1 above for window)")
    }
  }

  test("SPARK-55848: checkpointed partially-clustered join with dedup") {
    withTempDir { dir =>
      spark.sparkContext.setCheckpointDir(dir.getPath)
      val items_partitions = Array(identity("id"))
      createTable(items, itemsColumns, items_partitions)
      sql(s"INSERT INTO testcat.ns.$items VALUES " +
          "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
          "(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
          "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
          "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

      val purchases_partitions = Array(identity("item_id"))
      createTable(purchases, purchasesColumns, purchases_partitions)
      sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
          "(1, 42.0, cast('2020-01-01' as timestamp)), " +
          "(1, 50.0, cast('2020-01-02' as timestamp)), " +
          "(2, 11.0, cast('2020-01-01' as timestamp)), " +
          "(3, 19.5, cast('2020-02-01' as timestamp))")

      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> true.toString) {
        // Checkpoint the JOIN result (not the scan) so the checkpoint node carries the
        // partially-clustered KeyGroupedPartitioning. The dedup on top must still insert
        // the required grouping operator because partially-clustered partitioning does not
        // satisfy ClusteredDistribution.
        val joinedDf = sql(
          s"""${selectWithMergeJoinHint("i", "p")} i.id, i.name, i.price
             |FROM testcat.ns.$items i
             |JOIN testcat.ns.$purchases p ON i.id = p.item_id""".stripMargin)
        val checkpointedDf = joinedDf.checkpoint()
        val df = checkpointedDf.select("id").distinct()
        checkAnswer(df, Seq(Row(1), Row(2), Row(3)))

        val checkpointScans = collect(df.queryExecution.executedPlan) {
          case r: RDDScanExec => r
        }
        assert(checkpointScans.exists(_.outputPartitioning match {
          case kp: physical.KeyedPartitioning => !kp.isGrouped
          case _ => false
        }), "checkpoint (RDDScanExec) should have ungrouped KeyedPartitioning")

        val allGroupPartitions = collectAllGroupPartitions(df.queryExecution.executedPlan)
        assert(allGroupPartitions.size === 1,
          "expected 1 GroupPartitionsExec above the checkpointed join for dedup")
      }
    }
  }

  test("SPARK-41471: shuffle one side: only one side reports partitioning") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 42.0, cast('2020-01-01' as timestamp)), " +
      "(3, 19.5, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach { shuffle =>
      withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> shuffle.toString) {
        val df = createJoinTestDF(Seq("id" -> "item_id"))
        val shuffles = collectShuffles(df.queryExecution.executedPlan)
        if (shuffle) {
          assert(shuffles.size == 1, "only shuffle one side not report partitioning")
        } else {
          assert(shuffles.size == 2, "should add two side shuffle when bucketing shuffle one side" +
            " is not enabled")
        }

        checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0), Row(3, "bb", 10.0, 19.5)))
      }
    }
  }

  test("SPARK-41471: shuffle one side: shuffle side has more partition value") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 42.0, cast('2020-01-01' as timestamp)), " +
      "(3, 19.5, cast('2020-02-01' as timestamp)), " +
      "(5, 26.0, cast('2023-01-01' as timestamp)), " +
      "(6, 50.0, cast('2023-02-01' as timestamp))")

    Seq(true, false).foreach { shuffle =>
      withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> shuffle.toString) {
        Seq("", "LEFT OUTER", "RIGHT OUTER", "FULL OUTER").foreach { joinType =>
          val df = createJoinTestDF(Seq("id" -> "item_id"), joinType = joinType)
          val shuffles = collectShuffles(df.queryExecution.executedPlan)
          if (shuffle) {
            assert(shuffles.size == 1, "only shuffle one side not report partitioning")
          } else {
            assert(shuffles.size == 2, "should add two side shuffle when bucketing shuffle one " +
              "side is not enabled")
          }
          joinType match {
            case "" =>
              checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0), Row(3, "bb", 10.0, 19.5)))
            case "LEFT OUTER" =>
              checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0), Row(3, "bb", 10.0, 19.5),
                Row(4, "cc", 15.5, null)))
            case "RIGHT OUTER" =>
              checkAnswer(df, Seq(Row(null, null, null, 26.0), Row(null, null, null, 50.0),
                Row(1, "aa", 40.0, 42.0), Row(3, "bb", 10.0, 19.5)))
            case "FULL OUTER" =>
              checkAnswer(df, Seq(Row(null, null, null, 26.0), Row(null, null, null, 50.0),
                Row(1, "aa", 40.0, 42.0), Row(3, "bb", 10.0, 19.5),
                Row(4, "cc", 15.5, null)))
          }
        }
      }
    }
  }

  test("SPARK-41471: shuffle one side: only one side reports partitioning with two identity") {
    val items_partitions = Array(identity("id"), identity("arrive_time"))
    createTable(items, itemsColumns, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 42.0, cast('2020-01-01' as timestamp)), " +
      "(3, 19.5, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach { shuffle =>
      withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> shuffle.toString) {
        val df = createJoinTestDF(Seq("id" -> "item_id", "arrive_time" -> "time"))
        val shuffles = collectShuffles(df.queryExecution.executedPlan)
        if (shuffle) {
          assert(shuffles.size == 1, "only shuffle one side not report partitioning")
        } else {
          assert(shuffles.size == 2, "should add two side shuffle when bucketing shuffle one side" +
            " is not enabled")
        }

        checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0)))
      }
    }
  }

  test("SPARK-41471: shuffle one side: partitioning with transform") {
    val items_partitions = Array(years("arrive_time"))
    createTable(items, itemsColumns, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      "(4, 'cc', 15.5, cast('2021-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 42.0, cast('2020-01-01' as timestamp)), " +
      "(3, 19.5, cast('2021-02-01' as timestamp))")

    Seq(true, false).foreach { shuffle =>
      withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> shuffle.toString) {
        val df = createJoinTestDF(Seq("arrive_time" -> "time"))
        val shuffles = collectShuffles(df.queryExecution.executedPlan)
        if (shuffle) {
          assert(shuffles.size == 1, "partitioning with transform should trigger SPJ")
        } else {
          assert(shuffles.size == 2, "should add two side shuffle when bucketing shuffle one side" +
            " is not enabled")
        }

        checkAnswer(df, Seq(
          Row(1, "aa", 40.0, 42.0),
          Row(3, "bb", 10.0, 42.0),
          Row(4, "cc", 15.5, 19.5)))
      }
    }
  }

  test("SPARK-41471: shuffle one side: work with group partition split") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 42.0, cast('2020-01-01' as timestamp)), " +
      "(3, 19.5, cast('2020-02-01' as timestamp)), " +
      "(5, 26.0, cast('2023-01-01' as timestamp)), " +
      "(6, 50.0, cast('2023-02-01' as timestamp))")

    Seq(true, false).foreach { shuffle =>
      withSQLConf(
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> shuffle.toString,
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true") {
        val df = createJoinTestDF(Seq("id" -> "item_id"))
        checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0), Row(3, "bb", 10.0, 19.5)))
      }
    }
  }

  test("SPARK-59054: shuffle one side: partition keys with binary type") {
    val items_partitions = Array(identity("id"))
    createTable(items, Array(
      Column.create("id", BinaryType),
      Column.create("name", StringType),
      Column.create("price", DoubleType)), items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(X'0101', 'aa', 40.0), " +
      "(X'0202', 'bb', 10.0), " +
      "(X'0303', 'cc', 15.5), " +
      "(X'0404', 'dd', 20.0)")

    createTable(purchases, Array(
      Column.create("item_id", BinaryType),
      Column.create("price", DoubleType)), Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(X'0101', 42.0), (X'0101', 44.0), (X'0202', 11.0), (X'0202', 19.5), " +
      "(X'0303', 26.0), (X'0303', 30.0), (X'0404', 50.0), (X'0404', 60.0)")

    Seq(true, false).foreach { shuffle =>
      withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> shuffle.toString) {
        val df = createJoinTestDF(Seq("id" -> "item_id"))
        val shuffles = collectShuffles(df.queryExecution.executedPlan)
        if (shuffle) {
          assert(shuffles.size == 1, "only shuffle one side not report partitioning")
        } else {
          assert(shuffles.size == 2, "should add two side shuffle when bucketing shuffle one " +
            "side is not enabled")
        }

        checkAnswer(df, Seq(
          Row(Array[Byte](1, 1), "aa", 40.0, 42.0),
          Row(Array[Byte](1, 1), "aa", 40.0, 44.0),
          Row(Array[Byte](2, 2), "bb", 10.0, 11.0),
          Row(Array[Byte](2, 2), "bb", 10.0, 19.5),
          Row(Array[Byte](3, 3), "cc", 15.5, 26.0),
          Row(Array[Byte](3, 3), "cc", 15.5, 30.0),
          Row(Array[Byte](4, 4), "dd", 20.0, 50.0),
          Row(Array[Byte](4, 4), "dd", 20.0, 60.0)))
      }
    }
  }

  test("SPARK-59054: shuffle one side: struct partition keys with different field names") {
    // Struct equality ignores field names, so joining STRUCT<a:INT> with STRUCT<b:INT> is legal
    // and SPJ stays eligible. The shuffled side's lookup keys carry its own schema while the
    // partitioner's map keys come from the keyed side, so key comparison must not depend on
    // the field names.
    val items_partitions = Array(identity("id"))
    createTable(items, Array(
      Column.create("id", new StructType().add("a", IntegerType)),
      Column.create("name", StringType),
      Column.create("price", DoubleType)), items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(named_struct('a', 1), 'aa', 40.0), " +
      "(named_struct('a', 2), 'bb', 10.0), " +
      "(named_struct('a', 3), 'cc', 15.5), " +
      "(named_struct('a', 4), 'dd', 20.0)")

    createTable(purchases, Array(
      Column.create("item_id", new StructType().add("b", IntegerType)),
      Column.create("price", DoubleType)), Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(named_struct('b', 1), 42.0), (named_struct('b', 2), 19.5), " +
      "(named_struct('b', 3), 26.0), (named_struct('b', 4), 50.0)")

    Seq(true, false).foreach { shuffle =>
      withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> shuffle.toString) {
        val df = createJoinTestDF(Seq("id" -> "item_id"))
        val shuffles = collectShuffles(df.queryExecution.executedPlan)
        if (shuffle) {
          assert(shuffles.size == 1, "only shuffle one side not report partitioning")
        } else {
          assert(shuffles.size == 2, "should add two side shuffle when bucketing shuffle one " +
            "side is not enabled")
        }

        checkAnswer(df, Seq(
          Row(Row(1), "aa", 40.0, 42.0),
          Row(Row(2), "bb", 10.0, 19.5),
          Row(Row(3), "cc", 15.5, 26.0),
          Row(Row(4), "dd", 20.0, 50.0)))
      }
    }
  }

  test("SPARK-59054: shuffle one side: partition transform collapsing -0.0 and 0.0") {
    withFunction(UnboundSignedZerosFunction) {
      // `signed_zeros` maps id 1 to -0.0 and id 2 to 0.0: two partition keys that are equal
      // under SQL semantics but distinct bitwise, which the grouped side collapses into one
      // partition. Rows of both forms on the shuffled side must land in that partition.
      val items_partitions = Array(
        Expressions.apply("signed_zeros", Expressions.column("id")))
      createTable(items, itemsColumns, items_partitions)

      sql(s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

      createTable(purchases, purchasesColumns, Array.empty)
      sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(2, 19.5, cast('2020-02-01' as timestamp)), " +
        "(3, 26.0, cast('2023-01-01' as timestamp))")

      Seq(true, false).foreach { shuffle =>
        withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> shuffle.toString) {
          val df = createJoinTestDF(Seq("id" -> "item_id"))
          val shuffles = collectShuffles(df.queryExecution.executedPlan)
          if (shuffle) {
            assert(shuffles.size == 1, "only shuffle one side not report partitioning")
          } else {
            assert(shuffles.size == 2, "should add two side shuffle when bucketing shuffle one " +
              "side is not enabled")
          }

          checkAnswer(df, Seq(
            Row(1, "aa", 40.0, 42.0),
            Row(2, "bb", 10.0, 19.5),
            Row(3, "cc", 15.5, 26.0)))
        }
      }
    }
  }

  test("SPARK-44641: duplicated records when SPJ is not triggered") {
    val items_partitions = Array(bucket(8, "id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"""
        INSERT INTO testcat.ns.$items VALUES
        (1, 'aa', 40.0, cast('2020-01-01' as timestamp)),
        (1, 'aa', 41.0, cast('2020-01-15' as timestamp)),
        (2, 'bb', 10.0, cast('2020-01-01' as timestamp)),
        (2, 'bb', 10.5, cast('2020-01-01' as timestamp)),
        (3, 'cc', 15.5, cast('2020-02-01' as timestamp))""")

    val purchases_partitions = Array(bucket(8, "item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"""INSERT INTO testcat.ns.$purchases VALUES
        (1, 42.0, cast('2020-01-01' as timestamp)),
        (1, 44.0, cast('2020-01-15' as timestamp)),
        (1, 45.0, cast('2020-01-15' as timestamp)),
        (2, 11.0, cast('2020-01-01' as timestamp)),
        (3, 19.5, cast('2020-02-01' as timestamp))""")

    Seq(true, false).foreach { pushDownValues =>
      Seq(true, false).foreach { partiallyClusteredEnabled =>
        withSQLConf(
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
          SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
              partiallyClusteredEnabled.toString) {

          // join keys are not the same as the partition keys, therefore SPJ is not triggered.
          val df = createJoinTestDF(Seq("arrive_time" -> "time"), extraColumns = Seq("p.item_id"))
          val shuffles = collectShuffles(df.queryExecution.executedPlan)
          assert(shuffles.nonEmpty, "shuffle should exist when SPJ is not used")

          checkAnswer(df,
            Seq(
              Row(1, "aa", 40.0, 11.0, 2),
              Row(1, "aa", 40.0, 42.0, 1),
              Row(1, "aa", 41.0, 44.0, 1),
              Row(1, "aa", 41.0, 45.0, 1),
              Row(2, "bb", 10.0, 11.0, 2),
              Row(2, "bb", 10.0, 42.0, 1),
              Row(2, "bb", 10.5, 11.0, 2),
              Row(2, "bb", 10.5, 42.0, 1),
              Row(3, "cc", 15.5, 19.5, 3)
            )
          )
        }
      }
    }
  }

  test("SPARK-48065: SPJ: allowKeysSubsetOfPartitionKeys is too strict") {
    val table1 = "tab1e1"
    val table2 = "table2"
    val partition = Array(identity("id"))
    createTable(table1, columns, partition)
    sql(s"INSERT INTO testcat.ns.$table1 VALUES " +
        "(1, 'aa', cast('2020-01-01' as timestamp)), " +
        "(2, 'bb', cast('2020-01-01' as timestamp)), " +
        "(2, 'cc', cast('2020-01-01' as timestamp)), " +
        "(3, 'dd', cast('2020-01-01' as timestamp)), " +
        "(3, 'dd', cast('2020-01-01' as timestamp)), " +
        "(3, 'ee', cast('2020-01-01' as timestamp)), " +
        "(3, 'ee', cast('2020-01-01' as timestamp))")

    createTable(table2, columns, partition)
    sql(s"INSERT INTO testcat.ns.$table2 VALUES " +
        "(4, 'zz', cast('2020-01-01' as timestamp)), " +
        "(4, 'zz', cast('2020-01-01' as timestamp)), " +
        "(3, 'dd', cast('2020-01-01' as timestamp)), " +
        "(3, 'dd', cast('2020-01-01' as timestamp)), " +
        "(3, 'xx', cast('2020-01-01' as timestamp)), " +
        "(3, 'xx', cast('2020-01-01' as timestamp)), " +
        "(2, 'ww', cast('2020-01-01' as timestamp))")

    Seq(true, false).foreach { pushDownValues =>
      Seq(true, false).foreach { partiallyClustered =>
        withSQLConf(
          SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
          SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
            partiallyClustered.toString,
          SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
          val df = sql(
            s"""
               |${selectWithMergeJoinHint("t1", "t2")}
               |t1.id AS id, t1.data AS t1data, t2.data AS t2data
               |FROM testcat.ns.$table1 t1 JOIN testcat.ns.$table2 t2
               |ON t1.id = t2.id AND t1.data = t2.data ORDER BY t1.id, t1data, t2data
               |""".stripMargin)
          val shuffles = collectShuffles(df.queryExecution.executedPlan)
          assert(shuffles.isEmpty, "SPJ should be triggered")

          val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
            .map(_.outputPartitioning.numPartitions)
          if (partiallyClustered) {
            assert(groupPartitions == Seq(8, 8))
          } else {
            assert(groupPartitions == Seq(4, 4))
          }
          checkAnswer(df, Seq(
            Row(3, "dd", "dd"),
            Row(3, "dd", "dd"),
            Row(3, "dd", "dd"),
            Row(3, "dd", "dd")
          ))
        }
      }
    }
  }

  test("SPARK-44647: SPJ: test join key is subset of cluster key " +
      "with push values and partially-clustered") {
    val table1 = "tab1e1"
    val table2 = "table2"
    val partition = Array(identity("id"), identity("data"))
    createTable(table1, columns, partition)
    sql(s"INSERT INTO testcat.ns.$table1 VALUES " +
        "(1, 'aa', cast('2020-01-01' as timestamp)), " +
        "(2, 'bb', cast('2020-01-01' as timestamp)), " +
        "(2, 'cc', cast('2020-01-01' as timestamp)), " +
        "(3, 'dd', cast('2020-01-01' as timestamp)), " +
        "(3, 'dd', cast('2020-01-01' as timestamp)), " +
        "(3, 'ee', cast('2020-01-01' as timestamp)), " +
        "(3, 'ee', cast('2020-01-01' as timestamp))")

    createTable(table2, columns, partition)
    sql(s"INSERT INTO testcat.ns.$table2 VALUES " +
        "(4, 'zz', cast('2020-01-01' as timestamp)), " +
        "(4, 'zz', cast('2020-01-01' as timestamp)), " +
        "(3, 'yy', cast('2020-01-01' as timestamp)), " +
        "(3, 'yy', cast('2020-01-01' as timestamp)), " +
        "(3, 'xx', cast('2020-01-01' as timestamp)), " +
        "(3, 'xx', cast('2020-01-01' as timestamp)), " +
        "(2, 'ww', cast('2020-01-01' as timestamp))")

    Seq(true, false).foreach { pushDownValues =>
      Seq(true, false).foreach { filter =>
        Seq(true, false).foreach { partiallyClustered =>
          Seq(true, false).foreach { allowKeysSubsetOfPartitionKeys =>
            withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
                  partiallyClustered.toString,
              SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> filter.toString,
              SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key ->
                  allowKeysSubsetOfPartitionKeys.toString) {
              val df = sql(
                s"""
                  |${selectWithMergeJoinHint("t1", "t2")}
                  |t1.id AS id, t1.data AS t1data, t2.data AS t2data
                  |FROM testcat.ns.$table1 t1 JOIN testcat.ns.$table2 t2
                  |ON t1.id = t2.id ORDER BY t1.id, t1data, t2data
                  |""".stripMargin)
              val shuffles = collectShuffles(df.queryExecution.executedPlan)
              if (allowKeysSubsetOfPartitionKeys) {
                assert(shuffles.isEmpty, "SPJ should be triggered")
              } else {
                assert(shuffles.nonEmpty, "SPJ should not be triggered")
              }

              val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
                .map(_.outputPartitioning.numPartitions)
              (allowKeysSubsetOfPartitionKeys, partiallyClustered, filter) match {
                // SPJ, partially-clustered, with filter
                case (true, true, true) => assert(groupPartitions == Seq(6, 6))

                // SPJ, partially-clustered, no filter
                case (true, true, false) => assert(groupPartitions == Seq(8, 8))

                // SPJ and not partially-clustered, with filter
                case (true, false, true) => assert(groupPartitions == Seq(2, 2))

                // SPJ and not partially-clustered, no filter
                case (true, false, false) => assert(groupPartitions == Seq(4, 4))

                // No SPJ
                case _ => assert(groupPartitions == Seq.empty)
              }

              checkAnswer(df, Seq(
                Row(2, "bb", "ww"),
                Row(2, "cc", "ww"),
                Row(3, "dd", "xx"),
                Row(3, "dd", "xx"),
                Row(3, "dd", "xx"),
                Row(3, "dd", "xx"),
                Row(3, "dd", "yy"),
                Row(3, "dd", "yy"),
                Row(3, "dd", "yy"),
                Row(3, "dd", "yy"),
                Row(3, "ee", "xx"),
                Row(3, "ee", "xx"),
                Row(3, "ee", "xx"),
                Row(3, "ee", "xx"),
                Row(3, "ee", "yy"),
                Row(3, "ee", "yy"),
                Row(3, "ee", "yy"),
                Row(3, "ee", "yy")
              ))
            }
          }
        }
      }
    }
  }

  test("SPARK-47094: SPJ: Support compatible buckets") {
    val table1 = "tab1e1"
    val table2 = "table2"

    Seq(
      ((2, 4), (4, 2)),
      ((4, 2), (2, 4)),
      ((2, 2), (4, 6)),
      ((6, 2), (2, 2))).foreach {
      case ((table1buckets1, table1buckets2), (table2buckets1, table2buckets2)) =>
        catalog.clearTables()

        val partition1 = Array(bucket(table1buckets1, "store_id"),
          bucket(table1buckets2, "dept_id"))
        val partition2 = Array(bucket(table2buckets1, "store_id"),
          bucket(table2buckets2, "dept_id"))

        Seq((table1, partition1), (table2, partition2)).foreach { case (tab, part) =>
          createTable(tab, columns2, part)
          val insertStr = s"INSERT INTO testcat.ns.$tab VALUES " +
            "(0, 0, 'aa'), " +
            "(0, 0, 'ab'), " + // duplicate partition key
            "(0, 1, 'ac'), " +
            "(0, 2, 'ad'), " +
            "(0, 3, 'ae'), " +
            "(0, 4, 'af'), " +
            "(0, 5, 'ag'), " +
            "(1, 0, 'ah'), " +
            "(1, 0, 'ai'), " + // duplicate partition key
            "(1, 1, 'aj'), " +
            "(1, 2, 'ak'), " +
            "(1, 3, 'al'), " +
            "(1, 4, 'am'), " +
            "(1, 5, 'an'), " +
            "(2, 0, 'ao'), " +
            "(2, 0, 'ap'), " + // duplicate partition key
            "(2, 1, 'aq'), " +
            "(2, 2, 'ar'), " +
            "(2, 3, 'as'), " +
            "(2, 4, 'at'), " +
            "(2, 5, 'au'), " +
            "(3, 0, 'av'), " +
            "(3, 0, 'aw'), " + // duplicate partition key
            "(3, 1, 'ax'), " +
            "(3, 2, 'ay'), " +
            "(3, 3, 'az'), " +
            "(3, 4, 'ba'), " +
            "(3, 5, 'bb'), " +
            "(4, 0, 'bc'), " +
            "(4, 0, 'bd'), " + // duplicate partition key
            "(4, 1, 'be'), " +
            "(4, 2, 'bf'), " +
            "(4, 3, 'bg'), " +
            "(4, 4, 'bh'), " +
            "(4, 5, 'bi'), " +
            "(5, 0, 'bj'), " +
            "(5, 0, 'bk'), " + // duplicate partition key
            "(5, 1, 'bl'), " +
            "(5, 2, 'bm'), " +
            "(5, 3, 'bn'), " +
            "(5, 4, 'bo'), " +
            "(5, 5, 'bp')"

            // additional unmatched partitions to test push down
            val finalStr = if (tab == table1) {
              insertStr ++ ", (8, 0, 'xa'), (8, 8, 'xx')"
            } else {
              insertStr ++ ", (9, 0, 'ya'), (9, 9, 'yy')"
            }

            sql(finalStr)
        }

        Seq(true, false).foreach { allowKeysSubsetOfPartitionKeys =>
          withSQLConf(
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
            SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false",
            SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key ->
              allowKeysSubsetOfPartitionKeys.toString,
            SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
            val df = sql(
              s"""
                 |${selectWithMergeJoinHint("t1", "t2")}
                 |t1.store_id, t1.dept_id, t1.data, t2.data
                 |FROM testcat.ns.$table1 t1 JOIN testcat.ns.$table2 t2
                 |ON t1.store_id = t2.store_id AND t1.dept_id = t2.dept_id
                 |ORDER BY t1.store_id, t1.dept_id, t1.data, t2.data
                 |""".stripMargin)

            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            assert(shuffles.isEmpty, "SPJ should be triggered")

            val partions = collectGroupPartitions(df.queryExecution.executedPlan)
              .map(_.outputPartitioning.numPartitions)
            val expectedBuckets = Math.min(table1buckets1, table2buckets1) *
              Math.min(table1buckets2, table2buckets2)
            assert(partions == Seq(expectedBuckets, expectedBuckets))

            checkAnswer(df, Seq(
              Row(0, 0, "aa", "aa"),
              Row(0, 0, "aa", "ab"),
              Row(0, 0, "ab", "aa"),
              Row(0, 0, "ab", "ab"),
              Row(0, 1, "ac", "ac"),
              Row(0, 2, "ad", "ad"),
              Row(0, 3, "ae", "ae"),
              Row(0, 4, "af", "af"),
              Row(0, 5, "ag", "ag"),
              Row(1, 0, "ah", "ah"),
              Row(1, 0, "ah", "ai"),
              Row(1, 0, "ai", "ah"),
              Row(1, 0, "ai", "ai"),
              Row(1, 1, "aj", "aj"),
              Row(1, 2, "ak", "ak"),
              Row(1, 3, "al", "al"),
              Row(1, 4, "am", "am"),
              Row(1, 5, "an", "an"),
              Row(2, 0, "ao", "ao"),
              Row(2, 0, "ao", "ap"),
              Row(2, 0, "ap", "ao"),
              Row(2, 0, "ap", "ap"),
              Row(2, 1, "aq", "aq"),
              Row(2, 2, "ar", "ar"),
              Row(2, 3, "as", "as"),
              Row(2, 4, "at", "at"),
              Row(2, 5, "au", "au"),
              Row(3, 0, "av", "av"),
              Row(3, 0, "av", "aw"),
              Row(3, 0, "aw", "av"),
              Row(3, 0, "aw", "aw"),
              Row(3, 1, "ax", "ax"),
              Row(3, 2, "ay", "ay"),
              Row(3, 3, "az", "az"),
              Row(3, 4, "ba", "ba"),
              Row(3, 5, "bb", "bb"),
              Row(4, 0, "bc", "bc"),
              Row(4, 0, "bc", "bd"),
              Row(4, 0, "bd", "bc"),
              Row(4, 0, "bd", "bd"),
              Row(4, 1, "be", "be"),
              Row(4, 2, "bf", "bf"),
              Row(4, 3, "bg", "bg"),
              Row(4, 4, "bh", "bh"),
              Row(4, 5, "bi", "bi"),
              Row(5, 0, "bj", "bj"),
              Row(5, 0, "bj", "bk"),
              Row(5, 0, "bk", "bj"),
              Row(5, 0, "bk", "bk"),
              Row(5, 1, "bl", "bl"),
              Row(5, 2, "bm", "bm"),
              Row(5, 3, "bn", "bn"),
              Row(5, 4, "bo", "bo"),
              Row(5, 5, "bp", "bp")
            ))
          }
        }
    }
  }

  test("SPARK-47094: SPJ:Support compatible buckets with common divisor") {
    val table1 = "tab1e1"
    val table2 = "table2"

    Seq(
      ((6, 4), (4, 6)),
      ((6, 6), (4, 4)),
      ((4, 4), (6, 6)),
      ((4, 6), (6, 4))).foreach {
      case ((table1buckets1, table1buckets2), (table2buckets1, table2buckets2)) =>
        catalog.clearTables()

        val partition1 = Array(bucket(table1buckets1, "store_id"),
          bucket(table1buckets2, "dept_id"))
        val partition2 = Array(bucket(table2buckets1, "store_id"),
          bucket(table2buckets2, "dept_id"))

        Seq((table1, partition1), (table2, partition2)).foreach { case (tab, part) =>
          createTable(tab, columns2, part)
          val insertStr = s"INSERT INTO testcat.ns.$tab VALUES " +
            "(0, 0, 'aa'), " +
            "(0, 0, 'ab'), " + // duplicate partition key
            "(0, 1, 'ac'), " +
            "(0, 2, 'ad'), " +
            "(0, 3, 'ae'), " +
            "(0, 4, 'af'), " +
            "(0, 5, 'ag'), " +
            "(1, 0, 'ah'), " +
            "(1, 0, 'ai'), " + // duplicate partition key
            "(1, 1, 'aj'), " +
            "(1, 2, 'ak'), " +
            "(1, 3, 'al'), " +
            "(1, 4, 'am'), " +
            "(1, 5, 'an'), " +
            "(2, 0, 'ao'), " +
            "(2, 0, 'ap'), " + // duplicate partition key
            "(2, 1, 'aq'), " +
            "(2, 2, 'ar'), " +
            "(2, 3, 'as'), " +
            "(2, 4, 'at'), " +
            "(2, 5, 'au'), " +
            "(3, 0, 'av'), " +
            "(3, 0, 'aw'), " + // duplicate partition key
            "(3, 1, 'ax'), " +
            "(3, 2, 'ay'), " +
            "(3, 3, 'az'), " +
            "(3, 4, 'ba'), " +
            "(3, 5, 'bb'), " +
            "(4, 0, 'bc'), " +
            "(4, 0, 'bd'), " + // duplicate partition key
            "(4, 1, 'be'), " +
            "(4, 2, 'bf'), " +
            "(4, 3, 'bg'), " +
            "(4, 4, 'bh'), " +
            "(4, 5, 'bi'), " +
            "(5, 0, 'bj'), " +
            "(5, 0, 'bk'), " + // duplicate partition key
            "(5, 1, 'bl'), " +
            "(5, 2, 'bm'), " +
            "(5, 3, 'bn'), " +
            "(5, 4, 'bo'), " +
            "(5, 5, 'bp')"

            // additional unmatched partitions to test push down
            val finalStr = if (tab == table1) {
              insertStr ++ ", (8, 0, 'xa'), (8, 8, 'xx')"
            } else {
              insertStr ++ ", (9, 0, 'ya'), (9, 9, 'yy')"
            }

            sql(finalStr)
        }

        Seq(true, false).foreach { allowKeysSubsetOfPartitionKeys =>
          withSQLConf(
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
            SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false",
            SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key ->
              allowKeysSubsetOfPartitionKeys.toString,
            SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
            val df = sql(
              s"""
                 |${selectWithMergeJoinHint("t1", "t2")}
                 |t1.store_id, t1.dept_id, t1.data, t2.data
                 |FROM testcat.ns.$table1 t1 JOIN testcat.ns.$table2 t2
                 |ON t1.store_id = t2.store_id AND t1.dept_id = t2.dept_id
                 |ORDER BY t1.store_id, t1.dept_id, t1.data, t2.data
                 |""".stripMargin)

            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            assert(shuffles.isEmpty, "SPJ should be triggered")

            val partitions = collectGroupPartitions(df.queryExecution.executedPlan)
              .map(_.outputPartitioning.numPartitions)
            def gcd(a: Int, b: Int): Int = BigInt(a).gcd(BigInt(b)).toInt
            val expectedPartitions = gcd(table1buckets1, table2buckets1) *
              gcd(table1buckets2, table2buckets2)
            assert(partitions == Seq(expectedPartitions, expectedPartitions))

            checkAnswer(df, Seq(
              Row(0, 0, "aa", "aa"),
              Row(0, 0, "aa", "ab"),
              Row(0, 0, "ab", "aa"),
              Row(0, 0, "ab", "ab"),
              Row(0, 1, "ac", "ac"),
              Row(0, 2, "ad", "ad"),
              Row(0, 3, "ae", "ae"),
              Row(0, 4, "af", "af"),
              Row(0, 5, "ag", "ag"),
              Row(1, 0, "ah", "ah"),
              Row(1, 0, "ah", "ai"),
              Row(1, 0, "ai", "ah"),
              Row(1, 0, "ai", "ai"),
              Row(1, 1, "aj", "aj"),
              Row(1, 2, "ak", "ak"),
              Row(1, 3, "al", "al"),
              Row(1, 4, "am", "am"),
              Row(1, 5, "an", "an"),
              Row(2, 0, "ao", "ao"),
              Row(2, 0, "ao", "ap"),
              Row(2, 0, "ap", "ao"),
              Row(2, 0, "ap", "ap"),
              Row(2, 1, "aq", "aq"),
              Row(2, 2, "ar", "ar"),
              Row(2, 3, "as", "as"),
              Row(2, 4, "at", "at"),
              Row(2, 5, "au", "au"),
              Row(3, 0, "av", "av"),
              Row(3, 0, "av", "aw"),
              Row(3, 0, "aw", "av"),
              Row(3, 0, "aw", "aw"),
              Row(3, 1, "ax", "ax"),
              Row(3, 2, "ay", "ay"),
              Row(3, 3, "az", "az"),
              Row(3, 4, "ba", "ba"),
              Row(3, 5, "bb", "bb"),
              Row(4, 0, "bc", "bc"),
              Row(4, 0, "bc", "bd"),
              Row(4, 0, "bd", "bc"),
              Row(4, 0, "bd", "bd"),
              Row(4, 1, "be", "be"),
              Row(4, 2, "bf", "bf"),
              Row(4, 3, "bg", "bg"),
              Row(4, 4, "bh", "bh"),
              Row(4, 5, "bi", "bi"),
              Row(5, 0, "bj", "bj"),
              Row(5, 0, "bj", "bk"),
              Row(5, 0, "bk", "bj"),
              Row(5, 0, "bk", "bk"),
              Row(5, 1, "bl", "bl"),
              Row(5, 2, "bm", "bm"),
              Row(5, 3, "bn", "bn"),
              Row(5, 4, "bo", "bo"),
              Row(5, 5, "bp", "bp")
            ))
          }
        }
    }
  }

  test("SPARK-47094: SPJ: Does not trigger when incompatible number of buckets on both side") {
    val table1 = "tab1e1"
    val table2 = "table2"

    Seq(
      (2, 3),
      (3, 4)
    ).foreach {
      case (table1buckets1, table2buckets1) =>
        catalog.clearTables()

        val partition1 = Array(bucket(table1buckets1, "store_id"))
        val partition2 = Array(bucket(table2buckets1, "store_id"))

        Seq((table1, partition1), (table2, partition2)).foreach { case (tab, part) =>
          createTable(tab, columns2, part)
          val insertStr = s"INSERT INTO testcat.ns.$tab VALUES " +
            "(0, 0, 'aa'), " +
            "(1, 0, 'ab'), " + // duplicate partition key
            "(2, 2, 'ac'), " +
            "(3, 3, 'ad'), " +
            "(4, 2, 'bc') "

          sql(insertStr)
        }

        Seq(true, false).foreach { allowKeysSubsetOfPartitionKeys =>
          withSQLConf(
            SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
            SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false",
            SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key ->
              allowKeysSubsetOfPartitionKeys.toString,
            SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
            val df = sql(
              s"""
                 |${selectWithMergeJoinHint("t1", "t2")}
                 |t1.store_id, t1.dept_id, t1.data, t2.data
                 |FROM testcat.ns.$table1 t1 JOIN testcat.ns.$table2 t2
                 |ON t1.store_id = t2.store_id AND t1.dept_id = t2.dept_id
                 |""".stripMargin)

            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            assert(shuffles.nonEmpty, "SPJ should not be triggered")
          }
        }
    }
  }

  test("SPARK-47094: Support compatible buckets with less join keys than partition keys") {
    val table1 = "tab1e1"
    val table2 = "table2"

    Seq((2, 4), (4, 2), (2, 6), (6, 2)).foreach {
      case (table1buckets, table2buckets) =>
        catalog.clearTables()

        val partition1 = Array(identity("data"),
          bucket(table1buckets, "dept_id"))
        val partition2 = Array(bucket(3, "store_id"),
          bucket(table2buckets, "dept_id"))

        createTable(table1, columns2, partition1)
        sql(s"INSERT INTO testcat.ns.$table1 VALUES " +
          "(0, 0, 'aa'), " +
          "(1, 0, 'ab'), " +
          "(2, 1, 'ac'), " +
          "(3, 2, 'ad'), " +
          "(4, 3, 'ae'), " +
          "(5, 4, 'af'), " +
          "(6, 5, 'ag'), " +

          // value without other side match
          "(6, 6, 'xx')"
        )

        createTable(table2, columns2, partition2)
        sql(s"INSERT INTO testcat.ns.$table2 VALUES " +
          "(6, 0, '01'), " +
          "(5, 1, '02'), " + // duplicate partition key
          "(5, 1, '03'), " +
          "(4, 2, '04'), " +
          "(3, 3, '05'), " +
          "(2, 4, '06'), " +
          "(1, 5, '07'), " +

          // value without other side match
          "(7, 7, '99')"
        )


        withSQLConf(
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false",
          SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
          SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
          val df = sql(
            s"""
               |${selectWithMergeJoinHint("t1", "t2")}
               |t1.store_id, t2.store_id, t1.dept_id, t2.dept_id, t1.data, t2.data
               |FROM testcat.ns.$table1 t1 JOIN testcat.ns.$table2 t2
               |ON t1.dept_id = t2.dept_id
               |ORDER BY t1.store_id, t1.dept_id, t1.data, t2.data
               |""".stripMargin)

          val shuffles = collectShuffles(df.queryExecution.executedPlan)
          assert(shuffles.isEmpty, "SPJ should be triggered")

          val partitions = collectGroupPartitions(df.queryExecution.executedPlan)
            .map(_.outputPartitioning.numPartitions)

          val expectedBuckets = Math.min(table1buckets, table2buckets)

          assert(partitions == Seq(expectedBuckets, expectedBuckets))

          checkAnswer(df, Seq(
            Row(0, 6, 0, 0, "aa", "01"),
            Row(1, 6, 0, 0, "ab", "01"),
            Row(2, 5, 1, 1, "ac", "02"),
            Row(2, 5, 1, 1, "ac", "03"),
            Row(3, 4, 2, 2, "ad", "04"),
            Row(4, 3, 3, 3, "ae", "05"),
            Row(5, 2, 4, 4, "af", "06"),
            Row(6, 1, 5, 5, "ag", "07")
          ))
        }
      }
  }

  test("SPARK-47094: Compatible buckets does not support SPJ with " +
    "push-down values or partially-clustered") {
    val table1 = "tab1e1"
    val table2 = "table2"

    val partition1 = Array(bucket(4, "store_id"),
      bucket(2, "dept_id"))
    val partition2 = Array(bucket(2, "store_id"),
      bucket(2, "dept_id"))

    createTable(table1, columns2, partition1)
    sql(s"INSERT INTO testcat.ns.$table1 VALUES " +
          "(0, 0, 'aa'), " +
          "(1, 1, 'bb'), " +
          "(2, 2, 'cc')"
        )

    createTable(table2, columns2, partition2)
    sql(s"INSERT INTO testcat.ns.$table2 VALUES " +
          "(0, 0, 'aa'), " +
          "(1, 1, 'bb'), " +
          "(2, 2, 'cc')"
        )

    Seq(true, false).foreach{ allowPushDown =>
      Seq(true, false).foreach{ partiallyClustered =>
        withSQLConf(
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> allowPushDown.toString,
          SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
            partiallyClustered.toString,
          SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
          SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
          val df = sql(
                s"""
                   |${selectWithMergeJoinHint("t1", "t2")}
                   |t1.store_id, t1.store_id, t1.dept_id, t2.dept_id, t1.data, t2.data
                   |FROM testcat.ns.$table1 t1 JOIN testcat.ns.$table2 t2
                   |ON t1.store_id = t2.store_id AND t1.dept_id = t2.dept_id
                   |ORDER BY t1.store_id, t1.dept_id, t1.data, t2.data
                   |""".stripMargin)

          val shuffles = collectShuffles(df.queryExecution.executedPlan)
          val partitions = collectGroupPartitions(df.queryExecution.executedPlan)
            .map(_.outputPartitioning.numPartitions)

          (allowPushDown, partiallyClustered) match {
            case (true, false) =>
              assert(shuffles.isEmpty, "SPJ should be triggered")
              assert(partitions == Seq(2, 2))
            case (_, _) =>
              assert(shuffles.nonEmpty, "SPJ should not be triggered")
              assert(partitions.isEmpty)
          }

          checkAnswer(df, Seq(
              Row(0, 0, 0, 0, "aa", "aa"),
              Row(1, 1, 1, 1, "bb", "bb"),
              Row(2, 2, 2, 2, "cc", "cc")
            ))
          }
      }
    }
  }

  test("SPARK-44647: test join key is the second cluster key") {
    val table1 = "tab1e1"
    val table2 = "table2"
    val partition = Array(identity("id"), identity("data"))
    createTable(table1, columns, partition)
    sql(s"INSERT INTO testcat.ns.$table1 VALUES " +
        "(1, 'aa', cast('2020-01-01' as timestamp)), " +
        "(2, 'bb', cast('2020-01-02' as timestamp)), " +
        "(3, 'cc', cast('2020-01-03' as timestamp))")

    createTable(table2, columns, partition)
    sql(s"INSERT INTO testcat.ns.$table2 VALUES " +
        "(4, 'aa', cast('2020-01-01' as timestamp)), " +
        "(5, 'bb', cast('2020-01-02' as timestamp)), " +
        "(6, 'cc', cast('2020-01-03' as timestamp))")

    Seq(true, false).foreach { pushDownValues =>
      Seq(true, false).foreach { partiallyClustered =>
        Seq(true, false).foreach { allowKeysSubsetOfPartitionKeys =>
          withSQLConf(
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key ->
                pushDownValues.toString,
            SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
                partiallyClustered.toString,
            SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key ->
                allowKeysSubsetOfPartitionKeys.toString) {

            val df = sql(
              s"""
                |${selectWithMergeJoinHint("t1", "t2")}
                |t1.id AS t1id, t2.id as t2id, t1.data AS data
                |FROM testcat.ns.$table1 t1 JOIN testcat.ns.$table2 t2
                |ON t1.data = t2.data
                |ORDER BY t1id, t1id, data
                |""".stripMargin)
            checkAnswer(df, Seq(Row(1, 4, "aa"), Row(2, 5, "bb"), Row(3, 6, "cc")))

            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            if (allowKeysSubsetOfPartitionKeys) {
              assert(shuffles.isEmpty, "SPJ should be triggered")
            } else {
              assert(shuffles.nonEmpty, "SPJ should not be triggered")
            }

            val partitions = collectGroupPartitions(df.queryExecution.executedPlan)
              .map(_.outputPartitioning.numPartitions)
            (pushDownValues, allowKeysSubsetOfPartitionKeys, partiallyClustered) match {
              // SPJ and partially-clustered
              case (_, true, _) => assert(partitions == Seq(3, 3))
              // non-SPJ or SPJ/partially-clustered
              case _ => assert(partitions.isEmpty)
            }
          }
        }
      }
    }
  }

  test("SPARK-44647: test join key is the second partition key and a transform") {
    val items_partitions = Array(bucket(8, "id"), days("arrive_time"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"), days("time"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 19.5, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach { pushDownValues =>
      Seq(true, false).foreach { partiallyClustered =>
        Seq(true, false).foreach { allowKeysSubsetOfPartitionKeys =>

          withSQLConf(
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
            SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
                partiallyClustered.toString,
            SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key ->
                allowKeysSubsetOfPartitionKeys.toString) {
            val df = createJoinTestDF(Seq("arrive_time" -> "time"), extraColumns = Seq("p.item_id"))
            // Currently SPJ for case where join key not same as partition key
            // only supported when push-part-values enabled
            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            if (allowKeysSubsetOfPartitionKeys) {
              assert(shuffles.isEmpty, "SPJ should be triggered")
            } else {
              assert(shuffles.nonEmpty, "SPJ should not be triggered")
            }

            val partitions = collectGroupPartitions(df.queryExecution.executedPlan)
              .map(_.outputPartitioning.numPartitions)
            (allowKeysSubsetOfPartitionKeys, partiallyClustered) match {
              // SPJ and partially-clustered
              case (true, true) => assert(partitions == Seq(5, 5))
              // SPJ and not partially-clustered
              case (true, false) => assert(partitions == Seq(3, 3))
              // No SPJ
              case _ => assert(partitions.isEmpty)
            }

            checkAnswer(df,
              Seq(
                Row(1, "aa", 40.0, 11.0, 2),
                Row(1, "aa", 40.0, 42.0, 1),
                Row(1, "aa", 41.0, 44.0, 1),
                Row(1, "aa", 41.0, 45.0, 1),
                Row(2, "bb", 10.0, 11.0, 2),
                Row(2, "bb", 10.0, 42.0, 1),
                Row(2, "bb", 10.5, 11.0, 2),
                Row(2, "bb", 10.5, 42.0, 1),
                Row(3, "cc", 15.5, 19.5, 3)
              )
            )
          }
        }
      }
    }
  }

  test("SPARK-44647: shuffle one side and join keys are less than partition keys") {
    val items_partitions = Array(identity("id"), identity("name"))
    createTable(items, itemsColumns, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(1, 'aa', 30.0, cast('2020-01-02' as timestamp)), " +
      "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 42.0, cast('2020-01-01' as timestamp)), " +
      "(1, 89.0, cast('2020-01-03' as timestamp)), " +
      "(3, 19.5, cast('2020-02-01' as timestamp)), " +
      "(5, 26.0, cast('2023-01-01' as timestamp)), " +
      "(6, 50.0, cast('2023-02-01' as timestamp))")

    Seq(true, false).foreach { pushdownValues =>
      withSQLConf(
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushdownValues.toString,
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false",
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
        val df = createJoinTestDF(Seq("id" -> "item_id"))
        val shuffles = collectShuffles(df.queryExecution.executedPlan)
        assert(shuffles.size == 1, "SPJ should be triggered")
        checkAnswer(df, Seq(Row(1, "aa", 30.0, 42.0),
          Row(1, "aa", 30.0, 89.0),
          Row(1, "aa", 40.0, 42.0),
          Row(1, "aa", 40.0, 89.0),
          Row(3, "bb", 10.0, 19.5)))
      }
    }
  }

  test("SPARK-59025: shuffle one side and join keys are less than partition keys " +
      "when the keyed side reports a PartitioningCollection") {
    val items_partitions = Array(identity("id"), identity("name"))
    createTable(items, itemsColumns, items_partitions)

    // 4 distinct (id, name) partition keys but only 3 distinct ids, so grouping by the join
    // key must reduce the keyed side from 4 partitions to 3.
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(1, 'ab', 30.0, cast('2020-01-02' as timestamp)), " +
      "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 42.0, cast('2020-01-01' as timestamp)), " +
      "(1, 89.0, cast('2020-01-03' as timestamp)), " +
      "(3, 19.5, cast('2020-02-01' as timestamp)), " +
      "(5, 26.0, cast('2023-01-01' as timestamp))")

    withSQLConf(
      SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false",
      SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      // Duplicating `id` under two aliases makes the projection report a
      // `PartitioningCollection` of `KeyedPartitioning`s, so the keyed side's shuffle spec
      // is a `ShuffleSpecCollection` wrapping a `KeyedShuffleSpec` with join key positions.
      val df = sql(
        s"""
           |${selectWithMergeJoinHint("i", "p")}
           |id1, id2, name, i.price AS purchase_price, p.price AS sale_price
           |FROM (SELECT id AS id1, id AS id2, name, price FROM testcat.ns.$items) i
           |JOIN testcat.ns.$purchases p ON i.id1 = p.item_id
           |ORDER BY id1, purchase_price, sale_price
           |""".stripMargin)
      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.size == 1, "only the non-keyed side should be shuffled")
      val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
      assert(groupPartitions.size == 1 && groupPartitions.head.joinKeyPositions.isDefined,
        "the keyed side should be grouped by the join keys")
      assert(groupPartitions.head.outputPartitioning.numPartitions == 3,
        "the keyed side should be grouped down to 3 partitions")
      assert(shuffles.head.outputPartitioning.numPartitions == 3,
        "the shuffled side should match the 3 grouped partitions")
      checkAnswer(df, Seq(
        Row(1, 1, "ab", 30.0, 42.0),
        Row(1, 1, "ab", 30.0, 89.0),
        Row(1, 1, "aa", 40.0, 42.0),
        Row(1, 1, "aa", 40.0, 89.0),
        Row(3, 3, "bb", 10.0, 19.5)))
    }
  }

  test("SPARK-48012: one-side shuffle with partition transforms") {
    val items_partitions = Array(bucket(2, "id"), identity("arrive_time"))
    val items_partitions2 = Array(identity("arrive_time"), bucket(2, "id"))

    Seq(items_partitions, items_partitions2).foreach { partition =>
      catalog.clearTables()

      createTable(items, itemsColumns, partition)
      sql(s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(1, 'bb', 30.0, cast('2020-01-01' as timestamp)), " +
        "(1, 'cc', 30.0, cast('2020-01-02' as timestamp)), " +
        "(3, 'dd', 10.0, cast('2020-01-01' as timestamp)), " +
        "(4, 'ee', 15.5, cast('2020-02-01' as timestamp)), " +
        "(5, 'ff', 32.1, cast('2020-03-01' as timestamp))")

      createTable(purchases, purchasesColumns, Array.empty)
      sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(2, 10.7, cast('2020-01-01' as timestamp))," +
        "(3, 19.5, cast('2020-02-01' as timestamp))," +
        "(4, 56.5, cast('2020-02-01' as timestamp))")

      withSQLConf(
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true") {
        val df = createJoinTestDF(Seq("id" -> "item_id", "arrive_time" -> "time"))
        val shuffles = collectShuffles(df.queryExecution.executedPlan)
        assert(shuffles.size == 1, "only shuffle side that does not report partitioning")

        checkAnswer(df, Seq(
          Row(1, "bb", 30.0, 42.0),
          Row(1, "aa", 40.0, 42.0),
          Row(4, "ee", 15.5, 56.5)))
      }
    }
  }

  test("SPARK-48012: one-side shuffle with partition transforms and pushdown values") {
    val items_partitions = Array(bucket(2, "id"), identity("arrive_time"))
    createTable(items, itemsColumns, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(1, 'bb', 30.0, cast('2020-01-01' as timestamp)), " +
      "(1, 'cc', 30.0, cast('2020-01-02' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 42.0, cast('2020-01-01' as timestamp)), " +
      "(2, 10.7, cast('2020-01-01' as timestamp))")

    Seq(true, false).foreach { pushDown => {
        withSQLConf(
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key ->
            pushDown.toString) {
          val df = createJoinTestDF(Seq("id" -> "item_id", "arrive_time" -> "time"))
          val shuffles = collectShuffles(df.queryExecution.executedPlan)
          assert(shuffles.size == 1, "only shuffle side that does not report partitioning")

          checkAnswer(df, Seq(
            Row(1, "bb", 30.0, 42.0),
            Row(1, "aa", 40.0, 42.0)))
        }
      }
    }
  }

  test("SPARK-48012: one-side shuffle with partition transforms " +
    "with fewer join keys than partition kes") {
    val items_partitions = Array(bucket(2, "id"), identity("name"))
    createTable(items, itemsColumns, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(1, 'aa', 30.0, cast('2020-01-02' as timestamp)), " +
      "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 42.0, cast('2020-01-01' as timestamp)), " +
      "(1, 89.0, cast('2020-01-03' as timestamp)), " +
      "(3, 19.5, cast('2020-02-01' as timestamp)), " +
      "(5, 26.0, cast('2023-01-01' as timestamp)), " +
      "(6, 50.0, cast('2023-02-01' as timestamp))")

   withSQLConf(
     SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
     SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
     SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false",
     SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
     val df = createJoinTestDF(Seq("id" -> "item_id"))
     val shuffles = collectShuffles(df.queryExecution.executedPlan)
     assert(shuffles.size == 1, "SPJ should be triggered")
     checkAnswer(df, Seq(Row(1, "aa", 30.0, 42.0),
       Row(1, "aa", 30.0, 89.0),
       Row(1, "aa", 40.0, 42.0),
       Row(1, "aa", 40.0, 89.0),
       Row(3, "bb", 10.0, 19.5)))
   }
  }

  test("SPARK-52246: one-side shuffle with join key tail part of the partition keys") {
    val items_partitions = Array(bucket(2, "id"))
    createTable(items, itemsColumns, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(1, 'aa', 30.0, cast('2020-01-02' as timestamp)), " +
      "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 42.0, cast('2020-01-01' as timestamp)), " +
      "(1, 89.0, cast('2020-01-03' as timestamp)), " +
      "(3, 19.5, cast('2020-02-01' as timestamp)), " +
      "(5, 26.0, cast('2023-01-01' as timestamp)), " +
      "(6, 50.0, cast('2023-02-01' as timestamp))")

    withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true") {
      val df = createJoinTestDF(Seq("arrive_time" -> "time", "id" -> "item_id"))
      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.size == 1, "SPJ should be triggered")
      checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0)))
    }
  }

  test("SPARK-48949: test partition filters inner join") {
    val items_partitions = Array(bucket(8, "id"), days("arrive_time"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(0, 'aa', 39.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 41.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 'bb', 42.0, cast('2020-01-04' as timestamp)), " +
        s"(4, 'cc', 43.5, cast('2020-01-05' as timestamp)), " +
        s"(5, 'cc', 44.5, cast('2020-01-15' as timestamp)), " +
        s"(6, 'dd', 45.5, cast('2020-02-07' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"), days("time"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(5, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(7, 46.5, cast('2020-02-08' as timestamp))")

    withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "true") {

      val df = createJoinTestDF(Seq("id" -> "item_id", "arrive_time" -> "time"))
      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
      checkAnswer(df,
        Seq(Row(1, "aa", 40.0, 42.0), Row(5, "cc", 44.5, 44.0))
      )
      val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
      assert(groupPartitions.forall(_.outputPartitioning.numPartitions == 2))
    }
  }

  test("SPARK-48949: test partition filters with no matches") {
    val items_partitions = Array(bucket(8, "id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(0, 'aa', 39.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 40.0, cast('2020-01-02' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(4, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(5, 44.0, cast('2020-01-15' as timestamp))")

    withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "true") {

      val df = createJoinTestDF(Seq("id" -> "item_id"))
      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
      assert(df.collect().isEmpty, "should return no results")
      val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
      assert(groupPartitions.forall(_.outputPartitioning.numPartitions == 0))
    }
  }

  test("SPARK-48949: test partition filters with right outer") {
    val items_partitions = Array(bucket(8, "id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(0, 'aa', 39.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 40.0, cast('2020-01-02' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 40.0, cast('2020-01-01' as timestamp)), " +
        s"(4, 42.0, cast('2020-01-02' as timestamp)), " +
        s"(5, 44.0, cast('2020-01-15' as timestamp))")

    withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "true") {

      val df = createJoinTestDF(Seq("id" -> "item_id"), joinType = "RIGHT OUTER")
      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")

      checkAnswer(df,
        Seq(Row(null, null, null, 42.0),
          Row(null, null, null, 44.0),
          Row(1, "aa", 40.0, 40.0))
      )

      val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
      assert(groupPartitions.forall(_.outputPartitioning.numPartitions == 3))
    }
  }

  test("SPARK-48949: test partition filters with full outer") {
    val items_partitions = Array(bucket(8, "id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(0, 'aa', 39.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 40.0, cast('2020-01-02' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 40.0, cast('2020-01-01' as timestamp)), " +
        s"(4, 42.0, cast('2020-01-02' as timestamp)), " +
        s"(5, 44.0, cast('2020-01-15' as timestamp))")

    withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "true") {

      val df = createJoinTestDF(Seq("id" -> "item_id"), joinType = "FULL OUTER")
      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")

      checkAnswer(df,
        Seq(Row(null, null, null, 42.0),
          Row(null, null, null, 44.0),
          Row(0, "aa", 39.0, null),
          Row(1, "aa", 40.0, 40.0))
      )

      val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
      assert(groupPartitions.forall(_.outputPartitioning.numPartitions == 4))
    }
  }

  test("SPARK-48949: test partition filters with left outer") {
    val items_partitions = Array(bucket(8, "id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(0, 'aa', 38.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 39.0, cast('2020-01-02' as timestamp)), " +
        s"(4, 'aa', 40.0, cast('2020-01-02' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(4, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(5, 44.0, cast('2020-01-15' as timestamp))")

    withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "true") {

      val df = createJoinTestDF(Seq("id" -> "item_id"), joinType = "LEFT OUTER")
      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")

      checkAnswer(df,
        Seq(Row(0, "aa", 38.0, null),
          Row(1, "aa", 39.0, null),
          Row(4, "aa", 40.0, 42.0))
      )

      val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
      assert(groupPartitions.forall(_.outputPartitioning.numPartitions == 3))
    }
  }

  test("SPARK-48949: test partition filters with compatible transforms") {
    val items_partitions = Array(bucket(8, "id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(0, 'aa', 39.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 41.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 'bb', 42.0, cast('2020-01-04' as timestamp)), " +
        s"(4, 'cc', 43.5, cast('2020-01-05' as timestamp)), " +
        s"(5, 'cc', 44.5, cast('2020-01-15' as timestamp)), " +
        s"(6, 'dd', 45.5, cast('2020-02-07' as timestamp))")

    val purchases_partitions = Array(bucket(4, "item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(5, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(7, 46.5, cast('2020-02-08' as timestamp))")

    withSQLConf(
      SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {

      val df = createJoinTestDF(Seq("id" -> "item_id"))
      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
      checkAnswer(df,
        Seq(Row(1, "aa", 40.0, 42.0), Row(5, "cc", 44.5, 44.0))
      )
      val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
      assert(groupPartitions.forall(_.outputPartitioning.numPartitions == 2))
    }
  }

  test("SPARK-53322: checkpointed scans avoid shuffles for aggregates") {
    withTempDir { dir =>
      spark.sparkContext.setCheckpointDir(dir.getPath)
      val itemsPartitions = Array(identity("id"))
      createTable(items, itemsColumns, itemsPartitions)
      sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

      val scanDF = spark.read.table(s"testcat.ns.$items").checkpoint()
      val df = scanDF.groupBy("id").agg(max("price").as("res")).select("res")
      checkAnswer(df.sort("res"), Seq(Row(10.0), Row(15.5), Row(41.0)))

      val shuffles = collectAllShuffles(df.queryExecution.executedPlan)
      assert(shuffles.isEmpty,
        "should not contain shuffle when not grouping by partition values")
      val groupPartitions = collectAllGroupPartitions(df.queryExecution.executedPlan)
      assert(groupPartitions.size === 1)
      assert(groupPartitions.head.outputPartitioning.numPartitions == 3)
    }
  }

  test("SPARK-53322: checkpointed scans are used for SPJ") {
    withTempDir { dir =>
      spark.sparkContext.setCheckpointDir(dir.getPath)
      val itemsPartitions = Array(identity("id"))
      createTable(items, itemsColumns, itemsPartitions)
      sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 41.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-02' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-01-03' as timestamp))")

      val purchase_partitions = Array(identity("item_id"))
      createTable(purchases, purchasesColumns, purchase_partitions)
      sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 40.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 25.5, cast('2020-01-03' as timestamp)), " +
        s"(4, 20.0, cast('2020-01-04' as timestamp))")

      for {
        pushdownValues <- Seq(true, false)
        checkpointBothScans <- Seq(true, false)
      } {
        withSQLConf(
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushdownValues.toString) {
          val scanDF1 = spark.read.table(s"testcat.ns.$items").checkpoint().as("i")
          val scanDF2 = if (checkpointBothScans) {
            spark.read.table(s"testcat.ns.$purchases").checkpoint().as("p")
          } else {
            spark.read.table(s"testcat.ns.$purchases").as("p")
          }

          val df = scanDF1
            .join(scanDF2, col("id") === col("item_id"))
            .selectExpr("id", "name", "i.price AS purchase_price", "p.price AS sale_price")
            .orderBy("id", "purchase_price", "sale_price")
          checkAnswer(
            df,
            Seq(Row(1, "aa", 41.0, 40.0), Row(3, "cc", 15.5, 25.5))
          )
          if (pushdownValues) {
            // 1 shuffle for SORT and 2 group partitions for JOIN are expected.
            assert(collectAllShuffles(df.queryExecution.executedPlan).length === 1)
            assert(collectAllGroupPartitions(df.queryExecution.executedPlan).length === 2)
          } else {
            // 1 shuffle for SORT and 2 shuffles for JOIN are expected.
            assert(collectAllShuffles(df.queryExecution.executedPlan).length === 3)
            assert(collectAllGroupPartitions(df.queryExecution.executedPlan).length === 0)
          }
        }
      }
    }
  }

  test("SPARK-53322: checkpointed scans can shuffle other children on SPJ") {
    withTempDir { dir =>
      spark.sparkContext.setCheckpointDir(dir.getPath)
      val itemsPartitions = Array(identity("id"))
      createTable(items, itemsColumns, itemsPartitions)
      sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 41.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-02' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-01-03' as timestamp))")

      createTable(purchases, purchasesColumns, Array.empty)
      sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 40.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 25.5, cast('2020-01-03' as timestamp)), " +
        s"(4, 20.0, cast('2020-01-04' as timestamp))")

      Seq(true, false).foreach { pushdownValues =>
        withSQLConf(
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
            SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushdownValues.toString) {
          val scanDF1 = spark.read.table(s"testcat.ns.$items").checkpoint().as("i")
          val scanDF2 = spark.read.table(s"testcat.ns.$purchases").as("p")

          val df = scanDF1
            .join(scanDF2, col("id") === col("item_id"))
            .selectExpr("id", "name", "i.price AS purchase_price", "p.price AS sale_price")
            .orderBy("id", "purchase_price", "sale_price")
          checkAnswer(
            df,
            Seq(Row(1, "aa", 41.0, 40.0), Row(3, "cc", 15.5, 25.5))
          )
          // 1 shuffle for SORT and 1 shuffle for JOIN are expected.
          assert(collectAllShuffles(df.queryExecution.executedPlan).length === 2)
          // 0 group partitions are expected because both sides of the join are clustered from scans
          assert(collectAllGroupPartitions(df.queryExecution.executedPlan).length === 0)
        }
      }
    }
  }

  test("SPARK-54439: KeyedPartitioning and join key size mismatch") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 42.0, cast('2020-01-01' as timestamp)), " +
      "(3, 19.5, cast('2020-02-01' as timestamp))")

    withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true") {
      // `time` and `item_id` in the required `ClusteredDistribution` for `purchases`, but `item` is
      // storage partitioned only by `id`
      val df = createJoinTestDF(Seq("arrive_time" -> "time", "id" -> "item_id"))
      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.size == 1, "only shuffle one side not report partitioning")

      checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0)))
    }
  }

  test("SPARK-54439: KeyedPartitioning with transform and join key size mismatch") {
    val items_partitions = Array(years("arrive_time"))
    createTable(items, itemsColumns, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(1, 'bb', 10.0, cast('2021-01-01' as timestamp)), " +
      "(4, 'cc', 15.5, cast('2021-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 42.0, cast('2020-01-01' as timestamp)), " +
      "(3, 19.5, cast('2021-02-01' as timestamp))")

    withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true") {
      // `item_id` and `time` in the required `ClusteredDistribution` for `purchases`, but `item` is
      // storage partitioned only by `year(arrive_time)`
      val df = createJoinTestDF(Seq("id" -> "item_id", "arrive_time" -> "time"))
      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.size == 1, "only shuffle one side not report partitioning")

      checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0)))
    }
  }

  test("SPARK-55302: Custom metrics of grouped partitions") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(4, 'bb', 10.0, cast('2021-01-01' as timestamp)), " +
      "(4, 'cc', 15.5, cast('2021-02-01' as timestamp))")

    val metrics = runAndFetchMetrics {
      val df = sql(s"SELECT id, count(*) FROM testcat.ns.$items GROUP BY id")
      df.collect()
      val scans = collectScans(df.queryExecution.executedPlan)
      assert(scans(0).inputRDD.partitions.length === 3, "items scan should have 3 partitions")
      val groupPartitions = collectAllGroupPartitions(df.queryExecution.executedPlan)
      assert(groupPartitions(0).outputPartitioning.numPartitions === 2,
        "group partitions should have 2 partition groups")
    }
    assert(metrics.collect {
      case ((_, "BatchScan testcat.ns.items", "number of rows read"), v) => v
    } === Seq("3"))
  }

  test("SPARK-55619: Custom metrics of coalesced partitions") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(2, 'bb', 10.0, cast('2021-01-01' as timestamp))")

    val metrics = runAndFetchMetrics {
      val df = sql(s"SELECT * FROM testcat.ns.$items").coalesce(1)
      df.collect()
    }
    assert(metrics.collect {
      case ((_, "BatchScan testcat.ns.items", "number of rows read"), v) => v
    } === Seq("2"))
  }

  test("SPARK-55715: Custom metrics of sorted-merge coalesced partitions") {
    // items has id=1 on three splits with interleaved arrive_times -- out of order across splits.
    // purchases has item_id=1 on two splits, also out of order. Both sides coalesce under SMJ,
    // using SortedMergeCoalescedRDD with multiple concurrent readers per task. This test verifies
    // that all rows from both tables (5 + 4 = 9) are accounted for in the per-scan metrics.
    val itemOrdering = Array(
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("arrive_time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST))
    createTable(items, itemsColumns, Array(identity("id")), itemOrdering)
    // Rows inserted out of order: id=1 lands on partitions 1, 3, 4 with arrive_times
    // [2022-03-10, 2021-05-20, 2025-09-01] -- out of order.
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(3, 'dd', 40.0, cast('2024-01-01' as timestamp)), " +
      "(1, 'bb', 20.0, cast('2022-03-10' as timestamp)), " +
      "(2, 'cc', 30.0, cast('2023-06-15' as timestamp)), " +
      "(1, 'aa', 10.0, cast('2021-05-20' as timestamp)), " +
      "(1, 'ee', 50.0, cast('2025-09-01' as timestamp))")

    val purchaseOrdering = Array(
      sort(FieldReference("item_id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST))
    createTable(purchases, purchasesColumns, Array(identity("item_id")), purchaseOrdering)
    // item_id=1 lands on partitions 1 and 3 with times [2022-03-10, 2021-05-20] -- out of order.
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(2, 30.0, cast('2023-06-15' as timestamp)), " +
      "(1, 20.0, cast('2022-03-10' as timestamp)), " +
      "(3, 40.0, cast('2024-01-01' as timestamp)), " +
      "(1, 10.0, cast('2021-05-20' as timestamp))")

    withSQLConf(
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key -> "true") {
      val metrics = runAndFetchMetrics {
        val df = sql(
          s"""${selectWithMergeJoinHint("i", "p")}
             |i.id, i.name
             |FROM testcat.ns.$items i
             |JOIN testcat.ns.$purchases p ON p.item_id = i.id AND p.time = i.arrive_time
             |""".stripMargin)
        checkAnswer(df, Seq(Row(1, "aa"), Row(1, "bb"), Row(2, "cc"), Row(3, "dd")))
        val plan = df.queryExecution.executedPlan
        val groupPartitions = collectAllGroupPartitions(plan)
        val coalescingGP = groupPartitions.filter(_.groupedPartitions.exists(_._2.size > 1))
        assert(coalescingGP.nonEmpty, "expected a coalescing GroupPartitionsExec")
        coalescingGP.foreach { gp =>
          assert(gp.execute().isInstanceOf[SortedMergeCoalescedRDD[_]],
            "should use SortedMergeCoalescedRDD when preserve-ordering config is enabled")
        }
      }
      assert(metrics.collect {
        case ((_, "BatchScan testcat.ns.items", "number of rows read"), v) => v
      } === Seq("5"))
      assert(metrics.collect {
        case ((_, "BatchScan testcat.ns.purchases", "number of rows read"), v) => v
      } === Seq("4"))
    }
  }

  test("SPARK-55411: Fix ArrayIndexOutOfBoundsException when join keys " +
    "are less than cluster keys") {
    withSQLConf(
      SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false",
      SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {

      val customers_partitions = Array(identity("customer_name"), bucket(4, "customer_id"))
      createTable(customers, customersColumns, customers_partitions)
      sql(s"INSERT INTO testcat.ns.$customers VALUES " +
        s"('aaa', 10, 1), ('bbb', 20, 2), ('ccc', 30, 3)")

      createTable(orders, ordersColumns, Array.empty)
      sql(s"INSERT INTO testcat.ns.$orders VALUES " +
        s"(100.0, 1), (200.0, 1), (150.0, 2), (250.0, 2), (350.0, 2), (400.50, 3)")

      val df = sql(
        s"""${selectWithMergeJoinHint("c", "o")}
           |customer_name, customer_age, order_amount
           |FROM testcat.ns.$customers c JOIN testcat.ns.$orders o
           |ON c.customer_id = o.customer_id ORDER BY c.customer_id, order_amount
           |""".stripMargin)

      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.length == 1)

      checkAnswer(df, Seq(
        Row("aaa", 10, 100.0),
        Row("aaa", 10, 200.0),
        Row("bbb", 20, 150.0),
        Row("bbb", 20, 250.0),
        Row("bbb", 20, 350.0),
        Row("ccc", 30, 400.50)))
    }
  }

  test("SPARK-55092: Scans should not group partitions") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(4, 'bb', 10.0, cast('2021-01-01' as timestamp)), " +
      "(4, 'cc', 15.5, cast('2021-02-01' as timestamp))")

    val purchases_partitions = Array(years("time"))
    createTable(purchases, purchasesColumns, purchases_partitions)

    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 42.0, cast('2020-01-01' as timestamp)), " +
      "(3, 19.5, cast('2020-02-01' as timestamp))")

    val df = sql(s"SELECT * FROM testcat.ns.$items")
    val scans = collectScans(df.queryExecution.executedPlan)
    assert(scans(0).inputRDD.partitions.length === 3,
      "items scan should not group partitions")

    Seq((true, 1), (false, 2)).foreach { case (bucketingShuffle, expectedShuffleCount) =>
      withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> bucketingShuffle.toString) {
        val df = createJoinTestDF(Seq("id" -> "item_id"))

        val shuffles = collectShuffles(df.queryExecution.executedPlan)
        assert(shuffles.size == expectedShuffleCount)

        val scans = collectScans(df.queryExecution.executedPlan)
        assert(scans(0).inputRDD.partitions.length === 3,
          "items scan should not group partitions")
        assert(scans(1).inputRDD.partitions.length === 2,
          "purchases scan should not group partitions")

        checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0)))
      }
    }
  }

  test("SPARK-55535: Multi table join granular partition grouping") {
    withSQLConf(
      SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val items_partitions = Array(identity("id"), years("arrive_time"))
      createTable(items, itemsColumns, items_partitions)

      sql(s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 10.0, cast('2021-01-01' as timestamp)), " +
        "(1, 'aa', 20.0, cast('2022-01-01' as timestamp)), " +
        "(2, 'aa', 30.0, cast('2021-01-01' as timestamp)), " +
        "(2, 'aa', 40.0, cast('2022-01-01' as timestamp))")

      val purchases_partitions = Array(identity("item_id"), years("time"))
      createTable(purchases, purchasesColumns, purchases_partitions)

      sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(2, 10.0, cast('2021-01-01' as timestamp)), " +
        "(2, 20.0, cast('2022-01-01' as timestamp)), " +
        "(3, 30.0, cast('2021-01-01' as timestamp)), " +
        "(3, 40.0, cast('2022-01-01' as timestamp))")

      val details_partitions = Array(identity("item_id"))
      createTable(details, detailsColumns, details_partitions)

      sql(s"INSERT INTO testcat.ns.$details VALUES " +
        "(2, 'cc', cast('2021-01-01' as timestamp)), " +
        "(3, 'cc', cast('2022-01-01' as timestamp))")

      val df = sql(
        s"""
           |SELECT i.id, i.arrive_time, p.item_id, d.item_id
           |FROM testcat.ns.$items i
           |JOIN testcat.ns.$purchases p ON p.item_id = i.id AND p.time = i.arrive_time
           |JOIN testcat.ns.$details d ON d.item_id = i.id
           |""".stripMargin)

      checkAnswer(df, Seq(
        Row(2, Timestamp.valueOf("2021-01-01 00:00:00"), 2, 2),
        Row(2, Timestamp.valueOf("2022-01-01 00:00:00"), 2, 2)))
      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.isEmpty, "should not contain any shuffle")
      val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
      // Expect 6 partitions in the inner join node legs because partitioning uses 2 attributes.
      // Expect 3 partitions in the outer join node legs because partitioning uses 1 attributes.
      assert(groupPartitions.map(_.outputPartitioning.numPartitions) === Seq(3, 6, 6, 3))
    }
  }

  test("SPARK-55535: Multi table join partial clustering") {
    withSQLConf(SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true") {
      val items_partitions = Array(identity("id"))
      createTable(items, itemsColumns, items_partitions)

      sql(s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 10.0, cast('2021-01-01' as timestamp)), " +
        "(1, 'aa', 20.0, cast('2022-01-01' as timestamp)), " +
        "(2, 'aa', 30.0, cast('2021-01-01' as timestamp)), " +
        "(2, 'aa', 40.0, cast('2022-01-01' as timestamp))")

      val purchases_partitions = Array(identity("item_id"))
      createTable(purchases, purchasesColumns, purchases_partitions)

      sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(2, 10.0, cast('2021-01-01' as timestamp)), " +
        "(3, 20.0, cast('2022-01-01' as timestamp))")

      val details_partitions = Array(identity("item_id"))
      createTable(details, detailsColumns, details_partitions)

      sql(s"INSERT INTO testcat.ns.$details VALUES " +
        "(2, 'cc', cast('2021-01-01' as timestamp)), " +
        "(4, 'cc', cast('2022-01-01' as timestamp))")

      val df = sql(
        s"""
           |SELECT i.id, i.price, p.price, d.description
           |FROM testcat.ns.$items i
           |JOIN testcat.ns.$purchases p ON p.item_id = i.id
           |JOIN testcat.ns.$details d ON d.item_id = i.id
           |""".stripMargin)

      checkAnswer(df, Seq(
        Row(2, 30.0, 10.0, "cc"),
        Row(2, 40.0, 10.0, "cc")))
      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.isEmpty, "should not contain any shuffle")
      val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
      // Expect 5 partitions in the inner join node legs because 4 from the partially clustered
      // items table and 1 new from clustered purchases table.
      // Expect 6 partitions in the outer join node legs because 5 from the partially clustered
      // inner join result and 1 new from clustered details table.
      assert(groupPartitions.map(_.outputPartitioning.numPartitions) === Seq(6, 5, 5, 6))
    }
  }

  test("SPARK-55535: Empty partitioned table") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val items_partitions = Array(identity("id"))
      createTable(items, itemsColumns, items_partitions)

      val purchases_partitions = Array(identity("item_id"))
      createTable(purchases, purchasesColumns, purchases_partitions)

      val df = createJoinTestDF(Seq("id" -> "item_id"))
      checkAnswer(df, Seq.empty)

      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.size === 2,
        "both legs should be shuffled as empty tables should not report KeyedPartitioning")

      val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
      assert(groupPartitions.isEmpty,
        "no legs should be grouped as empty tables should not report KeyedPartitioning")
    }
  }

  test("SPARK-55535: Empty group partitions due to filtered partitions") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      s"(1, 'aa', 39.0, cast('2020-01-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)

    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      s"(2, 42.0, cast('2020-01-01' as timestamp))")

    withSQLConf(SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "true") {
      val df = createJoinTestDF(Seq("id" -> "item_id"))
      checkAnswer(df, Seq.empty)

      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.isEmpty, "no legs should be shuffled")

      val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
      assert(groupPartitions.forall(_.outputPartitioning.numPartitions == 0),
        "group partitions should not have any (common) partitions")
    }
  }

  test("SPARK-55535: Order by on partitions keys") {
    withSQLConf(SQLConf.V2_BUCKETING_SORTING_ENABLED.key -> "true") {
      val items_partitions = Array(identity("id"))
      createTable(items, itemsColumns, items_partitions)

      sql(s"INSERT INTO testcat.ns.$items VALUES " +
        "(2, 'aa', 10.0, cast('2021-01-01' as timestamp)), " +
        "(3, 'aa', 20.0, cast('2022-01-01' as timestamp)), " +
        "(1, 'aa', 40.0, cast('2022-01-01' as timestamp))")

      val df = sql(s"SELECT id FROM testcat.ns.$items i ORDER BY id")

      val expected = (1 to 3).map(Row(_))
      checkAnswer(df, expected)

      val reverseDf = sql(s"SELECT id FROM testcat.ns.$items i ORDER BY id DESC")

      checkAnswer(reverseDf, expected.reverse)

      sql(s"INSERT INTO testcat.ns.$items VALUES (2, 'aa', 30.0, cast('2021-01-01' as timestamp))")

      val dfWithDuplicate = sql(s"SELECT id FROM testcat.ns.$items i ORDER BY id")

      val expectedWithDuplicate = Seq(1, 2, 2, 3).map(Row(_))
      checkAnswer(dfWithDuplicate, expectedWithDuplicate)

      val reverseDfWithDuplicate = sql(s"SELECT id FROM testcat.ns.$items i ORDER BY id DESC")

      checkAnswer(reverseDfWithDuplicate, expectedWithDuplicate.reverse)

      Seq(
        df -> Seq.empty,
        reverseDf -> Seq(3),
        dfWithDuplicate -> Seq.empty,
        reverseDfWithDuplicate -> Seq(4)
      ).foreach {
        case (df, expectedPartitions) =>
          val shuffles = collectAllShuffles(df.queryExecution.executedPlan)
          assert(shuffles.isEmpty, "should not contain any shuffle")

          val groupPartitions = collectAllGroupPartitions(df.queryExecution.executedPlan)
          assert(groupPartitions.map(_.outputPartitioning.numPartitions) == expectedPartitions)
      }
    }
  }

  test("SPARK-55992: GroupPartitions string in simple and extended explain") {
    val items_partitions = Array(bucket(4, "id"), years("arrive_time"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES (1, 'aa', 10.0, cast('2021-01-01' as timestamp))")
    val purchases_partitions = Array(bucket(6, "item_id"), years("time"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES (2, 10.0, cast('2021-01-01' as timestamp))")
    withSQLConf(
      SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
      SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
      val df = sql(
        s"""
           |${selectWithMergeJoinHint("i", "p")}
           |*
           |FROM testcat.ns.$items i
           |JOIN testcat.ns.$purchases p ON p.item_id = i.id
           |""".stripMargin)
      val simpleAndExtendedKeyword =
        "GroupPartitions JoinKeyPositions: [0] ExpectedPartitionKeys: 2 " +
        "Reducers: [BucketReducer(2)] DistributePartitions: false"
      val formattedKeyword =
        "Arguments: JoinKeyPositions: [0], ExpectedPartitionKeys: 2, " +
        "Reducers: [BucketReducer(2)], DistributePartitions: false"
      checkKeywordsExistsInExplain(df, SimpleMode, simpleAndExtendedKeyword)
      checkKeywordsExistsInExplain(df, ExtendedMode, simpleAndExtendedKeyword)
      checkKeywordsExistsInExplain(df, FormattedMode, formattedKeyword)
    }
  }

  test("SPARK-56046: Reducers with same result types") {
    val items_partitions = Array(days("arrive_time"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      s"(0, 'aa', 39.0, cast('2020-01-01' as timestamp)), " +
      s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 'bb', 41.0, cast('2021-01-03' as timestamp)), " +
      s"(3, 'bb', 42.0, cast('2021-01-04' as timestamp))")

    val purchases_partitions = Array(years("time"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
      s"(5, 44.0, cast('2020-01-15' as timestamp)), " +
      s"(7, 46.5, cast('2021-02-08' as timestamp))")

    // A third table partitioned by `identity(time)` joins on the same timestamps: its side
    // reduces onto the first join's reported `years(arrive_time)`, so the chain plans without a
    // shuffle only while the reduced keys are reported under the type-correct target transform.
    val shipments = "shipments"
    createTable(shipments, purchasesColumns, Array(identity("time")))
    sql(s"INSERT INTO testcat.ns.$shipments VALUES " +
      s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
      s"(9, 46.5, cast('2021-02-08' as timestamp))")

    withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
        Seq(
          s"testcat.ns.$items i JOIN testcat.ns.$purchases p ON p.time = i.arrive_time " +
            s"JOIN testcat.ns.$shipments s ON i.arrive_time = s.time",
          s"testcat.ns.$purchases p JOIN testcat.ns.$items i ON i.arrive_time = p.time " +
            s"JOIN testcat.ns.$shipments s ON i.arrive_time = s.time"
        ).foreach { joinString =>
          val df = sql(
            s"""
               |${selectWithMergeJoinHint("i", "p")} i.id, p.item_id
               |FROM $joinString
               |ORDER BY i.id, p.item_id
               |""".stripMargin)

          val shuffles = collectShuffles(df.queryExecution.executedPlan)
          assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
          val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
          assert(groupPartitions.forall(_.outputPartitioning.numPartitions == 2))

          checkAnswer(df, Seq(Row(0, 1), Row(1, 1)))
        }
      }
  }

  test("SPARK-56046: Reducers with different result types") {
    withFunction(UnboundDaysFunctionWithToYearsReducerWithDateResult) {
      val items_partitions = Array(days("arrive_time"))
      createTable(items, itemsColumns, items_partitions)
      sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(0, 'aa', 39.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 41.0, cast('2021-01-03' as timestamp)), " +
        s"(3, 'bb', 42.0, cast('2021-01-04' as timestamp))")

      val purchases_partitions = Array(years("time"))
      createTable(purchases, purchasesColumns, purchases_partitions)
      sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(5, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(7, 46.5, cast('2021-02-08' as timestamp))")

      withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
        Seq(
          s"testcat.ns.$items i JOIN testcat.ns.$purchases p ON p.time = i.arrive_time",
          s"testcat.ns.$purchases p JOIN testcat.ns.$items i ON i.arrive_time = p.time"
        ).foreach { joinString =>
          val e = intercept[SparkException] {
            sql(
              s"""
                 |${selectWithMergeJoinHint("i", "p")} id, item_id
                 |FROM $joinString
                 |ORDER BY id, item_id
                 |""".stripMargin).collect()
          }
          assert(e.getMessage.contains(
            "Storage-partition join partition transforms produced incompatible reduced types"))
        }
      }
    }
  }

  test("SPARK-56164: Reducers with different result types to original keys") {
    withFunction(
      UnboundDaysFunctionWithToYearsReducerWithLongResult,
      UnboundYearsFunctionWithToYearsReducerWithLongResult) {
      val items_partitions = Array(days("arrive_time"))
      createTable(items, itemsColumns, items_partitions)
      sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(0, 'aa', 39.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 41.0, cast('2021-01-03' as timestamp)), " +
        s"(3, 'bb', 42.0, cast('2021-01-04' as timestamp))")

      val purchases_partitions = Array(years("time"))
      createTable(purchases, purchasesColumns, purchases_partitions)
      sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(5, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(7, 46.5, cast('2021-02-08' as timestamp))")

      withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
        Seq(
          s"testcat.ns.$items i JOIN testcat.ns.$purchases p ON p.time = i.arrive_time",
          s"testcat.ns.$purchases p JOIN testcat.ns.$items i ON i.arrive_time = p.time"
        ).foreach { joinString =>
          val df = sql(
            s"""
               |${selectWithMergeJoinHint("i", "p")} id, item_id
               |FROM $joinString
               |ORDER BY id, item_id
               |""".stripMargin)

          val shuffles = collectShuffles(df.queryExecution.executedPlan)
          assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
          val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
          assert(groupPartitions.forall(_.outputPartitioning.numPartitions == 2))

          // SPARK-59121: neither side's transform describes its keys any more, since both were
          // reduced onto one year space. They were reduced together, so they must still be
          // co-partitioned, and refusing to compare reduced keys must not go so far as to break
          // this. Validate the join subtree rather than the whole plan, because
          // `ValidateRequirements` walks children and a query stage is a leaf, so validating an
          // AQE plan checks nothing.
          val joins = collect(stripAQEPlan(df.queryExecution.executedPlan)) {
            case smj: SortMergeJoinExec => smj
          }
          assert(joins.size == 1)
          assert(ValidateRequirements.validate(joins.head))

          checkAnswer(df, Seq(Row(0, 1), Row(1, 1)))
        }
      }
    }
  }

  test("SPARK-56182: Reduce identity to other transforms") {
    val items_partitions = Array(bucket(4, "id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      s"(0, 'aa', 39.0, cast('2020-01-01' as timestamp)), " +
      s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 'bb', 41.0, cast('2021-01-03' as timestamp)), " +
      s"(3, 'bb', 42.0, cast('2021-01-04' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      s"(3, 42.0, cast('2020-01-01' as timestamp)), " +
      s"(0, 44.0, cast('2020-01-15' as timestamp)), " +
      s"(1, 46.5, cast('2021-02-08' as timestamp))")

    withSQLConf(
      SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
      Seq(
        s"testcat.ns.$items i JOIN testcat.ns.$purchases p ON p.item_id = i.id",
        s"testcat.ns.$purchases p JOIN testcat.ns.$items i ON i.id = p.item_id"
      ).foreach { joinString =>
        val df = sql(
          s"""
             |${selectWithMergeJoinHint("i", "p")} id, item_id
             |FROM $joinString
             |ORDER BY id, item_id
             |""".stripMargin)

        val shuffles = collectShuffles(df.queryExecution.executedPlan)
        assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
        val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
        assert(groupPartitions.forall(_.outputPartitioning.numPartitions == 4))

        checkAnswer(df, Seq(Row(0, 0), Row(1, 1), Row(3, 3)))
      }
    }
  }

  test("SPARK-56241: scan with KeyedPartitioning reports key-derived outputOrdering") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(3, 'cc', 30.0, cast('2021-01-01' as timestamp)), " +
      "(1, 'aa', 10.0, cast('2022-01-01' as timestamp)), " +
      "(2, 'bb', 20.0, cast('2022-01-01' as timestamp))")

    val df = sql(s"SELECT id, name FROM testcat.ns.$items")
    val plan = df.queryExecution.executedPlan
    val scans = collectScans(plan)
    assert(scans.size === 1)
    // With the config disabled (default), ordering derivation is suppressed.
    assert(scans.head.outputOrdering.isEmpty)
    // When enabled, the scan derives an ascending sort on the partition key `id`.
    // identity transforms are unwrapped to AttributeReferences by V2ExpressionUtils.
    withSQLConf(SQLConf.V2_BUCKETING_PARTITION_KEY_ORDERING_ENABLED.key -> "true") {
      val scansEnabled = collectScans(df.queryExecution.executedPlan)
      assert(scansEnabled.size === 1)
      val ordering = scansEnabled.head.outputOrdering
      assert(ordering.length === 1)
      assert(ordering.head.direction === Ascending)
      val keyExpr = ordering.head.child
      assert(keyExpr.isInstanceOf[AttributeReference])
      assert(keyExpr.asInstanceOf[AttributeReference].name === "id")
    }
  }

  test("SPARK-56241: GroupPartitionsExec non-coalescing passes through child ordering, " +
      "no pre-join SortExec needed before SortMergeJoin") {
    // Non-identical key sets force GroupPartitionsExec to be inserted on both sides align them,
    // but each group has exactly one partition — no coalescing.
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 10.0, cast('2021-01-01' as timestamp)), " +
      "(2, 'bb', 20.0, cast('2021-01-01' as timestamp)), " +
      "(3, 'cc', 30.0, cast('2021-01-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 100.0, cast('2021-01-01' as timestamp)), " +
      "(2, 200.0, cast('2021-01-01' as timestamp))")

    // GroupPartitionsExec passes through the child's key-derived outputOrdering.
    // EnsureRequirements checks outputOrdering directly so no SortExec should be inserted before
    // the SMJ.
    withSQLConf(SQLConf.V2_BUCKETING_PARTITION_KEY_ORDERING_ENABLED.key -> "true") {
      val df = sql(
        s"""
           |${selectWithMergeJoinHint("i", "p")}
           |i.id, i.name
           |FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON p.item_id = i.id
           |""".stripMargin)

      checkAnswer(df, Seq(Row(1, "aa"), Row(2, "bb")))

      val plan = df.queryExecution.executedPlan
      val groupPartitions = collectGroupPartitions(plan)
      assert(groupPartitions.nonEmpty, "expected GroupPartitionsExec in plan")
      assert(groupPartitions.forall(_.groupedPartitions.forall(_._2.size <= 1)),
        "expected non-coalescing GroupPartitionsExec")
      val smjs = collect(plan) { case j: SortMergeJoinExec => j }
      assert(smjs.nonEmpty, "expected SortMergeJoinExec in plan")
      smjs.foreach { smj =>
        val sorts = smj.children.flatMap(child => collect(child) { case s: SortExec => s })
        assert(sorts.isEmpty, "should not add SortExec before SMJ when ordering passes through " +
          "non-coalescing GroupPartitions")
      }
    }
  }

  test("SPARK-56241: GroupPartitionsExec coalescing derives ordering from key expressions, " +
      "no pre-join SortExec needed before SortMergeJoin") {
    // Duplicate key 1 on both sides causes coalescing.
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 10.0, cast('2021-01-01' as timestamp)), " +
      "(1, 'ab', 11.0, cast('2021-06-01' as timestamp)), " +
      "(2, 'bb', 20.0, cast('2021-01-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 100.0, cast('2021-01-01' as timestamp)), " +
      "(1, 110.0, cast('2021-06-01' as timestamp)), " +
      "(2, 200.0, cast('2021-01-01' as timestamp))")

    // GroupPartitionsExec derives outputOrdering from the key expressions after coalescing.
    // EnsureRequirements checks outputOrdering directly so no SortExec should be inserted before
    // the SMJ.
    withSQLConf(
      SQLConf.V2_BUCKETING_PARTITION_KEY_ORDERING_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_PRESERVE_KEY_ORDERING_ON_COALESCE_ENABLED.key -> "true") {
      val df = sql(
        s"""
           |${selectWithMergeJoinHint("i", "p")}
           |i.id, i.name
           |FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON p.item_id = i.id
           |""".stripMargin)

      checkAnswer(df, Seq(
        Row(1, "aa"), Row(1, "aa"), Row(1, "ab"), Row(1, "ab"),
        Row(2, "bb")))

      val plan = df.queryExecution.executedPlan
      val groupPartitions = collectGroupPartitions(plan)
      assert(groupPartitions.nonEmpty, "expected GroupPartitionsExec in plan")
      assert(groupPartitions.exists(_.groupedPartitions.exists(_._2.size > 1)),
        "expected coalescing GroupPartitionsExec")
      val smjs = collect(plan) { case j: SortMergeJoinExec => j }
      assert(smjs.nonEmpty, "expected SortMergeJoinExec in plan")
      smjs.foreach { smj =>
        val sorts = smj.children.flatMap(child => collect(child) { case s: SortExec => s })
        assert(sorts.isEmpty, "should not add SortExec before SMJ when ordering is derived " +
          "from coalesced partition key")
      }
    }
  }

  test("SPARK-55715: preserve outputOrdering when coalescing partitions with sorted merge") {
    // Both tables are partitioned by their id column and report ordering [id ASC, price ASC]
    // via SupportsReportOrdering. Each has two rows with id=1 (two splits), so GroupPartitionsExec
    // must coalesce them. We join on (id, price) = (item_id, price) using SMJ.
    //
    // With config enabled:  SortedMergeCoalescedRDD performs a k-way merge preserving the full
    //   [id ASC, price ASC] ordering -> EnsureRequirements is satisfied -> no SortExec added.
    // With config disabled: simple CoalescedRDD concatenates the splits and only the key-derived
    //   [id ASC] ordering survives -> price ordering is lost -> SortExec is added for price.
    val itemOrdering = Array(
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("arrive_time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST))
    createTable(items, itemsColumns, Array(identity("id")), itemOrdering)
    // Rows inserted out of order: id values are interleaved and arrive_time is not monotone
    // within each id group, so ordering by [id ASC, arrive_time ASC] is non-trivial.
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(2, 'cc', 30.0, cast('2023-06-15' as timestamp)), " +
      "(1, 'bb', 20.0, cast('2022-03-10' as timestamp)), " +
      "(3, 'dd', 40.0, cast('2024-01-01' as timestamp)), " +
      "(1, 'aa', 10.0, cast('2021-05-20' as timestamp)), " +
      "(2, 'ee', 50.0, cast('2025-09-01' as timestamp))")

    val purchaseOrdering = Array(
      sort(FieldReference("item_id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST))
    createTable(purchases, purchasesColumns, Array(identity("item_id")), purchaseOrdering)
    // Also inserted out of order
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(2, 50.0, cast('2025-09-01' as timestamp)), " +
      "(1, 10.0, cast('2021-05-20' as timestamp)), " +
      "(3, 40.0, cast('2024-01-01' as timestamp)), " +
      "(2, 30.0, cast('2023-06-15' as timestamp)), " +
      "(1, 20.0, cast('2022-03-10' as timestamp))")

    Seq(true, false).foreach { preserveOrdering =>
      withSQLConf(
          SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
          SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
          SQLConf.V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key ->
            preserveOrdering.toString) {
        val df = sql(
          s"""
             |${selectWithMergeJoinHint("i", "p")}
             |i.id, i.name
             |FROM testcat.ns.$items i
             |JOIN testcat.ns.$purchases p ON p.item_id = i.id AND p.time = i.arrive_time
             |""".stripMargin)
        checkAnswer(df, Seq(
          Row(1, "aa"), Row(1, "bb"), Row(2, "cc"), Row(2, "ee"), Row(3, "dd")))

        val plan = df.queryExecution.executedPlan
        assert(collectAllShuffles(plan).isEmpty, "should not contain any shuffle")

        val groupPartitions = collectAllGroupPartitions(plan)
        assert(groupPartitions.nonEmpty, "should contain GroupPartitionsExec for coalescing")
        assert(groupPartitions.exists(_.groupedPartitions.exists(_._2.size > 1)),
          "expected coalescing GroupPartitionsExec")

        val smjs = collect(plan) { case j: SortMergeJoinExec => j }
        assert(smjs.nonEmpty, "expected SortMergeJoinExec in plan")
        smjs.foreach { smj =>
          val sorts = smj.children.flatMap(child => collect(child) { case s: SortExec => s })
          if (preserveOrdering) {
            assert(sorts.isEmpty,
              "config enabled: SortedMergeCoalescedRDD preserves [id ASC, arrive_time ASC], " +
                "no SortExec should be added before SMJ")

            // Also verify the k-way merge RDD is actually used
            val coalescingGP = groupPartitions.filter(_.groupedPartitions.exists(_._2.size > 1))
            coalescingGP.foreach { gp =>
              assert(gp.execute().isInstanceOf[SortedMergeCoalescedRDD[_]],
                "config enabled: should use SortedMergeCoalescedRDD")
            }
          } else {
            assert(sorts.nonEmpty,
              "config disabled: simple coalescing loses arrive_time ordering, " +
                "SortExec should be added before SMJ")
          }
        }
      }
    }
  }

  test("SPARK-55715: preserve outputOrdering when coalescing transform-partitioned splits") {
    // Both tables are partitioned by years("arrive_time") / years("time") and report ordering
    // [arrive_time ASC] / [time ASC]. Two rows share the same year bucket (2022 and 2023), so
    // GroupPartitionsExec coalesces two splits per year. We join solely on
    // p.time = i.arrive_time (the partition key expression) using SMJ.
    //
    // With config enabled:  SortedMergeCoalescedRDD k-way merge preserves [arrive_time ASC]
    //   ordering -> EnsureRequirements is satisfied -> no SortExec added.
    // With config disabled: simple CoalescedRDD only preserves the key-derived year ordering ->
    //   arrive_time ordering within a year is lost -> SortExec is added.
    val itemOrdering = Array(
      sort(FieldReference("arrive_time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST))
    createTable(items, itemsColumns, Array(years("arrive_time")), itemOrdering)
    // Inserted out of order: within year 2022, September is before March in insertion order
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(2, 'bb', 20.0, cast('2022-09-20' as timestamp)), " +
      "(4, 'dd', 40.0, cast('2023-11-05' as timestamp)), " +
      "(1, 'aa', 10.0, cast('2022-03-15' as timestamp)), " +
      "(3, 'cc', 30.0, cast('2023-01-10' as timestamp))")

    val purchaseOrdering = Array(
      sort(FieldReference("time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST))
    createTable(purchases, purchasesColumns, Array(years("time")), purchaseOrdering)
    // Also inserted out of order
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(2, 20.0, cast('2022-09-20' as timestamp)), " +
      "(4, 40.0, cast('2023-11-05' as timestamp)), " +
      "(1, 10.0, cast('2022-03-15' as timestamp)), " +
      "(3, 30.0, cast('2023-01-10' as timestamp))")

    Seq(true, false).foreach { preserveOrdering =>
      withSQLConf(
          SQLConf.V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key ->
            preserveOrdering.toString) {
        val df = sql(
          s"""
             |${selectWithMergeJoinHint("i", "p")}
             |i.id, i.name
             |FROM testcat.ns.$items i
             |JOIN testcat.ns.$purchases p ON p.time = i.arrive_time
             |""".stripMargin)
        checkAnswer(df, Seq(Row(1, "aa"), Row(2, "bb"), Row(3, "cc"), Row(4, "dd")))

        val plan = df.queryExecution.executedPlan
        assert(collectAllShuffles(plan).isEmpty, "should not contain any shuffle")

        val groupPartitions = collectAllGroupPartitions(plan)
        assert(groupPartitions.nonEmpty, "should contain GroupPartitionsExec for coalescing")
        assert(groupPartitions.exists(_.groupedPartitions.exists(_._2.size > 1)),
          "expected coalescing GroupPartitionsExec")

        val smjs = collect(plan) { case j: SortMergeJoinExec => j }
        assert(smjs.nonEmpty, "expected SortMergeJoinExec in plan")
        smjs.foreach { smj =>
          val sorts = smj.children.flatMap(child => collect(child) { case s: SortExec => s })
          if (preserveOrdering) {
            assert(sorts.isEmpty,
              "config enabled: SortedMergeCoalescedRDD preserves [arrive_time ASC], " +
                "no SortExec should be added before SMJ")

            val coalescingGP = groupPartitions.filter(_.groupedPartitions.exists(_._2.size > 1))
            coalescingGP.foreach { gp =>
              assert(gp.execute().isInstanceOf[SortedMergeCoalescedRDD[_]],
                "config enabled: should use SortedMergeCoalescedRDD")
            }
          } else {
            assert(sorts.nonEmpty,
              "config disabled: simple coalescing loses arrive_time ordering within a year, " +
                "SortExec should be added before SMJ")
          }
        }
      }
    }
  }

  test("SPARK-56549: k-way merge enabled only when parent requires ordering") {
    // Both tables are partitioned by id/item_id and report a two-column ordering.
    // Key 1 appears on two splits on each side, so GroupPartitionsExec must coalesce.
    //
    // Dynamic gate: with the config enabled, k-way merge must be activated only when the parent
    // actually requires ordering (SMJ), and must stay off when the parent does not (hash join).
    val itemOrdering = Array(
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("arrive_time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST))
    createTable(items, itemsColumns, Array(identity("id")), itemOrdering)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(2, 'cc', 30.0, cast('2023-06-15' as timestamp)), " +
      "(1, 'bb', 20.0, cast('2022-03-10' as timestamp)), " +
      "(3, 'dd', 40.0, cast('2024-01-01' as timestamp)), " +
      "(1, 'aa', 10.0, cast('2021-05-20' as timestamp)), " +
      "(2, 'ee', 50.0, cast('2025-09-01' as timestamp))")

    val purchaseOrdering = Array(
      sort(FieldReference("item_id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST))
    createTable(purchases, purchasesColumns, Array(identity("item_id")), purchaseOrdering)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(2, 50.0, cast('2025-09-01' as timestamp)), " +
      "(1, 10.0, cast('2021-05-20' as timestamp)), " +
      "(3, 40.0, cast('2024-01-01' as timestamp)), " +
      "(2, 30.0, cast('2023-06-15' as timestamp)), " +
      "(1, 20.0, cast('2022-03-10' as timestamp))")

    withSQLConf(
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
        SQLConf.V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key -> "true"
    ) {
      val hashDf = sql(
        s"""
           |SELECT /*+ SHUFFLE_HASH(i, p) */ i.id, i.name
           |FROM testcat.ns.$items i
           |JOIN testcat.ns.$purchases p ON p.item_id = i.id AND p.time = i.arrive_time
           |""".stripMargin)
      checkAnswer(hashDf, Seq(Row(1, "aa"), Row(1, "bb"), Row(2, "cc"), Row(2, "ee"), Row(3, "dd")))
      val hashPlan = hashDf.queryExecution.executedPlan
      assert(collect(hashPlan) { case j: ShuffledHashJoinExec => j }.nonEmpty,
        "expected ShuffledHashJoinExec")
      assert(collectAllShuffles(hashPlan).isEmpty, "should not shuffle for compatible partitioning")
      val hashCoalescing =
        collectAllGroupPartitions(hashPlan).filter(_.groupedPartitions.exists(_._2.size > 1))
      assert(hashCoalescing.nonEmpty, "expected coalescing GroupPartitionsExec")
      hashCoalescing.foreach { gp =>
        assert(!gp.enableSortedMerge,
          "hash join does not require ordering: enableSortedMerge must stay false")
        assert(!gp.execute().isInstanceOf[SortedMergeCoalescedRDD[_]],
          "hash join does not require ordering: must use simple CoalescedRDD")
      }

      val smjDf = sql(
        s"""
           |${selectWithMergeJoinHint("i", "p")}
           |i.id, i.name
           |FROM testcat.ns.$items i
           |JOIN testcat.ns.$purchases p ON p.item_id = i.id AND p.time = i.arrive_time
           |""".stripMargin)
      checkAnswer(smjDf, Seq(Row(1, "aa"), Row(1, "bb"), Row(2, "cc"), Row(2, "ee"), Row(3, "dd")))
      val smjPlan = smjDf.queryExecution.executedPlan
      assert(collectAllShuffles(smjPlan).isEmpty, "should not shuffle for compatible partitioning")
      val smjCoalescing =
        collectAllGroupPartitions(smjPlan).filter(_.groupedPartitions.exists(_._2.size > 1))
      assert(smjCoalescing.nonEmpty, "expected coalescing GroupPartitionsExec")
      smjCoalescing.foreach { gp =>
        assert(gp.enableSortedMerge,
          "sort-merge join requires ordering: enableSortedMerge must be true")
        assert(gp.execute().isInstanceOf[SortedMergeCoalescedRDD[_]],
          "sort-merge join requires ordering: must use SortedMergeCoalescedRDD")
      }
    }
  }

  test("SPARK-46367: partition key alias in subquery projects KeyedPartitioning") {
    // A subquery that renames a partition key (id -> pk) creates a ProjectExec between the scan and
    // the join. This test verifies that KeyedPartitioning expressions are correctly projected
    // through aliases so that SPJ still works without a shuffle. Both sides have the same partition
    // key sequence so no GroupPartitionsExec is needed.
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 19.5, cast('2020-02-01' as timestamp))")

    val df = sql(
      s"""
         |${selectWithMergeJoinHint("sub", "p")}
         |sub.pk, p.price AS purchase_price
         |FROM (SELECT id AS pk FROM testcat.ns.$items) sub
         |JOIN testcat.ns.$purchases p
         |ON sub.pk = p.item_id
         |ORDER BY pk, purchase_price
         |""".stripMargin)

    val shuffles = collectShuffles(df.queryExecution.executedPlan)
    assert(shuffles.isEmpty, "should not add shuffle when partition key is aliased in subquery")

    checkAnswer(df, Seq(Row(1, 42.0f), Row(2, 11.0f), Row(3, 19.5f)))
  }

  test("SPARK-46367: narrowing projection requires allowKeysSubsetOfPartitionKeys") {
    // items is partitioned by (id, name). The subquery projects away 'name', narrowing
    // KeyedPartitioning([id, name]) -> KeyedPartitioning([id]) with isCollapsed=true.
    // Because id=1 maps to two original partitions ("aa" and "bb"), isGrouped=false.
    // GroupPartitionsExec would merge them, carrying the same skew risk as subset partition
    // keys -- so SPJ requires allowKeysSubsetOfPartitionKeys to be enabled.
    val items_partitions = Array(identity("id"), identity("name"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      s"(1, 'bb', 41.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 'cc', 10.0, cast('2020-01-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 11.0, cast('2020-01-01' as timestamp))")

    Seq(true, false).foreach { allowSubset =>
      withSQLConf(
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key ->
          allowSubset.toString) {

        val df = sql(
          s"""
             |${selectWithMergeJoinHint("sub", "p")}
             |sub.id, p.price AS purchase_price
             |FROM (SELECT id FROM testcat.ns.$items WHERE name >= 'aa') sub
             |JOIN testcat.ns.$purchases p
             |ON sub.id = p.item_id
             |ORDER BY sub.id, purchase_price
             |""".stripMargin)

        val shuffles = collectShuffles(df.queryExecution.executedPlan)
        if (allowSubset) {
          assert(shuffles.isEmpty, "SPJ should be triggered with config enabled")
        } else {
          assert(shuffles.nonEmpty, "SPJ should not be triggered without config")
        }

        checkAnswer(df, Seq(Row(1, 42.0f), Row(1, 42.0f), Row(2, 11.0f)))
      }
    }
  }

  test("SPARK-46367: narrowing projection with distinct projected keys does not require " +
      "allowKeysSubsetOfPartitionKeys") {
    // items is partitioned by (id, name) but each id value is unique, so projecting away 'name'
    // produces KeyedPartitioning([id]) with isGrouped=true and isCollapsed=false. No two original
    // partitions share the same projected key, so the projection lost no distinct key and
    // GroupPartitionsExec would merge nothing. There is no skew risk, so SPJ works without config.
    val items_partitions = Array(identity("id"), identity("name"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 19.5, cast('2020-02-01' as timestamp))")

    withSQLConf(
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "false") {
      val df = sql(
        s"""
           |${selectWithMergeJoinHint("sub", "p")}
           |sub.id, p.price AS purchase_price
           |FROM (SELECT id FROM testcat.ns.$items WHERE name >= 'aa') sub
           |JOIN testcat.ns.$purchases p
           |ON sub.id = p.item_id
           |ORDER BY sub.id, purchase_price
           |""".stripMargin)

      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.isEmpty,
        "should not add shuffle: the projected KP stays grouped, so there is no skew risk")

      checkAnswer(df, Seq(Row(1, 42.0f), Row(2, 11.0f), Row(3, 19.5f)))
    }
  }

  test("SPARK-46367: aggregate with GROUP BY subset of partition keys uses GroupPartitionsExec " +
      "with allowKeysSubsetOfPartitionKeys") {
    // Table partitioned by (id, name): id=1 maps to two distinct partition keys (1,'aa') and
    // (1,'bb'). The partial HashAggregate (a PartitioningPreservingUnaryExecNode) projects away
    // 'name', collapsing KP([id,name]) to KP([id], isCollapsed=true, isGrouped=false).
    // By default a shuffle is required; with allowKeysSubsetOfPartitionKeys enabled,
    // EnsureRequirements inserts GroupPartitionsExec to coalesce both id=1 partitions so the final
    // aggregate sees all id=1 partial results in one task -- correct and shuffle-free.
    val items_partitions = Array(identity("id"), identity("name"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      s"(1, 'bb', 41.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 'cc', 10.0, cast('2020-01-01' as timestamp))")

    // Use MAX(name) so that 'name' stays in the scan output and is not column-pruned away.
    // Without it, V2ScanPartitioningAndOrdering drops KP([id,name]) when 'name' is absent
    // from the output, making the scan report UnknownPartitioning and always shuffling for
    // a different reason -- which would mask the narrowing behaviour we are testing here.
    val query =
      s"SELECT id, MAX(name) AS max_name, COUNT(*) AS cnt FROM testcat.ns.$items GROUP BY id"
    val expected = Seq(Row(1L, "bb", 2L), Row(2L, "cc", 1L))

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = sql(query)
      checkAnswer(df, expected)
      assert(collectAllShuffles(df.queryExecution.executedPlan).nonEmpty,
        "shuffle required: KP([id,name]) does not satisfy ClusteredDistribution([id])")

      withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
        val subsetDf = sql(query)
        checkAnswer(subsetDf, expected)
        assert(collectAllGroupPartitions(subsetDf.queryExecution.executedPlan).nonEmpty,
          "GroupPartitionsExec expected to coalesce partitions sharing the narrowed key [id]")
      }
    }
  }

  test("SPARK-46367: window with PARTITION BY subset of partition keys uses GroupPartitionsExec " +
      "with allowKeysSubsetOfPartitionKeys") {
    // Same narrowing mechanism as the aggregate test: the partial HashAggregate (a
    // PartitioningPreservingUnaryExecNode) for the inner GROUP BY id, price projects away 'name',
    // collapsing KP([id,name]) to KP([id], isCollapsed=true, isGrouped=false). With
    // allowKeysSubsetOfPartitionKeys enabled, EnsureRequirements inserts GroupPartitionsExec to
    // coalesce both id=1 partitions for the final aggregate. The window PARTITION BY id then sees
    // KP([id], isGrouped=true) from the aggregate output and needs no further exchange.
    //
    // MAX(name) in the subquery keeps 'name' in the scan output so that
    // V2ScanPartitioningAndOrdering does not drop the KP before PartitioningPreservingUnaryExecNode
    // narrowing can happen.
    val items_partitions = Array(identity("id"), identity("name"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      s"(1, 'aa', 10.0, cast('2020-01-01' as timestamp)), " +
      s"(1, 'bb', 20.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 'cc', 30.0, cast('2020-01-01' as timestamp))")

    val query =
      s"""SELECT id, SUM(price) OVER (PARTITION BY id ORDER BY price) AS running_sum, max_name
         |FROM (SELECT id, MAX(name) AS max_name, price FROM testcat.ns.$items GROUP BY id, price)
         |""".stripMargin
    val expected = Seq(Row(1L, 10.0, "aa"), Row(1L, 30.0, "bb"), Row(2L, 30.0, "cc"))

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = sql(query)
      checkAnswer(df, expected)
      assert(collectAllShuffles(df.queryExecution.executedPlan).nonEmpty,
        "shuffle required: KP([id,name]) does not satisfy ClusteredDistribution([id])")

      withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
        val subsetDf = sql(query)
        checkAnswer(subsetDf, expected)
        assert(collectAllGroupPartitions(subsetDf.queryExecution.executedPlan).nonEmpty,
          "GroupPartitionsExec expected to coalesce partitions sharing the narrowed key [id]")
      }
    }
  }

  test("SPARK-58988: v2 bucketed table with subset join keys joining v1 table") {
    // The v2 table is partitioned by an extra identity key `dt` plus `bucket(16, c1)`, while the
    // join is only on `c1`. allowKeysSubsetOfPartitionKeys lets the operation key `c1` be a subset
    // of the partition keys `[dt, bucket(16, c1)]`, so EnsureRequirements projects the keyed side
    // to `[bucket(16, c1)]`. v2BucketingShuffleEnabled then re-shuffles only the v1 side using that
    // projected KeyedPartitioning. ShuffledJoin wraps the two output partitionings into a
    // PartitioningCollection, which requires all KeyedPartitionings to share equal partitionKeys.
    // The v2 side's keys are sorted by GroupPartitionsExec, while the keys re-used for the v1 side
    // keep their first-occurrence order from createShuffleSpec, so the two sequences disagree and
    // the collection construction used to fail.
    val cols = Array(
      Column.create("c1", LongType),
      Column.create("c2", StringType),
      Column.create("dt", StringType))
    val partitions = Array(identity("dt"), bucket(16, "c1"))

    createTable("iceberg_t2", cols, partitions)
    sql("INSERT INTO testcat.ns.iceberg_t2 VALUES (2, 'cc', '2020'), (1, 'aa', '2021')")

    withTable("t1") {
      sql("CREATE TABLE t1 (c1 BIGINT, c2 STRING) USING parquet")
      sql("INSERT INTO t1 VALUES (1, 'aa'), (2, 'cc')")

      withSQLConf(
          SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql("SELECT * FROM testcat.ns.iceberg_t2 t0 JOIN t1 ON t0.c1 = t1.c1")
        val plan = df.queryExecution.executedPlan
        // Only the v1 side is re-shuffled; the v2 side is regrouped onto the join key instead.
        assert(collectShuffles(plan).length == 1)
        assert(collectGroupPartitions(plan).length == 1)
        checkAnswer(df, Seq(
          Row(1L, "aa", "2021", 1L, "aa"),
          Row(2L, "cc", "2020", 2L, "cc")))
      }
    }
  }

  test("SPARK-58968: a window over reduced partition keys coalesces partitions") {
    // The window keyed on `a.ts` is a subset of the partition keys, so it needs a node that
    // projects the keys to position 0 and merges the partitions that share the projected key.
    // Here the two rows do share a `ts` but sit on separate partitions of the join's (year,
    // bucket) grouping, so without the projection the result is wrong.
    //
    // The join reduces the identity side onto the year key space, which leaves keys the left
    // partitioning's expressions no longer describe, `IntegerType` years under `identity(ts)`.
    // Reading them at the expression's type threw at planning until SPARK-59120 made every reader
    // take its types from the keys.
    val cols = Array(
      Column.create("id", IntegerType),
      Column.create("ts", TimestampType),
      Column.create("v", IntegerType))
    withTable("t_identity", "t_years") {
      createTable("t_identity", cols, Array(identity("ts"), bucket(4, "id")))
      createTable("t_years", cols, Array(years("ts"), bucket(4, "id")))
      Seq("t_identity", "t_years").foreach { t =>
        sql(s"INSERT INTO testcat.ns.$t VALUES " +
          s"(1, cast('2020-01-01' as timestamp), 10), (2, cast('2020-01-01' as timestamp), 20)")
      }

      val query =
        """SELECT /*+ MERGE(a, b) */ a.id, b.v,
          |  SUM(b.v) OVER (PARTITION BY a.ts) AS s
          |FROM testcat.ns.t_identity a JOIN testcat.ns.t_years b
          |ON a.ts = b.ts AND a.id = b.id
          |""".stripMargin

      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true",
          SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
        val df = sql(query)
        val plan = df.queryExecution.executedPlan
        assert(collectAllShuffles(plan).isEmpty, "should not contain any shuffle")
        assert(plan.outputPartitioning.numPartitions == 1,
          "projecting to the year column merges the two partitions that share a ts")
        checkAnswer(df, Seq(Row(1, 10, 30), Row(2, 20, 30)))
      }
    }
  }

  test("SPARK-58968: no GroupPartitionsExec when a join collection member needs none") {
    // An inner join reports `PartitioningCollection(left, right)`, and unlike a projection it does
    // not enumerate the mixed combinations, so the two members are all there is. A window keyed on
    // a.k1 with b.k2 and b.k3 therefore sees one member covering position 0 only (a.k1 is the
    // cluster key, a.k2 and a.k3 are not) and one covering positions 1 and 2.
    //
    // Every partition holds a distinct k1, so rows sharing (k1, k2, k3) share a partition and the
    // left member satisfies the window's distribution as it is. Projecting the right member to
    // (k2, k3) would merge the two partitions holding (9, 9) instead, for nothing. The member that
    // needs no node has to win even though the other one covers more operation keys.
    val cols = Array(
      Column.create("k1", IntegerType),
      Column.create("k2", IntegerType),
      Column.create("k3", IntegerType),
      Column.create("v", IntegerType))
    val partitions = Array(identity("k1"), identity("k2"), identity("k3"))
    withTable("t1", "t2") {
      createTable("t1", cols, partitions)
      createTable("t2", cols, partitions)
      Seq("t1", "t2").foreach { t =>
        sql(s"INSERT INTO testcat.ns.$t VALUES (1, 9, 9, 10), (2, 9, 9, 20), " +
          s"(3, 8, 8, 30), (4, 7, 7, 40)")
      }

      // Selecting every key column keeps a pruning `ProjectExec` out of the plan. One would rebuild
      // the collection as the cross-product of the per-position alternatives, which does contain a
      // member covering all three positions, and the question would not arise.
      val query =
        """SELECT /*+ MERGE(a, b) */ a.k1, a.k2, a.k3, a.v, b.k1, b.k2, b.k3, b.v,
          |  SUM(b.v) OVER (PARTITION BY a.k1, b.k2, b.k3) AS s
          |FROM testcat.ns.t1 a JOIN testcat.ns.t2 b
          |ON a.k1 = b.k1 AND a.k2 = b.k2 AND a.k3 = b.k3
          |""".stripMargin

      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
        val df = sql(query)
        val plan = df.queryExecution.executedPlan
        assert(collectAllShuffles(plan).isEmpty, "should not contain any shuffle")
        assert(collectAllGroupPartitions(plan).isEmpty,
          "the join's left member satisfies the window's distribution as it is")
        assert(plan.outputPartitioning.numPartitions == 4,
          "coalescing on (k2, k3) would leave 3 partitions and merge nothing that had to merge")
        checkAnswer(df, Seq(
          Row(1, 9, 9, 10, 1, 9, 9, 10, 10),
          Row(2, 9, 9, 20, 2, 9, 9, 20, 20),
          Row(3, 8, 8, 30, 3, 8, 8, 30, 30),
          Row(4, 7, 7, 40, 4, 7, 7, 40, 40)))
      }
    }
  }

  test("SPARK-58968: window top-k over PARTITION BY subset of partition keys coalesces " +
      "partitions") {
    // items is partitioned by (id, name). A top-k window that ranks by PARTITION BY id (a subset of
    // the partition keys) must coalesce the (1,'aa') and (1,'bb') partitions before ranking so that
    // id=1 is ranked across both rows and yields a single row. Otherwise each partition is ranked
    // independently and id=1 surfaces twice.
    //
    // The second spec repeats the key, so the required clustering carries a duplicate. The
    // projection is decided per partition expression, so a duplicate in the clustering cannot
    // change it - that case does not fail on its own, it only pins that the duplicate is harmless.
    val items_partitions = Array(identity("id"), identity("name"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      s"(1, 'aa', 10.0, cast('2020-01-01' as timestamp)), " +
      s"(1, 'bb', 20.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 'cc', 30.0, cast('2020-01-01' as timestamp))")

    val expected = Seq(Row(1L, "bb", 20.0f), Row(2L, "cc", 30.0f))

    Seq("id", "id, id").foreach { partitionSpec =>
      val query =
        s"""SELECT id, name, price FROM (
           |  SELECT id, name, price,
           |    ROW_NUMBER() OVER (PARTITION BY $partitionSpec ORDER BY price DESC) rn
           |  FROM testcat.ns.$items
           |) t WHERE rn = 1
           |""".stripMargin

      // Result correctness does not depend on AQE: EnsureRequirements also runs in AQE's
      // queryStagePreparationRules and likewise skips the GroupPartitionsExec, ranking id=1
      // per-partition. Verify the wrong result under the default (AQE on) configuration.
      withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
        checkAnswer(sql(query), expected)
      }

      // The plan-shape assertion needs a static, fully-planned tree, so disable AQE here.
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
        val groupPartitions = collectAllGroupPartitions(sql(query).queryExecution.executedPlan)
        // The node has to project down to [id], not only coalesce: `name` is a partition key the
        // window does not group by, so coalescing on (id, name) would merge nothing.
        assert(groupPartitions.map(_.joinKeyPositions) == Seq(Some(Seq(0))),
          s"PARTITION BY $partitionSpec: GroupPartitionsExec expected to project to the subset " +
            "key [id] and coalesce the partitions sharing it")
      }
    }
  }

  test("SPARK-58968: window top-k over union output partitioning coalesces partitions") {
    // t1 and t2 are both partitioned by (id, name). With union output partitioning enabled, the
    // union reports a KeyedPartitioning over (id, name), so a top-k window over PARTITION BY id (a
    // strict subset) must still coalesce the (1,'aa') and (1,'bb') partitions coming from t1.
    val partitions = Array(identity("id"), identity("name"))
    withTable("t1", "t2") {
      createTable("t1", itemsColumns, partitions)
      sql("INSERT INTO testcat.ns.t1 VALUES " +
        "(1, 'aa', 10.0, cast('2020-01-01' as timestamp)), " +
        "(1, 'bb', 20.0, cast('2020-01-01' as timestamp))")
      createTable("t2", itemsColumns, partitions)
      sql("INSERT INTO testcat.ns.t2 VALUES (2, 'cc', 30.0, cast('2020-01-01' as timestamp))")

      val query =
        """SELECT id, name, price FROM (
          |  SELECT id, name, price,
          |    ROW_NUMBER() OVER (PARTITION BY id ORDER BY price DESC) rn
          |  FROM (
          |    SELECT id, name, price FROM testcat.ns.t1
          |    UNION ALL
          |    SELECT id, name, price FROM testcat.ns.t2
          |  )
          |) t WHERE rn = 1
          |""".stripMargin
      val expected = Seq(Row(1L, "bb", 20.0f), Row(2L, "cc", 30.0f))

      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
          SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true") {
        val df = sql(query)
        checkAnswer(df, expected)
        assert(collectAllGroupPartitions(df.queryExecution.executedPlan).nonEmpty,
          "GroupPartitionsExec expected to coalesce union partitions sharing the key [id]")
      }
    }
  }

  test("SPARK-59022: keyed shuffle follows the declared partition key order") {
    val cols = Array(
      Column.create("id", LongType),
      Column.create("dt", StringType))
    val partitions = Array[Transform](identity("dt"), identity("id"))

    createTable("nt", cols, partitions)
    // The scan reports its keys sorted on the full key: [('2020', 2), ('2021', 1)]. Narrowing them
    // to `[id]` gives [2, 1], which is not sorted -- projecting a sorted sequence onto a subset of
    // key positions does not preserve sortedness.
    sql("INSERT INTO testcat.ns.nt VALUES (2, '2020'), (1, '2021')")

    withTable("t1") {
      sql("CREATE TABLE t1 (id BIGINT, data STRING) USING parquet")
      sql("INSERT INTO t1 VALUES (1, 'a'), (2, 'b')")

      withSQLConf(
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "false",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        // `length(dt) > 2` is not pushed down, so `dt` reaches the scan while the projection above
        // it drops the column, narrowing the KeyedPartitioning to `[identity(id)]`.
        val df = sql(
          """
            |SELECT nt.id, t1.data
            |FROM (SELECT id FROM testcat.ns.nt WHERE length(dt) > 2) nt
            |JOIN t1 ON nt.id = t1.id
            |""".stripMargin)
        val plan = df.queryExecution.executedPlan
        // Only the v1 side is shuffled, onto the narrowed KeyedPartitioning of the v2 side, and
        // nothing re-groups the v2 side -- so the shuffle is the only thing that can align them.
        assert(collectShuffles(plan).length == 1)
        assert(collectGroupPartitions(plan).isEmpty)
        checkAnswer(df, Seq(Row(1L, "a"), Row(2L, "b")))
      }
    }
  }

  test("SPARK-59022: keyed shuffle follows the declared partition key order over a union") {
    val cols = Array(
      Column.create("id", LongType),
      Column.create("data", StringType))
    val partitions = Array[Transform](identity("id"))

    createTable("nt1", cols, partitions)
    createTable("nt2", cols, partitions)
    // `UnionExec` concatenates its children's partition keys in child order, so the merged keys are
    // [3, 4] ++ [1, 2]. They are unique, hence grouped, and no projection is involved, hence not
    // narrowed -- but they are not sorted.
    sql("INSERT INTO testcat.ns.nt1 VALUES (3, 'c'), (4, 'd')")
    sql("INSERT INTO testcat.ns.nt2 VALUES (1, 'a'), (2, 'b')")

    withTable("t1") {
      sql("CREATE TABLE t1 (id BIGINT, x STRING) USING parquet")
      sql("INSERT INTO t1 VALUES (1, 'x'), (2, 'x'), (3, 'x'), (4, 'x')")

      withSQLConf(
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql(
          """
            |SELECT u.id, u.data, t1.x
            |FROM (SELECT * FROM testcat.ns.nt1 UNION ALL SELECT * FROM testcat.ns.nt2) u
            |JOIN t1 ON u.id = t1.id
            |""".stripMargin)
        val plan = df.queryExecution.executedPlan
        // Only the v1 side is shuffled, onto the union's KeyedPartitioning, and nothing re-groups
        // the union -- so the shuffle is the only thing that can align them.
        assert(collectShuffles(plan).length == 1)
        assert(collectGroupPartitions(plan).isEmpty)
        checkAnswer(df, Seq(
          Row(1L, "a", "x"), Row(2L, "b", "x"), Row(3L, "c", "x"), Row(4L, "d", "x")))
      }

      // The plan-shape assertions above need AQE off, but the wrong results were live in the
      // default configuration, so pin the answer with AQE on as well.
      withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true") {
        checkAnswer(
          sql(
            """
              |SELECT u.id, u.data, t1.x
              |FROM (SELECT * FROM testcat.ns.nt1 UNION ALL SELECT * FROM testcat.ns.nt2) u
              |JOIN t1 ON u.id = t1.id
              |""".stripMargin),
          Seq(Row(1L, "a", "x"), Row(2L, "b", "x"), Row(3L, "c", "x"), Row(4L, "d", "x")))
      }
    }
  }

  test("SPARK-59027: v2 bucketed table with subset join keys left-outer joining v1 table") {
    // Same shape as the SPARK-58988 test above, but LEFT OUTER: `ShuffledJoin` then exposes only
    // the left side's partitioning, so no `PartitioningCollection` invariant compares the two
    // sides' declared keys at planning time. If the order declared by `createShuffleSpec` and the
    // physical layouts of the two sides (`GroupPartitionsExec` on the keyed side, the shuffle
    // partitioner on the other) ever diverged, this join would silently lose matches instead of
    // failing planning, so the answer check is the guard here. The unmatched row distinguishes a
    // legitimate outer-join null from a lost match.
    val cols = Array(
      Column.create("c1", LongType),
      Column.create("c2", StringType),
      Column.create("dt", StringType))
    val partitions = Array(identity("dt"), bucket(16, "c1"))

    createTable("iceberg_t3", cols, partitions)
    sql("INSERT INTO testcat.ns.iceberg_t3 VALUES " +
      "(2, 'cc', '2020'), (1, 'aa', '2021'), (3, 'ee', '2022')")

    withTable("t1") {
      sql("CREATE TABLE t1 (c1 BIGINT, c2 STRING) USING parquet")
      sql("INSERT INTO t1 VALUES (1, 'aa'), (2, 'cc')")

      withSQLConf(
          SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql("SELECT * FROM testcat.ns.iceberg_t3 t0 LEFT JOIN t1 ON t0.c1 = t1.c1")
        val plan = df.queryExecution.executedPlan
        // Only the v1 side is re-shuffled; the v2 side is regrouped onto the join key instead.
        assert(collectShuffles(plan).length == 1)
        assert(collectGroupPartitions(plan).length == 1)
        checkAnswer(df, Seq(
          Row(1L, "aa", "2021", 1L, "aa"),
          Row(2L, "cc", "2020", 2L, "cc"),
          Row(3L, "ee", "2022", null, null)))
      }
    }
  }

  test("SPARK-58968: non-grouped KeyedPartitioning with PARTITION BY subset of partition keys " +
      "coalesces partitions") {
    // `numRowsPerSplit = 1`, so the two rows sharing (1, 'aa') produce two splits for that key and
    // the scan reports a non-grouped KeyedPartitioning([id, name]). A plain window (no
    // WindowGroupLimit above it) is the only operator requiring ClusteredDistribution([id]) here,
    // so the GroupPartitionsExec inserted for it must both coalesce the duplicate (1, 'aa') splits
    // and project down to [id]. Coalescing alone leaves id=1 on two partitions.
    val items_partitions = Array(identity("id"), identity("name"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      s"(1, 'aa', 10.0, cast('2020-01-01' as timestamp)), " +
      s"(1, 'aa', 15.0, cast('2020-01-01' as timestamp)), " +
      s"(1, 'bb', 20.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 'cc', 30.0, cast('2020-01-01' as timestamp))")

    val query =
      s"""SELECT id, name, price, SUM(price) OVER (PARTITION BY id) AS s
         |FROM testcat.ns.$items
         |""".stripMargin
    val expected = Seq(
      Row(1L, "aa", 10.0f, 45.0), Row(1L, "aa", 15.0f, 45.0), Row(1L, "bb", 20.0f, 45.0),
      Row(2L, "cc", 30.0f, 30.0))

    withSQLConf(SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      checkAnswer(sql(query), expected)
    }

    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      val groupPartitions =
        collectAllGroupPartitions(sql(query).queryExecution.executedPlan)
      assert(groupPartitions.map(_.joinKeyPositions) == Seq(Some(Seq(0))),
        "the GroupPartitionsExec must project to the operation key [id], not only coalesce the " +
          "duplicate (id, name) splits")
    }
  }

  test("SPARK-58968: no GroupPartitionsExec when projecting to the operation keys coalesces " +
      "nothing") {
    // Every id has exactly one name, so projecting KeyedPartitioning([id, name]) down to [id]
    // leaves the same number of partitions. Every id already lives on a single partition, so the
    // partitioning satisfies ClusteredDistribution([id]) as it is. Inserting a GroupPartitionsExec
    // would only add a CoalescedRDD layer and narrow the reported partitioning to [id].
    val items_partitions = Array(identity("id"), identity("name"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      s"(1, 'aa', 10.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 'bb', 20.0, cast('2020-01-01' as timestamp)), " +
      s"(3, 'cc', 30.0, cast('2020-01-01' as timestamp))")

    val query =
      s"""SELECT id, name, price, SUM(price) OVER (PARTITION BY id) AS s
         |FROM testcat.ns.$items
         |""".stripMargin
    val expected = Seq(
      Row(1L, "aa", 10.0f, 10.0), Row(2L, "bb", 20.0f, 20.0), Row(3L, "cc", 30.0f, 30.0))

    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      val df = sql(query)
      checkAnswer(df, expected)
      val plan = df.queryExecution.executedPlan
      assert(collectAllGroupPartitions(plan).isEmpty,
        "projecting to [id] coalesces nothing, so no GroupPartitionsExec is needed")
      assert(collectAllShuffles(plan).isEmpty, "no shuffle either")
    }
  }

  test("SPARK-57881: storage-partitioned join leverages union output KeyedPartitioning to " +
      "avoid shuffle") {
    val cols = Array(
      Column.create("id", LongType),
      Column.create("data", StringType))
    val partitions = Array(identity("id"))
    withTable("t1", "t2", "t3") {
      createTable("t1", cols, partitions)
      sql("INSERT INTO testcat.ns.t1 VALUES (1, 'a1'), (2, 'a2')")
      createTable("t2", cols, partitions)
      sql("INSERT INTO testcat.ns.t2 VALUES (2, 'b2'), (3, 'b3')")
      createTable("t3", cols, partitions)
      sql("INSERT INTO testcat.ns.t3 VALUES (1, 'c1'), (2, 'c2'), (3, 'c3')")

      // Disable AQE for a deterministic, fully-planned tree to inspect.
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true") {
        val df = sql(
          """SELECT /*+ MERGE(u, t3) */ u.id, u.data, t3.data AS t3data
            |FROM (
            |  SELECT id, data FROM testcat.ns.t1
            |  UNION ALL
            |  SELECT id, data FROM testcat.ns.t2
            |) u
            |JOIN testcat.ns.t3 ON u.id = t3.id
            |""".stripMargin)
        val plan = df.queryExecution.executedPlan
        // The union reports a KeyedPartitioning over `id`, which the SMJ leverages for a
        // storage-partitioned join, so no shuffle is needed.
        assert(collectShuffles(plan).isEmpty)
        checkAnswer(df,
          Seq(Row(1, "a1", "c1"), Row(2, "a2", "c2"), Row(2, "b2", "c2"), Row(3, "b3", "c3")))
      }
    }
  }

  test("SPARK-57881: storage-partitioned join over union: compatible expressions, " +
      "disjoint child partition keys") {
    // Both children are partitioned by identity(id) (compatible expressions) but hold
    // disjoint key sets: t1=[1,2], t2=[3,4]. The union merges the keys into [1,2,3,4];
    // because no key repeats across children, the merged KeyedPartitioning is already
    // grouped, so no GroupPartitionsExec is needed on the union side. t3 carries the same
    // keys in the same order, so SPJ matches the two legs directly without a shuffle.
    val cols = Array(Column.create("id", LongType), Column.create("data", StringType))
    val partitions = Array(identity("id"))
    withTable("t1", "t2", "t3") {
      createTable("t1", cols, partitions)
      sql("INSERT INTO testcat.ns.t1 VALUES (1, 'a1'), (2, 'a2')")
      createTable("t2", cols, partitions)
      sql("INSERT INTO testcat.ns.t2 VALUES (3, 'b3'), (4, 'b4')")
      createTable("t3", cols, partitions)
      sql("INSERT INTO testcat.ns.t3 VALUES (1, 'c1'), (2, 'c2'), (3, 'c3'), (4, 'c4')")

      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true") {
        val df = sql(
          """SELECT /*+ MERGE(u, t3) */ u.id, u.data, t3.data AS t3data
            |FROM (
            |  SELECT id, data FROM testcat.ns.t1
            |  UNION ALL
            |  SELECT id, data FROM testcat.ns.t2
            |) u
            |JOIN testcat.ns.t3 ON u.id = t3.id
            |""".stripMargin)
        val plan = df.queryExecution.executedPlan

        // The merged descriptor carries the concatenation of the children's keys; disjoint
        // keys leave it grouped, which the SMJ consumes directly.
        val union = collect(plan) { case u: UnionExec => u }.head
        val kp = union.outputPartitioning.asInstanceOf[physical.KeyedPartitioning]
        assert(kp.numPartitions == 4, "one key per physical partition of the concatenation")
        assert(kp.isGrouped, "disjoint child keys merge without duplicates")

        assert(collectShuffles(plan).isEmpty, "no shuffle: merged grouped keys match t3")
        assert(collectGroupPartitions(plan).isEmpty,
          "no GroupPartitionsExec: merged keys are already grouped and aligned")
        checkAnswer(df, Seq(
          Row(1, "a1", "c1"), Row(2, "a2", "c2"), Row(3, "b3", "c3"), Row(4, "b4", "c4")))
      }
    }
  }

  test("SPARK-57881: storage-partitioned join over union: compatible expressions, " +
      "union keys are a strict subset of the other leg") {
    // Expressions are compatible (both identity(id)), but the join legs carry different
    // partition key sets: the union (t1=[1,2] UNION t2=[2,3]) groups to [1,2,3] while t3
    // holds [1,2,3,4,5]. The merged KeyedPartitioning has a duplicate key (2 from both
    // children), so isGrouped=false and EnsureRequirements inserts a GroupPartitionsExec.
    // With pushPartValues enabled, SPJ computes the superset [1,2,3,4,5] and pads the union
    // side with empty partitions for the missing keys 4 and 5, avoiding a shuffle. With
    // pushPartValues disabled the key mismatch cannot be reconciled, so both legs shuffle.
    val cols = Array(Column.create("id", LongType), Column.create("data", StringType))
    val partitions = Array(identity("id"))
    withTable("t1", "t2", "t3") {
      createTable("t1", cols, partitions)
      sql("INSERT INTO testcat.ns.t1 VALUES (1, 'a1'), (2, 'a2')")
      createTable("t2", cols, partitions)
      sql("INSERT INTO testcat.ns.t2 VALUES (2, 'b2'), (3, 'b3')")
      createTable("t3", cols, partitions)
      sql("INSERT INTO testcat.ns.t3 VALUES (1, 'c1'), (2, 'c2'), (3, 'c3'), (4, 'c4'), (5, 'c5')")

      Seq(true, false).foreach { pushPartValues =>
        withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
            SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true",
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushPartValues.toString) {
          val df = sql(
            """SELECT /*+ MERGE(u, t3) */ u.id, u.data, t3.data AS t3data
              |FROM (
              |  SELECT id, data FROM testcat.ns.t1
              |  UNION ALL
              |  SELECT id, data FROM testcat.ns.t2
              |) u
              |JOIN testcat.ns.t3 ON u.id = t3.id
              |""".stripMargin)
          val plan = df.queryExecution.executedPlan

          // The merged descriptor is ungrouped regardless of the pushPartValues flag, since
          // key 2 is duplicated across the two children.
          val union = collect(plan) { case u: UnionExec => u }.head
          val kp = union.outputPartitioning.asInstanceOf[physical.KeyedPartitioning]
          assert(!kp.isGrouped, "overlapping child keys merge with duplicates")

          val shuffles = collectShuffles(plan)
          val groupPartitions = collectGroupPartitions(plan)
          if (pushPartValues) {
            assert(shuffles.isEmpty, "no shuffle: superset of keys pushed to both legs")
            assert(groupPartitions.nonEmpty &&
              groupPartitions.forall(_.outputPartitioning.numPartitions === 5),
              "both legs aligned to the 5-key superset")
          } else {
            assert(shuffles.length == 2,
              "both legs shuffled when keys mismatch and pushPartValues is off")
            assert(groupPartitions.isEmpty,
              "GroupPartitionsExec is dropped once a shuffle is inserted")
          }
          // Inner join: keys 4 and 5 have no match on the union side.
          checkAnswer(df, Seq(
            Row(1, "a1", "c1"), Row(2, "a2", "c2"), Row(2, "b2", "c2"), Row(3, "b3", "c3")))
        }
      }
    }
  }

  test("SPARK-57881: storage-partitioned join over union: compatible expressions, " +
      "the other leg is a strict subset of the union keys") {
    // Expressions compatible (identity(id)); partition keys mismatch in the other direction:
    // the union (t1=[1,2] UNION t2=[2,3,4]) groups to [1,2,3,4] while t3 only holds [2,3].
    // SPJ pushes the superset [1,2,3,4] to t3, padding keys 1 and 4 with empty partitions.
    // No shuffle.
    val cols = Array(Column.create("id", LongType), Column.create("data", StringType))
    val partitions = Array(identity("id"))
    withTable("t1", "t2", "t3") {
      createTable("t1", cols, partitions)
      sql("INSERT INTO testcat.ns.t1 VALUES (1, 'a1'), (2, 'a2')")
      createTable("t2", cols, partitions)
      sql("INSERT INTO testcat.ns.t2 VALUES (2, 'b2'), (3, 'b3'), (4, 'b4')")
      createTable("t3", cols, partitions)
      sql("INSERT INTO testcat.ns.t3 VALUES (2, 'c2'), (3, 'c3')")

      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true") {
        val df = sql(
          """SELECT /*+ MERGE(u, t3) */ u.id, u.data, t3.data AS t3data
            |FROM (
            |  SELECT id, data FROM testcat.ns.t1
            |  UNION ALL
            |  SELECT id, data FROM testcat.ns.t2
            |) u
            |JOIN testcat.ns.t3 ON u.id = t3.id
            |""".stripMargin)
        val plan = df.queryExecution.executedPlan

        assert(collectShuffles(plan).isEmpty, "no shuffle: superset pushed to the t3 leg")
        assert(collectGroupPartitions(plan).nonEmpty &&
          collectGroupPartitions(plan).forall(_.outputPartitioning.numPartitions === 4),
          "both legs aligned to the 4-key superset")
        // Inner join: only ids 2 and 3 match.
        checkAnswer(df, Seq(
          Row(2, "a2", "c2"), Row(2, "b2", "c2"), Row(3, "b3", "c3")))
      }
    }
  }

  test("SPARK-57881: storage-partitioned join over union: bucket transform partitioning") {
    val cols = Array(Column.create("id", LongType), Column.create("data", StringType))
    val partitions = Array(bucket(4, "id"))
    withTable("t1", "t2", "t3") {
      createTable("t1", cols, partitions)
      sql("INSERT INTO testcat.ns.t1 VALUES (1, 'a1'), (2, 'a2')")
      createTable("t2", cols, partitions)
      sql("INSERT INTO testcat.ns.t2 VALUES (2, 'b2'), (3, 'b3')")
      createTable("t3", cols, partitions)
      sql("INSERT INTO testcat.ns.t3 VALUES (1, 'c1'), (2, 'c2'), (3, 'c3')")

      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true") {
        val df = sql(
          """SELECT /*+ MERGE(u, t3) */ u.id, u.data, t3.data AS t3data
            |FROM (
            |  SELECT id, data FROM testcat.ns.t1
            |  UNION ALL
            |  SELECT id, data FROM testcat.ns.t2
            |) u
            |JOIN testcat.ns.t3 ON u.id = t3.id
            |""".stripMargin)
        val plan = df.queryExecution.executedPlan

        // The union reports a KeyedPartitioning whose expression is the `bucket(4, id)` transform.
        val union = collect(plan) { case u: UnionExec => u }.head
        val kp = union.outputPartitioning.asInstanceOf[physical.KeyedPartitioning]
        assert(kp.expressions.length == 1 && kp.expressions.head.isInstanceOf[TransformExpression],
          "merged KeyedPartitioning carries the bucket transform expression")

        assert(collectShuffles(plan).isEmpty, "no shuffle: SPJ over the bucket transform")
        checkAnswer(df,
          Seq(Row(1, "a1", "c1"), Row(2, "a2", "c2"), Row(2, "b2", "c2"), Row(3, "b3", "c3")))
      }
    }
  }

  test("SPARK-57881: storage-partitioned join over union: a union leg is entirely " +
      "runtime-pruned") {
    // The merged descriptor is built from each leg's unfiltered `inputPartitions`, while the union
    // RDD concatenates each leg's `filteredPartitions` (pruned splits kept as `None`). Here dynamic
    // partition filtering prunes the entire t1 leg (only t3 ids [3, 4] survive), so this guards
    // that the per-leg partition-count == partitionKeys.length alignment holds under pruning.
    val cols = Array(Column.create("id", LongType), Column.create("data", StringType))
    val partitions = Array(identity("id"))
    withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "10") {
      withTable("t1", "t2", "t3") {
        createTable("t1", cols, partitions)
        sql("INSERT INTO testcat.ns.t1 VALUES (1, 'a1'), (2, 'a2')")
        createTable("t2", cols, partitions)
        sql("INSERT INTO testcat.ns.t2 VALUES (3, 'b3'), (4, 'b4')")
        createTable("t3", cols, partitions)
        sql("INSERT INTO testcat.ns.t3 VALUES (1, 'c1'), (2, 'c2'), (3, 'c3'), (4, 'c4')")

        val df = sql(
          """SELECT /*+ MERGE(u, t3) */ u.id, u.data, t3.data AS t3data
            |FROM (
            |  SELECT id, data FROM testcat.ns.t1
            |  UNION ALL
            |  SELECT id, data FROM testcat.ns.t2
            |) u
            |JOIN testcat.ns.t3 ON u.id = t3.id
            |WHERE t3.data IN ('c3', 'c4')
            |""".stripMargin)
        val plan = df.queryExecution.executedPlan

        // The merged descriptor carries the concatenation of both legs' unfiltered keys
        // ([1, 2] ++ [3, 4]), independent of runtime pruning.
        val union = collect(plan) { case u: UnionExec => u }.head
        val kp = union.outputPartitioning.asInstanceOf[physical.KeyedPartitioning]
        assert(kp.numPartitions == 4,
          "merged descriptor keeps one key per unfiltered physical partition of both legs")

        assert(collectShuffles(plan).isEmpty, "no shuffle: merged grouped keys match t3")

        // Force execution, then verify the t1 leg is entirely runtime-pruned (all `None`) while
        // its keys still live in the merged descriptor above.
        checkAnswer(df, Seq(Row(3, "b3", "c3"), Row(4, "b4", "c4")))
        val unionScans = collectScans(union)
        assert(unionScans.exists(_.filteredPartitions.forall(_.isEmpty)),
          "one union leg must be entirely pruned to None while its keys remain in the descriptor")
      }
    }
  }

  test("SPARK-58558: SPJ on a join key column partitioned by multiple transforms") {
    // Partition expressions outnumber the join keys because `id` is partitioned twice, but
    // every join key is covered, so SPJ works with default configs.
    val items_partitions = Array(bucket(8, "id"), identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"), identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(2, 19.5, cast('2020-02-01' as timestamp))")

    val df = createJoinTestDF(Seq("id" -> "item_id"))
    val shuffles = collectShuffles(df.queryExecution.executedPlan)
    assert(shuffles.isEmpty, "should not contain any shuffle")
    checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0), Row(2, "bb", 10.0, 19.5)))
  }

  test("SPARK-58974: the collapse skew guard applies regardless of requireAllClusterKeys") {
    // Same shape as "SPARK-46367: narrowing projection requires allowKeysSubsetOfPartitionKeys":
    // items is partitioned by (id, name) and id=1 maps to two of its partitions, so projecting
    // `name` away narrows [id, name] to [id] and collapses the keys to [1, 1, 2].
    //
    // The collapse skew guard describes a risk that does not depend on which key sets count as
    // matching, so it must apply for either value of `requireAllClusterKeys`. It used to sit inside
    // the `requireAllClusterKeys = false` arm of the key matching, so with that setting enabled
    // such a partitioning was grouped anyway -- taking exactly the exposure
    // `allowKeysSubsetOfPartitionKeys` exists to gate, with nobody opting in.
    createTable(items, itemsColumns, Array(identity("id"), identity("name")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      s"(1, 'bb', 41.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 'cc', 10.0, cast('2020-01-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array(identity("item_id")))
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 11.0, cast('2020-01-01' as timestamp))")

    val query =
      s"""
         |${selectWithMergeJoinHint("sub", "p")}
         |sub.id, p.price AS purchase_price
         |FROM (SELECT id FROM testcat.ns.$items WHERE name >= 'aa') sub
         |JOIN testcat.ns.$purchases p
         |ON sub.id = p.item_id
         |""".stripMargin
    val expected = Seq(Row(1, 42.0), Row(1, 42.0), Row(2, 11.0))

    for {
      requireAll <- Seq(true, false)
      allowSubset <- Seq(true, false)
      keyedShuffle <- Seq(true, false)
    } {
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_DISTRIBUTION.key -> requireAll.toString,
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> keyedShuffle.toString,
          SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key ->
            allowSubset.toString) {
        val settings = s"requireAllClusterKeys=$requireAll, v2BucketingShuffle=$keyedShuffle"
        val df = sql(query)
        val plan = df.queryExecution.executedPlan

        val collapsed = keyedPartitioningsOf(collect(plan) { case p: ProjectExec => p })
          .filter(_.isCollapsed)
        assert(collapsed.exists(!_.isGrouped),
          "this test needs a collapsed, ungrouped partitioning to be meaningful")

        // Count over the whole plan, so a shuffle or a grouping anywhere in it is caught, not
        // only the ones inside the join subtree.
        val groupPartitions = collectAllGroupPartitions(plan)
        val shuffles = collectAllShuffles(plan)
        if (allowSubset) {
          // The opt-in is the only way to keep the coalescing, and it must work for either value
          // of `requireAllClusterKeys` -- before the fix it was reachable only for `false`.
          assert(groupPartitions.nonEmpty, s"$settings: the opt-in must restore the coalescing")
          assert(shuffles.isEmpty, s"$settings: the opt-in must avoid the shuffles")
        } else {
          // `requireAllClusterKeys` says which key sets count as matching; it does not authorise
          // coalescing a collapsed partitioning, so the guard behaves the same either way.
          assert(groupPartitions.isEmpty,
            s"$settings must not coalesce a collapsed partitioning without " +
              "allowKeysSubsetOfPartitionKeys")
          // Both sides are shuffled, unless `v2BucketingShuffleEnabled` lets the refused side be
          // laid out on the other side's declared partition keys.
          val expectedShuffles = if (keyedShuffle) 1 else 2
          assert(shuffles.size == expectedShuffles,
            s"$settings must shuffle instead, on $expectedShuffles side(s)")
        }
        checkAnswer(df, expected)
      }
    }
  }

  private def createTsTable(name: String, partitions: Array[Transform]): Unit = {
    createTable(name, columns, partitions)
    sql(s"INSERT INTO testcat.ns.$name VALUES " +
      s"(1, 'aa', cast('2020-01-01' as timestamp)), " +
      s"(2, 'bb', cast('2021-06-01' as timestamp))")
  }

  test("SPARK-59120: reduced partition keys are read at the types they were built with") {
    // The join reduces the identity side onto the year key space, so its keys become `IntegerType`
    // years while the partitioning still reports `identity(ts)`, declaring `TimestampType`. With
    // the subset opt-in on, AQE re-runs `createShuffleSpec` on the already reduced children through
    // `ValidateRequirements`, which projects and sorts those keys. The mechanism is in
    // `ShuffleSpecSuite`'s "createShuffleSpec sorts the projected keys at their built-with types".
    withTable("t_identity", "t_years") {
      createTsTable("t_identity", Array(identity("ts")))
      createTsTable("t_years", Array(years("ts")))

      val query =
        s"""${selectWithMergeJoinHint("a", "b")} a.id, b.data
           |FROM testcat.ns.t_identity a JOIN testcat.ns.t_years b ON a.ts = b.ts
           |""".stripMargin

      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true",
          SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
        val df = sql(query)
        checkAnswer(df, Seq(Row(1, "aa"), Row(2, "bb")))
        assert(collectShuffles(df.queryExecution.executedPlan).isEmpty,
          "the two sides are reduced onto one key space, so they stay co-partitioned")
      }
    }
  }

  test("SPARK-59120: a second join with a non-reducing side plans on the reduced keys") {
    // The first join reduces the identity side onto the year key space and reports the reduced
    // expression `years(a.ts)`, so the second join's identity side reduces onto it as well and
    // the whole query plans without a shuffle.
    withTable("t_identity", "t_years", "t_identity2") {
      createTsTable("t_identity", Array(identity("ts")))
      createTsTable("t_years", Array(years("ts")))
      createTsTable("t_identity2", Array(identity("ts")))

      val query =
        """SELECT /*+ MERGE(a, b), MERGE(a, c) */ a.id, c.data
          |FROM testcat.ns.t_identity a JOIN testcat.ns.t_years b ON a.ts = b.ts
          |JOIN testcat.ns.t_identity2 c ON a.ts = c.ts
          |""".stripMargin

      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
        val df = sql(query)
        checkAnswer(df, Seq(Row(1, "aa"), Row(2, "bb")))
        assert(collectAllShuffles(df.queryExecution.executedPlan).isEmpty,
          "the second join reduces onto the type-correct reported expression")
      }
    }
  }

  test("SPARK-59120: another child is shuffled onto the type-correct reduced keys") {
    // The reduced side's expression is reported as `years(a.ts)`, which describes the reduced
    // keys, so `canCreatePartitioning`'s shape gate accepts the reduced layout and only the
    // unpartitioned side is shuffled onto it.
    withTable("t_identity", "t_years", "t_plain") {
      createTsTable("t_identity", Array(identity("ts")))
      createTsTable("t_years", Array(years("ts")))
      createTsTable("t_plain", Array.empty[Transform])

      val query =
        """SELECT /*+ MERGE(a, b), MERGE(c) */ a.id, c.data
          |FROM testcat.ns.t_identity a JOIN testcat.ns.t_years b ON a.ts = b.ts
          |JOIN testcat.ns.t_plain c ON a.ts = c.ts
          |""".stripMargin

      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
        val df = sql(query)
        checkAnswer(df, Seq(Row(1, "aa"), Row(2, "bb")))
        assert(collectAllShuffles(df.queryExecution.executedPlan).size == 1,
          "the second join shuffles only the unpartitioned side, onto the years(ts) layout")
      }
    }
  }
  test("SPARK-59057: several splits per partition key are grouped without " +
      "allowKeysSubsetOfPartitionKeys") {
    // The scan reports one partition key per split, so a table with two splits for the same
    // (id, dept) value already has duplicate keys before any projection. Dropping dept maps
    // (1, 'x'), (1, 'x') onto 1 and (2, 'y') onto 2: two distinct keys before, two after, so the
    // projection collapsed nothing. Grouping merges only the two splits that already shared a key,
    // which is what happens for any partitioning that never went through a projection, so it must
    // not need the opt-in.
    val cols = Array(
      Column.create("id", LongType),
      Column.create("dept", StringType),
      Column.create("data", StringType))
    val t2cols = Array(Column.create("id", LongType), Column.create("data", StringType))
    withTable("t1", "t2") {
      createTable("t1", cols, Array(identity("id"), identity("dept")))
      sql("INSERT INTO testcat.ns.t1 VALUES (1, 'x', 'a1'), (1, 'x', 'a2'), (2, 'y', 'a3')")
      createTable("t2", t2cols, Array(identity("id")))
      sql("INSERT INTO testcat.ns.t2 VALUES (1, 'b1'), (2, 'b2')")

      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "false") {
        // `dept RLIKE ...` has no V2 translation, so the scan must output dept and the Project
        // above the Filter is what drops it.
        val df = sql(
          """SELECT /*+ MERGE(u, t2) */ u.id, t2.data
            |FROM (SELECT id FROM testcat.ns.t1 WHERE dept RLIKE 'x|y') u
            |JOIN testcat.ns.t2 ON u.id = t2.id
            |""".stripMargin)
        val plan = df.queryExecution.executedPlan

        val projected = keyedPartitioningsOf(collect(plan) { case p: ProjectExec => p })
        assert(projected.exists(kp => !kp.isGrouped && !kp.isCollapsed),
          "this test needs an ungrouped partitioning that the projection did not collapse")

        assert(collectAllGroupPartitions(plan).nonEmpty,
          "the duplicate splits must be grouped, no opt-in needed")
        assert(collectAllShuffles(plan).isEmpty, "grouping must replace the shuffles")
        checkAnswer(df, Seq(Row(1, "b1"), Row(1, "b1"), Row(2, "b2")))
      }
    }
  }

  test("SPARK-59057: filtering partition keys out is not a key collapse") {
    // With partition filtering on, an inner join plans both sides on the intersection of their
    // partition keys, so a side that had more keys ends up with fewer than it reported. That is
    // pruning rather than merging, and counting it as a collapse would make the sticky flag refuse
    // grouping further up the plan. Note that these nodes neither project nor reduce, so this pins
    // the gate that keeps a pruning-only node from reporting a collapse at all. The node that does
    // prune a collapsed key is the next test.
    val cols = Array(Column.create("id", LongType), Column.create("data", StringType))
    withTable("t1", "t2", "t3") {
      createTable("t1", cols, Array(identity("id")))
      sql("INSERT INTO testcat.ns.t1 VALUES (1, 'a1'), (2, 'a2'), (3, 'a3')")
      createTable("t2", cols, Array(identity("id")))
      sql("INSERT INTO testcat.ns.t2 VALUES (1, 'b1'), (2, 'b2')")
      createTable("t3", cols, Array(identity("id")))
      sql("INSERT INTO testcat.ns.t3 VALUES (1, 'c1'), (2, 'c2')")

      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "false") {
        val df = sql(
          """SELECT id, count(*) AS cnt FROM (
            |  SELECT /*+ MERGE(t1, t2) */ t1.id FROM testcat.ns.t1 JOIN testcat.ns.t2
            |  ON t1.id = t2.id
            |  UNION ALL
            |  SELECT id FROM testcat.ns.t3
            |) GROUP BY id
            |""".stripMargin)
        val plan = df.queryExecution.executedPlan

        val groupPartitions = collectAllGroupPartitions(plan)
        val keyed = keyedPartitioningsOf(groupPartitions)
        assert(keyed.nonEmpty, "the join must be planned as a storage-partitioned join")
        assert(keyed.forall(!_.isCollapsed),
          "key 3 was filtered out, not merged into another partition")
        assert(collectAllShuffles(plan).isEmpty,
          "nothing collapsed, so the aggregate above the union must not need a shuffle")
        checkAnswer(df, Seq(Row(1, 2), Row(2, 2)))
      }
    }
  }

  test("SPARK-59057: a shuffle built from a template that collapsed nothing needs no opt-in") {
    // The chain apache#58316 built for the shuffle-template path, with the expectation the collapse
    // semantics call for. items is partitioned by (id, name) with *unique* ids, so projecting
    // `name` away drops a key position but loses no distinct key, so nothing collapsed. purchases
    // reports no partitioning, so with v2BucketingShuffleEnabled its side is shuffled using the
    // projected partitioning as the template. A RIGHT OUTER join then exposes only that shuffled
    // side, and the union with t3 (overlapping keys) makes the merged partitioning ungrouped. The
    // duplicate keys there come from the two children holding the same ids, not from the
    // projection, so the final aggregate may group them with the opt-in off.
    createTable(items, itemsColumns, Array(identity("id"), identity("name")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
      s"(3, 19.5, cast('2020-02-01' as timestamp))")

    createTable("t3", Array(Column.create("id", LongType)), Array(identity("id")))
    sql("INSERT INTO testcat.ns.t3 VALUES (1), (2)")

    val query =
      s"""
         |SELECT id, COUNT(*) AS cnt FROM (
         |  ${selectWithMergeJoinHint("sub", "p")}
         |  p.item_id AS id
         |  FROM (SELECT id FROM testcat.ns.$items WHERE name >= 'aa') sub
         |  RIGHT OUTER JOIN testcat.ns.$purchases p
         |  ON sub.id = p.item_id
         |  UNION ALL
         |  SELECT id FROM testcat.ns.t3
         |) GROUP BY id
         |""".stripMargin

    Seq(false, true).foreach { allowSubset =>
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true",
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> allowSubset.toString) {
        val plan = sql(query).queryExecution.executedPlan

        val union = collect(plan) { case u: UnionExec => u }.head
        val kp = union.outputPartitioning.asInstanceOf[physical.KeyedPartitioning]
        assert(!kp.isGrouped, "keys 1 and 2 repeat across the union children")
        assert(!kp.isCollapsed,
          "the ids were unique, so dropping `name` collapsed nothing to carry down this chain")
        assert(collectAllGroupPartitions(plan).nonEmpty,
          s"allowSubset=$allowSubset: grouping merges only partitions that already shared a key")
        assert(collectAllShuffles(plan).size == 1,
          s"allowSubset=$allowSubset: only the purchases side shuffles")
        checkAnswer(sql(query), Seq(Row(1L, 2L), Row(2L, 2L), Row(3L, 1L)))
      }
    }
  }

  test("SPARK-59057: a collapse is reported when the splits are distributed, not replicated") {
    // Under partially clustered distribution `GroupPartitionsExec` spreads a key's splits over one
    // partition each instead of replicating them, so the partitions it emits never hold more than
    // one split. The collapse has to be read off the key groups it keeps, or this whole mode
    // silently reports nothing collapsed. Here dropping `name` maps the distinct keys (1, 'aa')
    // and (1, 'bb') onto key 1, which is exactly the state the gate exists for.
    createTable(items, itemsColumns, Array(identity("id"), identity("name")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      s"(1, 'bb', 41.0, cast('2020-01-01' as timestamp)), " +
      s"(2, 'cc', 10.0, cast('2020-01-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array(identity("item_id")))
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
      s"(1, 44.0, cast('2020-01-02' as timestamp)), " +
      s"(2, 11.0, cast('2020-01-01' as timestamp))")

    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      val df = sql(s"${selectWithMergeJoinHint("i", "p")} i.id, i.name, p.price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON p.item_id = i.id")
      val plan = df.queryExecution.executedPlan

      val distributing = collectAllGroupPartitions(plan).filter(_.distributePartitions)
      assert(distributing.nonEmpty, "this test needs a distributing GroupPartitionsExec")
      val keyed = keyedPartitioningsOf(distributing)
      assert(keyed.nonEmpty && keyed.forall(_.isCollapsed),
        "dropping `name` merged two distinct keys, however the splits are laid out afterwards")
      checkAnswer(df, Seq(Row(1, "aa", 42.0), Row(1, "aa", 44.0),
        Row(1, "bb", 42.0), Row(1, "bb", 44.0), Row(2, "cc", 11.0)))
    }
  }

  test("SPARK-59057: a collapse confined to keys the other side filters out is not one") {
    // items is partitioned by `identity(id)` with ids 0, 4, 5 and purchases by `bucket(4, item_id)`
    // holding only bucket 1, so items' keys are reduced onto buckets [0, 0, 1], three distinct ids
    // onto two buckets. Bucket 0 is then dropped by partition filtering, since purchases has
    // no rows for it, so the only key the grouping outputs is bucket 1, covering the single id 5.
    // Counting distinct keys before the filtering reports a collapse (2 < 3) for a partitioning
    // where nothing was merged, and the flag is sticky, so it would keep saying so downstream.
    createTable(items, itemsColumns, Array(identity("id")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      s"(0, 'aa', 39.0, cast('2020-01-01' as timestamp)), " +
      s"(4, 'bb', 40.0, cast('2020-01-01' as timestamp)), " +
      s"(5, 'cc', 41.0, cast('2020-01-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array(bucket(4, "item_id")))
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      s"(5, 42.0, cast('2020-01-01' as timestamp))")

    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "false") {
      val df = sql(s"${selectWithMergeJoinHint("i", "p")} i.id, p.price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON p.item_id = i.id")
      val plan = df.queryExecution.executedPlan

      val keyed = keyedPartitioningsOf(collectAllGroupPartitions(plan))
      assert(keyed.nonEmpty, "the reduced join must be planned as a storage-partitioned join")
      assert(keyed.forall(!_.isCollapsed),
        "the only surviving key covers one source id, so nothing was merged")
      checkAnswer(df, Seq(Row(5, 42.0)))
    }
  }

  test("SPARK-59057: reducing keys onto a coarser transform collapses keys") {
    // `identity(item_id)` reduced onto `bucket(4, id)` maps several ids onto one bucket, so keys
    // that were distinct in the source land on the same key. One output partition then covers what
    // were separate id partitions. That is a collapse, and unlike a projection it drops no key
    // position, so the old provenance flag missed it.
    createTable(items, itemsColumns, Array(bucket(4, "id")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      s"(0, 'aa', 39.0, cast('2020-01-01' as timestamp)), " +
      s"(4, 'bb', 40.0, cast('2020-01-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array(identity("item_id")))
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      s"(0, 42.0, cast('2020-01-01' as timestamp)), " +
      s"(4, 44.0, cast('2020-01-15' as timestamp))")

    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
      val df = sql(s"${selectWithMergeJoinHint("i", "p")} i.id, p.price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON p.item_id = i.id")
      val plan = df.queryExecution.executedPlan

      val keyed = keyedPartitioningsOf(collectAllGroupPartitions(plan))
      assert(keyed.nonEmpty, "the reduced join must be planned as a storage-partitioned join")
      assert(keyed.exists(_.isCollapsed),
        "the side whose keys were reduced onto a coarser transform reports a collapse")
      checkAnswer(df, Seq(Row(0, 42.0), Row(4, 44.0)))
    }
  }

}

/**
 * Runs the runtime filtering tests against a catalog whose scans take runtime filters as connector
 * predicates, via [[org.apache.spark.sql.connector.read.SupportsRuntimeFiltering]].
 */
@ExtendedSQLTest
class KeyGroupedPartitioningRuntimeFilterSuite
  extends KeyGroupedPartitioningSuiteBase with KeyGroupedPartitioningRuntimeFilterTests {

  after {
    catalog.clearTables()
  }
}

/**
 * Runs the runtime filtering tests against a catalog whose scans take runtime filters as Catalyst
 * expressions, via
 * [[org.apache.spark.sql.internal.connector.SupportsRuntimeCatalystFiltering]].
 */
@ExtendedSQLTest
class KeyGroupedPartitioningCatalystRuntimeFilterSuite
  extends KeyGroupedPartitioningSuiteBase with KeyGroupedPartitioningRuntimeFilterTests {

  override protected def catalogClassName: String =
    classOf[InMemoryCatalystRuntimeFilterCatalog].getName

  after {
    catalog.clearTables()
  }
}
