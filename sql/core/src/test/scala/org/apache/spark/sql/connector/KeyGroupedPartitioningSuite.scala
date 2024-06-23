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

import java.util.Collections

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Literal, TransformExpression}
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.connector.catalog.{Column, Identifier, InMemoryTableCatalog}
import org.apache.spark.sql.connector.catalog.functions._
import org.apache.spark.sql.connector.distributions.Distributions
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.expressions.Expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.types._

class KeyGroupedPartitioningSuite extends DistributionAndOrderingSuiteBase {
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

  private val emptyProps: java.util.Map[String, String] = {
    Collections.emptyMap[String, String]
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

  test("clustered distribution: output partitioning should be KeyGroupedPartitioning") {
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
    val partitionValues = Seq(50, 51, 52).map(v => InternalRow.fromSeq(Seq(v)))
    val projectedPositions = catalystDistribution.clustering.indices

    checkQueryPlan(df, catalystDistribution,
      physical.KeyGroupedPartitioning(catalystDistribution.clustering, projectedPositions,
        partitionValues, partitionValues))

    // multiple group keys should work too as long as partition keys are subset of them
    df = sql(s"SELECT count(*) FROM testcat.ns.$table GROUP BY id, ts")
    checkQueryPlan(df, catalystDistribution,
      physical.KeyGroupedPartitioning(catalystDistribution.clustering, projectedPositions,
        partitionValues, partitionValues))
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
    val partitionValues = Seq(31).map(v => InternalRow.fromSeq(Seq(v)))
    checkQueryPlan(df, distribution,
      physical.KeyGroupedPartitioning(distribution.clustering, 1, partitionValues, partitionValues))
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
      case physical.ClusteredDistribution(clustering, _, _) =>
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

  private def createTable(
      table: String,
      columns: Array[Column],
      partitions: Array[Transform],
      catalog: InMemoryTableCatalog = catalog): Unit = {
    catalog.createTable(Identifier.of(Array("ns"), table),
      columns, partitions, emptyProps, Distributions.unspecified(), Array.empty, None, None,
      numRowsPerSplit = 1)
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

  private def testWithCustomersAndOrders(
      customers_partitions: Array[Transform],
      orders_partitions: Array[Transform],
      expectedNumOfShuffleExecs: Int): Unit = {
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

    checkAnswer(df,
      Seq(Row("aaa", 10, 100.0), Row("aaa", 10, 200.0), Row("bbb", 20, 150.0),
        Row("bbb", 20, 250.0), Row("bbb", 20, 350.0), Row("ccc", 30, 400.50)))
  }

  private def collectAllShuffles(plan: SparkPlan): Seq[ShuffleExchangeExec] = {
    collect(plan) {
      case s: ShuffleExchangeExec => s
    }
  }

  private def collectShuffles(plan: SparkPlan): Seq[ShuffleExchangeExec] = {
    // here we skip collecting shuffle operators that are not associated with SMJ
    collect(plan) {
      case s: SortMergeJoinExec => s
    }.flatMap(smj =>
      collect(smj) {
        case s: ShuffleExchangeExec => s
      })
  }

  private def collectScans(plan: SparkPlan): Seq[BatchScanExec] = {
    collect(plan) { case s: BatchScanExec => s }
  }

  test("partitioned join: exact distribution (same number of buckets) from both sides") {
    val customers_partitions = Array(bucket(4, "customer_id"))
    val orders_partitions = Array(bucket(4, "customer_id"))

    testWithCustomersAndOrders(customers_partitions, orders_partitions, 0)
  }

  test("partitioned join: number of buckets mismatch should trigger shuffle") {
    val customers_partitions = Array(bucket(4, "customer_id"))
    val orders_partitions = Array(bucket(2, "customer_id"))

    // should shuffle both sides when number of buckets are not the same
    testWithCustomersAndOrders(customers_partitions, orders_partitions, 2)
  }

  test("partitioned join: only one side reports partitioning") {
    val customers_partitions = Array(bucket(4, "customer_id"))

    testWithCustomersAndOrders(customers_partitions, Array.empty, 2)
  }

  private val items: String = "items"
  private val itemsColumns: Array[Column] = Array(
    Column.create("id", LongType),
    Column.create("name", StringType),
    Column.create("price", FloatType),
    Column.create("arrive_time", TimestampType))

  private val purchases: String = "purchases"
  private val purchasesColumns: Array[Column] = Array(
    Column.create("item_id", LongType),
    Column.create("price", FloatType),
    Column.create("time", TimestampType))

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
      "should contain shuffle when not grouping by partition values")

    checkAnswer(df.sort("res"), Seq(Row(10.0), Row(15.5), Row(41.0)))
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
        if (pushDownValues) {
          assert(shuffles.isEmpty, "should not add shuffle when partition values mismatch")
        } else {
          assert(shuffles.nonEmpty, "should add shuffle when partition values mismatch, and " +
              "pushing down partition values is not enabled")
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
        if (pushDownValues) {
          assert(shuffles.isEmpty, "should not add shuffle when partition values mismatch")
        } else {
          assert(shuffles.nonEmpty, "should add shuffle when partition values mismatch, and " +
              "pushing down partition values is not enabled")
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
        if (pushDownValues) {
          assert(shuffles.isEmpty, "should not add shuffle when partition values mismatch")
        } else {
          assert(shuffles.nonEmpty, "should add shuffle when partition values mismatch, and " +
              "pushing down partition values is not enabled")
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
        if (pushDownValues) {
          assert(shuffles.isEmpty, "should not add shuffle when partition values mismatch")
        } else {
          assert(shuffles.nonEmpty, "should add shuffle when partition values mismatch, and " +
              "pushing down partition values is not enabled")
        }

        checkAnswer(df, Seq.empty)
      }
    }
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
              val scans = collectScans(df.queryExecution.executedPlan)
              assert(scans.forall(_.inputRDD.partitions.length == expected))
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
              val scans = collectScans(df.queryExecution.executedPlan)
              assert(scans.forall(_.inputRDD.partitions.length == expected))
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
      Seq(("true", 10), ("false", 5)).foreach {
        case (enable, expected) =>
          withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable) {
            val df = createJoinTestDF(Seq("id" -> "item_id"))
            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            if (pushDownValues) {
              assert(shuffles.isEmpty, "should not contain any shuffle")
              val scans = collectScans(df.queryExecution.executedPlan)
              assert(scans.forall(_.inputRDD.partitions.length == expected))
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
      Seq(("true", 9), ("false", 5)).foreach {
        case (enable, expected) =>
          withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable) {
            val df = createJoinTestDF(Seq("id" -> "item_id"))
            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            if (pushDownValues) {
              assert(shuffles.isEmpty, "should not contain any shuffle")
              val scans = collectScans(df.queryExecution.executedPlan)
              assert(scans.forall(_.inputRDD.partitions.length == expected))
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
      Seq(("true", 6), ("false", 5)).foreach {
        case (enable, expected) =>
          withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable) {
            val df = createJoinTestDF(Seq("id" -> "item_id"))
            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            if (pushDownValues) {
              assert(shuffles.isEmpty, "should not contain any shuffle")
              val scans = collectScans(df.queryExecution.executedPlan)
              assert(scans.forall(_.inputRDD.partitions.length == expected))
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
      Seq(("true", 7), ("false", 5)).foreach {
        case (enable, expected) =>
          withSQLConf(
            SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> false.toString,
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
            SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable) {
            val df = createJoinTestDF(
              Seq("id" -> "item_id", "arrive_time" -> "time"), joinType = "LEFT")
            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            if (pushDownValues) {
              assert(shuffles.isEmpty, "should not contain any shuffle")
              val scans = collectScans(df.queryExecution.executedPlan)
              assert(scans.forall(_.inputRDD.partitions.length == expected),
              s"Expected $expected but got ${scans.head.inputRDD.partitions.length}")
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
              val scans = collectScans(df.queryExecution.executedPlan)
              assert(scans.map(_.inputRDD.partitions.length).toSet.size == 1)
              assert(scans.forall(_.inputRDD.partitions.length == expected),
                s"Expected $expected but got ${scans.head.inputRDD.partitions.length}")
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
              val scans = collectScans(df.queryExecution.executedPlan)
              assert(scans.map(_.inputRDD.partitions.length).toSet.size == 1)
              assert(scans.forall(_.inputRDD.partitions.length == expected),
                s"Expected $expected but got ${scans.head.inputRDD.partitions.length}")
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
          checkAnswer(df, Seq(Row(131)))

          // dynamic filtering doesn't change partitioning so storage-partitioned join should kick
          // in
          df = sql(s"SELECT sum(p.price) from testcat.ns.$items i, testcat.ns.$purchases p " +
              "WHERE i.id = p.item_id AND i.price >= 10.0")
          val shuffles = collectShuffles(df.queryExecution.executedPlan)
          assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
          checkAnswer(df, Seq(Row(303.5)))
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

            // storage-partitioned join should kick in and fill the missing partitions & splits
            // after dynamic filtering with empty partitions & splits, respectively.
            val df = sql(s"SELECT sum(p.price) from " +
                s"testcat.ns.$purchases p, testcat.ns.$items i WHERE " +
                s"p.item_id = i.id AND p.price < 45.0")

            checkAnswer(df, Seq(Row(213.5)))
            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            if (pushDownValues) {
              assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
              val scans = collectScans(df.queryExecution.executedPlan)
              assert(scans.forall(_.inputRDD.partitions.length == expected))
            } else {
              assert(shuffles.nonEmpty,
                "should contain shuffle when not pushing down partition values")
            }
          }
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

  test("SPARK-48065: SPJ: allowJoinKeysSubsetOfPartitionKeys is too strict") {
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
          SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
          val df = sql(
            s"""
               |${selectWithMergeJoinHint("t1", "t2")}
               |t1.id AS id, t1.data AS t1data, t2.data AS t2data
               |FROM testcat.ns.$table1 t1 JOIN testcat.ns.$table2 t2
               |ON t1.id = t2.id AND t1.data = t2.data ORDER BY t1.id, t1data, t2data
               |""".stripMargin)
          val shuffles = collectShuffles(df.queryExecution.executedPlan)
          assert(shuffles.isEmpty, "SPJ should be triggered")

          val scans = collectScans(df.queryExecution.executedPlan)
            .map(_.inputRDD.partitions.length)
          if (partiallyClustered) {
            assert(scans == Seq(8, 8))
          } else {
            assert(scans == Seq(4, 4))
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

  test("SPARK-44647: test join key is subset of cluster key " +
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
      Seq(true, false).foreach { partiallyClustered =>
        Seq(true, false).foreach { allowJoinKeysSubsetOfPartitionKeys =>

          withSQLConf(
            SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
            SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
                partiallyClustered.toString,
            SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key ->
                allowJoinKeysSubsetOfPartitionKeys.toString) {
            val df = sql(
              s"""
                |${selectWithMergeJoinHint("t1", "t2")}
                |t1.id AS id, t1.data AS t1data, t2.data AS t2data
                |FROM testcat.ns.$table1 t1 JOIN testcat.ns.$table2 t2
                |ON t1.id = t2.id ORDER BY t1.id, t1data, t2data
                |""".stripMargin)
            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            if (allowJoinKeysSubsetOfPartitionKeys) {
              assert(shuffles.isEmpty, "SPJ should be triggered")
            } else {
              assert(shuffles.nonEmpty, "SPJ should not be triggered")
            }

            val scans = collectScans(df.queryExecution.executedPlan)
                .map(_.inputRDD.partitions.length)

            (allowJoinKeysSubsetOfPartitionKeys, partiallyClustered) match {
              // SPJ and partially-clustered
              case (true, true) => assert(scans == Seq(8, 8))
              // SPJ and not partially-clustered
              case (true, false) => assert(scans == Seq(4, 4))
              // No SPJ
              case _ => assert(scans == Seq(5, 4))
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

  test("SPARK-47094: Support compatible buckets") {
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

        Seq(true, false).foreach { allowJoinKeysSubsetOfPartitionKeys =>
          withSQLConf(
            SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
            SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false",
            SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key ->
              allowJoinKeysSubsetOfPartitionKeys.toString,
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

            val scans = collectScans(df.queryExecution.executedPlan).map(_.inputRDD.
              partitions.length)
            val expectedBuckets = Math.min(table1buckets1, table2buckets1) *
              Math.min(table1buckets2, table2buckets2)
            assert(scans == Seq(expectedBuckets, expectedBuckets))

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

  test("SPARK-47094: Support compatible buckets with common divisor") {
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

        Seq(true, false).foreach { allowJoinKeysSubsetOfPartitionKeys =>
          withSQLConf(
            SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
            SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false",
            SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key ->
              allowJoinKeysSubsetOfPartitionKeys.toString,
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

            val scans = collectScans(df.queryExecution.executedPlan).map(_.inputRDD.
              partitions.length)

            def gcd(a: Int, b: Int): Int = BigInt(a).gcd(BigInt(b)).toInt
            val expectedBuckets = gcd(table1buckets1, table2buckets1) *
              gcd(table1buckets2, table2buckets2)
            assert(scans == Seq(expectedBuckets, expectedBuckets))

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
          SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false",
          SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
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

          val scans = collectScans(df.queryExecution.executedPlan).map(_.inputRDD.
            partitions.length)

          val expectedBuckets = Math.min(table1buckets, table2buckets)

          assert(scans == Seq(expectedBuckets, expectedBuckets))

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
          SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> allowPushDown.toString,
          SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
            partiallyClustered.toString,
          SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
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
          val scans = collectScans(df.queryExecution.executedPlan).map(_.inputRDD.
            partitions.length)

          (allowPushDown, partiallyClustered) match {
            case (true, false) =>
              assert(shuffles.isEmpty, "SPJ should be triggered")
              assert(scans == Seq(2, 2))
            case (_, _) =>
              assert(shuffles.nonEmpty, "SPJ should not be triggered")
              assert(scans == Seq(3, 2))
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
        Seq(true, false).foreach { allowJoinKeysSubsetOfPartitionKeys =>
          withSQLConf(
            SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key ->
                pushDownValues.toString,
            SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
                partiallyClustered.toString,
            SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key ->
                allowJoinKeysSubsetOfPartitionKeys.toString) {

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
            if (allowJoinKeysSubsetOfPartitionKeys) {
              assert(shuffles.isEmpty, "SPJ should be triggered")
            } else {
              assert(shuffles.nonEmpty, "SPJ should not be triggered")
            }

            val scans = collectScans(df.queryExecution.executedPlan)
                .map(_.inputRDD.partitions.length)
            (pushDownValues, allowJoinKeysSubsetOfPartitionKeys, partiallyClustered) match {
              // SPJ and partially-clustered
              case (true, true, true) => assert(scans == Seq(3, 3))
              // non-SPJ or SPJ/partially-clustered
              case _ => assert(scans == Seq(3, 3))
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
        Seq(true, false).foreach { allowJoinKeysSubsetOfPartitionKeys =>

          withSQLConf(
            SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
            SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
                partiallyClustered.toString,
            SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key ->
                allowJoinKeysSubsetOfPartitionKeys.toString) {
            val df = createJoinTestDF(Seq("arrive_time" -> "time"), extraColumns = Seq("p.item_id"))
            // Currently SPJ for case where join key not same as partition key
            // only supported when push-part-values enabled
            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            if (allowJoinKeysSubsetOfPartitionKeys) {
              assert(shuffles.isEmpty, "SPJ should be triggered")
            } else {
              assert(shuffles.nonEmpty, "SPJ should not be triggered")
            }

            val scans = collectScans(df.queryExecution.executedPlan)
                .map(_.inputRDD.partitions.length)
            (allowJoinKeysSubsetOfPartitionKeys, partiallyClustered) match {
              // SPJ and partially-clustered
              case (true, true) => assert(scans == Seq(5, 5))
              // SPJ and not partially-clustered
              case (true, false) => assert(scans == Seq(3, 3))
              // No SPJ
              case _ => assert(scans == Seq(4, 4))
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
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
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
     SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
     SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
     SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
     SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false",
     SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
     val df = createJoinTestDF(Seq("id" -> "item_id"))
     val shuffles = collectShuffles(df.queryExecution.executedPlan)
     assert(shuffles.size == 2, "SPJ should not be triggered for transform expression with" +
       "less join keys than partition keys for now.")
     checkAnswer(df, Seq(Row(1, "aa", 30.0, 42.0),
       Row(1, "aa", 30.0, 89.0),
       Row(1, "aa", 40.0, 42.0),
       Row(1, "aa", 40.0, 89.0),
       Row(3, "bb", 10.0, 19.5)))
   }
  }
}
