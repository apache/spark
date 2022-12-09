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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Literal, TransformExpression}
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.InMemoryTableCatalog
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

  private var originalV2BucketingEnabled: Boolean = false
  private var originalAutoBroadcastJoinThreshold: Long = -1

  override def beforeAll(): Unit = {
    super.beforeAll()
    originalV2BucketingEnabled = conf.getConf(V2_BUCKETING_ENABLED)
    conf.setConf(V2_BUCKETING_ENABLED, true)
    originalAutoBroadcastJoinThreshold = conf.getConf(AUTO_BROADCASTJOIN_THRESHOLD)
    conf.setConf(AUTO_BROADCASTJOIN_THRESHOLD, -1L)
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      conf.setConf(V2_BUCKETING_ENABLED, originalV2BucketingEnabled)
      conf.setConf(AUTO_BROADCASTJOIN_THRESHOLD, originalAutoBroadcastJoinThreshold)
    }
  }

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
  private val schema = new StructType()
      .add("id", IntegerType)
      .add("data", StringType)
      .add("ts", TimestampType)

  test("clustered distribution: output partitioning should be KeyGroupedPartitioning") {
    val partitions: Array[Transform] = Array(Expressions.years("ts"))

    // create a table with 3 partitions, partitioned by `years` transform
    createTable(table, schema, partitions)
    sql(s"INSERT INTO testcat.ns.$table VALUES " +
        s"(0, 'aaa', CAST('2022-01-01' AS timestamp)), " +
        s"(1, 'bbb', CAST('2021-01-01' AS timestamp)), " +
        s"(2, 'ccc', CAST('2020-01-01' AS timestamp))")

    var df = sql(s"SELECT count(*) FROM testcat.ns.$table GROUP BY ts")
    val catalystDistribution = physical.ClusteredDistribution(
      Seq(TransformExpression(YearsFunction, Seq(attr("ts")))))
    val partitionValues = Seq(50, 51, 52).map(v => InternalRow.fromSeq(Seq(v)))

    checkQueryPlan(df, catalystDistribution,
      physical.KeyGroupedPartitioning(catalystDistribution.clustering, partitionValues))

    // multiple group keys should work too as long as partition keys are subset of them
    df = sql(s"SELECT count(*) FROM testcat.ns.$table GROUP BY id, ts")
    checkQueryPlan(df, catalystDistribution,
      physical.KeyGroupedPartitioning(catalystDistribution.clustering, partitionValues))
  }

  test("non-clustered distribution: no partition") {
    val partitions: Array[Transform] = Array(bucket(32, "ts"))
    createTable(table, schema, partitions)

    val df = sql(s"SELECT * FROM testcat.ns.$table")
    val distribution = physical.ClusteredDistribution(
      Seq(TransformExpression(BucketFunction, Seq(attr("ts")), Some(32))))

    checkQueryPlan(df, distribution, physical.UnknownPartitioning(0))
  }

  test("non-clustered distribution: single partition") {
    val partitions: Array[Transform] = Array(bucket(32, "ts"))
    createTable(table, schema, partitions)
    sql(s"INSERT INTO testcat.ns.$table VALUES (0, 'aaa', CAST('2020-01-01' AS timestamp))")

    val df = sql(s"SELECT * FROM testcat.ns.$table")
    val distribution = physical.ClusteredDistribution(
      Seq(TransformExpression(BucketFunction, Seq(attr("ts")), Some(32))))

    checkQueryPlan(df, distribution, physical.SinglePartition)
  }

  test("non-clustered distribution: no V2 catalog") {
    spark.conf.set("spark.sql.catalog.testcat2", classOf[InMemoryTableCatalog].getName)
    val nonFunctionCatalog = spark.sessionState.catalogManager.catalog("testcat2")
        .asInstanceOf[InMemoryTableCatalog]
    val partitions: Array[Transform] = Array(bucket(32, "ts"))
    createTable(table, schema, partitions, catalog = nonFunctionCatalog)
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
    createTable(table, schema, partitions)
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
      createTable(table, schema, partitions)
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
    createTable(table, schema, partitions)
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
      schema: StructType,
      partitions: Array[Transform],
      catalog: InMemoryTableCatalog = catalog): Unit = {
    catalog.createTable(Identifier.of(Array("ns"), table),
      schema, partitions, emptyProps, Distributions.unspecified(), Array.empty, None)
  }

  private val customers: String = "customers"
  private val customers_schema = new StructType()
      .add("customer_name", StringType)
      .add("customer_age", IntegerType)
      .add("customer_id", LongType)

  private val orders: String = "orders"
  private val orders_schema = new StructType()
      .add("order_amount", DoubleType)
      .add("customer_id", LongType)

  private def testWithCustomersAndOrders(
      customers_partitions: Array[Transform],
      orders_partitions: Array[Transform],
      expectedNumOfShuffleExecs: Int): Unit = {
    createTable(customers, customers_schema, customers_partitions)
    sql(s"INSERT INTO testcat.ns.$customers VALUES " +
        s"('aaa', 10, 1), ('bbb', 20, 2), ('ccc', 30, 3)")

    createTable(orders, orders_schema, orders_partitions)
    sql(s"INSERT INTO testcat.ns.$orders VALUES " +
        s"(100.0, 1), (200.0, 1), (150.0, 2), (250.0, 2), (350.0, 2), (400.50, 3)")

    val df = sql("SELECT customer_name, customer_age, order_amount " +
        s"FROM testcat.ns.$customers c JOIN testcat.ns.$orders o " +
        "ON c.customer_id = o.customer_id ORDER BY c.customer_id, order_amount")

    val shuffles = collectShuffles(df.queryExecution.executedPlan)
    assert(shuffles.length == expectedNumOfShuffleExecs)

    checkAnswer(df,
      Seq(Row("aaa", 10, 100.0), Row("aaa", 10, 200.0), Row("bbb", 20, 150.0),
        Row("bbb", 20, 250.0), Row("bbb", 20, 350.0), Row("ccc", 30, 400.50)))
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
    val orders_partitions = Array(bucket(2, "customer_id"))

    testWithCustomersAndOrders(customers_partitions, orders_partitions, 2)
  }

  private val items: String = "items"
  private val items_schema: StructType = new StructType()
      .add("id", LongType)
      .add("name", StringType)
      .add("price", FloatType)
      .add("arrive_time", TimestampType)

  private val purchases: String = "purchases"
  private val purchases_schema: StructType = new StructType()
      .add("item_id", LongType)
      .add("price", FloatType)
      .add("time", TimestampType)

  test("partitioned join: join with two partition keys and matching & sorted partitions") {
    val items_partitions = Array(bucket(8, "id"), days("arrive_time"))
    createTable(items, items_schema, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"), days("time"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 19.5, cast('2020-02-01' as timestamp))")

    val df = sql("SELECT id, name, i.price as purchase_price, p.price as sale_price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
        "ON i.id = p.item_id AND i.arrive_time = p.time ORDER BY id, purchase_price, sale_price")

    val shuffles = collectShuffles(df.queryExecution.executedPlan)
    assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
    checkAnswer(df,
      Seq(Row(1, "aa", 40.0, 42.0), Row(1, "aa", 41.0, 44.0), Row(1, "aa", 41.0, 45.0),
        Row(2, "bb", 10.0, 11.0), Row(2, "bb", 10.5, 11.0), Row(3, "cc", 15.5, 19.5))
    )
  }

  test("partitioned join: join with two partition keys and unsorted partitions") {
    val items_partitions = Array(bucket(8, "id"), days("arrive_time"))
    createTable(items, items_schema, items_partitions)
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp)), " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"), days("time"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
        s"(3, 19.5, cast('2020-02-01' as timestamp))")

    val df = sql("SELECT id, name, i.price as purchase_price, p.price as sale_price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
        "ON i.id = p.item_id AND i.arrive_time = p.time ORDER BY id, purchase_price, sale_price")

    val shuffles = collectShuffles(df.queryExecution.executedPlan)
    assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
    checkAnswer(df,
      Seq(Row(1, "aa", 40.0, 42.0), Row(1, "aa", 41.0, 44.0), Row(1, "aa", 41.0, 45.0),
        Row(2, "bb", 10.0, 11.0), Row(2, "bb", 10.5, 11.0), Row(3, "cc", 15.5, 19.5))
    )
  }

  test("partitioned join: join with two partition keys and different # of partition keys") {
    val items_partitions = Array(bucket(8, "id"), days("arrive_time"))
    createTable(items, items_schema, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"), days("time"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 11.0, cast('2020-01-01' as timestamp))")

    val df = sql("SELECT id, name, i.price as purchase_price, p.price as sale_price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
        "ON i.id = p.item_id AND i.arrive_time = p.time ORDER BY id, purchase_price, sale_price")

    val shuffles = collectShuffles(df.queryExecution.executedPlan)
    assert(shuffles.nonEmpty, "should add shuffle when partition keys mismatch")
  }

  test("data source partitioning + dynamic partition filtering") {
    withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "10") {
      val items_partitions = Array(identity("id"))
      createTable(items, items_schema, items_partitions)
      sql(s"INSERT INTO testcat.ns.$items VALUES " +
          s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
          s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
          s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
          s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp)), " +
          s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

      val purchases_partitions = Array(identity("item_id"))
      createTable(purchases, purchases_schema, purchases_partitions)
      sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
          s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
          s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
          s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
          s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
          s"(3, 19.5, cast('2020-02-01' as timestamp))")

      // number of unique partitions changed after dynamic filtering - the gap should be filled
      // with empty partitions and the job should still succeed
      var df = sql(s"SELECT sum(p.price) from testcat.ns.$items i, testcat.ns.$purchases p WHERE " +
          s"i.id = p.item_id AND i.price > 40.0")
      checkAnswer(df, Seq(Row(131)))

      // dynamic filtering doesn't change partitioning so storage-partitioned join should kick in
      df = sql(s"SELECT sum(p.price) from testcat.ns.$items i, testcat.ns.$purchases p WHERE " +
          s"i.id = p.item_id AND i.price >= 10.0")
      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
      checkAnswer(df, Seq(Row(303.5)))
    }
  }
}
