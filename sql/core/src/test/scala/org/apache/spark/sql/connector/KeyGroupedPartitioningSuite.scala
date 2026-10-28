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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Literal, TransformExpression}
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.catalyst.plans.physical.KeyGroupedPartitioning
import org.apache.spark.sql.connector.catalog.{Column, Identifier, InMemoryTableCatalog}
import org.apache.spark.sql.connector.catalog.functions._
import org.apache.spark.sql.connector.distributions.Distributions
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.expressions.Expressions._
import org.apache.spark.sql.connector.read.HasPartitionKey
import org.apache.spark.sql.execution.{RDDScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.types._

class KeyGroupedPartitioningSuite extends DistributionAndOrderingSuiteBase {
  private val functions = Seq(
    UnboundYearsFunction,
    UnboundDaysFunction,
    UnboundBucketFunction,
    UnboundTruncateFunction,
    UnboundSignedZerosFunction)

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
        partitionValues, partitionValues, isPartiallyClustered = false,
        mayContainUnknownPartitionKeys = false))

    // multiple group keys should work too as long as partition keys are subset of them
    df = sql(s"SELECT count(*) FROM testcat.ns.$table GROUP BY id, ts")
    checkQueryPlan(df, catalystDistribution,
      physical.KeyGroupedPartitioning(catalystDistribution.clustering, projectedPositions,
        partitionValues, partitionValues, isPartiallyClustered = false,
        mayContainUnknownPartitionKeys = false))
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
    val partitionValues = Seq(0).map(v => InternalRow.fromSeq(Seq(v)))
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

        def verifyShuffle(cmd: String, answer: Seq[Row]): Unit = {
          val df = sql(cmd)
          if (sortingEnabled) {
            assert(collectAllShuffles(df.queryExecution.executedPlan).isEmpty,
              "should contain no shuffle when sorting by partition values")
          } else {
            assert(collectAllShuffles(df.queryExecution.executedPlan).size == 1,
              "should contain one shuffle when optimization is disabled")
          }
          checkAnswer(df, answer)
        }: Unit

        verifyShuffle(
          s"SELECT price, id FROM testcat.ns.$items ORDER BY price ASC, id ASC",
          Seq(Row(null, 3), Row(10.0, 2), Row(15.5, null),
            Row(15.5, 3), Row(40.0, 1), Row(41.0, 1)))

        verifyShuffle(
          s"SELECT price, id FROM testcat.ns.$items " +
            s"ORDER BY price ASC NULLS LAST, id ASC NULLS LAST",
          Seq(Row(10.0, 2), Row(15.5, 3), Row(15.5, null),
            Row(40.0, 1), Row(41.0, 1), Row(null, 3)))

        verifyShuffle(
          s"SELECT price, id FROM testcat.ns.$items ORDER BY price DESC, id ASC",
          Seq(Row(41.0, 1), Row(40.0, 1), Row(15.5, null),
            Row(15.5, 3), Row(10.0, 2), Row(null, 3)))

        verifyShuffle(
          s"SELECT price, id FROM testcat.ns.$items ORDER BY price DESC, id DESC",
          Seq(Row(41.0, 1), Row(40.0, 1), Row(15.5, 3),
            Row(15.5, null), Row(10.0, 2), Row(null, 3)))

        verifyShuffle(
          s"SELECT price, id FROM testcat.ns.$items " +
            s"ORDER BY price DESC NULLS FIRST, id DESC NULLS FIRST",
          Seq(Row(null, 3), Row(41.0, 1), Row(40.0, 1),
            Row(15.5, null), Row(15.5, 3), Row(10.0, 2)));
      }
    }
  }

  test("SPARK-59905: ORDER BY a join-key expression a join reports as a partition expression") {
    createTable("ident", Array(Column.create("id", LongType)), Array(identity("id")))
    sql("INSERT INTO testcat.ns.ident VALUES " + (94 to 100).map(i => s"($i)").mkString(", "))
    createTable("neg", Array(Column.create("id", LongType)), Array(identity("id")))
    sql("INSERT INTO testcat.ns.neg VALUES " + (0 to 6).map(i => s"(${-i})").mkString(", "))
    createTable("plain", Array(Column.create("b", LongType), Column.create("c", LongType),
      Column.create("s", new StructType().add("a", LongType))), Array.empty)
    sql("INSERT INTO testcat.ns.plain VALUES " +
      (0 to 6).map(b => s"($b, 94, named_struct('a', ${94 + b}L))").mkString(", "))
    createTable("ident2", Array(Column.create("x", LongType), Column.create("y", LongType)),
      Array(identity("x"), identity("y")))
    sql("INSERT INTO testcat.ns.ident2 VALUES (94, 1), (94, 2), (95, 1)")
    createTable("plain2", Array(Column.create("b", LongType), Column.create("c", LongType)),
      Array.empty)
    sql("INSERT INTO testcat.ns.plain2 VALUES (6, 1), (6, 2), (5, 1)")

    // The join lays `p` out on `i`'s keys and reports the join key, e.g. `100 - b`, as `p`'s
    // partition expression. A one-side shuffle does that under the shuffle conf. An inner
    // broadcast join does it too, by reporting `i`'s layout over `p`'s join key. Over the broadcast
    // join's layout, the ORDER BY is served by sorting the partition values, with no range
    // shuffle. On this branch a one-side shuffle is followed by a range shuffle for the ORDER BY.
    case class Query(left: String, right: String, on: String, order: String, select: String,
        expected: Seq[Row])
    val ids = (94 to 100).map(i => Row(i.toLong))
    val singleColumnKeys = Seq(
      Query("neg", "plain", "i.id = -p.b", "-p.b", "p.b", (6 to 0 by -1).map(b => Row(b.toLong))),
      Query("neg", "plain", "i.id = -p.b", "-p.b DESC", "p.b", (0 to 6).map(b => Row(b.toLong))),
      Query("ident", "plain", "i.id = 100 - p.b", "100 - p.b", "p.b",
        (6 to 0 by -1).map(b => Row(b.toLong))),
      Query("ident", "plain", "i.id = 100 - p.b", "100 - p.b DESC", "p.b",
        (0 to 6).map(b => Row(b.toLong))),
      Query("ident", "plain", "i.id = p.s.a", "p.s.a", "i.id", ids),
      Query("ident2", "plain2", "i.x = 100 - p.b AND i.y = p.c", "100 - p.b, p.c DESC", "p.b, p.c",
        Seq(Row(6L, 2L), Row(6L, 1L), Row(5L, 1L))))
    // As on master, `b + c` runs with AQE off only.
    val twoColumnKey = Query("ident", "plain", "i.id = p.b + p.c", "p.b + p.c", "i.id", ids)

    for {
      (query, aqe) <-
        singleColumnKeys.flatMap(q => Seq(q -> true, q -> false)) :+ (twoColumnKey -> false)
      broadcast <- Seq(false, true)
    } {
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> (!broadcast).toString,
          SQLConf.V2_BUCKETING_SORTING_ENABLED.key -> "true") {
        val hint = if (broadcast) "/*+ BROADCAST(p) */ " else ""
        val df = sql(
          s"""SELECT $hint${query.select} FROM testcat.ns.${query.left} i
             |JOIN testcat.ns.${query.right} p ON ${query.on} ORDER BY ${query.order}
             |""".stripMargin)
        val clue = s"${query.order}, aqe = $aqe, broadcast = $broadcast"
        checkAnswer(df, query.expected)
        // A one-side shuffle and the range shuffle after it, and none for a broadcast join.
        assert(collectAllShuffles(df.queryExecution.executedPlan).size == (if (broadcast) 0 else 2),
          clue)
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

  test("SPARK-49205: KeyGroupedPartitioning should inherit HashPartitioningLike") {
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
          SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> false.toString,
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
          val scans = collectScans(df.queryExecution.executedPlan)
          assert(scans.forall(_.inputRDD.partitions.length === 3))
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
          Seq(true, false).foreach { allowJoinKeysSubsetOfPartitionKeys =>
            withSQLConf(
              SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
                  partiallyClustered.toString,
              SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> filter.toString,
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

              val scannedPartitions = collectScans(df.queryExecution.executedPlan)
                  .map(_.inputRDD.partitions.length)
              (allowJoinKeysSubsetOfPartitionKeys, partiallyClustered, filter) match {
                // SPJ, partially-clustered, with filter
                case (true, true, true) => assert(scannedPartitions == Seq(6, 6))

                // SPJ, partially-clustered, no filter
                case (true, true, false) => assert(scannedPartitions == Seq(8, 8))

                // SPJ and not partially-clustered, with filter
                case (true, false, true) => assert(scannedPartitions == Seq(2, 2))

                // SPJ and not partially-clustered, no filter
                case (true, false, false) => assert(scannedPartitions == Seq(4, 4))

                // No SPJ
                case _ => assert(scannedPartitions == Seq(5, 4))
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

            val partions = collectScans(df.queryExecution.executedPlan).map(_.inputRDD.
              partitions.length)
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
            val expectedPartitions = gcd(table1buckets1, table2buckets1) *
              gcd(table1buckets2, table2buckets2)
            assert(scans == Seq(expectedPartitions, expectedPartitions))

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

  test("SPARK-59080: both sides of the join land on the same collection member") {
    // `arrive_time` is selected twice under two aliases, so the projected partitioning is a
    // `PartitioningCollection` whose members cover different numbers of the join keys: one covers
    // (id, t1), another only id. Each member's spec is projected onto its own subset, so the specs
    // disagree on `numPartitions`, and asking the collection for one partitioning fails with
    // "expected all specs in the collection to have the same number of partitions".
    //
    // `EnsureRequirements` now resolves the member the two sides agreed on before it asks, so the
    // keyed side is grouped on both join keys and the shuffled side is laid out on those same keys.
    val items_partitions = Array(identity("id"), identity("arrive_time"))
    createTable(items, itemsColumns, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(1, 'ab', 30.0, cast('2020-01-02' as timestamp)), " +
      "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 42.0, cast('2020-01-01' as timestamp)), " +
      "(1, 89.0, cast('2020-01-02' as timestamp)), " +
      "(3, 19.5, cast('2020-01-01' as timestamp)), " +
      "(5, 26.0, cast('2023-01-01' as timestamp))")

    withSQLConf(
      SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false",
      SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      val df = sql(
        s"""
           |${selectWithMergeJoinHint("i", "p")}
           |id, t1, t2, i.price AS purchase_price, p.price AS sale_price
           |FROM (SELECT id, arrive_time AS t1, arrive_time AS t2, price FROM testcat.ns.$items) i
           |JOIN testcat.ns.$purchases p ON i.id = p.item_id AND i.t1 = p.time
           |""".stripMargin)
      val plan = df.queryExecution.executedPlan
      // On this branch the resolved member is recorded on the scan rather than on a grouping node.
      val positions = collectScans(plan).flatMap(_.spjParams.joinKeyPositions)
      assert(positions === Seq(Seq(0, 1)),
        "the keyed side must be grouped on both join keys, the finest granularity available")
      assert(collectAllShuffles(plan).size == 1, "only the unpartitioned side shuffles")
      checkAnswer(df, Seq(
        Row(1, java.sql.Timestamp.valueOf("2020-01-01 00:00:00"),
          java.sql.Timestamp.valueOf("2020-01-01 00:00:00"), 40.0, 42.0),
        Row(1, java.sql.Timestamp.valueOf("2020-01-02 00:00:00"),
          java.sql.Timestamp.valueOf("2020-01-02 00:00:00"), 30.0, 89.0),
        Row(3, java.sql.Timestamp.valueOf("2020-01-01 00:00:00"),
          java.sql.Timestamp.valueOf("2020-01-01 00:00:00"), 10.0, 19.5)))
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
      SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      // Duplicating `id` under two aliases makes the projection report a
      // `PartitioningCollection` of `KeyGroupedPartitioning`s, so the keyed side's shuffle spec
      // is a `ShuffleSpecCollection` wrapping a `KeyGroupedShuffleSpec` with join key positions.
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
      val keyedScans = collectScans(df.queryExecution.executedPlan)
        .filter(_.spjParams.joinKeyPositions.isDefined)
      assert(keyedScans.size == 1, "the keyed side should be grouped by the join keys")
      assert(keyedScans.head.outputPartitioning.numPartitions == 3,
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

  test("SPARK-58783: SPJ with runtime filtering when the join key is a trailing partition key") {
    val table1 = "table1"
    val table2 = "table2"
    // the join key is the second partition key of `table1`, so its scan partitioning is projected
    // down to `dept_id` when SPJ is applied
    createTable(table1, columns2, Array(identity("store_id"), identity("dept_id")))
    sql(s"INSERT INTO testcat.ns.$table1 VALUES " +
        "(100, 1, 'aa'), " +
        "(100, 2, 'ab'), " +
        "(200, 1, 'ac'), " +
        "(200, 3, 'ad')")

    createTable(table2, columns2, Array(identity("dept_id")))
    sql(s"INSERT INTO testcat.ns.$table2 VALUES " +
        "(100, 1, 'x'), " +
        "(100, 2, 'y'), " +
        "(100, 3, 'x')")

    Seq(true, false).foreach { partiallyClustered =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "10",
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
            partiallyClustered.toString) {
        val df = sql(
          s"""
             |${selectWithMergeJoinHint("t1", "t2")}
             |t1.store_id, t1.dept_id, t1.data AS t1data, t2.data AS t2data
             |FROM testcat.ns.$table1 t1 JOIN testcat.ns.$table2 t2
             |ON t1.dept_id = t2.dept_id WHERE t2.data = 'x'
             |ORDER BY t1.store_id, t1.dept_id, t1data
             |""".stripMargin)
        // the dynamic filter must land on the `table1` scan, the side whose partitioning is
        // projected down to a non-prefix of its partition keys
        val scans = collectScans(df.queryExecution.executedPlan)
        assert(
          scans.exists(s =>
            s.runtimeFilters.nonEmpty && s.spjParams.joinKeyPositions.contains(Seq(1))),
          "the dynamic filter should be pushed to the scan with the projected partitioning")
        val shuffles = collectShuffles(df.queryExecution.executedPlan)
        assert(shuffles.isEmpty, "SPJ should be triggered")
        checkAnswer(df, Seq(
          Row(100, 1, "aa", "x"),
          Row(200, 1, "ac", "x"),
          Row(200, 3, "ad", "x")))
      }
    }
  }

  test("SPARK-58783: SPJ with runtime filtering when the join keys have a gap in the " +
      "partition keys") {
    val table1 = "table1"
    val table2 = "table2"
    // the join keys are the first and the third partition key of `table1`, so its scan partitioning
    // is projected down to `data, dept_id` when SPJ is applied. The partition key the projection
    // drops, `store_id`, sits between them, which is what makes the values it is compared against
    // line up with the wrong column.
    createTable(table1, columns2,
      Array(identity("data"), identity("store_id"), identity("dept_id")))
    sql(s"INSERT INTO testcat.ns.$table1 VALUES " +
        "(100, 1, 'aa'), " +
        "(200, 2, 'aa'), " +
        "(100, 3, 'bb')")

    createTable(table2, columns2, Array(identity("data"), identity("dept_id")))
    sql(s"INSERT INTO testcat.ns.$table2 VALUES " +
        "(500, 1, 'aa'), " +
        "(500, 2, 'aa'), " +
        "(500, 3, 'bb')")

    Seq(true, false).foreach { partiallyClustered =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "10",
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
            partiallyClustered.toString) {
        val df = sql(
          s"""
             |${selectWithMergeJoinHint("t1", "t2")}
             |t1.store_id, t1.dept_id, t1.data, t2.store_id AS t2store_id
             |FROM testcat.ns.$table1 t1 JOIN testcat.ns.$table2 t2
             |ON t1.data = t2.data AND t1.dept_id = t2.dept_id WHERE t2.data = 'aa'
             |ORDER BY t1.store_id, t1.dept_id
             |""".stripMargin)
        val scans = collectScans(df.queryExecution.executedPlan)
        assert(
          scans.exists(s =>
            s.runtimeFilters.nonEmpty && s.spjParams.joinKeyPositions.contains(Seq(0, 2))),
          "the dynamic filter should be pushed to the scan with the projected partitioning")
        val shuffles = collectShuffles(df.queryExecution.executedPlan)
        assert(shuffles.isEmpty, "SPJ should be triggered")
        checkAnswer(df, Seq(
          Row(100, 1, "aa", 500),
          Row(200, 2, "aa", 500)))
      }
    }
  }

  test("SPARK-58783: SPJ with runtime filtering when the projection changes the partition key " +
      "types") {
    val table1 = "table1"
    val table2 = "table2"
    // `data` is the second partition key, so the projection down to it leaves the wrapper with a
    // single `StringType`, which it then reads off the first field of the raw key - an
    // `IntegerType`
    createTable(table1, columns2, Array(identity("dept_id"), identity("data")))
    sql(s"INSERT INTO testcat.ns.$table1 VALUES " +
        "(100, 1, 'aa'), " +
        "(200, 2, 'aa'), " +
        "(300, 3, 'bb')")

    createTable(table2, columns2, Array(identity("data")))
    sql(s"INSERT INTO testcat.ns.$table2 VALUES " +
        "(500, 1, 'aa'), " +
        "(500, 2, 'bb')")

    Seq(true, false).foreach { partiallyClustered =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "10",
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
            partiallyClustered.toString) {
        val df = sql(
          s"""
             |${selectWithMergeJoinHint("t1", "t2")}
             |t1.store_id, t1.dept_id, t1.data, t2.store_id AS t2store_id
             |FROM testcat.ns.$table1 t1 JOIN testcat.ns.$table2 t2
             |ON t1.data = t2.data WHERE t2.dept_id = 1
             |ORDER BY t1.store_id, t1.dept_id
             |""".stripMargin)
        val scans = collectScans(df.queryExecution.executedPlan)
        assert(
          scans.exists(s =>
            s.runtimeFilters.nonEmpty && s.spjParams.joinKeyPositions.contains(Seq(1))),
          "the dynamic filter should be pushed to the scan with the projected partitioning")
        val shuffles = collectShuffles(df.queryExecution.executedPlan)
        assert(shuffles.isEmpty, "SPJ should be triggered")
        checkAnswer(df, Seq(
          Row(100, 1, "aa", 500),
          Row(200, 2, "aa", 500)))
      }
    }
  }

  test("SPARK-58783: SPJ with runtime filtering and partition filtering, without a projection") {
    val table1 = "table1"
    val table2 = "table2"
    // no projection here - the join keys are all of the partition keys. The partition values are
    // replaced by the intersection of the two sides instead, and `table1` has a partition key that
    // `table2` does not, so the keys it reports back after filtering are not all in that
    // intersection.
    createTable(table1, columns2, Array(identity("store_id"), identity("dept_id")))
    sql(s"INSERT INTO testcat.ns.$table1 VALUES " +
        "(100, 1, 'aa'), " +
        "(100, 2, 'ab'), " +
        "(200, 3, 'ac')")

    createTable(table2, columns2, Array(identity("store_id"), identity("dept_id")))
    sql(s"INSERT INTO testcat.ns.$table2 VALUES " +
        "(100, 1, 'x'), " +
        "(100, 2, 'y')")

    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "10",
      SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "true") {
      val df = sql(
        s"""
           |${selectWithMergeJoinHint("t1", "t2")}
           |t1.store_id, t1.dept_id, t1.data AS t1data, t2.data AS t2data
           |FROM testcat.ns.$table1 t1 JOIN testcat.ns.$table2 t2
           |ON t1.store_id = t2.store_id AND t1.dept_id = t2.dept_id WHERE t2.data = 'x'
           |ORDER BY t1.store_id, t1.dept_id
           |""".stripMargin)
      val scans = collectScans(df.queryExecution.executedPlan)
      assert(
        scans.exists(s => s.runtimeFilters.nonEmpty && s.table.name().endsWith(table1) &&
          s.spjParams.joinKeyPositions.isEmpty),
        s"the dynamic filter should be pushed to the unprojected $table1 scan")
      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.isEmpty, "SPJ should be triggered")
      checkAnswer(df, Seq(Row(100, 1, "aa", "x")))
    }
  }

  test("SPARK-58783: SPJ with runtime filtering and partition filtering on an outer join") {
    val table1 = "table1"
    val table2 = "table2"
    // for an outer join the partition values become one side's, so here they are `table1`'s and the
    // scan that reports keys outside them is `table2`, the side an outer join can prune
    createTable(table1, columns2, Array(identity("store_id"), identity("dept_id")))
    sql(s"INSERT INTO testcat.ns.$table1 VALUES " +
        "(100, 1, 'aa'), " +
        "(300, 5, 'ab')")

    createTable(table2, columns2, Array(identity("store_id"), identity("dept_id")))
    sql(s"INSERT INTO testcat.ns.$table2 VALUES " +
        "(100, 1, 'x'), " +
        "(200, 2, 'y')")

    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "10",
      SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "true") {
      val df = sql(
        s"""
           |${selectWithMergeJoinHint("t1", "t2")}
           |t1.store_id, t1.dept_id, t1.data AS t1data, t2.data AS t2data
           |FROM testcat.ns.$table1 t1 LEFT OUTER JOIN testcat.ns.$table2 t2
           |ON t1.store_id = t2.store_id AND t1.dept_id = t2.dept_id WHERE t1.data = 'aa'
           |ORDER BY t1.store_id, t1.dept_id
           |""".stripMargin)
      val scans = collectScans(df.queryExecution.executedPlan)
      assert(
        scans.exists(s => s.runtimeFilters.nonEmpty && s.table.name().endsWith(table2)),
        s"the dynamic filter should be pushed to the $table2 scan")
      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.isEmpty, "SPJ should be triggered")
      checkAnswer(df, Seq(Row(100, 1, "aa", "x")))
    }
  }

  test("SPARK-58783: SPJ with runtime filtering and compatible bucket transforms") {
    val table1 = "table1"
    val table2 = "table2"
    // the two sides bucket into different numbers of buckets, so the partition values pushed down
    // are reduced into the smaller bucket space while the keys the scan reports stay in its own
    createTable(table1, columns2, Array(bucket(4, "store_id"), bucket(4, "dept_id")))
    sql(s"INSERT INTO testcat.ns.$table1 VALUES " +
        "(0, 0, 'aa'), " +
        "(2, 3, 'ab')")

    createTable(table2, columns2, Array(bucket(2, "store_id"), bucket(2, "dept_id")))
    sql(s"INSERT INTO testcat.ns.$table2 VALUES " +
        "(0, 0, 'x'), " +
        "(1, 1, 'y')")

    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "10",
      SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
      val df = sql(
        s"""
           |${selectWithMergeJoinHint("t1", "t2")}
           |t1.store_id, t1.dept_id, t1.data AS t1data, t2.data AS t2data
           |FROM testcat.ns.$table1 t1 JOIN testcat.ns.$table2 t2
           |ON t1.store_id = t2.store_id AND t1.dept_id = t2.dept_id WHERE t2.data = 'x'
           |ORDER BY t1.store_id, t1.dept_id
           |""".stripMargin)
      val scans = collectScans(df.queryExecution.executedPlan)
      assert(
        scans.exists(s => s.runtimeFilters.nonEmpty && s.spjParams.reducers.nonEmpty),
        "the dynamic filter should be pushed to the scan whose partition keys are reduced")
      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.isEmpty, "SPJ should be triggered")
      checkAnswer(df, Seq(Row(0, 0, "aa", "x")))
    }
  }

  test("SPARK-60203: SPJ with join-key subset projects originalPartitionValues") {
    val table1 = "table1"
    val table2 = "table2"
    val cols = Array(
      Column.create("id", StringType),
      Column.create("date", StringType),
      Column.create("data", StringType))
    // The join key is the second partition key of `table1` (`bucket(32, id)`), after the
    // leading identity key `date`. After SPJ pushdown, the scan's `outputPartitioning`
    // must carry `originalPartitionValues` projected to the join keys. When they are not,
    // `KeyGroupedPartitioning.createShuffleSpec` re-projects the full rows with the trimmed
    // expressions; connectors such as Iceberg throw a type mismatch, while the in-memory
    // source here only misaligns the values.
    createTable(table1, cols,
      Array(identity("date"), bucket(32, "id"), bucket(32, "data")))
    sql(s"INSERT INTO testcat.ns.$table1 VALUES " +
        "('a', '2026-05-01', 'aa'), " +
        "('a', '2026-05-02', 'cc'), " +
        "('b', '2026-05-01', 'bb'), " +
        "('c', '2026-05-02', 'aa')")

    createTable(table2, cols, Array(bucket(32, "id")))
    sql(s"INSERT INTO testcat.ns.$table2 VALUES " +
        "('a', '2026-05-01', 'xx'), " +
        "('b', '2026-05-01', 'yy'), " +
        "('d', '2026-05-01', 'zz')")

    Seq(true, false).foreach { partiallyClustered =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.SKEW_JOIN_ENABLED.key -> "true",
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
            partiallyClustered.toString) {
        val df = sql(
          s"""
             |${selectWithMergeJoinHint("t1", "t2")}
             |t1.id, t1.date, t1.data AS t1data, t2.data AS t2data
             |FROM testcat.ns.$table1 t1 JOIN testcat.ns.$table2 t2
             |ON t1.id = t2.id
             |ORDER BY t1.id, t1.date, t1data
             |""".stripMargin)
        val executedPlan = df.queryExecution.executedPlan
        val scans = collectScans(executedPlan)
        assert(
          scans.exists(_.spjParams.joinKeyPositions.contains(Seq(1))),
          "SPJ should project the scan partitioning down to the bucket join key")
        val shuffles = collectShuffles(executedPlan)
        assert(shuffles.isEmpty, "SPJ should be triggered")
        val smjs = collect(executedPlan) { case j: SortMergeJoinExec => j }
        assert(smjs.size == 1, "expected a sort-merge join")
        val projectedScan = scans.find(_.spjParams.joinKeyPositions.contains(Seq(1))).get
        val kgp = projectedScan.outputPartitioning.asInstanceOf[KeyGroupedPartitioning]
        assert(kgp.originalPartitionValues.nonEmpty)
        assert(kgp.originalPartitionValues.forall(_.numFields == kgp.expressions.length),
          "originalPartitionValues should be projected to the join keys")
        // Each split's `bucket(32, id)` value, the second field of its raw partition key.
        val expectedOriginals = projectedScan.inputPartitions.map(
          _.asInstanceOf[HasPartitionKey].partitionKey().getInt(1))
        assert(kgp.originalPartitionValues.size == expectedOriginals.size)
        assert(kgp.originalPartitionValues.map(_.getInt(0)).sorted == expectedOriginals.sorted)
        checkAnswer(df, Seq(
          Row("a", "2026-05-01", "aa", "xx"),
          Row("a", "2026-05-02", "cc", "xx"),
          Row("b", "2026-05-01", "bb", "yy")))
      }
    }
  }

  test("SPARK-60232: nested SPJ preserves inner join key positions") {
    val cols = Array(
      Column.create("store_id", IntegerType),
      Column.create("dept_id", IntegerType),
      Column.create("data", StringType))
    // t4 is partitioned by (store_id, dept_id) and joins on dept_id only. The inner join
    // pushes joinKeyPositions [1] into the t4 scan. The outer join must not overwrite that
    // with [0] against the already projected dept_id, or t4 is grouped by store_id and
    // the three-way join returns no rows.
    createTable("t4", cols, Array(identity("store_id"), identity("dept_id")))
    sql("INSERT INTO testcat.ns.t4 VALUES (1, 2, 'a'), (2, 3, 'b'), (3, 1, 'c')")

    createTable("t5", cols, Array(identity("dept_id")))
    sql("INSERT INTO testcat.ns.t5 VALUES (0, 1, 'x1'), (0, 2, 'x2'), (0, 3, 'x3')")

    createTable("t6", cols, Array(identity("dept_id")))
    sql("INSERT INTO testcat.ns.t6 VALUES (0, 1, 'x1'), (0, 2, 'x2'), (0, 3, 'x3')")

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
        val df = sql(
          """
            |SELECT /*+ MERGE(t4, t5, t6) */
            |  t4.store_id, t4.dept_id, t4.data, t5.data, t6.data
            |FROM testcat.ns.t4
            |JOIN testcat.ns.t5 ON t4.dept_id = t5.dept_id
            |JOIN testcat.ns.t6 ON t4.dept_id = t6.dept_id
            |ORDER BY t4.store_id, t4.dept_id
            |""".stripMargin)

        val plan = df.queryExecution.executedPlan
        assert(collectShuffles(plan).isEmpty, "SPJ should be triggered for both joins")
        val t4Scan = collectScans(plan).find(_.table.name().endsWith("t4")).get
        assert(t4Scan.spjParams.joinKeyPositions.contains(Seq(1)),
          "t4 scan should keep the inner join's key position [1], not [0]")
        checkAnswer(df, Seq(
          Row(1, 2, "a", "x2", "x2"),
          Row(2, 3, "b", "x3", "x3"),
          Row(3, 1, "c", "x1", "x1")))
      }
    }
  }

  test("SPARK-60232: nested SPJ with mixed partition key types preserves key positions") {
    val cols = Array(
      Column.create("id", StringType),
      Column.create("date", StringType),
      Column.create("data", StringType))
    // Same nested join-key overwrite as above, but the leading partition key has a different
    // type than the bucket join key. Without the fix this shape throws ClassCastException in
    // connectors such as Iceberg; here the in-memory source only misaligns values.
    createTable("t1", cols,
      Array(identity("date"), bucket(32, "id"), bucket(32, "data")))
    sql("INSERT INTO testcat.ns.t1 VALUES " +
        "('a', '2026-05-01', 'aa'), " +
        "('a', '2026-05-02', 'cc'), " +
        "('b', '2026-05-01', 'bb')")

    createTable("t2", cols, Array(bucket(32, "id")))
    sql("INSERT INTO testcat.ns.t2 VALUES " +
        "('a', '2026-05-01', 'xx'), " +
        "('b', '2026-05-01', 'yy')")

    createTable("t3", cols, Array(bucket(32, "id")))
    sql("INSERT INTO testcat.ns.t3 VALUES " +
        "('a', '2026-05-01', 'xx'), " +
        "('b', '2026-05-01', 'yy')")

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
        val df = sql(
          """
            |SELECT /*+ MERGE(t1, t2, t3) */
            |  t1.id, t1.date, t1.data, t2.data, t3.data
            |FROM testcat.ns.t1
            |JOIN testcat.ns.t2 ON t1.id = t2.id
            |JOIN testcat.ns.t3 ON t1.id = t3.id
            |ORDER BY t1.id, t1.date, t1.data
            |""".stripMargin)

        val plan = df.queryExecution.executedPlan
        assert(collectShuffles(plan).isEmpty, "SPJ should be triggered for both joins")
        val t1Scan = collectScans(plan).find(_.table.name().endsWith("t1")).get
        assert(t1Scan.spjParams.joinKeyPositions.contains(Seq(1)),
          "t1 scan should keep the inner join's key position [1], not [0]")
        checkAnswer(df, Seq(
          Row("a", "2026-05-01", "aa", "xx", "xx"),
          Row("a", "2026-05-02", "cc", "xx", "xx"),
          Row("b", "2026-05-01", "bb", "yy", "yy")))
      }
    }
  }

  test("SPARK-60232: nested SPJ preserves inner join reducers") {
    val idCols = Array(Column.create("id", IntegerType))
    createTable("b4", idCols, Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.b4 VALUES (4)")

    createTable("b12", idCols, Array(bucket(12, "id")))
    sql("INSERT INTO testcat.ns.b12 VALUES (4)")

    createTable("b8", idCols, Array(bucket(8, "id")))
    sql("INSERT INTO testcat.ns.b8 VALUES (1), (4)")

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
        val df = sql(
          """
            |SELECT /*+ MERGE(b4, b12, b8) */ b4.id, b12.id, b8.id
            |FROM testcat.ns.b4
            |JOIN testcat.ns.b12 ON b4.id = b12.id
            |JOIN testcat.ns.b8 ON b4.id = b8.id
            |""".stripMargin)

        val plan = df.queryExecution.executedPlan
        assert(collectShuffles(plan).isEmpty, "SPJ should be triggered for both joins")
        val scans = collectScans(plan)
        def reducersOf(table: String): Option[Seq[Option[Reducer[_, _]]]] =
          scans.find(_.table.name().endsWith(s".$table")).get.spjParams.reducers
        assert(reducersOf("b4").isEmpty, "b4 is the coarsest side and needs no reducer")
        assert(reducersOf("b12").exists(_.length == 1) && reducersOf("b12").get.head.isDefined,
          "b12 scan should keep the inner join's reducer onto bucket(4, id)")
        assert(reducersOf("b8").exists(_.length == 1) && reducersOf("b8").get.head.isDefined,
          "b8 scan should get the outer join's reducer onto bucket(4, id)")
        checkAnswer(df, Seq(Row(4, 4, 4)))
      }
    }
  }

  test("SPARK-60232: nested SPJ preserves join key positions over common partition values") {
    val items2 = "items2"
    val itemsPartitions = Array(identity("id"), identity("name"))

    val inserts = Seq(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 10.0, cast('2020-01-01' as timestamp)), " +
        "(2, 'bb', 20.0, cast('2020-01-01' as timestamp))",
      s"INSERT INTO testcat.ns.$items2 VALUES " +
        "(1, 'aa', 10.0, cast('2020-01-01' as timestamp)), " +
        "(2, 'bb', 20.0, cast('2020-01-01' as timestamp))",
      "INSERT INTO testcat.ns.purchases VALUES " +
        "(1, 100.0, cast('2020-01-01' as timestamp)), " +
        "(2, 200.0, cast('2020-01-01' as timestamp))")
    val expected = Seq(Row(1, "aa", 10.0, 100.0), Row(2, "bb", 20.0, 200.0))
    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
        catalog.clearTables()
        createTable(items, itemsColumns, itemsPartitions)
        createTable(items2, itemsColumns, itemsPartitions)
        createTable(purchases, purchasesColumns, Array.empty)
        inserts.foreach(sql)

        val df = sql(
          s"""
             |SELECT /*+ MERGE(i, j, p) */ i.id, i.name, i.price, p.price
             |FROM testcat.ns.$items i JOIN testcat.ns.$items2 j
             |ON i.id = j.id AND i.name = j.name
             |JOIN testcat.ns.$purchases p ON i.id = p.item_id
             |ORDER BY i.id, i.name
             |""".stripMargin)

        val plan = df.queryExecution.executedPlan
        assert(collectShuffles(plan).size == 1,
          "only the unpartitioned purchases side should shuffle")
        val itemsScan = collectScans(plan).find(_.table.name().endsWith(items)).get
        assert(itemsScan.spjParams.commonPartitionValues.nonEmpty,
          "inner join should push common partition values into the items scan")
        assert(itemsScan.spjParams.joinKeyPositions.contains(Seq(0)),
          "outer join should project common partition values down to the id join key")
        checkAnswer(df, expected)
      }
    }
  }

  test("SPARK-60232: nested SPJ with multiple names per id over common partition values") {
    val items2 = "items2"
    val itemsPartitions = Array(identity("id"), identity("name"))
    val inserts = Seq(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 10.0, cast('2020-01-01' as timestamp)), " +
        "(1, 'bb', 20.0, cast('2020-01-02' as timestamp))",
      s"INSERT INTO testcat.ns.$items2 VALUES " +
        "(1, 'aa', 10.0, cast('2020-01-01' as timestamp)), " +
        "(1, 'bb', 20.0, cast('2020-01-02' as timestamp))",
      "INSERT INTO testcat.ns.purchases VALUES " +
        "(1, 100.0, cast('2020-01-01' as timestamp))")
    val expected = Seq(Row(1, "aa", 10.0, 100.0), Row(1, "bb", 20.0, 100.0))
    Seq(true, false).foreach { aqe =>
        catalog.clearTables()
        createTable(items, itemsColumns, itemsPartitions)
        createTable(items2, itemsColumns, itemsPartitions)
        createTable(purchases, purchasesColumns, Array.empty)
        inserts.foreach(sql)

        withSQLConf(
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
          SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
          val df = sql(
            s"""
               |SELECT /*+ MERGE(i, j, p) */ i.id, i.name, i.price, p.price
               |FROM testcat.ns.$items i JOIN testcat.ns.$items2 j
               |ON i.id = j.id AND i.name = j.name
               |JOIN testcat.ns.$purchases p ON i.id = p.item_id
               |ORDER BY i.id, i.name
               |""".stripMargin)

          val plan = df.queryExecution.executedPlan
          assert(collectShuffles(plan).size == 1,
            "only the unpartitioned purchases side should shuffle")
          checkAnswer(df, expected)
        }
    }
  }

  private def createNestedSpjIdTables(): Unit = {
    val cols = Array(
      Column.create("store", IntegerType),
      Column.create("id", IntegerType),
      Column.create("data", StringType))
    Seq("e1", "e2", "e3", "e5").foreach(t => createTable(t, cols, Array(identity("id"))))
    sql("INSERT INTO testcat.ns.e1 VALUES (0, 1, 'a'), (0, 2, 'b')")
    sql("INSERT INTO testcat.ns.e2 VALUES (0, 1, 'a'), (0, 2, 'b')")
    sql("INSERT INTO testcat.ns.e3 VALUES (0, 1, 'a'), (0, 2, 'b'), (0, 3, 'c')")
    sql("INSERT INTO testcat.ns.e5 VALUES (0, 2, 'b'), (0, 3, 'c')")
    createTable("e4", cols, Array(identity("store"), identity("id")))
    sql("INSERT INTO testcat.ns.e4 VALUES (10, 1, 'a'), (20, 2, 'b'), (30, 3, 'c')")
  }

  test("SPARK-60232: nested SPJ pushes outer merged values into a nested join") {
    createNestedSpjIdTables()
    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "true") {
        val df = sql(
          """
            |SELECT /*+ MERGE(e1, e2, e5) */ e1.id, e2.id, e5.id
            |FROM testcat.ns.e1 JOIN testcat.ns.e2 ON e1.id = e2.id
            |RIGHT JOIN testcat.ns.e5 ON e1.id = e5.id
            |""".stripMargin)
        checkAnswer(df, Seq(Row(2, 2, 2), Row(null, null, 3)))
      }

      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
        val df = sql(
          """
            |SELECT /*+ MERGE(e1, e2, e3) */ e1.id, e2.id, e3.id
            |FROM testcat.ns.e1 JOIN testcat.ns.e2 ON e1.id = e2.id
            |JOIN testcat.ns.e3 ON e1.id = e3.id
            |""".stripMargin)
        assert(collectShuffles(df.queryExecution.executedPlan).isEmpty,
          "SPJ should be triggered for both joins")
        checkAnswer(df, Seq(Row(1, 1, 1), Row(2, 2, 2)))
      }
    }
  }

  test("SPARK-60232: nested SPJ with subset join keys on a two-key partitioned side") {
    createNestedSpjIdTables()
    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
        val df = sql(
          """
            |SELECT /*+ MERGE(e1, e2, e4) */ e1.id, e2.id, e4.id, e4.store
            |FROM testcat.ns.e1 JOIN testcat.ns.e2 ON e1.id = e2.id
            |JOIN testcat.ns.e4 ON e1.id = e4.id
            |""".stripMargin)
        checkAnswer(df, Seq(Row(1, 1, 1, 10), Row(2, 2, 2, 20)))
      }
    }
  }

  test("SPARK-60232: nested SPJ on all partition keys, then on a subset") {
    val cols = Array(
      Column.create("store_id", IntegerType),
      Column.create("dept_id", IntegerType),
      Column.create("data", StringType))
    createTable("a", cols, Array(identity("store_id"), identity("dept_id")))
    createTable("b", cols, Array(identity("store_id"), identity("dept_id")))
    createTable("c", cols, Array(identity("dept_id")))
    sql("INSERT INTO testcat.ns.a VALUES (1, 2, 'a1'), (2, 2, 'a2'), (3, 1, 'a3')")
    sql("INSERT INTO testcat.ns.b VALUES (1, 2, 'b1'), (2, 2, 'b2'), (3, 1, 'b3')")
    sql("INSERT INTO testcat.ns.c VALUES (0, 1, 'c1'), (0, 2, 'c2'), (0, 3, 'c3')")

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
        val expected = Seq(
          Row(1, 2, "a1", 1, 2, "b1", 0, 2, "c2"),
          Row(2, 2, "a2", 2, 2, "b2", 0, 2, "c2"),
          Row(3, 1, "a3", 3, 1, "b3", 0, 1, "c1"))
        Seq("a.dept_id", "b.dept_id").foreach { outerKey =>
          val df = sql(
            s"""
               |SELECT /*+ MERGE(a, b, c) */ *
               |FROM testcat.ns.a JOIN testcat.ns.b
               |ON a.store_id = b.store_id AND a.dept_id = b.dept_id
               |JOIN testcat.ns.c ON $outerKey = c.dept_id
               |""".stripMargin)
          checkAnswer(df, expected)
        }
      }
    }
  }

  test("SPARK-60232: nested SPJ dedupes common values that project to the same join key") {
    val cols = Array(
      Column.create("store_id", IntegerType),
      Column.create("dept_id", IntegerType),
      Column.create("data", StringType))
    createTable("a", cols, Array(identity("store_id"), identity("dept_id")))
    createTable("b", cols, Array(identity("store_id"), identity("dept_id")))
    createTable("c", cols, Array(identity("dept_id")))
    sql("INSERT INTO testcat.ns.a VALUES (1, 2, 'a1'), (2, 2, 'a2')")
    sql("INSERT INTO testcat.ns.b VALUES (1, 2, 'b1'), (2, 2, 'b2')")
    sql("INSERT INTO testcat.ns.c VALUES (0, 1, 'c1'), (0, 2, 'c2'), (0, 3, 'c3')")

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
        val df = sql(
          """
            |SELECT /*+ MERGE(a, b, c) */ a.data, b.data, c.data
            |FROM testcat.ns.a JOIN testcat.ns.b
            |ON a.store_id = b.store_id AND a.dept_id = b.dept_id
            |JOIN testcat.ns.c ON a.dept_id = c.dept_id
            |""".stripMargin)
        checkAnswer(df, Seq(Row("a1", "b1", "c2"), Row("a2", "b2", "c2")))
      }
    }
  }

  test("SPARK-60232: nested SPJ with several names per id and a partitioned third side") {
    val cols = Array(
      Column.create("id", IntegerType),
      Column.create("name", StringType),
      Column.create("price", DoubleType))
    createTable("i", cols, Array(identity("id"), identity("name")))
    createTable("j", cols, Array(identity("id"), identity("name")))
    createTable("k", Array(Column.create("id", IntegerType), Column.create("v", DoubleType)),
      Array(identity("id")))
    sql("INSERT INTO testcat.ns.i VALUES (1, 'aa', 1.0), (1, 'bb', 2.0)")
    sql("INSERT INTO testcat.ns.j VALUES (1, 'aa', 1.0), (1, 'bb', 2.0)")
    sql("INSERT INTO testcat.ns.k VALUES (1, 10.0), (2, 20.0)")

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
        checkAnswer(
          sql(
            """
              |SELECT /*+ MERGE(i, j, k) */ i.id, i.name, j.name, k.v
              |FROM testcat.ns.i JOIN testcat.ns.j ON i.id = j.id AND i.name = j.name
              |JOIN testcat.ns.k ON i.id = k.id
              |""".stripMargin),
          Seq(Row(1, "aa", "aa", 10.0), Row(1, "bb", "bb", 10.0)))
        checkAnswer(
          sql(
            """
              |SELECT /*+ MERGE(i, j, k) */ i.id, i.name, k.v
              |FROM testcat.ns.i LEFT ANTI JOIN testcat.ns.j
              |ON i.id = j.id AND i.name = j.name
              |JOIN testcat.ns.k ON i.id = k.id
              |""".stripMargin),
          Seq.empty)
      }
    }
  }

  test("SPARK-60232: four-way nested SPJ joining on a key dropped by an earlier join") {
    val cols = Array(
      Column.create("id", IntegerType),
      Column.create("name", StringType),
      Column.create("price", DoubleType))
    createTable("i", cols, Array(identity("id"), identity("name")))
    createTable("j", cols, Array(identity("id"), identity("name")))
    createTable("k", Array(Column.create("id", IntegerType), Column.create("v", DoubleType)),
      Array(identity("id")))
    createTable("m", Array(Column.create("name", StringType), Column.create("w", DoubleType)),
      Array(identity("name")))
    sql("INSERT INTO testcat.ns.i VALUES (1, 'aa', 1.0), (2, 'bb', 2.0)")
    sql("INSERT INTO testcat.ns.j VALUES (1, 'aa', 1.0), (2, 'bb', 2.0)")
    sql("INSERT INTO testcat.ns.k VALUES (1, 10.0), (2, 20.0)")
    sql("INSERT INTO testcat.ns.m VALUES ('aa', 100.0), ('bb', 200.0)")

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
        val df = sql(
          """
            |SELECT /*+ MERGE(i, j, k, m) */ i.id, i.name, k.v, m.w
            |FROM testcat.ns.i JOIN testcat.ns.j ON i.id = j.id AND i.name = j.name
            |JOIN testcat.ns.k ON i.id = k.id
            |JOIN testcat.ns.m ON i.name = m.name
            |""".stripMargin)
        checkAnswer(df, Seq(Row(1, "aa", 10.0, 100.0), Row(2, "bb", 20.0, 200.0)))
      }
    }
  }

  test("SPARK-60232: nested SPJ with compatible transforms across three bucket counts") {
    val idCols = Array(Column.create("id", IntegerType))
    Seq("b2" -> 2, "b4" -> 4, "b8" -> 8, "b12" -> 12, "c12" -> 12).foreach { case (t, n) =>
      createTable(t, idCols, Array(bucket(n, "id")))
      sql(s"INSERT INTO testcat.ns.$t SELECT id FROM range(24)")
    }
    val expected = (0 until 24).map(i => Row(i, i, i))

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
        // When the outer key is on the reduced side, the outer join falls back to a shuffle.
        Seq(
          ("b12", "b4", "c12", 2),
          ("b4", "b12", "c12", 0),
          ("b8", "b4", "b2", 2),
          ("b4", "b8", "b2", 0)).foreach { case (t1, t2, t3, numShuffles) =>
          val df = sql(
            s"""
               |SELECT /*+ MERGE($t1, $t2, $t3) */ $t1.id, $t2.id, $t3.id
               |FROM testcat.ns.$t1 JOIN testcat.ns.$t2 ON $t1.id = $t2.id
               |JOIN testcat.ns.$t3 ON $t1.id = $t3.id
               |""".stripMargin)
          assert(collectShuffles(df.queryExecution.executedPlan).size == numShuffles,
            s"$t1 JOIN $t2 JOIN $t3 should plan $numShuffles shuffles")
          checkAnswer(df, expected)
        }
      }
    }
  }

  test("SPARK-60232: nested SPJ narrows common values already projected by an inner join") {
    val cols = Array(
      Column.create("a", IntegerType),
      Column.create("b", IntegerType),
      Column.create("c", IntegerType))
    createTable("ta", cols, Array(identity("a"), identity("b"), identity("c")))
    createTable("tb", cols, Array(identity("b"), identity("c")))
    createTable("tc", cols, Array.empty)
    sql("INSERT INTO testcat.ns.ta VALUES (1, 10, 100), (2, 20, 100), (3, 30, 300)")
    sql("INSERT INTO testcat.ns.tb VALUES (0, 10, 100), (0, 20, 100), (0, 30, 300)")
    sql("INSERT INTO testcat.ns.tc VALUES (0, 0, 100), (0, 0, 300)")

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
        val df = sql(
          """
            |SELECT /*+ MERGE(ta, tb, tc) */ ta.a, ta.b, ta.c, tc.c
            |FROM testcat.ns.ta JOIN testcat.ns.tb ON ta.b = tb.b AND ta.c = tb.c
            |JOIN testcat.ns.tc ON ta.c = tc.c
            |""".stripMargin)
        checkAnswer(df, Seq(Row(1, 10, 100, 100), Row(2, 20, 100, 100), Row(3, 30, 300, 300)))
      }
    }
  }

  test("SPARK-60232: nested SPJ keeps projected values in the order a one-side shuffle uses") {
    val cols = Array(
      Column.create("store", IntegerType),
      Column.create("dept", IntegerType),
      Column.create("data", StringType))
    createTable("oa", cols, Array(identity("store"), identity("dept")))
    createTable("ob", cols, Array(identity("store"), identity("dept")))
    createTable("oc", cols, Array.empty)
    // The projected dept keys come out as 20, 10, which is not their sorted order.
    sql("INSERT INTO testcat.ns.oa VALUES (1, 20, 'a1'), (2, 10, 'a2')")
    sql("INSERT INTO testcat.ns.ob VALUES (1, 20, 'b1'), (2, 10, 'b2')")
    sql("INSERT INTO testcat.ns.oc VALUES (0, 10, 'c10'), (0, 20, 'c20')")

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
        val df = sql(
          """
            |SELECT /*+ MERGE(oa, ob, oc) */ oa.store, oa.data, ob.data, oc.data
            |FROM testcat.ns.oa JOIN testcat.ns.ob ON oa.store = ob.store AND oa.dept = ob.dept
            |JOIN testcat.ns.oc ON oa.dept = oc.dept
            |""".stripMargin)
        assert(collectShuffles(df.queryExecution.executedPlan).size == 1,
          "only the unpartitioned oc side should shuffle")
        checkAnswer(df, Seq(Row(1, "a1", "b1", "c20"), Row(2, "a2", "b2", "c10")))
      }
    }
  }

  test("SPARK-60232: nested SPJ does not shuffle the other side onto a reduced scan's layout") {
    val idCols = Array(Column.create("id", LongType))
    Seq("l12" -> 12, "l4" -> 4, "l2" -> 2).foreach { case (t, n) =>
      createTable(t, idCols, Array(bucket(n, "id")))
      sql(s"INSERT INTO testcat.ns.$t SELECT id FROM range(24)")
    }
    createTable("ld", idCols, Array.empty)
    sql("INSERT INTO testcat.ns.ld SELECT id FROM range(24)")
    val expected = (0L until 24L).map(i => Row(i, i, i))

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
        Seq("l2" -> 1, "ld" -> 2).foreach { case (third, numShuffles) =>
          val df = sql(
            s"""
               |SELECT /*+ MERGE(l12, l4, $third) */ l12.id, l4.id, $third.id
               |FROM testcat.ns.l12 JOIN testcat.ns.l4 ON l12.id = l4.id
               |JOIN testcat.ns.$third ON l12.id = $third.id
               |""".stripMargin)
          assert(collectShuffles(df.queryExecution.executedPlan).size == numShuffles,
            s"the join with $third should plan $numShuffles shuffles")
          checkAnswer(df, expected)
        }

        // An outer join reports a single partitioning rather than a collection.
        val outerDf = sql(
          """
            |SELECT /*+ MERGE(l12, l4, ld) */ l12.id, l4.id, ld.id
            |FROM testcat.ns.l12 LEFT JOIN testcat.ns.l4 ON l12.id = l4.id
            |JOIN testcat.ns.ld ON l12.id = ld.id
            |""".stripMargin)
        checkAnswer(outerDf, expected)
      }
    }
  }

  test("SPARK-60232: nested SPJ keeps SPJ on a key the inner join did not reduce") {
    val cols = Array(Column.create("store", IntegerType), Column.create("id", IntegerType))
    createTable("ra", cols, Array(identity("store"), bucket(12, "id")))
    createTable("rb", cols, Array(identity("store"), bucket(4, "id")))
    createTable("rc", Array(Column.create("store", IntegerType)), Array(identity("store")))
    sql("INSERT INTO testcat.ns.ra SELECT CAST(id % 3 AS INT), CAST(id AS INT) FROM range(12)")
    sql("INSERT INTO testcat.ns.rb SELECT CAST(id % 3 AS INT), CAST(id AS INT) FROM range(12)")
    sql("INSERT INTO testcat.ns.rc VALUES (0), (1), (2)")

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
        val df = sql(
          """
            |SELECT /*+ MERGE(ra, rb, rc) */ ra.id, rb.id, rc.store
            |FROM testcat.ns.ra JOIN testcat.ns.rb ON ra.store = rb.store AND ra.id = rb.id
            |JOIN testcat.ns.rc ON ra.store = rc.store
            |""".stripMargin)
        assert(collectShuffles(df.queryExecution.executedPlan).isEmpty,
          "SPJ should be triggered for both joins")
        checkAnswer(df, (0 until 12).map(i => Row(i, i, i % 3)))
      }
    }
  }

  test("SPARK-60232: nested SPJ does not push into a broadcast build side") {
    createTable("qa", Array(Column.create("id", IntegerType), Column.create("x", StringType)),
      Array(identity("id")))
    createTable("qd", Array(Column.create("x", StringType), Column.create("region", IntegerType)),
      Array(identity("region")))
    createTable("qc", Array(Column.create("id", IntegerType)), Array(identity("id")))
    sql("INSERT INTO testcat.ns.qa VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    sql("INSERT INTO testcat.ns.qd VALUES ('a', 10), ('b', 20), ('c', 30)")
    sql("INSERT INTO testcat.ns.qc VALUES (1), (2), (3)")

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
        val df = sql(
          """
            |SELECT /*+ BROADCAST(qd) */ qa.id, qd.region, qc.id
            |FROM testcat.ns.qa JOIN testcat.ns.qd ON qa.x = qd.x
            |JOIN testcat.ns.qc ON qa.id = qc.id
            |""".stripMargin)
        checkAnswer(df, Seq(Row(1, 10, 1), Row(2, 20, 2), Row(3, 30, 3)))
      }
    }
  }

  test("SPARK-60232: ordering a subset-key SPJ keeps the join's key positions") {
    val cols = Array(Column.create("a", IntegerType), Column.create("b", IntegerType))
    createTable("oa3", cols, Array(identity("a"), identity("b")))
    sql("INSERT INTO testcat.ns.oa3 VALUES (1, 10), (2, 20), (3, 10), (4, 30)")

    createTable("ob3", cols, Array(identity("b")))
    sql("INSERT INTO testcat.ns.ob3 VALUES (0, 10), (0, 20)")

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_SORTING_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
        val df = sql(
          """
            |SELECT /*+ MERGE(oa3, ob3) */ oa3.b, oa3.a, ob3.a
            |FROM testcat.ns.oa3 LEFT JOIN testcat.ns.ob3 ON oa3.b = ob3.b
            |ORDER BY oa3.b
            |""".stripMargin)

        // Rows with the same b may come in either order, so check the order on b alone.
        val rows = df.collect().toSeq
        assert(rows.map(_.getInt(0)) == Seq(10, 10, 20, 30), "rows should be ordered by b")
        assert(rows.toSet ==
          Set(Row(10, 1, 0), Row(10, 3, 0), Row(20, 2, 0), Row(30, 4, null)))
      }
    }
  }

  test("SPARK-60232: nested SPJ sees a reduced scan through an alias") {
    val idCols = Array(Column.create("id", LongType))
    Seq("l12" -> 12, "l4" -> 4, "l3" -> 3).foreach { case (t, n) =>
      createTable(t, idCols, Array(bucket(n, "id")))
      sql(s"INSERT INTO testcat.ns.$t SELECT id FROM range(24)")
    }

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
        // The nested join may also output the original column next to its alias.
        Seq("", "l12.id, ").foreach { extraColumn =>
          val df = sql(
            s"""
               |SELECT /*+ MERGE(x, l3) */ x.id2, x.id4, l3.id
               |FROM (
               |  SELECT /*+ MERGE(l12, l4) */ ${extraColumn}l12.id AS id2, l4.id AS id4
               |  FROM testcat.ns.l12 JOIN testcat.ns.l4 ON l12.id = l4.id) x
               |JOIN testcat.ns.l3 ON x.id2 = l3.id
               |""".stripMargin)
          assert(collectShuffles(df.queryExecution.executedPlan).size == 2,
            "the outer join should fall back to shuffling both sides")
          checkAnswer(df, (0L until 24L).map(i => Row(i, i, i)))
        }
      }
    }
  }

  test("SPARK-60232: nested SPJ shuffles a compatible nested join built on a reduced scan") {
    val idCols = Array(Column.create("id", LongType))
    Seq("nb4" -> 4, "nb12" -> 12).foreach { case (t, n) =>
      createTable(t, idCols, Array(bucket(n, "id")))
      sql(s"INSERT INTO testcat.ns.$t SELECT id FROM range(24)")
    }
    createTable("nc6", idCols, Array(bucket(6, "id")))
    sql("INSERT INTO testcat.ns.nc6 VALUES (0), (1), (2), (3), (6)")

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
        val df = sql(
          """
            |SELECT /*+ MERGE(nb4, nb12, nc6) */ nb4.id, nb12.id, nc6.id
            |FROM testcat.ns.nb4 JOIN testcat.ns.nb12 ON nb4.id = nb12.id
            |JOIN testcat.ns.nc6 ON nb12.id = nc6.id
            |""".stripMargin)
        assert(collectShuffles(df.queryExecution.executedPlan).size == 1,
          "the nested join should be shuffled onto nc6's layout")
        checkAnswer(df, Seq(0L, 1L, 2L, 3L, 6L).map(i => Row(i, i, i)))

        // An outer join reports a single partitioning rather than a collection.
        val outerDf = sql(
          """
            |SELECT /*+ MERGE(nb4, nb12, nc6) */ nb12.id, nb4.id, nc6.id
            |FROM testcat.ns.nb12 LEFT JOIN testcat.ns.nb4 ON nb12.id = nb4.id
            |JOIN testcat.ns.nc6 ON nb12.id = nc6.id
            |""".stripMargin)
        assert(collectShuffles(outerDf.queryExecution.executedPlan).size == 1,
          "the outer join below should be shuffled onto nc6's layout")
        checkAnswer(outerDf, Seq(0L, 1L, 2L, 3L, 6L).map(i => Row(i, i, i)))
      }
    }
  }

  test("SPARK-60232: nested SPJ does not reshuffle a nested join above a shuffle") {
    val idCols = Array(Column.create("id", LongType))
    Seq("l12" -> 12, "l4" -> 4, "c12" -> 12, "d12" -> 12).foreach { case (t, n) =>
      createTable(t, idCols, Array(bucket(n, "id")))
      sql(s"INSERT INTO testcat.ns.$t SELECT id FROM range(24)")
    }

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
        // The second join shuffles the nested one onto c12's layout. The reduced l12 below that
        // shuffle no longer decides the layout the third join sees.
        val df = sql(
          """
            |SELECT /*+ MERGE(l12, l4, c12, d12) */ l12.id, l4.id, c12.id, d12.id
            |FROM testcat.ns.l12 JOIN testcat.ns.l4 ON l12.id = l4.id
            |JOIN testcat.ns.c12 ON l12.id = c12.id
            |JOIN testcat.ns.d12 ON l12.id = d12.id
            |""".stripMargin)
        // collectShuffles would count that shuffle once for each join above it.
        assert(collectAllShuffles(df.queryExecution.executedPlan).size == 1,
          "only the nested join should be shuffled, once")
        checkAnswer(df, (0L until 24L).map(i => Row(i, i, i, i)))
      }
    }
  }

  test("SPARK-60232: nested SPJ sees a reduced scan through a broadcast join's build key") {
    val idCols = Array(Column.create("id", LongType))
    Seq("l12" -> 12, "l4" -> 4, "l3" -> 3).foreach { case (t, n) =>
      createTable(t, idCols, Array(bucket(n, "id")))
      sql(s"INSERT INTO testcat.ns.$t SELECT id FROM range(24)")
    }
    createTable("lx", idCols, Array.empty)
    sql("INSERT INTO testcat.ns.lx SELECT id FROM range(24)")

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
        // The broadcast join also reports the reduced l12's partitioning on lx.id.
        val df = sql(
          """
            |SELECT /*+ MERGE(l12, l4), BROADCAST(lx), MERGE(l3) */ l12.id, lx.id, l3.id
            |FROM testcat.ns.l12 JOIN testcat.ns.l4 ON l12.id = l4.id
            |JOIN testcat.ns.lx ON l12.id = lx.id
            |JOIN testcat.ns.l3 ON lx.id = l3.id
            |""".stripMargin)
        checkAnswer(df, (0L until 24L).map(i => Row(i, i, i)))
      }
    }
  }

  test("SPARK-60232: nested SPJ survives EnsureRequirements re-running for a skewed join") {
    val idCols = Array(Column.create("id", LongType))
    Seq("b4" -> 4, "b12" -> 12, "b6" -> 6).foreach { case (t, n) =>
      createTable(t, idCols, Array(bucket(n, "id")))
      sql(s"INSERT INTO testcat.ns.$t SELECT id FROM range(24)")
    }

    withTempView("skewData1", "skewData2") {
      spark.range(0, 1000, 1, 10)
        .selectExpr("id % 3 as key1", "id as value1")
        .createOrReplaceTempView("skewData1")
      spark.range(0, 1000, 1, 10)
        .selectExpr("id % 1 as key2", "id as value2")
        .createOrReplaceTempView("skewData2")
      val skewQuery = "SELECT key1 AS k, count(*) AS c " +
        "FROM skewData1 JOIN skewData2 ON key1 = key2 GROUP BY key1"
      val expected = sql(skewQuery).collect().toSeq ++ (0L until 24L).map(i => Row(i, 1L))

      // With forced skew join optimization, OptimizeSkewedJoin re-applies EnsureRequirements to
      // the whole plan, including the nested SPJ in the other branch that is already planned.
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_OPTIMIZE_SKEWS_IN_REBALANCE_PARTITIONS_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "100",
        SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "100",
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "10",
        SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN.key -> "true",
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
        val df = sql(
          s"""
             |$skewQuery
             |UNION ALL
             |SELECT /*+ MERGE(b4, b12, b6) */ b4.id AS k, 1L AS c
             |FROM testcat.ns.b4 JOIN testcat.ns.b12 ON b4.id = b12.id
             |JOIN testcat.ns.b6 ON b4.id = b6.id
             |""".stripMargin)
        checkAnswer(df, expected)
      }
    }
  }

  test("SPARK-60232: re-running EnsureRequirements does not apply a reducer twice") {
    Seq(ReducibleHoursFunction, ReducibleDaysFunction, ReducibleYearsFunction).foreach { f =>
      catalog.createFunction(Identifier.of(Array.empty, f.name()), new UnboundReducibleFunction(f))
    }
    val cols = Array(Column.create("id", LongType), Column.create("ts", TimestampType))
    Seq("qh" -> hours("ts"), "qd" -> days("ts"), "qy" -> years("ts")).foreach { case (t, p) =>
      createTable(t, cols, Array(p))
      sql(s"INSERT INTO testcat.ns.$t " +
        "SELECT id, TIMESTAMP '2026-01-01 00:00:00' + id * INTERVAL '1' HOUR FROM range(72)")
    }

    withTempView("skewData1", "skewData2") {
      spark.range(0, 1000, 1, 10)
        .selectExpr("id % 3 as key1", "id as value1")
        .createOrReplaceTempView("skewData1")
      spark.range(0, 1000, 1, 10)
        .selectExpr("id % 1 as key2", "id as value2")
        .createOrReplaceTempView("skewData2")
      val skewQuery = "SELECT key1 AS k, count(*) AS c " +
        "FROM skewData1 JOIN skewData2 ON key1 = key2 GROUP BY key1"
      val expected = sql(skewQuery).collect().toSeq ++ (0L until 72L).map(i => Row(i, 1L))

      // The re-run's inner join must not derive the hours-to-days reducer again and chain it after
      // the one it pushed the first time, since floorDiv(h, 24) applied twice is wrong.
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_OPTIMIZE_SKEWS_IN_REBALANCE_PARTITIONS_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "100",
        SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "100",
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "10",
        SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN.key -> "true",
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
        val df = sql(
          s"""
             |$skewQuery
             |UNION ALL
             |SELECT /*+ MERGE(qh, qd, qy) */ qh.id AS k, 1L AS c
             |FROM testcat.ns.qh JOIN testcat.ns.qd ON qh.ts = qd.ts
             |JOIN testcat.ns.qy ON qd.ts = qy.ts
             |""".stripMargin)
        checkAnswer(df, expected)
      }
    }
  }

  test("SPARK-60232: nested SPJ falls back when a broadcast build key cannot be traced") {
    val longCols = Array(Column.create("id", LongType))
    val intCols = Array(Column.create("id", IntegerType))
    Seq("l12" -> 12, "l4" -> 4, "l3" -> 3).foreach { case (t, n) =>
      createTable(t, longCols, Array(bucket(n, "id")))
    }
    Seq("lx", "ly").foreach(t => createTable(t, longCols, Array.empty))
    createTable("lxi", intCols, Array.empty)
    createTable("l3i", intCols, Array(bucket(3, "id")))
    Seq("l12", "l4", "l3", "lx", "ly", "lxi", "l3i").foreach { t =>
      sql(s"INSERT INTO testcat.ns.$t SELECT id FROM range(24)")
    }

    // Each query reaches the reduced l12 through a broadcast build key that maps back to no
    // single unreduced position, so the outer join has to shuffle.
    val longRows = (0L until 24L).map(i => Row(i, i, i))
    val queries = Seq(
      // A build key equated to two streamed keys.
      """
        |SELECT /*+ MERGE(l12, l4), BROADCAST(lx), MERGE(l3) */ l12.id, lx.id, l3.id
        |FROM testcat.ns.l12 JOIN testcat.ns.l4 ON l12.id = l4.id
        |JOIN testcat.ns.lx ON lx.id = l12.id AND lx.id = l4.id
        |JOIN testcat.ns.l3 ON lx.id = l3.id
        |""".stripMargin -> longRows,
      // A cast build key.
      """
        |SELECT /*+ MERGE(l12, l4), BROADCAST(lxi), MERGE(l3i) */ l12.id, lxi.id, l3i.id
        |FROM testcat.ns.l12 JOIN testcat.ns.l4 ON l12.id = l4.id
        |JOIN testcat.ns.lxi ON l12.id = lxi.id
        |JOIN testcat.ns.l3i ON lxi.id = l3i.id
        |""".stripMargin -> (0 until 24).map(i => Row(i.toLong, i, i)),
      // A build key that a broadcast inside the build side also uses.
      """
        |SELECT /*+ MERGE(l12, l4), BROADCAST(b), MERGE(l3) */ l12.id, b.id, l3.id
        |FROM testcat.ns.l12 JOIN testcat.ns.l4 ON l12.id = l4.id
        |JOIN (
        |  SELECT /*+ BROADCAST(ly) */ lx.id AS xid, ly.id
        |  FROM testcat.ns.lx JOIN testcat.ns.ly ON lx.id = ly.id) b ON l12.id = b.id
        |JOIN testcat.ns.l3 ON b.id = l3.id
        |""".stripMargin -> longRows)

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true") {
        queries.foreach { case (query, expected) =>
          val df = sql(query)
          assert(collectAllShuffles(df.queryExecution.executedPlan).size == 2,
            "the outer join should fall back to shuffling both sides")
          checkAnswer(df, expected)
        }
      }
    }
  }

  test("SPARK-60232: nested SPJ keeps SPJ on an unreduced column with the same transform") {
    val cols = Array(Column.create("a", LongType), Column.create("b", LongType))
    createTable("pa", cols, Array(bucket(12, "a"), bucket(12, "b")))
    createTable("pb", cols, Array(bucket(4, "a"), bucket(12, "b")))
    createTable("pc", cols, Array(bucket(12, "b")))
    Seq("pa", "pb", "pc").foreach { t =>
      sql(s"INSERT INTO testcat.ns.$t SELECT id, id FROM range(24)")
    }

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
        // The inner join reduces pa at a only, so bucket(12, b) still holds pa's own values.
        val df = sql(
          """
            |SELECT /*+ MERGE(pa, pb, pc) */ pa.a, pc.b
            |FROM testcat.ns.pa JOIN testcat.ns.pb ON pa.a = pb.a AND pa.b = pb.b
            |JOIN testcat.ns.pc ON pa.b = pc.b
            |""".stripMargin)
        assert(collectShuffles(df.queryExecution.executedPlan).isEmpty,
          "SPJ should be triggered for both joins")
        checkAnswer(df, (0L until 24L).map(i => Row(i, i)))
      }
    }
  }

  test("SPARK-60232: ordering a subset-key SPJ range-shuffles when a one-side shuffle is below") {
    val cols = Array(
      Column.create("store", IntegerType),
      Column.create("dept", IntegerType),
      Column.create("data", StringType))
    createTable("sa", cols, Array(identity("store"), identity("dept")))
    // The dept keys come out as 20, 10, so ordering by dept reorders them.
    sql("INSERT INTO testcat.ns.sa VALUES (1, 20, 'a1'), (2, 10, 'a2')")

    createTable("sb", cols, Array.empty)
    sql("INSERT INTO testcat.ns.sb VALUES (0, 10, 'b10'), (0, 20, 'b20')")

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_SORTING_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
        Seq("LEFT JOIN", "JOIN").foreach { joinType =>
          val df = sql(
            s"""
               |SELECT /*+ MERGE(sa, sb) */ sa.dept, sa.data, sb.data, sa.store
               |FROM testcat.ns.sa $joinType testcat.ns.sb ON sa.dept = sb.dept
               |ORDER BY sa.dept
               |""".stripMargin)
          checkAnswer(df, Seq(Row(10, "a2", "b10", 2), Row(20, "a1", "b20", 1)))
        }
      }
    }
  }

  test("SPARK-60232: SPJ does not push common values below a shuffle") {
    createTable("sa7", Array(Column.create("dept", IntegerType)), Array(identity("dept")))
    sql("INSERT INTO testcat.ns.sa7 VALUES (1), (2), (3)")

    createTable("sb7", Array(Column.create("dept", IntegerType),
      Column.create("region", IntegerType)), Array(identity("region")))
    sql("INSERT INTO testcat.ns.sb7 VALUES (1, 10), (2, 20), (3, 30)")

    createTable("pa", Array(Column.create("id", IntegerType)), Array(identity("id")))
    sql("INSERT INTO testcat.ns.pa VALUES (1), (2), (3)")

    createTable("pb", Array(Column.create("id", IntegerType),
      Column.create("region", IntegerType)), Array(identity("region")))
    sql("INSERT INTO testcat.ns.pb VALUES (1, 10), (2, 20), (3, 30)")

    createTable("pc", Array(Column.create("id", IntegerType)), Array(identity("id")))
    sql("INSERT INTO testcat.ns.pc VALUES (1), (2), (3)")

    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_SORTING_ENABLED.key -> "true") {
        // sb7 is shuffled onto sa7's layout, and ordering then pushes sa7's values.
        val orderedDf = sql(
          """
            |SELECT /*+ MERGE(sa7, sb7) */ sa7.dept, sb7.region
            |FROM testcat.ns.sa7 LEFT JOIN testcat.ns.sb7 ON sa7.dept = sb7.dept
            |ORDER BY sa7.dept
            |""".stripMargin)
        checkAnswer(orderedDf, Seq(Row(1, 10), Row(2, 20), Row(3, 30)))

        // pb is shuffled for the inner join, and the outer join then pushes its values.
        val nestedDf = sql(
          """
            |SELECT /*+ MERGE(pa, pb, pc) */ pa.id, pb.region, pc.id
            |FROM testcat.ns.pa JOIN testcat.ns.pb ON pa.id = pb.id
            |JOIN testcat.ns.pc ON pa.id = pc.id
            |""".stripMargin)
        checkAnswer(nestedDf, Seq(Row(1, 10, 1), Row(2, 20, 2), Row(3, 30, 3)))
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
      val scans = collectScans(df.queryExecution.executedPlan)
      assert(scans.forall(_.inputRDD.partitions.length == 2))
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
      val scans = collectScans(df.queryExecution.executedPlan)
      assert(scans.forall(_.inputRDD.partitions.length == 0))
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

      val scans = collectScans(df.queryExecution.executedPlan)
      assert(scans.forall(_.inputRDD.partitions.length == 3))
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

      val scans = collectScans(df.queryExecution.executedPlan)
      assert(scans.forall(_.inputRDD.partitions.length == 4))
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

      val scans = collectScans(df.queryExecution.executedPlan)
      assert(scans.forall(_.inputRDD.partitions.length == 3))
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
      val scans = collectScans(df.queryExecution.executedPlan)
      assert(scans.forall(_.inputRDD.partitions.length == 2))
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
    }
  }

  test("SPARK-53322: checkpointed scans aren't used for SPJ") {
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
          // 1 shuffle for SORT and 2 shuffles for JOIN are expected.
          assert(collectAllShuffles(df.queryExecution.executedPlan).length === 3)
        }
      }
    }
  }

  test("SPARK-53322: checkpointed scans can't shuffle other children on SPJ") {
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
          // 1 shuffle for SORT and 2 shuffles for JOIN are expected.
          assert(collectAllShuffles(df.queryExecution.executedPlan).length === 3)
        }
      }
    }
  }

  test("SPARK-53322: checkpointed scans can be shuffled by children on SPJ") {
    withTempDir { dir =>
      spark.sparkContext.setCheckpointDir(dir.getPath)
      val itemsPartitions = Array(identity("id"))
      createTable(items, itemsColumns, itemsPartitions)
      sql(s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 41.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-02' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-01-03' as timestamp))")

      createTable(purchases, purchasesColumns, Array(identity("item_id")))
      sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 40.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 25.5, cast('2020-01-03' as timestamp)), " +
        s"(4, 20.0, cast('2020-01-04' as timestamp))")

      withSQLConf(
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true") {
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

        // One shuffle for the sort and one shuffle for one side of the JOIN are expected.
        assert(collectAllShuffles(df.queryExecution.executedPlan).length === 2)
      }
    }
  }

  test("SPARK-54439: KeyGroupedPartitioning and join key size mismatch") {
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

  test("SPARK-54439: KeyGroupedPartitioning with transform and join key size mismatch") {
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
      val df = sql(s"SELECT * FROM testcat.ns.$items")
      val scans = collectScans(df.queryExecution.executedPlan)
      assert(scans(0).inputRDD.partitions.length === 2, "items scan should have 2 partition groups")
      df.collect()
    }
    assert(metrics("number of rows read") == "3")
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
    assert(metrics("number of rows read") == "2")
  }

  test("SPARK-55411: Fix ArrayIndexOutOfBoundsException when join keys " +
    "are less than cluster keys") {
    withSQLConf(
      SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
      SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false",
      SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {

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

  test("SPARK-55848: dropDuplicates after SPJ with partial clustering should produce " +
      "correct results") {
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
      // dropDuplicates on the join key after a partially-clustered SPJ must still
      // produce the correct number of distinct ids.  Before the fix, the
      // partially-clustered partitioning was incorrectly treated as satisfying
      // ClusteredDistribution, so EnsureRequirements did not insert an Exchange
      // before the dedup, leading to duplicate rows.
      val df = sql(
        s"""
           |SELECT DISTINCT i.id
           |FROM testcat.ns.$items i
           |JOIN testcat.ns.$purchases p ON i.id = p.item_id
           |""".stripMargin)
      checkAnswer(df, Seq(Row(1), Row(2), Row(3)))

      val allShuffles = collectAllShuffles(df.queryExecution.executedPlan)
      assert(allShuffles.nonEmpty,
        "should contain a shuffle for the post-join dedup with partial clustering")

      val scans = collectScans(df.queryExecution.executedPlan)
      assert(scans.exists(_.outputPartitioning match {
        case kgp: physical.KeyGroupedPartitioning => kgp.isPartiallyClustered
        case _ => false
      }), "at least one BatchScanExec should have partially-clustered KeyGroupedPartitioning")
    }
  }

  test("SPARK-55848: Window dedup after SPJ with partial clustering should produce " +
      "correct results") {
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
      // clustering the plan must insert a shuffle so that the window
      // produces exactly one row per id.
      val df = sql(
        s"""
           |SELECT id, price FROM (
           |  SELECT i.id, i.price,
           |    ROW_NUMBER() OVER (PARTITION BY i.id ORDER BY i.price DESC) AS rn
           |  FROM testcat.ns.$items i
           |  JOIN testcat.ns.$purchases p ON i.id = p.item_id
           |) t WHERE rn = 1
           |""".stripMargin)
      checkAnswer(df, Seq(Row(1, 41.0f), Row(2, 10.0f), Row(3, 15.5f)))

      val allShuffles = collectAllShuffles(df.queryExecution.executedPlan)
      assert(allShuffles.nonEmpty,
        "should contain a shuffle for the post-join window with partial clustering")

      val scans = collectScans(df.queryExecution.executedPlan)
      assert(scans.exists(_.outputPartitioning match {
        case kgp: physical.KeyGroupedPartitioning => kgp.isPartiallyClustered
        case _ => false
      }), "at least one BatchScanExec should have partially-clustered KeyGroupedPartitioning")
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
        // Checkpoint the JOIN result (not the scan) so the plan behind the
        // checkpoint carries partially-clustered KeyGroupedPartitioning.
        // The dedup on top must still insert an Exchange because the
        // isPartiallyClustered flag causes satisfies0()=false for
        // ClusteredDistribution.
        val joinedDf = spark.sql(
          s"""SELECT i.id, i.name, i.price
             |FROM testcat.ns.$items i
             |JOIN testcat.ns.$purchases p ON i.id = p.item_id""".stripMargin)
        val checkpointedDf = joinedDf.checkpoint()
        val df = checkpointedDf.select("id").distinct()

        checkAnswer(df, Seq(Row(1), Row(2), Row(3)))

        val allShuffles = collectAllShuffles(df.queryExecution.executedPlan)
        assert(allShuffles.nonEmpty,
          "should contain a shuffle for the dedup after checkpointed " +
          "partially-clustered join")

        val rddScans = collect(df.queryExecution.executedPlan) {
          case r: RDDScanExec => r
        }
        assert(rddScans.exists(_.outputPartitioning match {
          case kgp: physical.KeyGroupedPartitioning => kgp.isPartiallyClustered
          case _ => false
        }), "checkpoint (RDDScanExec) should have " +
          "partially-clustered KeyGroupedPartitioning")
      }
    }
  }

  private def assertShuffleMayContainUnknownPartitionKeys(
      plan: SparkPlan,
      expected: Seq[Boolean]): Unit = {
    val shuffles = collectAllShuffles(plan)
    assert(shuffles.size === expected.size,
      s"expected ${expected.size} shuffles, got ${shuffles.size}:\n$plan")
    shuffles.zip(expected).foreach { case (shuffle, hasUnknown) =>
      shuffle.outputPartitioning match {
        case k: KeyGroupedPartitioning =>
          assert(k.mayContainUnknownPartitionKeys === hasUnknown,
            s"expected shuffle output mayContainUnknownPartitionKeys=$hasUnknown, got " +
              s"${k.mayContainUnknownPartitionKeys}:\n$plan")
        case p =>
          fail(s"expected a KeyGroupedPartitioning shuffle, got $p:\n$plan")
      }
    }
  }


  test("SPARK-59050: SPJ: one-side shuffle with out-of-set keys loses matches in a following " +
    "SPJ join") {
    // a: keyed on id, keys {1, 2}. t: v1 parquet, keys {1, 2, 3}. u: keyed on id, keys {1, 2, 3}.
    // With shuffle.enabled, a RIGHT OUTER JOIN t shuffles t onto a's declared keys {1, 2}; t's
    // id=3 row is out-of-set, so the join output's partitioning has unknown keys. A following
    // storage-partitioned join against u must not trust it and falls back to a shuffle.
    createTable("a", columns, Array(identity("id")))
    createTable("u", columns, Array(identity("id")))
    sql("INSERT INTO testcat.ns.a VALUES (1, 'a1', NULL), (2, 'a2', NULL)")
    sql("INSERT INTO testcat.ns.u VALUES (1, 'u1', NULL), (2, 'u2', NULL), (3, 'u3', NULL)")

    withTable("t") {
      sql("CREATE TABLE t (id INT, data STRING) USING parquet")
      sql("INSERT INTO t VALUES (1, 't1'), (2, 't2'), (3, 't3')")

      val query =
        """
          |SELECT r.id, u.data
          |FROM (SELECT t.id AS id FROM testcat.ns.a a RIGHT OUTER JOIN t ON a.id = t.id) r
          |JOIN testcat.ns.u u ON r.id = u.id
          |""".stripMargin
      val expected = Seq(Row(1, "u1"), Row(2, "u2"), Row(3, "u3"))

      // Baseline: no SPJ -> all three rows.
      withSQLConf(SQLConf.V2_BUCKETING_ENABLED.key -> "false") {
        checkAnswer(sql(query), expected)
      }

      withSQLConf(
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql(query)
        checkAnswer(df, expected)
        // Two one-side shuffles: t onto a's keys, then the first join's output (unknown-keyed)
        // onto u's keys. Both are keyed with unknown partition keys; neither join
        // storage-partitions.
        assertShuffleMayContainUnknownPartitionKeys(df.queryExecution.executedPlan,
          Seq(true, true))
      }
    }
  }

  test("SPARK-59050: SPJ: preserved non-keyed side of outer join falls back to shuffle " +
    "downstream") {
    // Same hazard for every outer join type whose preserved side is the non-keyed table: the
    // one-side shuffle marks the preserved side's partitioning as having unknown keys, so a
    // downstream storage-partitioned join against a larger key set must fall back to a shuffle.
    createTable("a", columns, Array(identity("id")))
    createTable("u", columns, Array(identity("id")))
    sql("INSERT INTO testcat.ns.a VALUES (1, 'a1', NULL), (2, 'a2', NULL)")
    sql("INSERT INTO testcat.ns.u VALUES (1, 'u1', NULL), (2, 'u2', NULL), (3, 'u3', NULL)")

    withTable("t") {
      sql("CREATE TABLE t (id INT, data STRING) USING parquet")
      sql("INSERT INTO t VALUES (1, 't1'), (2, 't2'), (3, 't3')")

      val expected = Seq(Row(1, "u1"), Row(2, "u2"), Row(3, "u3"))

      // RIGHT OUTER preserves the non-keyed t on the right.
      val rightQuery =
        """
          |SELECT r.id, u.data
          |FROM (SELECT t.id AS id FROM testcat.ns.a a RIGHT OUTER JOIN t ON a.id = t.id) r
          |JOIN testcat.ns.u u ON r.id = u.id
          |""".stripMargin
      withSQLConf(
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql(rightQuery)
        checkAnswer(df, expected)
        assertShuffleMayContainUnknownPartitionKeys(df.queryExecution.executedPlan,
          Seq(true, true))
      }

      // FULL OUTER exposes UnknownPartitioning, so it is already safe regardless of the shuffle
      // direction; correctness is the guard.
      val fullQuery =
        """
          |SELECT r.id, u.data
          |FROM (SELECT t.id AS id FROM testcat.ns.a a FULL OUTER JOIN t ON a.id = t.id) r
          |JOIN testcat.ns.u u ON r.id = u.id
          |""".stripMargin
      withSQLConf(
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql(fullQuery)
        checkAnswer(df, expected)
        assertShuffleMayContainUnknownPartitionKeys(df.queryExecution.executedPlan,
          Seq(true, true))
        // The downstream join must not storage-partition on the first join's unknown-keyed
        // layout; FULL OUTER keeps it safe only because the join output exposes
        // UnknownPartitioning.
      }

      // t LEFT OUTER JOIN a preserves the non-keyed t on the left.
      val leftQuery =
        """
          |SELECT r.id, u.data
          |FROM (SELECT t.id AS id FROM t LEFT OUTER JOIN testcat.ns.a a ON t.id = a.id) r
          |JOIN testcat.ns.u u ON r.id = u.id
          |""".stripMargin
      withSQLConf(
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql(leftQuery)
        checkAnswer(df, expected)
        assertShuffleMayContainUnknownPartitionKeys(df.queryExecution.executedPlan,
          Seq(true, true))
      }
    }
  }

  test("SPARK-59050: SPJ: keyed preserved side of outer join still uses the one-side shuffle") {
    // a (keyed) preserved on the left, t (non-keyed) nullable on the right: t is shuffled onto
    // a's keys (its partitioning is marked as having unknown keys), but the LEFT OUTER join
    // exposes only a's accurate partitioning, so the one-side shuffle stays sound. This branch
    // has no alignment for differing partition counts, so the second join one-side-shuffles the
    // join output onto u's larger layout; both exchanges carry the marker and the answer is
    // complete.
    createTable("a", columns, Array(identity("id")))
    createTable("u", columns, Array(identity("id")))
    sql("INSERT INTO testcat.ns.a VALUES (1, 'a1', NULL), (2, 'a2', NULL)")
    sql("INSERT INTO testcat.ns.u VALUES (1, 'u1', NULL), (2, 'u2', NULL), (3, 'u3', NULL)")

    withTable("t") {
      sql("CREATE TABLE t (id INT, data STRING) USING parquet")
      sql("INSERT INTO t VALUES (1, 't1'), (2, 't2'), (3, 't3')")

      val query =
        """
          |SELECT r.id, u.data
          |FROM (SELECT a.id AS id FROM testcat.ns.a a LEFT OUTER JOIN t ON a.id = t.id) r
          |JOIN testcat.ns.u u ON r.id = u.id
          |""".stripMargin
      withSQLConf(
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql(query)
        checkAnswer(df, Seq(Row(1, "u1"), Row(2, "u2")))
        assertShuffleMayContainUnknownPartitionKeys(df.queryExecution.executedPlan,
          Seq(true, true))
      }
    }
  }

  test("SPARK-59050: SPJ: one-side shuffle with out-of-set keys loses matches in a following " +
      "SPJ join (bucket)") {
    // Same hazard as the identity variant, but the keyed sides are partitioned by bucket(4, id):
    // a covers buckets {0, 1, 2} (ids 0, 1, 2), while t holds id 3 (bucket 3), which a does
    // not, so the one-side shuffle misplaces t's bucket-3 row while still declaring a's layout.
    // `id` is LONG because `BucketFunction` binds its value argument to LongType.
    val cols = Array(Column.create("id", LongType), Column.create("data", StringType))
    createTable("a", cols, Array(bucket(4, "id")))
    createTable("u", cols, Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.a VALUES (0, 'a0'), (1, 'a1'), (2, 'a2')")
    sql("INSERT INTO testcat.ns.u VALUES (0, 'u0'), (1, 'u1'), (2, 'u2'), (3, 'u3')")

    withTable("t") {
      sql("CREATE TABLE t (id BIGINT, data STRING) USING parquet")
      sql("INSERT INTO t VALUES (0, 't0'), (1, 't1'), (2, 't2'), (3, 't3')")

      val query =
        """
          |SELECT r.id, u.data
          |FROM (SELECT t.id AS id FROM testcat.ns.a a RIGHT OUTER JOIN t ON a.id = t.id) r
          |JOIN testcat.ns.u u ON r.id = u.id
          |""".stripMargin
      val expected = Seq(Row(0L, "u0"), Row(1L, "u1"), Row(2L, "u2"), Row(3L, "u3"))

      // Baseline: no SPJ -> all four rows.
      withSQLConf(SQLConf.V2_BUCKETING_ENABLED.key -> "false") {
        checkAnswer(sql(query), expected)
      }

      withSQLConf(
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql(query)
        checkAnswer(df, expected)
        assertShuffleMayContainUnknownPartitionKeys(df.queryExecution.executedPlan,
          Seq(true, true))
      }
    }
  }

  test("SPARK-59050: SPJ: unknown-keyed partitioning still joins a subset-keyed partner") {
    // r (from a RIGHT OUTER JOIN t) has unknown partition keys {1, 2}, but the downstream u is
    // keyed on a subset {1}: every key u can have is co-located on r's declared layout, so the
    // marker's subset rule does not refuse the partner. This branch has no alignment for
    // differing partition counts, so the second join one-side-shuffles the join output onto
    // u's layout; the subset relationship keeps that re-shuffle sound and the answer complete.
    createTable("a", columns, Array(identity("id")))
    createTable("u", columns, Array(identity("id")))
    sql("INSERT INTO testcat.ns.a VALUES (1, 'a1', NULL), (2, 'a2', NULL)")
    sql("INSERT INTO testcat.ns.u VALUES (1, 'u1', NULL)")

    withTable("t") {
      sql("CREATE TABLE t (id INT, data STRING) USING parquet")
      sql("INSERT INTO t VALUES (1, 't1'), (2, 't2'), (3, 't3')")

      val query =
        """
          |SELECT r.id, u.data
          |FROM (SELECT t.id AS id FROM testcat.ns.a a RIGHT OUTER JOIN t ON a.id = t.id) r
          |JOIN testcat.ns.u u ON r.id = u.id
          |""".stripMargin
      withSQLConf(
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql(query)
        checkAnswer(df, Seq(Row(1, "u1")))
        assertShuffleMayContainUnknownPartitionKeys(df.queryExecution.executedPlan,
          Seq(true, true))
      }
    }
  }

  test("SPARK-59050: SPJ: project dropping a key position drops the unknown-keyed claim") {
    // The first join's output is keyed on (id, k) and may contain unknown keys (t's rows are all
    // out-of-set: a holds k=x, t holds k=z). The Project below the second join drops the k
    // position, so the declared key set coarsens from {(1, x) ... (4, x)} to {1, 2, 3, 4}, and
    // an out-of-set (id, k) can then land inside the projected declared set. The keyed claim must
    // be dropped entirely, otherwise the second join trusts the coarsened layout and silently
    // loses the misplaced rows' matches.
    val cols = Array(
      Column.create("id", IntegerType),
      Column.create("k", StringType),
      Column.create("data", StringType))
    createTable("a", cols, Array(identity("id"), identity("k")))
    createTable("u", cols, Array(identity("id")))
    sql("INSERT INTO testcat.ns.a VALUES " +
      "(1, 'x', 'a1'), (2, 'x', 'a2'), (3, 'x', 'a3'), (4, 'x', 'a4')")
    sql("INSERT INTO testcat.ns.u VALUES " +
      "(1, NULL, 'u1'), (2, NULL, 'u2'), (3, NULL, 'u3'), (4, NULL, 'u4')")

    withTable("t") {
      sql("CREATE TABLE t (id INT, k STRING, data STRING) USING parquet")
      sql("INSERT INTO t VALUES (1, 'z', 't1'), (2, 'z', 't2'), (3, 'z', 't3'), (4, 'z', 't4')")

      val query =
        """
          |SELECT r.id, u.data
          |FROM (SELECT t.id AS id FROM testcat.ns.a a RIGHT OUTER JOIN t
          |      ON a.id = t.id AND a.k = t.k) r
          |JOIN testcat.ns.u u ON r.id = u.id
          |""".stripMargin
      val expected = Seq(Row(1, "u1"), Row(2, "u2"), Row(3, "u3"), Row(4, "u4"))

      // Baseline: no SPJ -> all four rows.
      withSQLConf(SQLConf.V2_BUCKETING_ENABLED.key -> "false") {
        checkAnswer(sql(query), expected)
      }

      withSQLConf(
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql(query)
        checkAnswer(df, expected)
        // The projection drops the unknown-keyed claim, so the second join shuffles: two one-side
        // shuffles, both keyed with unknown partition keys, and no GroupPartitionsExec.
        assertShuffleMayContainUnknownPartitionKeys(df.queryExecution.executedPlan,
          Seq(true, true))
      }
    }
  }


  test("SPARK-59050: SPJ: join-key projection of an unknown-keyed layout drops the claim") {
    // Like the key-dropping-project repro, but the projection keeps both key positions: the
    // coarsening happens when the second join projects the declared keys down to its join key
    // (`id`) instead. A key that was out-of-set in the full key space lands inside the projected
    // declared set, so the unknown-keyed spec must be refused and the second join must shuffle.
    val cols = Array(
      Column.create("id", IntegerType),
      Column.create("k", StringType),
      Column.create("data", StringType))
    createTable("a", cols, Array(identity("id"), identity("k")))
    createTable("u", cols, Array(identity("id")))
    sql("INSERT INTO testcat.ns.a VALUES " +
      "(1, 'x', 'a1'), (2, 'x', 'a2'), (3, 'x', 'a3'), (4, 'x', 'a4')")
    sql("INSERT INTO testcat.ns.u VALUES " +
      "(1, NULL, 'u1'), (2, NULL, 'u2'), (3, NULL, 'u3'), (4, NULL, 'u4')")

    withTable("t") {
      sql("CREATE TABLE t (id INT, k STRING, data STRING) USING parquet")
      sql("INSERT INTO t VALUES (1, 'z', 't1'), (2, 'z', 't2'), (3, 'z', 't3'), (4, 'z', 't4')")

      val query =
        """
          |SELECT r.id, r.k, u.data
          |FROM (SELECT t.id AS id, t.k AS k FROM testcat.ns.a a RIGHT OUTER JOIN t
          |      ON a.id = t.id AND a.k = t.k) r
          |JOIN testcat.ns.u u ON r.id = u.id
          |""".stripMargin
      val expected = Seq(Row(1, "z", "u1"), Row(2, "z", "u2"), Row(3, "z", "u3"), Row(4, "z", "u4"))

      // Baseline: no SPJ -> all four rows.
      withSQLConf(SQLConf.V2_BUCKETING_ENABLED.key -> "false") {
        checkAnswer(sql(query), expected)
      }

      withSQLConf(
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql(query)
        checkAnswer(df, expected)
        // The second join refuses the projected unknown-keyed spec and shuffles instead: the
        // first join's one-side shuffle plus the re-shuffle of the first join's output.
        assertShuffleMayContainUnknownPartitionKeys(df.queryExecution.executedPlan,
          Seq(true, true))
      }
    }
  }

  test("SPARK-59050: SPJ: spurious marker of an inner join costs no extra shuffle") {
    // The spurious marker on the inner join's collection must cost nothing: consumers read a
    // joined collection with `exists`, so the accurate member still answers. This branch has
    // no reduced storage-partitioned join (bucket(4) onto bucket(2) differs in the partition
    // count and nothing here reconciles it), so the second join one-side-shuffles the join
    // output onto u's layout both with and without the marker: two exchanges either way.
    val cols = Array(Column.create("id", LongType), Column.create("data", StringType))
    createTable("a", cols, Array(bucket(4, "id")))
    createTable("u", cols, Array(bucket(2, "id")))
    sql("INSERT INTO testcat.ns.a VALUES (0, 'a0'), (1, 'a1'), (2, 'a2'), (3, 'a3')")
    sql("INSERT INTO testcat.ns.u VALUES (0, 'u0'), (1, 'u1'), (2, 'u2'), (3, 'u3')")

    withTable("t") {
      sql("CREATE TABLE t (id BIGINT, data STRING) USING parquet")
      sql("INSERT INTO t VALUES (0, 't0'), (1, 't1'), (2, 't2'), (3, 't3')")

      val query =
        """
          |SELECT a.id, u.data
          |FROM testcat.ns.a a JOIN t ON a.id = t.id
          |JOIN testcat.ns.u u ON a.id = u.id
          |""".stripMargin
      val expected = Seq(Row(0L, "u0"), Row(1L, "u1"), Row(2L, "u2"), Row(3L, "u3"))

      withSQLConf(
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql(query)
        checkAnswer(df, expected)
        // The first join's one-side shuffle plus the second join's re-shuffle of the join
        // output onto u's layout; the spurious marker adds no third exchange.
        assertShuffleMayContainUnknownPartitionKeys(df.queryExecution.executedPlan,
          Seq(true, true))
      }
    }
  }


  test("SPARK-59050: SPJ: genuine unknown keys survive an inner join on a key subset") {
    // Adversarial shape against the spurious-marker clearing: the RIGHT OUTER output `r` is a
    // single unknown-keyed KP carrying a GENUINE unknown row (1, 'zz'); it inner-joins `x`
    // (a keyed table declaring the same key set) on a strict subset of the key columns; the
    // join to `u` below matches on the full (id, k). The (1,'zz','xa','uz') match must survive:
    // clearing markers only applies to mixed collections where every row already carries a
    // declared key,
    // and here the planner re-shuffles `r`'s side onto u's layout (the (id)-only claim cannot
    // pair with the (id,k) spec), routing (1,'zz') by its full tuple into its own partition.
    val cols = Array(
      Column.create("id", IntegerType),
      Column.create("k", StringType),
      Column.create("data", StringType))
    createTable("a", cols, Array(identity("id"), identity("k")))
    createTable("x", cols, Array(identity("id"), identity("k")))
    createTable("u", cols, Array(identity("id"), identity("k")))
    sql("INSERT INTO testcat.ns.a VALUES (1, 'x', 'a1'), (2, 'x', 'a2')")
    sql("INSERT INTO testcat.ns.x VALUES (1, 'x', 'xa'), (2, 'x', 'xb')")
    sql("INSERT INTO testcat.ns.u VALUES (1, 'x', 'ux'), (2, 'x', 'ub'), (1, 'zz', 'uz')")

    withTable("t") {
      sql("CREATE TABLE t (id INT, k STRING, data STRING) USING parquet")
      sql("INSERT INTO t VALUES (1, 'x', 't1'), (2, 'x', 't2'), (1, 'zz', 't3')")

      val query =
        """
          |SELECT r.id, r.k, x.data, u.data
          |FROM (SELECT t.id AS id, t.k AS k FROM testcat.ns.a a RIGHT OUTER JOIN t
          |      ON a.id = t.id AND a.k = t.k) r
          |JOIN testcat.ns.x x ON r.id = x.id
          |JOIN testcat.ns.u u ON r.id = u.id AND r.k = u.k
          |""".stripMargin
      val expected = Seq(Row(1, "x", "xa", "ux"), Row(2, "x", "xb", "ub"),
        Row(1, "zz", "xa", "uz"))

      withSQLConf(SQLConf.V2_BUCKETING_ENABLED.key -> "false") {
        checkAnswer(sql(query), expected)
      }
      withSQLConf(
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql(query)
        checkAnswer(df, expected)
        // Pin the total exchange count so later silent shuffles surface: the marked (id, k)
        // side refuses to pair on the subset key, cascading one-side shuffles around it.
        val plan = df.queryExecution.executedPlan
        val shuffles = collectAllShuffles(plan)
        assert(shuffles.size == 4, s"expected 4 exchanges, got ${shuffles.size}:\n$plan")
      }
    }
  }

  test("SPARK-59050: SPJ: a window keyed on a subset of an unknown-keyed layout must shuffle") {
    // The right-outer join preserves pt's out-of-set row (1, 9): the surviving layout declares
    // (id, k) pairs (1, 0)..(4, 0) only. A window keyed on `id` alone must not run
    // partition-locally on it: the declared (1, 0) row and the hash-placed (1, 9) row can
    // sit in different partitions, splitting the count for id=1 into two groups of 1.
    // `satisfies0` refuses the subset relaxation for an unknown-keyed layout.
    val cols = Array(
      Column.create("id", IntegerType),
      Column.create("k", IntegerType),
      Column.create("data", StringType))
    createTable("pa", cols, Array(identity("id"), identity("k")))
    sql("INSERT INTO testcat.ns.pa VALUES " +
      "(1, 0, 'a1'), (2, 0, 'a2'), (3, 0, 'a3'), (4, 0, 'a4')")
    withTable("pt") {
      sql("CREATE TABLE pt (id INT, k INT, data STRING) USING parquet")
      sql("INSERT INTO pt VALUES " +
        "(1, 0, 't1'), (2, 0, 't2'), (3, 0, 't3'), (4, 0, 't4'), (1, 9, 'u1')")
      val query =
        """
          |SELECT id, k, COUNT(*) OVER (PARTITION BY id) FROM (
          |  SELECT /*+ MERGE */ pt.id AS id, pt.k AS k FROM testcat.ns.pa pa
          |  RIGHT OUTER JOIN pt ON pa.id = pt.id AND pa.k = pt.k) r
          |""".stripMargin
      // AQE off pins the plan shape; AQE on replans the same query over stage outputs and must
      // still return correct counts (the marker survives `ShuffleQueryStageExec`).
      for (adaptive <- Seq(false, true)) {
        withSQLConf(
            SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
            SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
            "spark.sql.autoBroadcastJoinThreshold" -> "-1",
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
          val df = sql(query)
          checkAnswer(df, Seq(Row(1, 0, 2), Row(2, 0, 1), Row(3, 0, 1), Row(4, 0, 1),
            Row(1, 9, 2)))
          if (!adaptive) {
            // Two shuffles: the first join's one-side shuffle plus the window's exchange.
            val plan = df.queryExecution.executedPlan
            val shuffles = collectAllShuffles(plan)
            assert(shuffles.size == 2, s"the window must pay an exchange, got:\n$plan")
            assert(shuffles.exists(!_.outputPartitioning.isInstanceOf[KeyGroupedPartitioning]),
              s"expected a non-keyed (window) exchange, got:\n$plan")
          }
        }
      }
    }
  }

  test("SPARK-59050: SPJ: a window keyed on the full key of an unknown-keyed layout does not " +
    "shuffle") {
    // Positive control for the marked branch of `satisfies0`: whole declared keys co-locate, so
    // a window keyed on the COMPLETE partition key of a marked layout still runs
    // partition-locally. A gate that over-rejects (e.g. also refusing full-key clustering) keeps
    // every wrong-results test green and only silently adds shuffles; this test would not.
    val cols = Array(
      Column.create("id", IntegerType),
      Column.create("k", IntegerType),
      Column.create("data", StringType))
    createTable("qa", cols, Array(identity("id"), identity("k")))
    sql("INSERT INTO testcat.ns.qa VALUES (1, 0, 'a1'), (2, 0, 'a2'), (3, 0, 'a3')")
    withTable("qt") {
      sql("CREATE TABLE qt (id INT, k INT, data STRING) USING parquet")
      sql("INSERT INTO qt VALUES (1, 0, 't1'), (2, 0, 't2'), (3, 0, 't3'), (1, 9, 'u1')")
      val query =
        """
          |SELECT id, k, COUNT(*) OVER (PARTITION BY id, k) FROM (
          |  SELECT /*+ MERGE */ qt.id AS id, qt.k AS k FROM testcat.ns.qa qa
          |  RIGHT OUTER JOIN qt ON qa.id = qt.id AND qa.k = qt.k) r
          |""".stripMargin
      withSQLConf(
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          "spark.sql.autoBroadcastJoinThreshold" -> "-1",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql(query)
        checkAnswer(df, Seq(Row(1, 0, 1), Row(2, 0, 1), Row(3, 0, 1), Row(1, 9, 1)))
        val plan = df.queryExecution.executedPlan
        val shuffles = collectAllShuffles(plan)
        // Only the first join's one-side shuffle; the window rides the marked layout.
        assert(shuffles.size == 1, s"the full-key window must not shuffle, got:\n$plan")
      }
    }
  }

  test("SPARK-59050: SPJ: a global ORDER BY over an unknown-keyed layout must " +
    "range-partition") {
    // The right-outer join preserves obt's out-of-set id=4, which the KeyGroupedPartitioner
    // hash-placed into the partition declaring id=1; without a range exchange the global sort
    // degrades to per-partition sorting and emits [1, 4, 2]. `satisfies0` rejects the ordering
    // claim of a marked layout with more than one partition.
    createTable("oba", columns, Array(identity("id")))
    sql("INSERT INTO testcat.ns.oba VALUES (1, 'x', NULL), (2, 'x', NULL)")
    withTable("obt") {
      sql("CREATE TABLE obt (id INT, data STRING) USING parquet")
      sql("INSERT INTO obt VALUES (1, 'p1'), (2, 'p2'), (4, 'p4')")
      val query =
        """
          |SELECT /*+ MERGE */ obt.id FROM testcat.ns.oba ba RIGHT OUTER JOIN obt
          |ON ba.id = obt.id
          |ORDER BY id
          |""".stripMargin
      withSQLConf(
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_SORTING_ENABLED.key -> "true",
          "spark.sql.autoBroadcastJoinThreshold" -> "-1",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql(query)
        // compare `collect` directly: checkAnswer would sort both sides and hide the order.
        val ordered = df.collect().map(_.getInt(0)).toSeq
        assert(ordered == Seq(1, 2, 4), s"global order broken: $ordered")
        val plan = df.queryExecution.executedPlan
        val shuffles = collectAllShuffles(plan)
        // Two exchanges: the first join's one-side shuffle plus the range partitioning.
        assert(shuffles.size == 2, s"expected 2 exchanges, got ${shuffles.size}:\n$plan")
        assert(shuffles.exists(_.outputPartitioning.isInstanceOf[physical.RangePartitioning]),
          s"a range exchange must precede the global sort, got:\n$plan")
      }
    }
  }

}
