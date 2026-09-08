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
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.catalyst.plans.physical.KeyedPartitioning
import org.apache.spark.sql.connector.catalog.{Column, Identifier, InMemoryTableCatalog}
import org.apache.spark.sql.connector.catalog.functions._
import org.apache.spark.sql.connector.distributions.Distributions
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.expressions.Expressions._
import org.apache.spark.sql.execution.{
  ExtendedMode,
  FormattedMode,
  LocalTableScanExec,
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

class KeyGroupedPartitioningSuite extends DistributionAndOrderingSuiteBase with ExplainSuiteHelper {
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

  /** Two structs of one shape, named differently, which is legal to join across. */
  private val structA = new StructType().add("a", IntegerType)
  private val structB = new StructType().add("b", IntegerType)

  /** Every `KeyedPartitioning` these nodes report, flattening any collection. */
  protected def keyedPartitioningsOf(
      nodes: Seq[SparkPlan]): Seq[physical.KeyedPartitioning] = {
    def flatten(p: physical.Partitioning): Seq[physical.Partitioning] = p match {
      case physical.PartitioningCollection(members) => members.flatMap(flatten)
      case other => Seq(other)
    }
    nodes.map(_.outputPartitioning).flatMap(flatten)
      .collect { case kp: physical.KeyedPartitioning => kp }
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
      ordering: Array[SortOrder] = Array.empty,
      catalog: InMemoryTableCatalog = catalog): Unit = {
    catalog.createTable(Identifier.of(Array("ns"), table),
      columns, partitions, emptyProps, Distributions.unspecified(), ordering, None, None,
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
    // here we skip collecting shuffle operators that are not associated with SMJ
    collect(plan) {
      case s: SortMergeJoinExec => s
    }.flatMap(smj =>
      collect(smj) {
        case g: GroupPartitionsExec => g
      })
  }.toSet.toSeq

  private def collectScans(plan: SparkPlan): Seq[BatchScanExec] = {
    collect(plan) { case s: BatchScanExec => s }
  }

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
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
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
    // This test deliberately leaves `V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS` off:
    // both joins use the whole partition key, and without the config the failure on base is
    // exercised through the second, independent trigger (`reduceKeys` at the second join) instead
    // of `createShuffleSpec` -> `toGrouped`.
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
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
        SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
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
      val child = new LocalTableScanExec(Seq(attr), Nil, None)
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
      val child = new LocalTableScanExec(Seq(attr, dt), Nil, None)
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
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
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
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
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
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
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

  test("SPARK-59252: a planned scan keeps the partitioning it was planned with") {
    createBucketedIdTable("l4", 4)
    createBucketedIdTable("r4", 4)

    val df = sql("SELECT l.id FROM testcat.ns.l4 l JOIN testcat.ns.r4 r ON l.id = r.id")
    val plan = stripAQEPlan(df.queryExecution.executedPlan)
    // The planner committed to the key-grouped layout: it dropped both shuffles and put a
    // `GroupPartitionsExec` on each side. Those nodes ask their child for the partitioning again at
    // execution, so the scan has to keep answering what it was planned with.
    assert(collectShuffles(plan).isEmpty)
    assert(collectGroupPartitions(plan).size == 2)

    withSQLConf(SQLConf.V2_BUCKETING_ENABLED.key -> "false") {
      checkAnswer(df, (0 until 12).map(i => Row(i.toLong)))
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

            // storage-partitioned join should kick in and fill the missing partitions & splits
            // after dynamic filtering with empty partitions & splits, respectively.
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
      Column.create("id", structA),
      Column.create("name", StringType),
      Column.create("price", DoubleType)), items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(named_struct('a', 1), 'aa', 40.0), " +
      "(named_struct('a', 2), 'bb', 10.0), " +
      "(named_struct('a', 3), 'cc', 15.5), " +
      "(named_struct('a', 4), 'dd', 20.0)")

    createTable(purchases, Array(
      Column.create("item_id", structB),
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

  test("SPARK-59187: two keyed sides whose struct field names differ join without a shuffle") {
    // The join is legal, since struct equality ignores field names, and the two sides hold the same
    // key values. Key rows are compared at types with the naming erased, so the two sides' keys
    // pair and the join needs no shuffle. Before that erasure this threw
    // STORAGE_PARTITION_JOIN_INCOMPATIBLE_REDUCED_TYPES, for a join that reduced nothing.
    withTable("s1", "s2") {
      createTable("s1", Array(Column.create("id", structA), Column.create("v", StringType)),
        Array(identity("id")))
      sql("INSERT INTO testcat.ns.s1 VALUES " +
        "(named_struct('a', 1), 'x'), (named_struct('a', 2), 'y')")
      createTable("s2", Array(Column.create("k", structB), Column.create("w", StringType)),
        Array(identity("k")))
      sql("INSERT INTO testcat.ns.s2 VALUES " +
        "(named_struct('b', 1), 'p'), (named_struct('b', 2), 'q')")

      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true") {
        val df = sql("SELECT s1.v, s2.w FROM testcat.ns.s1 JOIN testcat.ns.s2 ON s1.id = s2.k")
        assert(collectShuffles(df.queryExecution.executedPlan).isEmpty,
          "the two sides are laid out on the same keys")
        checkAnswer(df, Seq(Row("x", "p"), Row("y", "q")))
      }
    }
  }

  test("SPARK-59187: a shuffled side keeps its own struct field names over shared empty keys") {
    // End to end because the point is that the planner reaches this shape, not that two hand-built
    // partitionings agree. `createPartitioning` puts the other child's expressions over this side's
    // keys, so the two members of the join's partitioning carry `struct<a:int>` and `struct<b:int>`
    // over one shared key list. Struct equality ignores field names, so the join is legal, and with
    // the keys pruned to nothing each member answers for its key types from its own expressions.
    // The two answers describe the same key space and must not be held against each other.
    withTable("a1", "b1", "c1") {
      createTable("a1", Array(Column.create("id", structA)), Array(identity("id")))
      sql("INSERT INTO testcat.ns.a1 VALUES (named_struct('a', 1))")
      createTable("b1", Array(Column.create("id", structA)), Array(identity("id")))
      sql("INSERT INTO testcat.ns.b1 VALUES (named_struct('a', 2))")
      createTable("c1", Array(Column.create("k", structB)), Array.empty)
      sql("INSERT INTO testcat.ns.c1 VALUES (named_struct('b', 1))")

      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true") {
        // The GROUP BY is what asks the join for its `outputPartitioning`.
        val df = sql(
          """SELECT id, count(*) FROM (
            |  SELECT j.id, c1.k FROM
            |    (SELECT a1.id AS id FROM testcat.ns.a1 JOIN testcat.ns.b1 ON a1.id = b1.id) j
            |    JOIN testcat.ns.c1 ON j.id = c1.k
            |) GROUP BY id
            |""".stripMargin)
        val plan = df.queryExecution.executedPlan
        val members = keyedPartitioningsOf(collect(plan) { case j: SortMergeJoinExec => j })
        assert(members.map(_.expressions).distinct.size > 1,
          "test setup: a join reports the two sides' expressions over one layout")
        assert(members.map(_.keyDataTypes).distinct.size === 1,
          "and they answer for one key space, whatever each side calls its struct field")
        checkAnswer(df, Nil)
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

              val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
                .map(_.outputPartitioning.numPartitions)
              (allowJoinKeysSubsetOfPartitionKeys, partiallyClustered, filter) match {
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

            val partitions = collectGroupPartitions(df.queryExecution.executedPlan)
              .map(_.outputPartitioning.numPartitions)
            (pushDownValues, allowJoinKeysSubsetOfPartitionKeys, partiallyClustered) match {
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

            val partitions = collectGroupPartitions(df.queryExecution.executedPlan)
              .map(_.outputPartitioning.numPartitions)
            (allowJoinKeysSubsetOfPartitionKeys, partiallyClustered) match {
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
      val positions = collectAllGroupPartitions(plan).flatMap(_.joinKeyPositions)
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

  test("SPARK-58996: partially clustered join keeps its row count when EnsureRequirements " +
      "re-runs") {
    // The storage-partitioned join branch has no shuffle of its own, so the re-run of
    // `EnsureRequirements` (triggered by the other branch below) reaches it. With
    // `numRowsPerSplit = 1` the two id = 1 rows end up in two splits, which is what makes
    // partial clustering replicate a side across two expected partitions. Regrouping that
    // replicated layout on the second pass concatenated the replicas and replicated again,
    // duplicating every id = 1 row.
    val spColumns = Array(Column.create("id", LongType), Column.create("data", StringType))
    createTable("sp1", spColumns, Array(identity("id")))
    sql("INSERT INTO testcat.ns.sp1 VALUES (1, 'aa'), (1, 'ab'), (2, 'bb')")
    createTable("sp2", spColumns, Array(identity("id")))
    sql("INSERT INTO testcat.ns.sp2 VALUES (1, 'p'), (2, 'q')")

    // Unpartitioned, so this branch's join materializes shuffle stages and is converted to a
    // shuffled hash join, which hands the whole plan back to `EnsureRequirements`.
    createTable("np1", spColumns, Array.empty)
    sql("INSERT INTO testcat.ns.np1 VALUES (7, 'x')")
    createTable("np2", spColumns, Array.empty)
    sql("INSERT INTO testcat.ns.np2 VALUES (7, 'y')")

    withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_MAX_SHUFFLE_HASH_JOIN_LOCAL_MAP_THRESHOLD.key -> "100m") {
      val df = sql(
        """
          |SELECT /*+ MERGE(a, b) */ a.id AS k
          |FROM testcat.ns.sp1 a JOIN testcat.ns.sp2 b ON a.id = b.id
          |UNION ALL
          |SELECT c.id AS k
          |FROM testcat.ns.np1 c JOIN testcat.ns.np2 d ON c.id = d.id
          |""".stripMargin)
      checkAnswer(df, Seq(Row(1L), Row(1L), Row(2L), Row(7L)))

      // The re-run must leave the storage-partitioned side shuffle-free, with the single
      // grouping per child the first pass built: a grouping stacked over another re-derives the
      // alignment from an already-aligned layout and duplicates rows.
      assert(collectShuffles(df.queryExecution.executedPlan).isEmpty,
        "the storage-partitioned join must stay shuffle-free")
      val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
      assert(groupPartitions.nonEmpty, "the storage-partitioned join must keep its groupings")
      groupPartitions.foreach { g =>
        assert(collectAllGroupPartitions(g.child).isEmpty,
          s"a GroupPartitionsExec must not be stacked over another:\n${g.treeString}")
      }
    }
  }

  test("SPARK-58996: partially clustered join keeps its replicate-side choice when " +
      "EnsureRequirements re-runs") {
    // The smaller side is replicated, chosen by plan statistics on the first pass. On the re-run
    // the statistics must be read from the pre-alignment plan again: reading them from the
    // aligned layout skips the statistics branch and deterministically flips the choice. With
    // rows on both sides of the multi-split key the flip already gives wrong results on master;
    // with the smaller side holding five splits for id = 1, the flip also turns it into the
    // distributing side against an expected count of one, which `padTo` overflows into an
    // unequal number of partitions per join side.
    val spColumns = Array(Column.create("id", LongType), Column.create("data", StringType))
    createTable("sp_small", spColumns, Array(identity("id")))
    sql("INSERT INTO testcat.ns.sp_small VALUES " +
        "(1, 'a1'), (1, 'a2'), (1, 'a3'), (1, 'a4'), (1, 'a5'), (2, 'b')")
    createTable("sp_large", spColumns, Array(identity("id")))
    sql("INSERT INTO testcat.ns.sp_large VALUES " +
        "(1, 'p'), (2, 'q1'), (2, 'q2'), (2, 'q3'), (2, 'q4'), (2, 'q5'), (3, 'r')")

    createTable("np1", spColumns, Array.empty)
    sql("INSERT INTO testcat.ns.np1 VALUES (7, 'x')")
    createTable("np2", spColumns, Array.empty)
    sql("INSERT INTO testcat.ns.np2 VALUES (7, 'y')")

    withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_MAX_SHUFFLE_HASH_JOIN_LOCAL_MAP_THRESHOLD.key -> "100m") {
      val df = sql(
        """
          |SELECT /*+ MERGE(a, b) */ a.id AS k
          |FROM testcat.ns.sp_small a JOIN testcat.ns.sp_large b ON a.id = b.id
          |UNION ALL
          |SELECT c.id AS k
          |FROM testcat.ns.np1 c JOIN testcat.ns.np2 d ON c.id = d.id
          |""".stripMargin)
      checkAnswer(df, Seq.fill(5)(Row(1L)) ++ Seq.fill(5)(Row(2L)) :+ Row(7L))

      assert(collectShuffles(df.queryExecution.executedPlan).isEmpty,
        "the storage-partitioned join must stay shuffle-free")
      assert(collectGroupPartitions(df.queryExecution.executedPlan).nonEmpty,
        "the storage-partitioned join must keep its groupings")
    }
  }

  test("SPARK-58996: partially clustered subset-key join keeps its join key positions when " +
      "EnsureRequirements re-runs") {
    // The left table is partitioned by (extra, id) and the join uses only `id`, the second
    // partition key, so the first pass stores positions that project the raw partition keys down
    // to the join key. On the re-run the incoming positions are computed against the node's
    // already projected report and must not overwrite the stored ones: applied to the raw keys
    // again they would project a second time and group by the wrong key, and every expected key
    // misses. The second split for id = 1 makes the test fail on master too, where the stacked
    // re-grouping duplicates rows. The query must also select `extra`, or column pruning drops
    // it from the scan output and the scan stops reporting its partitioning at all.
    createTable("multi_part",
      Array(Column.create("id", LongType), Column.create("extra", LongType),
        Column.create("data", StringType)),
      Array(identity("extra"), identity("id")))
    sql("INSERT INTO testcat.ns.multi_part VALUES (1, 10, 'x'), (1, 11, 'x2'), (2, 20, 'y')")
    createTable("single_part",
      Array(Column.create("id", LongType), Column.create("data", StringType)),
      Array(identity("id")))
    sql("INSERT INTO testcat.ns.single_part VALUES (1, 'p'), (2, 'q')")

    val spColumns = Array(Column.create("id", LongType), Column.create("data", StringType))
    createTable("np1", spColumns, Array.empty)
    sql("INSERT INTO testcat.ns.np1 VALUES (7, 'x')")
    createTable("np2", spColumns, Array.empty)
    sql("INSERT INTO testcat.ns.np2 VALUES (7, 'y')")

    withSQLConf(
        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        // Co-partition elimination normally requires the partition keys to equal the join keys;
        // the left side is partitioned by (extra, id) for a join on `id`, so relax that here.
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
        SQLConf.ADAPTIVE_MAX_SHUFFLE_HASH_JOIN_LOCAL_MAP_THRESHOLD.key -> "100m") {
      val df = sql(
        """
          |SELECT /*+ MERGE(a, b) */ a.id AS k, a.extra AS e
          |FROM testcat.ns.multi_part a JOIN testcat.ns.single_part b ON a.id = b.id
          |UNION ALL
          |SELECT c.id AS k, 0L AS e
          |FROM testcat.ns.np1 c JOIN testcat.ns.np2 d ON c.id = d.id
          |""".stripMargin)
      checkAnswer(df, Seq(Row(1L, 10L), Row(1L, 11L), Row(2L, 20L), Row(7L, 0L)))

      assert(collectShuffles(df.queryExecution.executedPlan).isEmpty,
        "the storage-partitioned join must stay shuffle-free")
      assert(collectGroupPartitions(df.queryExecution.executedPlan).nonEmpty,
        "the storage-partitioned join must keep its groupings")
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
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
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
      SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
      SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
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
      SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
      SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
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
          SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
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

  private def createTsTable(name: String, partitions: Array[Transform]): Unit = {
    createTable(name, columns, partitions)
    sql(s"INSERT INTO testcat.ns.$name VALUES " +
      s"(1, 'aa', cast('2020-01-01' as timestamp)), " +
      s"(2, 'bb', cast('2021-06-01' as timestamp))")
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
          SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
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
          SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
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
      withSQLConf(SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
        checkAnswer(sql(query), expected)
      }

      // The plan-shape assertion needs a static, fully-planned tree, so disable AQE here.
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
        val groupPartitions = collectAllGroupPartitions(sql(query).queryExecution.executedPlan)
        // The node has to project down to [id], not only coalesce: `name` is a partition key the
        // window does not group by, so coalescing on (id, name) would merge nothing.
        assert(groupPartitions.map(_.joinKeyPositions) == Seq(Some(Seq(0))),
          s"PARTITION BY $partitionSpec: GroupPartitionsExec expected to project to the subset " +
            "key [id] and coalesce the partitions sharing it")
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

    withSQLConf(SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      checkAnswer(sql(query), expected)
    }

    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
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
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true") {
      val df = sql(query)
      checkAnswer(df, expected)
      val plan = df.queryExecution.executedPlan
      assert(collectAllGroupPartitions(plan).isEmpty,
        "projecting to [id] coalesces nothing, so no GroupPartitionsExec is needed")
      assert(collectAllShuffles(plan).isEmpty, "no shuffle either")
    }
  }

  /**
   * Asserts that the plan's shuffles (in tree order) are all `KeyedPartitioning`s carrying the
   * given `mayContainUnknownPartitionKeys` flags (a `KeyedPartitioning` produced by
   * `KeyedShuffleSpec.createPartitioning` always carries the marker).
   */
  /** Every `KeyedPartitioning` this partitioning reports, flattening collections. */
  private def flattenKeyedPartitionings(p: physical.Partitioning): Seq[KeyedPartitioning] =
    p match {
      case k: KeyedPartitioning => k :: Nil
      case c: physical.PartitioningCollection => c.partitionings.flatMap(flattenKeyedPartitionings)
      case _ => Nil
    }

  private def assertShuffleMayContainUnknownPartitionKeys(
      plan: SparkPlan,
      expected: Seq[Boolean]): Unit = {
    val shuffles = collectAllShuffles(plan)
    assert(shuffles.size === expected.size,
      s"expected ${expected.size} shuffles, got ${shuffles.size}:\n$plan")
    shuffles.zip(expected).foreach { case (shuffle, hasUnknown) =>
      shuffle.outputPartitioning match {
        case k: KeyedPartitioning =>
          assert(k.mayContainUnknownPartitionKeys === hasUnknown,
            s"expected shuffle output mayContainUnknownPartitionKeys=$hasUnknown, got " +
              s"${k.mayContainUnknownPartitionKeys}:\n$plan")
        case p =>
          fail(s"expected a KeyedPartitioning shuffle, got $p:\n$plan")
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
        // onto u's keys. Both are keyed with unknown partition keys; neither join GPEs.
        assertShuffleMayContainUnknownPartitionKeys(df.queryExecution.executedPlan,
          Seq(true, true))
        assert(collectGroupPartitions(df.queryExecution.executedPlan).isEmpty,
          s"second join must not storage-partition on an unknown-keyed layout, got: " +
            df.queryExecution.executedPlan)
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
        assert(collectGroupPartitions(df.queryExecution.executedPlan).isEmpty,
          s"downstream join must not storage-partition on an unknown-keyed layout, got: " +
            df.queryExecution.executedPlan)
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
        assert(collectGroupPartitions(df.queryExecution.executedPlan).isEmpty,
          s"downstream join must not storage-partition on an unknown-keyed layout, got: " +
            df.queryExecution.executedPlan)
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
        assert(collectGroupPartitions(df.queryExecution.executedPlan).isEmpty,
          s"downstream join must not storage-partition on an unknown-keyed layout, got: " +
            df.queryExecution.executedPlan)
      }
    }
  }

  test("SPARK-59050: SPJ: keyed preserved side of outer join still uses the one-side shuffle") {
    // a (keyed) preserved on the left, t (non-keyed) nullable on the right: t is shuffled onto
    // a's keys (its partitioning is marked as having unknown keys), but the LEFT OUTER join exposes
    // only a's accurate partitioning, so the one-side shuffle stays sound and the downstream SPJ
    // still runs (no shuffle for the second join).
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
          Seq(true))
        assert(collectGroupPartitions(df.queryExecution.executedPlan).nonEmpty,
          s"downstream join should storage-partition on the accurate keyed layout, got: " +
            df.queryExecution.executedPlan)
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
        assert(collectGroupPartitions(df.queryExecution.executedPlan).isEmpty,
          s"second join must not storage-partition on an unknown-keyed layout, got: " +
            df.queryExecution.executedPlan)
      }
    }
  }

  test("SPARK-59050: SPJ: unknown-keyed partitioning still joins a subset-keyed partner") {
    // r (from a RIGHT OUTER JOIN t) has unknown partition keys {1, 2}, but the downstream u is
    // keyed on a subset {1}, so the storage-partitioned join stays compatible and works: every
    // key u can have is co-located on r's declared layout.
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
        // Only the first join's one-side shuffle remains; the second join storage-partitions.
        assertShuffleMayContainUnknownPartitionKeys(df.queryExecution.executedPlan,
          Seq(true))
        assert(collectGroupPartitions(df.queryExecution.executedPlan).nonEmpty,
          s"subset-keyed partner should still storage-partition join, got: " +
            df.queryExecution.executedPlan)
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
        assert(collectGroupPartitions(df.queryExecution.executedPlan).isEmpty,
          s"second join must not storage-partition on the coarsened layout, got: " +
            df.queryExecution.executedPlan)
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

  test("SPARK-59050: SPJ: spurious marker of an inner join keeps the reduced SPJ") {
    // The spurious marker on the inner join's collection is cleared at construction (see
    // `ShuffledJoin`), so a following reduced storage-partitioned join (bucket(4) onto
    // bucket(2)) must still work. Reading the marker with `exists` used to make the
    // GroupPartitionsExec give up with a zero-partition UnknownPartitioning, which threw at
    // planning when a parent asked for the partitioning.
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
        // The reduced SPJ still runs: the first join's one-side shuffle is the only shuffle.
        assertShuffleMayContainUnknownPartitionKeys(df.queryExecution.executedPlan, Seq(true))
      }
    }
  }

  test("SPARK-59050: SPJ: inner join with in-set rows keeps the sound SPJ downstream") {
    // Same shape as the one-side-shuffle repro, but the inner join's second side holds only
    // in-set rows: the marker is spurious and cleared at the join (see `ShuffledJoin`), so it
    // costs the plan nothing. On this branch the Project between the joins drops the keyed
    // claim when the `k` column leaves the output, so the second join one-side-shuffles r onto
    // u's layout: both exchanges carry the marker, and u's side keeps its scan partitioning.
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
      sql("INSERT INTO t VALUES (1, 'x', 't1'), (2, 'x', 't2'), (3, 'x', 't3'), (4, 'x', 't4')")

      val query =
        """
          |SELECT r.id, u.data
          |FROM (SELECT t.id AS id FROM testcat.ns.a a JOIN t ON a.id = t.id AND a.k = t.k) r
          |JOIN testcat.ns.u u ON r.id = u.id
          |""".stripMargin
      val expected = Seq(Row(1, "u1"), Row(2, "u2"), Row(3, "u3"), Row(4, "u4"))

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

  test("SPARK-59050: SPJ: inner join clears the spurious marker on both member orders") {
    // `t`'s id=3 can match nothing on `a` (keys {1, 2}), so the inner join's marker is spurious
    // and cleared at construction (see `ShuffledJoin`). If it survived, the subset gate in
    // `areKeysCompatible` would refuse `u`'s wider key set and cost a shuffle no sibling order
    // can rescue. Both join orders must plan the expected shape: the single first-join shuffle
    // plus a `GroupPartitionsExec` on each side of the storage-partitioned second join.
    createTable("a", columns, Array(identity("id")))
    createTable("u", columns, Array(identity("id")))
    sql("INSERT INTO testcat.ns.a VALUES (1, 'a1', NULL), (2, 'a2', NULL)")
    sql("INSERT INTO testcat.ns.u VALUES (1, 'u1', NULL), (2, 'u2', NULL), (3, 'u3', NULL)")

    withTable("t") {
      sql("CREATE TABLE t (id INT, data STRING) USING parquet")
      sql("INSERT INTO t VALUES (1, 't1'), (2, 't2'), (3, 't3')")

      // `t` first puts the marked member ahead of its unmarked sibling in the collection;
      // `a` first is the mirror order.
      for (side <- Seq("t", "a")) {
        val query = if (side == "t") {
          """
            |SELECT t.id, u.data
            |FROM t JOIN testcat.ns.a a ON a.id = t.id
            |JOIN testcat.ns.u u ON t.id = u.id
            |""".stripMargin
        } else {
          """
            |SELECT a.id, u.data
            |FROM testcat.ns.a a JOIN t ON a.id = t.id
            |JOIN testcat.ns.u u ON a.id = u.id
            |""".stripMargin
        }
        withSQLConf(
            SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
            "spark.sql.autoBroadcastJoinThreshold" -> "-1",
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
          val df = sql(query)
          checkAnswer(df, Seq(Row(1, "u1"), Row(2, "u2")))
          assertShuffleMayContainUnknownPartitionKeys(df.queryExecution.executedPlan, Seq(true))
          assert(collectGroupPartitions(df.queryExecution.executedPlan).nonEmpty,
            s"side=$side: the second join should storage-partition, got: " +
              df.queryExecution.executedPlan)
        }
      }
    }
  }

  test("SPARK-59050: SPJ: inner join keeps the marker beside a keyless sibling") {
    // A keyless sibling makes no keyed claim about its rows, so it cannot prove the marked
    // side's out-of-set rows away: the join equality can still match them, and they stay where
    // the claim pins them. `keyedMarkerOf` answers `None` for such an input and the clearing
    // must read that as "no unmarked keyed member", not as "unmarked" -- mapping `None` to
    // `false` would clear the marker here while every sibling that carries a
    // `KeyedPartitioning` stays green.
    val attrA = AttributeReference("a", IntegerType)()
    val attrB = AttributeReference("b", IntegerType)()
    val keys = Seq(InternalRow(1), InternalRow(2))
    def markedExchange(e: AttributeReference): ShuffleExchangeExec =
      ShuffleExchangeExec(
        KeyedPartitioning(Seq(e), keys).copy(mayContainUnknownPartitionKeys = true),
        new LocalTableScanExec(Seq(e), Nil, None))
    def keylessExchange(e: AttributeReference): ShuffleExchangeExec =
      ShuffleExchangeExec(physical.HashPartitioning(Seq(e), keys.length),
        new LocalTableScanExec(Seq(e), Nil, None))
    val joins = Seq(
      SortMergeJoinExec(attrA :: Nil, attrB :: Nil, Inner, None,
        markedExchange(attrA), keylessExchange(attrB)),
      SortMergeJoinExec(attrA :: Nil, attrB :: Nil, Inner, None,
        keylessExchange(attrB), markedExchange(attrA)))
    joins.zipWithIndex.foreach { case (join, idx) =>
      val leaves = flattenKeyedPartitionings(join.outputPartitioning)
      assert(leaves.size == 1, s"order $idx: $leaves")
      assert(leaves.forall(_.mayContainUnknownPartitionKeys),
        s"order $idx: a keyless sibling must not clear the marker: $leaves")
    }
  }

  test("SPARK-59050: SPJ: inner join marker clearing reaches nested collections") {
    // `ShuffledJoin`'s `InnerLike` arm passes each child's partitioning into the joined
    // collection as reported, so the next inner join can find marked members nested inside a
    // collection inherited from an all-marked inner join below. SQL can produce that shape:
    // two one-side-shuffled RIGHT OUTER joins with the same declared keys each expose a marked
    // output, joining those two keeps an all-marked collection, and a following join against an
    // accurate keyed table supplies the unmarked sibling (the end-to-end counterpart below
    // pins that path). The nodes are hand-built here to isolate the shape from planner
    // choices, through the same exchange-leaf idiom used above.
    val attrA = AttributeReference("a", IntegerType)()
    val attrB = AttributeReference("b", IntegerType)()
    val keys = Seq(InternalRow(1), InternalRow(2))
    def markedKP(e: AttributeReference): KeyedPartitioning =
      KeyedPartitioning(Seq(e), keys).copy(mayContainUnknownPartitionKeys = true)
    def markedExchange(e: AttributeReference): ShuffleExchangeExec =
      ShuffleExchangeExec(markedKP(e), new LocalTableScanExec(Seq(e), Nil, None))
    def plainExchange(e: AttributeReference): ShuffleExchangeExec =
      ShuffleExchangeExec(KeyedPartitioning(Seq(e), keys),
        new LocalTableScanExec(Seq(e), Nil, None))
    // The first join: two marked children, no unmarked sibling -> its collection stays marked.
    val inner1 = SortMergeJoinExec(attrA :: Nil, attrA :: Nil, Inner, None,
      markedExchange(attrA), markedExchange(attrA))
    // The second join nests that collection next to an unmarked sibling -> the clearing must
    // reach inside it.
    val inner2 = SortMergeJoinExec(attrA :: Nil, attrB :: Nil, Inner, None,
      inner1, plainExchange(attrB))
    val cleared = flattenKeyedPartitionings(inner2.outputPartitioning)
    assert(cleared.size == 3, cleared.toString)
    assert(cleared.forall(!_.mayContainUnknownPartitionKeys),
      s"the unmarked sibling must clear every marked member: $cleared")
    // And a marked sibling of an all-marked nested collection excuses nothing: all stay marked.
    val inner3 = SortMergeJoinExec(attrA :: Nil, attrB :: Nil, Inner, None,
      markedExchange(attrA), inner1)
    val kept = flattenKeyedPartitionings(inner3.outputPartitioning)
    assert(kept.size == 3, kept.toString)
    assert(kept.forall(_.mayContainUnknownPartitionKeys),
      s"an all-marked chain must keep its markers: $kept")
  }

  test("SPARK-59050: SPJ: marker clearing reaches a nested all-marked collection built by SQL") {
    // End-to-end counterpart to the hand-built test above. The first two RIGHT OUTER joins
    // one-side-shuffle `t` and `u` onto `a`'s and `b`'s declared keys {1, 2}; their id=3 rows
    // are out-of-set, so both join outputs are marked. The inner join of those two outputs
    // pairs marked against marked on equal key sequences, so its collection stays all-marked.
    // The final join against the accurate keyed table `w` then supplies the unmarked sibling:
    // the clearing must reach inside the nested collection, and id=3's rows, dropped by `w`,
    // must not cost the plan its storage-partitioned final join.
    createTable("a", columns, Array(identity("id")))
    createTable("b", columns, Array(identity("id")))
    createTable("w", columns, Array(identity("id")))
    sql("INSERT INTO testcat.ns.a VALUES (1, 'a1', NULL), (2, 'a2', NULL)")
    sql("INSERT INTO testcat.ns.b VALUES (1, 'b1', NULL), (2, 'b2', NULL)")
    sql("INSERT INTO testcat.ns.w VALUES (1, 'w1', NULL), (2, 'w2', NULL)")

    withTable("t", "u") {
      sql("CREATE TABLE t (id INT, data STRING) USING parquet")
      sql("INSERT INTO t VALUES (1, 't1'), (2, 't2'), (3, 't3')")
      sql("CREATE TABLE u (id INT, data STRING) USING parquet")
      sql("INSERT INTO u VALUES (1, 'u1'), (2, 'u2'), (3, 'u3')")

      val query =
        """
          |SELECT r3.id, w.data
          |FROM (
          |  SELECT r1.id FROM (
          |    SELECT tt.id AS id FROM testcat.ns.a RIGHT OUTER JOIN t tt ON a.id = tt.id
          |  ) r1
          |  JOIN (
          |    SELECT uu.id AS id FROM testcat.ns.b RIGHT OUTER JOIN u uu ON b.id = uu.id
          |  ) r2 ON r1.id = r2.id
          |) r3
          |JOIN testcat.ns.w ON r3.id = w.id
          |""".stripMargin
      val expected = Seq(Row(1, "w1"), Row(2, "w2"))

      // Baseline without SPJ.
      withSQLConf(SQLConf.V2_BUCKETING_ENABLED.key -> "false") {
        checkAnswer(sql(query), expected)
      }

      withSQLConf(
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          "spark.sql.autoBroadcastJoinThreshold" -> "-1",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql(query)
        checkAnswer(df, expected)
        // Only the two one-side shuffles carry the marker: the final join storage-partitions
        // without a re-shuffle (a third shuffle here would mean the marked-vs-marked pairing
        // was refused).
        assertShuffleMayContainUnknownPartitionKeys(df.queryExecution.executedPlan,
          Seq(true, true))
        // The clearing reached the nested collection: every keyed leaf of the final output is
        // unmarked.
        val leaves = flattenKeyedPartitionings(df.queryExecution.executedPlan.outputPartitioning)
        assert(leaves.nonEmpty, s"expected keyed leaves in the final output: " +
          df.queryExecution.executedPlan)
        assert(leaves.forall(!_.mayContainUnknownPartitionKeys),
          s"the unmarked sibling must clear the nested collection: $leaves")
      }
    }
  }

  test("SPARK-59050: SPJ: inner join drops out-of-set rows before the cleared marker is trusted") {
    // Data-level companion to the spurious-marker tests: `t` here genuinely holds an out-of-set
    // row (5, 'z') that a's declared keys cannot match. The inner join drops it, so every row
    // reaching the cleared-marker layout carries a declared key; the downstream storage-
    // partitioned join must then return exactly the matched rows: ids 1..4, and NOT id=5 even
    // though u holds it. If the marker were cleared for a shape that keeps out-of-set rows,
    // this query would silently lose or misplace matches.
    val cols = Array(
      Column.create("id", IntegerType),
      Column.create("k", StringType),
      Column.create("data", StringType))
    createTable("a", cols, Array(identity("id"), identity("k")))
    createTable("u", cols, Array(identity("id")))
    sql("INSERT INTO testcat.ns.a VALUES " +
      "(1, 'x', 'a1'), (2, 'x', 'a2'), (3, 'x', 'a3'), (4, 'x', 'a4')")
    sql("INSERT INTO testcat.ns.u VALUES (1, NULL, 'u1'), (2, NULL, 'u2'), (3, NULL, 'u3'), " +
      "(4, NULL, 'u4'), (5, NULL, 'u5')")

    withTable("t") {
      sql("CREATE TABLE t (id INT, k STRING, data STRING) USING parquet")
      sql("INSERT INTO t VALUES (1, 'x', 't1'), (2, 'x', 't2'), (3, 'x', 't3'), " +
        "(4, 'x', 't4'), (5, 'z', 't5')")

      val query =
        """
          |SELECT r.id, u.data
          |FROM (SELECT t.id AS id FROM t JOIN testcat.ns.a a ON a.id = t.id AND a.k = t.k) r
          |JOIN testcat.ns.u u ON r.id = u.id
          |""".stripMargin
      // id=5 dropped by the inner join (a has no (5, *)); u's id=5 row has no partner.
      val expected = Seq(Row(1, "u1"), Row(2, "u2"), Row(3, "u3"), Row(4, "u4"))

      // Baseline without SPJ.
      withSQLConf(SQLConf.V2_BUCKETING_ENABLED.key -> "false") {
        checkAnswer(sql(query), expected)
      }

      withSQLConf(
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql(query)
        checkAnswer(df, expected)
        // On this branch the Project between the joins drops the keyed claim, so the second
        // join one-side-shuffles the join output onto u's layout; every row reaching it
        // carries a declared key, so the re-shuffle is sound.
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
    // `groupedSatisfies` refuses the subset relaxation for an unknown-keyed layout.
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
            assert(shuffles.exists(!_.outputPartitioning.isInstanceOf[KeyedPartitioning]),
              s"expected a non-keyed (window) exchange, got:\n$plan")
          }
        }
      }
    }
  }

  test("SPARK-59050: SPJ: a window keyed on the full key of an unknown-keyed layout does not " +
    "shuffle") {
    // Positive control for the marked branch of `groupedSatisfies`: whole declared keys
    // co-locate, so
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
    // degrades to per-partition sorting and emits [1, 4, 2]. `keysSatisfy` rejects the ordering
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


  test("SPARK-59050: SPJ: a reducer-free identity regrouping keeps the unknown-keyed claim") {
    // Positive counterpart to the regrouping test above. The first join one-side-shuffles pt
    // onto pa's sorted keys [1, 2, 3] and marks the layout (pt's id=4 is out-of-set). The second
    // join aligns that marked [1, 2, 3] superset with pb's [1, 2] by pushing the merged keys
    // [1, 2, 3] to both sides. The marked side already holds exactly the merged keys, so its
    // GroupPartitionsExec is a reducer-free identity grouping and must keep the claim; only the
    // subset side pads. This pins that the identity branch is reachable, not dead code.
    val cols = Array(Column.create("id", IntegerType), Column.create("data", StringType))
    createTable("pa", cols, Array(identity("id")))
    createTable("pb", cols, Array(identity("id")))
    sql("INSERT INTO testcat.ns.pa VALUES (1, 'a1'), (2, 'a2'), (3, 'a3')")
    sql("INSERT INTO testcat.ns.pb VALUES (1, 'b1'), (2, 'b2')")
    withTable("pt") {
      sql("CREATE TABLE pt (id INT, data STRING) USING parquet")
      sql("INSERT INTO pt VALUES (1, 't1'), (2, 't2'), (3, 't3'), (4, 't4')")
      val query =
        """
          |SELECT r.id, pb.data
          |FROM (SELECT pt.id AS id FROM testcat.ns.pa RIGHT OUTER JOIN pt ON pa.id = pt.id) r
          |JOIN testcat.ns.pb ON r.id = pb.id
          |""".stripMargin
      val expected = Seq(Row(1, "b1"), Row(2, "b2"))

      // Baseline: no SPJ.
      withSQLConf(SQLConf.V2_BUCKETING_ENABLED.key -> "false") {
        checkAnswer(sql(query), expected)
      }
      withSQLConf(
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          "spark.sql.autoBroadcastJoinThreshold" -> "-1",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql(query)
        checkAnswer(df, expected)
        val plan = df.queryExecution.executedPlan
        // A GroupPartitionsExec with no reducers whose output partition i holds exactly input
        // partition i is an identity grouping; the fix keeps the unknown-keyed claim for it.
        val identityGpe = collectAllGroupPartitions(plan).find { g =>
          g.reducers.isEmpty && g.groupedPartitions.zipWithIndex.forall {
            case ((_, inputIndices), outputIndex) =>
              inputIndices.lengthCompare(1) == 0 && inputIndices.head == outputIndex
          }
        }
        assert(identityGpe.isDefined,
          s"expected a reducer-free identity GroupPartitionsExec, got:\n$plan")
        identityGpe.get.outputPartitioning match {
          case k: KeyedPartitioning =>
            assert(k.mayContainUnknownPartitionKeys,
              "the identity grouping must keep the unknown-keyed claim")
          case other =>
            fail(s"expected a KeyedPartitioning output, got $other")
        }
      }
    }
  }

}
