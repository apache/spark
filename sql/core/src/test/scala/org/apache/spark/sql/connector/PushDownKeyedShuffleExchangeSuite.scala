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
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, AttributeReference, Explode, Expression, Literal, TransformExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, KeyedPartitioning, ROUTING_LOST}
import org.apache.spark.sql.connector.catalog.{Column, Identifier}
import org.apache.spark.sql.connector.catalog.functions._
import org.apache.spark.sql.connector.expressions.Expressions._
import org.apache.spark.sql.execution.{FilterExec, GenerateExec, LocalTableScanExec, ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{PushDownKeyedShuffleExchange, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, MergingSessionsExec, ObjectHashAggregateExec}
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, EnsureRequirements, ReusedExchangeExec, ShuffleExchangeExec, ShuffleExchangeLike, ValidateRequirements}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}
import org.apache.spark.sql.execution.window.{Final, Partial => WGLPartial, WindowExec, WindowGroupLimitExec}
import org.apache.spark.sql.expressions.{Aggregator, Window}
import org.apache.spark.sql.functions.{col, udaf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{FloatType, LongType, StringType}

class PushDownKeyedShuffleExchangeSuite extends KeyGroupedPartitioningSuiteBase {

  private val functions = Seq(
    UnboundYearsFunction,
    UnboundDaysFunction,
    UnboundBucketFunction,
    UnboundTruncateFunction)

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.V2_BUCKETING_ENABLED, true)
    .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, -1L)

  before {
    functions.foreach { f =>
      catalog.createFunction(Identifier.of(Array.empty, f.name()), f)
    }
  }

  after {
    catalog.clearTables()
    catalog.clearFunctions()
  }

  private def shuffles(plan: SparkPlan): Seq[ShuffleExchangeLike] =
    collect(plan) { case s: ShuffleExchangeLike => s }

  /** Whether a keyed shuffle sits below the final aggregate, i.e. right above a partial one. */
  private def isPushedDown(plan: SparkPlan): Boolean = shuffles(plan).exists { s =>
    s.outputPartitioning.isInstanceOf[KeyedPartitioning] &&
      s.child.collectFirst { case a: BaseAggregateExec => a }
        .exists(_.aggregateExpressions.forall(_.mode == Partial))
  }

  // Dynamic partition pruning injects a pruning subquery on a filtered join side, and the rule's
  // query-wide subquery gate then keeps both shuffles; "disabled for a query with subqueries"
  // covers that gate directly.
  private def withPushDownConf[T](confs: (String, String)*)(f: => T): T = {
    withSQLConf(Seq(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false",
        SQLConf.PUSH_DOWN_KEYED_SHUFFLE_ENABLED.key -> "true",
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
        SQLConf.SHUFFLE_PARTITIONS.key -> "2") ++ confs: _*)(f)
  }

  /** Runs `query`, checks its answer and returns its final adaptive plan. */
  private def run(query: String, expected: Seq[Row]): SparkPlan = {
    val df = sql(query)
    checkAnswer(df, expected)
    val plan = df.queryExecution.executedPlan
    assert(ValidateRequirements.validate(stripAQEPlan(plan)), s"plan does not validate:\n$plan")
    plan
  }

  private def check(
      query: String,
      expected: Seq[Row],
      expectedShuffles: Int,
      confs: (String, String)*): SparkPlan = withPushDownConf(confs: _*) {
    val plan = run(query, expected)
    assert(shuffles(plan).size == expectedShuffles,
      s"expected $expectedShuffles shuffles, got ${shuffles(plan).size}:\n$plan")
    plan
  }

  private def loadData(): Unit = {
    createTable(items, itemsColumns, Array(identity("id")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
      "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")
    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 40.0, cast('2020-01-01' as timestamp)), " +
      "(2, 10.0, cast('2020-01-02' as timestamp)), " +
      "(3, 25.5, cast('2020-01-03' as timestamp)), " +
      "(4, 20.0, cast('2020-01-04' as timestamp))")
  }

  private val aggregateJoin =
    s"""SELECT i.id, p.total FROM testcat.ns.$items i
       |JOIN (
       |  SELECT item_id, sum(price) AS total
       |  FROM testcat.ns.$purchases GROUP BY item_id
       |) p ON i.id = p.item_id""".stripMargin
  private val aggregateJoinAnswer =
    Seq(Row(1L, 40.0), Row(1L, 40.0), Row(2L, 10.0), Row(3L, 25.5))

  test("aggregate runs on the keyed shuffle") {
    loadData()
    val plan = check(aggregateJoin, aggregateJoinAnswer, expectedShuffles = 1)
    val stages = collect(plan) { case s: ShuffleQueryStageExec => s }
    assert(stages.size == 1, plan)
    assert(stages.head.shuffle.outputPartitioning.isInstanceOf[KeyedPartitioning], plan)
    assert(isPushedDown(plan), plan)
    // With more shuffle partitions than the keyed side's 3, `EnsureRequirements` shuffles both
    // sides on hash, so there is no keyed shuffle to push down.
    check(aggregateJoin, aggregateJoinAnswer, expectedShuffles = 2,
      SQLConf.SHUFFLE_PARTITIONS.key -> "8")
  }

  test("disabled by config, without the one-side shuffle or under partial clustering") {
    loadData()
    check(aggregateJoin, aggregateJoinAnswer, expectedShuffles = 2,
      SQLConf.PUSH_DOWN_KEYED_SHUFFLE_ENABLED.key -> "false")
    check(aggregateJoin, aggregateJoinAnswer, expectedShuffles = 2,
      SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "false")
    // `KeyedShuffleSpec.canCreatePartitioning` plans no keyed shuffle here, so both sides take a
    // hash shuffle with or without the rule's own gate.
    check(aggregateJoin, aggregateJoinAnswer, expectedShuffles = 2,
      SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true")
  }

  test("disabled for a query with subqueries") {
    loadData()
    val query =
      s"""SELECT i.id, p.total FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, sum(price) AS total
         |  FROM testcat.ns.$purchases GROUP BY item_id
         |) p ON i.id = p.item_id
         |WHERE p.total > (SELECT avg(price) FROM testcat.ns.$purchases)""".stripMargin
    val expected = Seq(Row(1L, 40.0), Row(1L, 40.0), Row(3L, 25.5))
    check(query, expected, expectedShuffles = 2)
    check(query, expected, expectedShuffles = 1,
      SQLConf.PUSH_DOWN_KEYED_SHUFFLE_ALLOW_WITH_SUBQUERIES_ENABLED.key -> "true")
    // The non-adaptive list applies the same gate.
    def nonAdaptivePlan(allow: String): SparkPlan = withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
        SQLConf.PUSH_DOWN_KEYED_SHUFFLE_ENABLED.key -> "true",
        SQLConf.PUSH_DOWN_KEYED_SHUFFLE_ALLOW_WITH_SUBQUERIES_ENABLED.key -> allow,
        SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      val df = sql(query)
      checkAnswer(df, expected)
      df.queryExecution.executedPlan
    }
    assert(shuffles(nonAdaptivePlan("false")).size == 2)
    assert(shuffles(nonAdaptivePlan("true")).size == 1)
  }

  test("non-adaptive plans are rewritten too") {
    loadData()
    def plan(rule: String): SparkPlan = withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
        SQLConf.PUSH_DOWN_KEYED_SHUFFLE_ENABLED.key -> rule,
        SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      val df = sql(aggregateJoin)
      checkAnswer(df, aggregateJoinAnswer)
      df.queryExecution.executedPlan
    }
    assert(shuffles(plan("false")).size == 2)
    assert(shuffles(plan("true")).size == 1)
  }

  test("session window aggregate") {
    createTable(items, itemsColumns, Array(identity("id")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")
    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 10.0, cast('2020-01-01 00:00:00' as timestamp)), " +
      "(1, 20.0, cast('2020-01-01 12:00:00' as timestamp)), " +
      "(1, 30.0, cast('2020-01-20 00:00:00' as timestamp)), " +
      "(2, 40.0, cast('2020-01-01 00:00:00' as timestamp)), " +
      "(2, 50.0, cast('2020-01-01 06:00:00' as timestamp)), " +
      "(4, 60.0, cast('2020-01-01 00:00:00' as timestamp))")
    val query =
      s"""SELECT i.id, p.cnt FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, count(*) AS cnt
         |  FROM testcat.ns.$purchases
         |  GROUP BY item_id, session_window(time, '1 day')
         |) p ON i.id = p.item_id""".stripMargin
    // Item 1 has two sessions (two rows within a day, one 19 days later), item 2 has one.
    val expected = Seq(Row(1L, 2L), Row(1L, 1L), Row(2L, 2L))
    val plan = check(query, expected, expectedShuffles = 1)
    assert(collect(plan) { case m: MergingSessionsExec => m }.nonEmpty, plan)
    // Only the merge step's input sort: `MergingSessionsExec` reports it, so the final
    // aggregate above needs none of its own.
    assert(collect(plan) { case s: SortExec => s }.size == 1, plan)
    assert(isPushedDown(plan), plan)
    // The same rows come from a hash shuffle over the whole grouping.
    check(query, expected, expectedShuffles = 2,
      SQLConf.PUSH_DOWN_KEYED_SHUFFLE_ENABLED.key -> "false")
  }

  test("a filter between the aggregate and the keyed shuffle") {
    loadData()
    val plan = check(
      s"""SELECT i.id, p.total FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, sum(price) AS total
         |  FROM testcat.ns.$purchases GROUP BY item_id HAVING sum(price) > 15
         |) p ON i.id = p.item_id""".stripMargin,
      Seq(Row(1L, 40.0), Row(1L, 40.0), Row(3L, 25.5)),
      expectedShuffles = 1)
    assert(collect(plan) {
      case f: FilterExec if f.child.isInstanceOf[BaseAggregateExec] => f
    }.nonEmpty, plan)
    assert(isPushedDown(plan), plan)
  }

  test("rows of one key stay together, declared by the keyed side or not") {
    createTable(items, itemsColumns, Array(identity("id")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
      "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")
    createTable(purchases, purchasesColumns, Array.empty)
    // One row per split. Keys 1, 4 and 5 repeat across splits, and the keyed side does not declare
    // 4 or 5, so the keyed shuffle routes those by hash.
    val rows = Seq(1 -> 1.0, 1 -> 2.0, 1 -> 4.0, 2 -> 8.0, 4 -> 16.0, 4 -> 32.0, 4 -> 64.0,
      5 -> 128.0, 5 -> 256.0, 3 -> 512.0)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " + rows.zipWithIndex.map {
      case ((k, p), i) => s"($k, $p, cast('2020-01-0${i % 9 + 1}' as timestamp))"
    }.mkString(", "))
    Seq(
      s"""SELECT p.item_id, p.total, p.c, i.id FROM (
         |  SELECT item_id, sum(price) AS total, count(*) AS c
         |  FROM testcat.ns.$purchases GROUP BY item_id
         |) p LEFT JOIN testcat.ns.$items i ON i.id = p.item_id""".stripMargin,
      s"""SELECT p.item_id, p.c, i.id FROM (
         |  SELECT item_id, count(DISTINCT price) AS c
         |  FROM testcat.ns.$purchases GROUP BY item_id
         |) p FULL OUTER JOIN testcat.ns.$items i ON i.id = p.item_id""".stripMargin,
      s"""SELECT p.item_id, p.rn, i.id FROM (
         |  SELECT item_id, row_number() OVER (PARTITION BY item_id ORDER BY price) AS rn
         |  FROM testcat.ns.$purchases
         |) p LEFT JOIN testcat.ns.$items i ON i.id = p.item_id""".stripMargin
    ).foreach { query =>
      val (expected, base) =
        withPushDownConf(SQLConf.PUSH_DOWN_KEYED_SHUFFLE_ENABLED.key -> "false") {
          val df = sql(query)
          val rows = df.collect().toSeq
          (rows, shuffles(df.queryExecution.executedPlan).size)
        }
      val plan = withPushDownConf() { run(query, expected) }
      assert(shuffles(plan).size == base - 1, plan)
    }
  }

  test("a non-deterministic operator ends the walk") {
    loadData()
    def query(column: String): String =
      s"""SELECT i.id, p.total FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, total, $column AS r FROM (
         |    SELECT item_id, sum(price) AS total
         |    FROM testcat.ns.$purchases GROUP BY item_id)
         |) p ON i.id = p.item_id AND p.r >= 0""".stripMargin
    check(query("total * 2"), aggregateJoinAnswer, expectedShuffles = 1)
    check(query("rand(1)"), aggregateJoinAnswer, expectedShuffles = 2)
  }

  test("aliased group key") {
    loadData()
    check(
      s"""SELECT i.id, p.k FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id AS k, sum(price) AS total
         |  FROM testcat.ns.$purchases GROUP BY item_id
         |) p ON i.id = p.k""".stripMargin,
      Seq(Row(1L, 1L), Row(1L, 1L), Row(2L, 2L), Row(3L, 3L)),
      expectedShuffles = 1)
  }

  test("a join key computed by the aggregate keeps both shuffles") {
    loadData()
    check(
      s"""SELECT i.id, p.cnt FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, count(*) AS cnt
         |  FROM testcat.ns.$purchases GROUP BY item_id
         |) p ON i.id = p.cnt""".stripMargin,
      Seq.fill(8)(Row(1L, 1L)),
      expectedShuffles = 2)
  }

  test("a layout on a subset of the group keys is admitted") {
    loadData()
    // The keyed layout on `item_id` alone covers only part of the aggregate's keys. Groups are
    // still co-located, in coarser partitions, as they are for a source reporting this layout.
    check(
      s"""SELECT i.id, p.cnt FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, price, count(*) AS cnt
         |  FROM testcat.ns.$purchases GROUP BY item_id, price
         |) p ON i.id = p.item_id""".stripMargin,
      Seq(Row(1L, 1L), Row(1L, 1L), Row(2L, 1L), Row(3L, 1L)),
      expectedShuffles = 1)
  }

  test("a layout on a subset of the group keys is refused when all keys are required") {
    loadData()
    val query =
      s"""SELECT i.id, p.cnt FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, price, count(*) AS cnt
         |  FROM testcat.ns.$purchases GROUP BY item_id, price
         |) p ON i.id = p.item_id""".stripMargin
    val expected = Seq(Row(1L, 1L), Row(1L, 1L), Row(2L, 1L), Row(3L, 1L))
    check(query, expected, expectedShuffles = 2,
      SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_DISTRIBUTION.key -> "true")
    // A layout covering every key is still admitted under the same setting.
    check(aggregateJoin, aggregateJoinAnswer, expectedShuffles = 1,
      SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_DISTRIBUTION.key -> "true")
  }

  test("a layout held by a later keyed member is admitted") {
    loadData()
    // The aggregate outputs `item_id` and an alias of it, so the layout is not the first keyed
    // member of the reported collection.
    check(
      s"""SELECT i.id, p.k, p.total FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, item_id AS k, sum(price) AS total
         |  FROM testcat.ns.$purchases GROUP BY item_id
         |) p ON i.id = p.item_id""".stripMargin,
      Seq(Row(1L, 1L, 40.0), Row(1L, 1L, 40.0), Row(2L, 2L, 10.0), Row(3L, 3L, 25.5)),
      expectedShuffles = 1)
  }

  test("a failable layout expression is not moved below a filter") {
    loadData()
    createTable("keyed_codes", Array(Column.create("id", LongType)), Array(identity("id")))
    sql("INSERT INTO testcat.ns.keyed_codes VALUES (1), (3)")
    createTable("codes",
      Array(Column.create("code", StringType), Column.create("price", FloatType)), Array.empty)
    sql("INSERT INTO testcat.ns.codes VALUES ('1', 40.0), ('3', 15.5), ('n/a', 1.0)")
    // The keyed shuffle evaluates `cast(code as bigint)` only on the groups `having` keeps, so the
    // rewrite must not move it below the filter, where the 'n/a' group would reach the cast.
    withPushDownConf() {
      val df = sql(
        s"""SELECT k.id, p.total FROM testcat.ns.keyed_codes k
           |JOIN (
           |  SELECT code, sum(price) AS total FROM testcat.ns.codes
           |  GROUP BY code HAVING sum(price) > 10
           |) p ON k.id = CAST(p.code AS BIGINT)""".stripMargin)
      checkAnswer(df, Seq(Row(1L, 40.0), Row(3L, 15.5)))
      assert(shuffles(df.queryExecution.executedPlan).size == 2,
        df.queryExecution.executedPlan)
    }
    // An identity layout is total, so a filter does not stop the rewrite.
    check(
      s"""SELECT i.id, p.total FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, sum(price) AS total FROM testcat.ns.$purchases
         |  GROUP BY item_id HAVING sum(price) > 15
         |) p ON i.id = p.item_id""".stripMargin,
      Seq(Row(1L, 40.0), Row(1L, 40.0), Row(3L, 25.5)),
      expectedShuffles = 1)
  }

  test("window runs on the keyed shuffle") {
    loadData()
    val plan = check(
      s"""SELECT i.id, p.item_id, p.rn FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, row_number() OVER (PARTITION BY item_id ORDER BY price) AS rn
         |  FROM testcat.ns.$purchases
         |) p ON i.id = p.item_id""".stripMargin,
      Seq(Row(1L, 1L, 1), Row(1L, 1L, 1), Row(2L, 2L, 1), Row(3L, 3L, 1)),
      expectedShuffles = 1,
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false")
    // The window's input sort also serves the join, so it is the only one.
    val sorts = collect(plan) { case s: SortExec => s }
    assert(sorts.size == 1, plan)
    val window = collect(plan) { case w: WindowExec => w }.head
    assert(window.child.fastEquals(sorts.head), plan)
    // The keyed shuffle is below that sort, so the window reads the storage layout.
    assert(collect(sorts.head) {
      case s: ShuffleQueryStageExec
          if s.shuffle.outputPartitioning.isInstanceOf[KeyedPartitioning] => s
    }.nonEmpty, plan)
    check(
      s"""SELECT i.id, p.k, p.rn FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id AS k,
         |    row_number() OVER (PARTITION BY item_id ORDER BY price) AS rn
         |  FROM testcat.ns.$purchases
         |) p ON i.id = p.k""".stripMargin,
      Seq(Row(1L, 1L, 1), Row(1L, 1L, 1), Row(2L, 2L, 1), Row(3L, 3L, 1)),
      expectedShuffles = 1)
  }

  test("window group limit runs on the keyed shuffle") {
    loadData()
    val plan = check(
      s"""SELECT i.id, p.price FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, price FROM (
         |    SELECT item_id, price,
         |      rank() OVER (PARTITION BY item_id ORDER BY price) AS rk
         |    FROM testcat.ns.$purchases)
         |  WHERE rk = 1
         |) p ON i.id = p.item_id""".stripMargin,
      Seq(Row(1L, 40.0f), Row(1L, 40.0f), Row(2L, 10.0f), Row(3L, 25.5f)),
      expectedShuffles = 1)
    assert(collect(plan) {
      case l: WindowGroupLimitExec if l.mode == Final => l
    }.nonEmpty, plan)
    assert(shuffles(plan).exists { s =>
      s.outputPartitioning.isInstanceOf[KeyedPartitioning] &&
        s.child.collectFirst { case l: WindowGroupLimitExec => l }.exists(_.mode == WGLPartial)
    }, plan)
  }

  test("a non-deterministic filter ends the walk") {
    loadData()
    def query(having: String): String =
      s"""SELECT i.id, p.total FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, sum(price) AS total
         |  FROM testcat.ns.$purchases GROUP BY item_id HAVING $having
         |) p ON i.id = p.item_id""".stripMargin
    check(query("sum(price) > 15"),
      Seq(Row(1L, 40.0), Row(1L, 40.0), Row(3L, 25.5)), expectedShuffles = 1)
    // A partition-seeded condition keeps a different set of rows under another partitioning.
    withPushDownConf() {
      val df = sql(query("rand(1) < 0.5"))
      df.collect()
      assert(shuffles(df.queryExecution.executedPlan).size == 2,
        df.queryExecution.executedPlan)
    }
  }

  test("a window expression mentioning a non-deterministic seed is pulled below the window") {
    loadData()
    withPushDownConf() {
      val df = sql(
        s"""SELECT i.id, p.f FROM testcat.ns.$items i
           |JOIN (
           |  SELECT item_id, first_value(rand()) OVER (PARTITION BY item_id ORDER BY price) AS f
           |  FROM testcat.ns.$purchases
           |) p ON i.id = p.item_id""".stripMargin)
      df.collect()
      val plan = df.queryExecution.executedPlan
      assert(collect(plan) { case w: WindowExec => w }
        .forall(_.expressions.forall(_.deterministic)), plan)
      assert(shuffles(plan).size == 1, plan)
    }
  }

  test("window on other keys keeps both shuffles") {
    loadData()
    check(
      s"""SELECT i.id, p.item_id, p.rn FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, row_number() OVER (PARTITION BY price ORDER BY item_id) AS rn
         |  FROM testcat.ns.$purchases
         |) p ON i.id = p.item_id""".stripMargin,
      Seq(Row(1L, 1L, 1), Row(1L, 1L, 1), Row(2L, 2L, 1), Row(3L, 3L, 1)),
      expectedShuffles = 2)
    check(
      s"""SELECT i.id, p.item_id FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, sum(item_id) OVER (PARTITION BY item_id) AS total
         |  FROM testcat.ns.$purchases
         |) p ON i.id = p.total""".stripMargin,
      Seq(Row(1L, 1L), Row(1L, 1L), Row(2L, 2L), Row(3L, 3L)),
      expectedShuffles = 2)
  }

  test("aggregate under a window over the same keys") {
    loadData()
    check(
      s"""SELECT i.id, p.total, p.rn FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, total, row_number() OVER (PARTITION BY item_id ORDER BY total) AS rn
         |  FROM (
         |    SELECT item_id, sum(price) AS total FROM testcat.ns.$purchases GROUP BY item_id)
         |) p ON i.id = p.item_id""".stripMargin,
      Seq(Row(1L, 40.0, 1), Row(1L, 40.0, 1), Row(2L, 10.0, 1), Row(3L, 25.5, 1)),
      expectedShuffles = 1)
  }

  test("object hash aggregate runs on the keyed shuffle") {
    loadData()
    val plan = check(
      s"""SELECT i.id, p.prices FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, collect_list(price) AS prices
         |  FROM testcat.ns.$purchases GROUP BY item_id
         |) p ON i.id = p.item_id""".stripMargin,
      Seq(Row(1L, Seq(40.0f)), Row(1L, Seq(40.0f)), Row(2L, Seq(10.0f)),
        Row(3L, Seq(25.5f))),
      expectedShuffles = 1)
    assert(collect(plan) { case a: ObjectHashAggregateExec => a }.nonEmpty, plan)
    // The aggregate reports no ordering, so the join keeps its own sort above it.
    assert(collect(plan) { case s: SortExec => s }.size == 1, plan)
    assert(isPushedDown(plan), plan)
  }

  test("sort aggregate drops the join's sort") {
    loadData()
    val plan = check(
      s"""SELECT i.id, p.total FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, max(cast(price as string)) AS total
         |  FROM testcat.ns.$purchases GROUP BY item_id
         |) p ON i.id = p.item_id""".stripMargin,
      Seq(Row(1L, "40.0"), Row(1L, "40.0"), Row(2L, "10.0"), Row(3L, "25.5")),
      expectedShuffles = 1,
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false")
    // The aggregate's output is ordered on the join key, so the join needs no sort above it.
    val sortsOverAggregate = collect(plan) {
      case s: SortExec if s.child.isInstanceOf[BaseAggregateExec] => s
    }
    assert(sortsOverAggregate.isEmpty, plan)
    // One sort for the final aggregate and one for the partial one; nothing else.
    assert(collect(plan) { case s: SortExec => s }.size == 2, plan)
    assert(isPushedDown(plan), plan)
  }

  test("broadcast join is crossed through its streamed side") {
    loadData()
    val plan = check(
      s"""SELECT i.id, p.total FROM testcat.ns.$items i
         |JOIN (
         |  SELECT /*+ BROADCAST(b) */ a.item_id, a.total
         |  FROM (
         |    SELECT item_id, sum(price) AS total
         |    FROM testcat.ns.$purchases GROUP BY item_id) a
         |  JOIN (
         |    SELECT item_id FROM testcat.ns.$purchases WHERE price >= 0) b
         |  ON a.item_id = b.item_id
         |) p ON i.id = p.item_id""".stripMargin,
      Seq(Row(1L, 40.0), Row(1L, 40.0), Row(2L, 10.0), Row(3L, 25.5)),
      expectedShuffles = 1)
    val broadcast = collect(plan) { case j: BroadcastHashJoinExec => j }.head
    // The keyed shuffle now feeds the streamed side's aggregate, under the broadcast join.
    val streamed = if (broadcast.buildSide == BuildLeft) broadcast.right else broadcast.left
    assert(collect(streamed) {
      case s: ShuffleQueryStageExec
          if s.shuffle.outputPartitioning.isInstanceOf[KeyedPartitioning] => s
    }.nonEmpty, plan)
    assert(isPushedDown(plan), plan)
  }

  test("generate is crossed") {
    loadData()
    val plan = check(
      s"""SELECT i.id, g.x FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, explode(array(total, total)) AS x
         |  FROM (
         |    SELECT item_id, sum(price) AS total
         |    FROM testcat.ns.$purchases GROUP BY item_id)
         |) g ON i.id = g.item_id""".stripMargin,
      Seq(Row(1L, 40.0), Row(1L, 40.0), Row(1L, 40.0), Row(1L, 40.0),
        Row(2L, 10.0), Row(2L, 10.0), Row(3L, 25.5), Row(3L, 25.5)),
      expectedShuffles = 1)
    val generate = collect(plan) { case g: GenerateExec => g }.head
    assert(collect(generate) {
      case s: ShuffleQueryStageExec
          if s.shuffle.outputPartitioning.isInstanceOf[KeyedPartitioning] => s
    }.nonEmpty, plan)
    assert(isPushedDown(plan), plan)
  }

  test("a broadcast join's build-side column as the join key keeps both shuffles") {
    createTable(items, itemsColumns, Array(identity("id")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")
    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 1.0, cast('2020-01-01' as timestamp)), " +
      "(2, 2.0, cast('2020-01-02' as timestamp)), " +
      "(3, 3.0, cast('2020-01-03' as timestamp))")
    // Only the build side provides `k`, so the streamed side's shuffle cannot be laid out on it.
    check(
      s"""SELECT i.id, p.total FROM testcat.ns.$items i
         |JOIN (
         |  SELECT /*+ BROADCAST(b) */ a.item_id, a.total, b.k
         |  FROM (
         |    SELECT item_id, sum(price) AS total
         |    FROM testcat.ns.$purchases GROUP BY item_id) a
         |  JOIN (
         |    SELECT item_id, item_id AS k FROM testcat.ns.$purchases WHERE price >= 0) b
         |  ON a.item_id = b.item_id
         |) p ON i.id = p.k""".stripMargin,
      Seq(Row(1L, 1.0), Row(2L, 2.0), Row(3L, 3.0)),
      expectedShuffles = 2)
  }

  test("a user repartition below the operator keeps both shuffles") {
    createTable(items, itemsColumns, Array(identity("id")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")
    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 40.0, cast('2020-01-01' as timestamp)), " +
      "(2, 10.0, cast('2020-01-02' as timestamp)), " +
      "(3, 25.5, cast('2020-01-03' as timestamp)), " +
      "(4, 20.0, cast('2020-01-04' as timestamp))")
    // The aggregate's input is the user's own repartition, which the rule does not retarget.
    check(
      s"""SELECT i.id, p.total FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, sum(price) AS total
         |  FROM (SELECT /*+ REPARTITION(2, item_id) */ * FROM testcat.ns.$purchases)
         |  GROUP BY item_id
         |) p ON i.id = p.item_id""".stripMargin,
      Seq(Row(1L, 40.0), Row(2L, 10.0), Row(3L, 25.5)),
      expectedShuffles = 2)
  }

  test("broadcast nested loop join is crossed through its streamed side") {
    createTable(items, itemsColumns, Array(identity("id")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")
    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 40.0, cast('2020-01-01' as timestamp)), " +
      "(2, 10.0, cast('2020-01-02' as timestamp)), " +
      "(3, 25.5, cast('2020-01-03' as timestamp)), " +
      "(4, 20.0, cast('2020-01-04' as timestamp))")
    val plan = check(
      s"""SELECT i.id, x.item_id FROM testcat.ns.$items i
         |JOIN (
         |  SELECT /*+ BROADCAST(b) */ a.item_id, a.total
         |  FROM (
         |    SELECT item_id, sum(price) AS total
         |    FROM testcat.ns.$purchases GROUP BY item_id) a
         |  JOIN (SELECT 1 AS one) b ON a.total > 0
         |) x ON i.id = x.item_id""".stripMargin,
      Seq(Row(1L, 1L), Row(2L, 2L), Row(3L, 3L)),
      expectedShuffles = 1)
    assert(collect(plan) { case j: BroadcastNestedLoopJoinExec => j }.nonEmpty, plan)
    assert(isPushedDown(plan), plan)
  }

  test("broadcast join building the left side is crossed through its streamed side") {
    createTable(items, itemsColumns, Array(identity("id")))
    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")
    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 40.0, cast('2020-01-01' as timestamp)), " +
      "(2, 10.0, cast('2020-01-02' as timestamp)), " +
      "(3, 25.5, cast('2020-01-03' as timestamp)), " +
      "(4, 20.0, cast('2020-01-04' as timestamp))")
    val plan = check(
      s"""SELECT i.id, x.item_id FROM testcat.ns.$items i
         |JOIN (
         |  SELECT /*+ BROADCAST(b) */ a.item_id, a.total
         |  FROM (SELECT item_id FROM testcat.ns.$purchases WHERE price >= 0) b
         |  JOIN (
         |    SELECT item_id, sum(price) AS total
         |    FROM testcat.ns.$purchases GROUP BY item_id) a
         |  ON b.item_id = a.item_id
         |) x ON i.id = x.item_id""".stripMargin,
      Seq(Row(1L, 1L), Row(2L, 2L), Row(3L, 3L)),
      expectedShuffles = 1)
    val broadcast = collect(plan) { case j: BroadcastHashJoinExec => j }.head
    assert(broadcast.buildSide == BuildLeft, plan)
    assert(collect(broadcast.right) {
      case s: ShuffleQueryStageExec
          if s.shuffle.outputPartitioning.isInstanceOf[KeyedPartitioning] => s
    }.nonEmpty, plan)
    assert(isPushedDown(plan), plan)
  }

  test("a window without a partition spec keeps both shuffles") {
    loadData()
    // Empty window keys ask for all tuples in one partition, which a keyed layout cannot serve.
    check(
      s"""SELECT i.id, p.rn FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, row_number() OVER (ORDER BY price) AS rn
         |  FROM testcat.ns.$purchases
         |) p ON i.id = p.item_id""".stripMargin,
      Seq(Row(1L, 4L), Row(1L, 4L), Row(2L, 1L), Row(3L, 3L)),
      expectedShuffles = 2)
  }

  test("the rewrite removes the stage AQE uses to broadcast the join") {
    val cols = Array(Column.create("id", LongType), Column.create("data", StringType))
    createTable("bucketed", cols, Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.bucketed VALUES (0, 'a'), (1, 'b'), (2, 'c'), (3, 'd')")
    createTable(purchases, purchasesColumns, Array.empty)
    val values = (0 until 20).map(i =>
      s"(${i % 2}, ${i + 1}.0, cast('2020-01-0${i % 9 + 1}' as timestamp))")
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " + values.mkString(", "))
    val query =
      s"""SELECT b.id, p.total FROM testcat.ns.bucketed b
         |JOIN (
         |  SELECT item_id, sum(price) AS total
         |  FROM testcat.ns.$purchases GROUP BY item_id
         |) p ON b.id = p.item_id""".stripMargin
    val expected = Seq(Row(0L, 100.0f), Row(1L, 110.0f))
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        // Above the final aggregate's output and below the partial aggregate's.
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "300",
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "2",
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true") {
      // The keyed stage above the aggregate reports the final output, which fits the threshold.
      val defaultPlan = run(query, expected)
      assert(collect(defaultPlan) { case _: BroadcastHashJoinExec => true }.nonEmpty,
        defaultPlan)
      // With the rewrite the only stage reports the partial output, which does not fit.
      val rewritten = withSQLConf(SQLConf.PUSH_DOWN_KEYED_SHUFFLE_ENABLED.key -> "true") {
        run(query, expected)
      }
      assert(collect(rewritten) { case _: BroadcastHashJoinExec => true }.isEmpty, rewritten)
      assert(shuffles(rewritten).size == 1, rewritten)
    }
  }

  test("a recorded user repartition layout refuses the rewrite") {
    loadData()
    val query =
      s"""SELECT i.id, p.total FROM testcat.ns.$items i
         |JOIN (
         |  SELECT /*+ REPARTITION(item_id) */ item_id, sum(price) AS total
         |  FROM testcat.ns.$purchases GROUP BY item_id
         |) p ON i.id = p.item_id""".stripMargin
    val expected = aggregateJoinAnswer
    // `EnsureRequirements` records the elided repartition's layout on the aggregate only when
    // union shuffle push-down is enabled, and the rewrite then refuses to replace it.
    check(query, expected, expectedShuffles = 2,
      SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ENABLED.key -> "true")
    check(query, expected, expectedShuffles = 1,
      SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ENABLED.key -> "false")
  }

  private object SumAgg extends Aggregator[Long, Long, Long] {
    override def zero: Long = 0L
    override def reduce(buffer: Long, input: Long): Long = buffer + input
    override def merge(b1: Long, b2: Long): Long = b1 + b2
    override def finish(reduction: Long): Long = reduction
    override def bufferEncoder: Encoder[Long] = Encoders.scalaLong
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

  test("a non-deterministic window function ends the walk") {
    loadData()
    def query(nondeterministic: Boolean): DataFrame = {
      val f = if (nondeterministic) udaf(SumAgg).asNondeterministic() else udaf(SumAgg)
      val windowed = spark.table(s"testcat.ns.$purchases")
        .select(col("item_id"),
          f(col("price").cast("long")).over(Window.partitionBy("item_id")).as("w"))
      spark.table(s"testcat.ns.$items")
        .join(windowed, col("id") === col("item_id"))
        .select(col("id"), col("item_id"), col("w"))
    }
    withPushDownConf() {
      val deterministic = query(nondeterministic = false)
      checkAnswer(deterministic,
        Seq(Row(1L, 1L, 40L), Row(1L, 1L, 40L), Row(2L, 2L, 10L), Row(3L, 3L, 25L)))
      assert(shuffles(deterministic.queryExecution.executedPlan).size == 1,
        deterministic.queryExecution.executedPlan)

      val nondeterministic = query(nondeterministic = true)
      nondeterministic.collect()
      assert(shuffles(nondeterministic.queryExecution.executedPlan).size == 2,
        nondeterministic.queryExecution.executedPlan)
    }
  }

  test("a non-deterministic generator ends the walk") {
    loadData()
    def query(generator: String): String =
      s"""SELECT i.id, g.x FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, $generator AS x
         |  FROM (
         |    SELECT item_id, sum(price) AS total
         |    FROM testcat.ns.$purchases GROUP BY item_id)
         |) g ON i.id = g.item_id""".stripMargin
    check(query("explode(array(total, total))"),
      Seq(Row(1L, 40.0), Row(1L, 40.0), Row(1L, 40.0), Row(1L, 40.0),
        Row(2L, 10.0), Row(2L, 10.0), Row(3L, 25.5), Row(3L, 25.5)),
      expectedShuffles = 1)
    withPushDownConf() {
      val df = sql(query("explode(array(total, rand(1)))"))
      df.collect()
      assert(shuffles(df.queryExecution.executedPlan).size == 2,
        df.queryExecution.executedPlan)
    }
  }

  test("a non-deterministic projection above a filter ends the walk") {
    loadData()
    def query(column: String): String =
      s"""SELECT i.id, p.total, p.r FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, total, $column AS r FROM (
         |    SELECT item_id, sum(price) AS total
         |    FROM testcat.ns.$purchases GROUP BY item_id HAVING sum(price) > 15)
         |) p ON i.id = p.item_id""".stripMargin
    check(query("total * 2"),
      Seq(Row(1L, 40.0, 80.0), Row(1L, 40.0, 80.0), Row(3L, 25.5, 51.0)),
      expectedShuffles = 1)
    withPushDownConf() {
      val df = sql(query("rand(1)"))
      df.collect()
      assert(shuffles(df.queryExecution.executedPlan).size == 2,
        df.queryExecution.executedPlan)
    }
  }

  test("a generate's own output cannot take the layout") {
    val a = AttributeReference("a", LongType)()
    val g = AttributeReference("g", LongType)()
    def plan(layoutOn: Expression): SparkPlan = {
      val scan = new LocalTableScanExec(Seq(a), Seq(InternalRow(1L)), None, false)
      val hash = ShuffleExchangeExec(HashPartitioning(Seq(a), 2), scan, ENSURE_REQUIREMENTS)
      val generate = GenerateExec(Explode(a), Seq(a), false, Seq(g), hash)
      ShuffleExchangeExec(
        KeyedPartitioning(Seq(layoutOn), Seq(InternalRow(1L), InternalRow(2L))), generate,
        ENSURE_REQUIREMENTS)
    }
    val rule = PushDownKeyedShuffleExchange(EnsureRequirements(), queryContainsSubqueries = false)
    withSQLConf(
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
        SQLConf.PUSH_DOWN_KEYED_SHUFFLE_ENABLED.key -> "true") {
      // The generator's output exists only above the Generate, so the shuffle cannot place rows
      // by it.
      val generatorKey = plan(g)
      assert(rule.apply(generatorKey) eq generatorKey, generatorKey)
      val childKey = plan(a)
      assert(rule.apply(childKey) ne childKey, childKey)
    }
  }

  test("a window over a user repartition keeps the repartition") {
    loadData()
    check(
      s"""SELECT i.id, p.rn FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id,
         |    row_number() OVER (PARTITION BY item_id ORDER BY price) AS rn
         |  FROM (SELECT /*+ REPARTITION(2, item_id) */ item_id, price
         |        FROM testcat.ns.$purchases)
         |) p ON i.id = p.item_id""".stripMargin,
      Seq(Row(1L, 1), Row(1L, 1), Row(2L, 1), Row(3L, 1)),
      expectedShuffles = 2)
  }

  test("every independent candidate is rewritten") {
    loadData()
    // Two branches, so the rewrite of the first must not hide the second candidate. `max` keeps
    // the branches from sharing an exchange.
    val other =
      s"""SELECT i.id, p.total FROM testcat.ns.$items i
         |JOIN (
         |  SELECT item_id, max(price) AS total
         |  FROM testcat.ns.$purchases GROUP BY item_id HAVING max(price) > 0
         |) p ON i.id = p.item_id""".stripMargin
    val otherAnswer = Seq(Row(1L, 40.0f), Row(1L, 40.0f), Row(2L, 10.0f), Row(3L, 25.5f))
    withPushDownConf() {
      val plan = run(s"$aggregateJoin UNION ALL $other", aggregateJoinAnswer ++ otherAnswer)
      assert(shuffles(plan).size == 2, plan)
    }
  }

  test("a refused candidate does not revert another branch") {
    loadData()
    // The hint is elided and recorded, so the second branch keeps both shuffles; the first branch
    // is independent of it and must still lose one.
    val hinted =
      s"""SELECT i.id, p.total FROM testcat.ns.$items i
         |JOIN (
         |  SELECT /*+ REPARTITION(item_id) */ item_id, max(price) AS total
         |  FROM testcat.ns.$purchases GROUP BY item_id HAVING max(price) > 0
         |) p ON i.id = p.item_id""".stripMargin
    val hintedAnswer = Seq(Row(1L, 40.0f), Row(1L, 40.0f), Row(2L, 10.0f), Row(3L, 25.5f))
    withPushDownConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ENABLED.key -> "true") {
      val plan = run(s"$aggregateJoin UNION ALL $hinted", aggregateJoinAnswer ++ hintedAnswer)
      assert(shuffles(plan).size == 3, plan)
    }
  }

  test("outer joins") {
    loadData()
    check(
      s"""SELECT i.id, p.total FROM testcat.ns.$items i
         |LEFT JOIN (
         |  SELECT item_id, sum(price) AS total
         |  FROM testcat.ns.$purchases GROUP BY item_id
         |) p ON i.id = p.item_id""".stripMargin,
      aggregateJoinAnswer,
      expectedShuffles = 1)
    check(
      s"""SELECT i.id, p.total FROM (
         |  SELECT item_id, sum(price) AS total
         |  FROM testcat.ns.$purchases GROUP BY item_id
         |) p LEFT JOIN testcat.ns.$items i ON i.id = p.item_id""".stripMargin,
      aggregateJoinAnswer :+ Row(null, 20.0),
      expectedShuffles = 1)
    check(
      s"""SELECT i.id, p.total FROM testcat.ns.$items i
         |FULL OUTER JOIN (
         |  SELECT item_id, sum(price) AS total
         |  FROM testcat.ns.$purchases GROUP BY item_id
         |) p ON i.id = p.item_id""".stripMargin,
      aggregateJoinAnswer :+ Row(null, 20.0),
      expectedShuffles = 1)
  }

  test("join key order differs from the group key order") {
    val cols = Array(
      Column.create("a", LongType),
      Column.create("b", LongType),
      Column.create("v", LongType))
    createTable("pairs", cols, Array(identity("a"), identity("b")))
    sql("INSERT INTO testcat.ns.pairs VALUES (1, 1, 0), (1, 2, 0), (2, 1, 0), (2, 2, 0)")
    createTable("facts", cols, Array.empty)
    sql("INSERT INTO testcat.ns.facts VALUES " +
      "(1, 1, 10), (1, 2, 20), (2, 1, 30), (2, 2, 40), (3, 3, 50)")
    val expected =
      Seq(Row(1L, 1L, 10L), Row(1L, 2L, 20L), Row(2L, 1L, 30L), Row(2L, 2L, 40L))
    check(
      """SELECT p.a, p.b, f.total FROM testcat.ns.pairs p
        |JOIN (
        |  SELECT b, a, sum(v) AS total FROM testcat.ns.facts GROUP BY b, a
        |) f ON p.a = f.a AND p.b = f.b""".stripMargin,
      expected,
      expectedShuffles = 1)
    check(
      """SELECT p.a, p.b, f.total FROM testcat.ns.pairs p
        |JOIN (
        |  SELECT b, a, sum(v) AS total FROM testcat.ns.facts GROUP BY a, b
        |) f ON p.b = f.b AND p.a = f.a""".stripMargin,
      expected,
      expectedShuffles = 1)
  }

  /** A keyed side whose layout is a `bucket` transform, so the layout's expressions are too. */
  private def loadBucketed(): Unit = {
    val cols = Array(Column.create("id", LongType), Column.create("data", StringType))
    createTable("bucketed", cols, Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.bucketed VALUES (0, 'a'), (1, 'b'), (2, 'c'), (3, 'd')")
    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(0, 10.0, cast('2020-01-01' as timestamp)), " +
      "(1, 20.0, cast('2020-01-02' as timestamp)), " +
      "(2, 30.0, cast('2020-01-03' as timestamp)), " +
      "(3, 40.0, cast('2020-01-04' as timestamp)), " +
      "(5, 50.0, cast('2020-01-05' as timestamp))")
  }

  private val bucketedJoin =
    s"""SELECT b.id, p.total, b.data FROM testcat.ns.bucketed b
       |JOIN (
       |  SELECT item_id, sum(price) AS total
       |  FROM testcat.ns.$purchases GROUP BY item_id
       |) p ON b.id = p.item_id""".stripMargin

  test("bucketed keyed side") {
    loadBucketed()
    check(bucketedJoin,
      Seq(Row(0L, 10.0, "a"), Row(1L, 20.0, "b"), Row(2L, 30.0, "c"), Row(3L, 40.0, "d")),
      expectedShuffles = 1)
  }

  test("a layout that cannot serve as a shuffle template is not pushed down") {
    loadBucketed()
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
        SQLConf.PUSH_DOWN_KEYED_SHUFFLE_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      // The prepared non-adaptive plan places the keyed shuffle above the aggregate and the hash
      // shuffle under it; the rule is what preparation runs on that shape. The rule is off while
      // the plan is planned so that it is handed to `apply` below unrewritten.
      val plan = sql(bucketedJoin).queryExecution.executedPlan
      withSQLConf(SQLConf.PUSH_DOWN_KEYED_SHUFFLE_ENABLED.key -> "true") {
      val rule = PushDownKeyedShuffleExchange(EnsureRequirements(), queryContainsSubqueries = false)

      def exchanges(plan: SparkPlan): Seq[ShuffleExchangeExec] =
        collect(plan) { case s: ShuffleExchangeExec => s }
      def keyed(plan: SparkPlan): Seq[ShuffleExchangeExec] =
        exchanges(plan).filter(_.outputPartitioning.isInstanceOf[KeyedPartitioning])
      // The planned keyed layout with one clause of `canTemplateShuffle` made to fail.
      def withKeyedLayout(f: KeyedPartitioning => KeyedPartitioning): SparkPlan =
        plan.transformUp {
          case exchange @ ShuffleExchangeExec(k: KeyedPartitioning, _, _, _, _) =>
            exchange.copy(outputPartitioning = f(k))
        }
      def assertDeclined(mutated: SparkPlan, clause: String): Unit = {
        assert(keyed(mutated).exists(!_.outputPartitioning.asInstanceOf[KeyedPartitioning]
          .canTemplateShuffle), s"$clause did not flip:\n$mutated")
        assert(rule.apply(mutated) eq mutated, s"$clause must stop the push-down:\n$mutated")
      }

      assert(exchanges(plan).size == 2 && keyed(plan).size == 1, plan)
      val pushed = rule.apply(plan)
      assert(pushed ne plan, s"the layout has to be a template:\n$plan")
      assert(exchanges(pushed).size == 1, pushed)

      assertDeclined(
        withKeyedLayout(_.withLayout(_.copy(isGrouped = false))), "isGrouped")
      assertDeclined(
        withKeyedLayout(_.withLayout(_.copy(unknownKeysOrigin = Some(ROUTING_LOST)))),
        "unknownKeysRouted")
      // The reduced-keys clause keeps the guard one predicate with the producer, but a plan-level
      // case cannot separate it: a layout that does not describe its keys fails the requirement
      // check as well, so the rewrite is refused either way.
      val reduced = withKeyedLayout { k =>
        k.copy(expressions = k.expressions.map {
          case t: TransformExpression => t.reducedTogetherWith(t)
          case e => e
        })
      }
      val reducedLayout =
        keyed(reduced).head.outputPartitioning.asInstanceOf[KeyedPartitioning]
      assert(reducedLayout.isGrouped && reducedLayout.unknownKeysRouted, reduced)
      assert(!reducedLayout.canTemplateShuffle, reduced)
      assert(!ValidateRequirements.validateWithOrigins(reduced, None), reduced)
      assert(rule.apply(reduced) eq reduced, reduced)

      // A hash shuffle feeding the operator with more partitions than the keyed layout offers:
      // retargeting would cut the operator's parallelism. Both join sides still pair, since the
      // keyed layout is the one the storage side reports, so only this guard refuses.
      val keyedPartitions = keyed(plan).head.outputPartitioning.numPartitions
      val wider = plan.transformUp {
        case e @ ShuffleExchangeExec(h: HashPartitioning, _, _, _, _) =>
          e.copy(outputPartitioning = h.copy(numPartitions = keyedPartitions + 1))
      }
      assert(keyedPartitions < exchanges(wider)
        .filterNot(_.outputPartitioning.isInstanceOf[KeyedPartitioning])
        .head.outputPartitioning.numPartitions, wider)
        assert(rule.apply(wider) eq wider, wider)
      }
    }
  }

  test("a shuffle that cannot evaluate the layout is not retargeted") {
    val a = AttributeReference("a", LongType)()
    val b = AttributeReference("b", LongType)()
    // A third-party rule can put an `ENSURE_REQUIREMENTS` shuffle under a projection, with no
    // clustered consumer between them.
    def plan(layoutOn: Expression): SparkPlan = {
      val scan = new LocalTableScanExec(Seq(a), Seq(InternalRow(1L)), None, false)
      val hash = ShuffleExchangeExec(HashPartitioning(Seq(a), 2), scan, ENSURE_REQUIREMENTS)
      val project = ProjectExec(Seq(a, Alias(Add(a, Literal(1L)), "b")()), hash)
      ShuffleExchangeExec(
        KeyedPartitioning(Seq(layoutOn), Seq(InternalRow(1L), InternalRow(2L))), project,
        ENSURE_REQUIREMENTS)
    }
    val rule = PushDownKeyedShuffleExchange(EnsureRequirements(), queryContainsSubqueries = false)
    withSQLConf(
        SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
        SQLConf.PUSH_DOWN_KEYED_SHUFFLE_ENABLED.key -> "true") {
      // `b` is computed by the projection, so the shuffle below cannot evaluate the layout.
      val rejected = plan(b)
      assert(rule.apply(rejected) eq rejected, rejected)
      val accepted = plan(a)
      assert(rule.apply(accepted) ne accepted, accepted)
    }
  }

  test("a hash shuffle reused elsewhere is kept") {
    loadData()
    // `p` is planned three times with one hash shuffle each. Retargeting the one under the keyed
    // join would lose the stage the other two reuse.
    val query =
      s"""WITH p AS (
         |  SELECT item_id, sum(price) AS total FROM testcat.ns.$purchases GROUP BY item_id)
         |SELECT i.id, p.total FROM testcat.ns.$items i JOIN p ON i.id = p.item_id
         |UNION ALL
         |SELECT p1.item_id, p2.total FROM p p1 JOIN p p2 ON p1.item_id = p2.item_id
         |""".stripMargin
    val expected = aggregateJoinAnswer ++
      Seq(Row(1L, 40.0), Row(2L, 10.0), Row(3L, 25.5), Row(4L, 20.0))
    val plan = withPushDownConf() { run(query, expected) }
    assert(collect(plan) { case r: ReusedExchangeExec => r }.nonEmpty, plan)
    assert(shuffles(plan).exists(_.outputPartitioning.isInstanceOf[KeyedPartitioning]), plan)
    assert(!isPushedDown(plan), plan)
    // With reuse off there is no stage to lose.
    withPushDownConf(SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
      assert(isPushedDown(run(query, expected)))
    }
  }
}
