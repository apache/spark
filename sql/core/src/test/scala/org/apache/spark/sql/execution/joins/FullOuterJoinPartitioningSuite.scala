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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{Coalesce, Literal}
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, AQEShuffleReadExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

class FullOuterJoinPartitioningSuite
  extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper {

  private val numShufflePartitions = 5

  private val baseConfs = Seq(
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
    SQLConf.SHUFFLE_PARTITIONS.key -> numShufflePartitions.toString)

  private val expandLimit = SQLConf.FULL_OUTER_JOIN_OUTPUT_PARTITIONING_EXPAND_LIMIT.key

  private val usingGroupBy =
    "SELECT k, count(*), sum(va), sum(vb) FROM a FULL OUTER JOIN b USING (k) GROUP BY k"

  private val aggregated = "(SELECT k, count(*) AS cnt FROM a GROUP BY k) x"

  /** A FULL OUTER join of a strict hash side and a null-aware side, in both orders. */
  private val mixedJoins = Seq(
    s"$aggregated FULL OUTER JOIN b ON x.k = b.k" -> "coalesce(x.k, b.k)",
    s"b FULL OUTER JOIN $aggregated ON b.k = x.k" -> "coalesce(b.k, x.k)")

  /** AQE coalescing that still leaves several partitions, so misplaced rows can show. */
  private val aqeCoalescing = Seq(
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
    SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
    SQLConf.SHUFFLE_PARTITIONS.key -> "10",
    SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "1200")

  private def assertCoalescedToSeveralPartitions(plan: SparkPlan): Unit = {
    val reads = collect(plan) { case r: AQEShuffleReadExec => r }
    assert(reads.nonEmpty, plan)
    assert(reads.forall(r => (2 until 10).contains(r.outputPartitioning.numPartitions)), plan)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Nullable keys with NULLs, overlapping key ranges, and duplicate keys on every side.
    spark.range(0, 30)
      .selectExpr("IF(id % 7 = 0, NULL, id % 10) AS k", "id % 3 AS k2", "id AS va")
      .createOrReplaceTempView("a")
    spark.range(0, 30)
      .selectExpr("IF(id % 5 = 0, NULL, id % 10 + 5) AS k", "id % 3 AS k2", "id AS vb")
      .createOrReplaceTempView("b")
    spark.range(0, 30)
      .selectExpr("IF(id % 4 = 0, NULL, id % 10 + 8) AS k", "id % 3 AS k2", "id AS vc")
      .createOrReplaceTempView("c")
  }

  override def afterAll(): Unit = {
    try {
      Seq("a", "b", "c").foreach(spark.catalog.dropTempView)
    } finally {
      super.afterAll()
    }
  }

  private def numShuffles(plan: SparkPlan): Int =
    collect(plan) { case s: ShuffleExchangeExec => s }.size

  private def fullOuterJoins(plan: SparkPlan): Seq[ShuffledJoin] =
    collect(plan) { case j: ShuffledJoin if j.joinType == FullOuter => j }

  /** The members of a reported partitioning, each over the coalesced join keys. */
  private def members(partitioning: Partitioning): Seq[Partitioning] = {
    val ps = PartitioningCollection.flatten(partitioning)
    ps.foreach {
      case h: HashPartitioningLike =>
        assert(h.expressions.forall(_.isInstanceOf[Coalesce]), partitioning)
      case other => fail(s"Unexpected output partitioning: $other")
    }
    ps
  }

  private def firstFullOuterJoin(df: DataFrame): ShuffledJoin = {
    val joins = fullOuterJoins(df.queryExecution.executedPlan)
    assert(joins.nonEmpty, df.queryExecution.executedPlan)
    joins.head
  }

  /**
   * Runs `query` under `confs` and again with the feature off, asserts equal answers, and returns
   * the number of shuffles (under `confs`, off). `confs` may set the expand limit.
   */
  private def compare(query: String, confs: (String, String)*): (Int, Int) = {
    def run(extra: Seq[(String, String)]): (Seq[Row], Int) =
      withSQLConf(baseConfs ++ extra: _*) {
        val df = sql(query)
        val rows = df.collect().toSeq
        (rows, numShuffles(df.queryExecution.executedPlan))
      }
    val (onRows, onShuffles) = run(confs)
    val (offRows, offShuffles) = run(confs.filter(_._1 != expandLimit) :+ (expandLimit -> "0"))
    assert(onRows.sortBy(_.toString) == offRows.sortBy(_.toString))
    (onShuffles, offShuffles)
  }

  test("USING join followed by GROUP BY on the merged key does not shuffle again") {
    val query = usingGroupBy
    assert(compare(query) == (2, 3))

    withSQLConf(baseConfs: _*) {
      val ps = members(firstFullOuterJoin(sql(query)).outputPartitioning)
      assert(ps.size == 2)
      assert(ps.forall {
        case HashPartitioning(Seq(_), `numShufflePartitions`) => true
        case _ => false
      })
      // A single NULL group: the NULL-keyed rows of both sides are co-located.
      checkAnswer(
        sql(s"SELECT count(*) FROM ($query) WHERE k IS NULL"),
        Row(1))
    }
    withSQLConf(baseConfs :+ (expandLimit -> "0"): _*) {
      assert(firstFullOuterJoin(sql(query)).outputPartitioning ==
        UnknownPartitioning(numShufflePartitions))
    }
  }

  test("GROUP BY an explicit COALESCE of the join keys does not shuffle again") {
    def query(merged: String): String =
      s"""SELECT c, count(*), sum(va), sum(vb) FROM (
         |  SELECT $merged AS c, va, vb FROM a FULL OUTER JOIN b ON a.k = b.k
         |) GROUP BY c""".stripMargin
    assert(compare(query("coalesce(a.k, b.k)")) == (2, 3))
    assert(compare(query("coalesce(b.k, a.k)")) == (2, 3))
    // With the left-first order only, the reversed order does not match the claim.
    val leftFirstOnly = expandLimit -> "1"
    assert(compare(query("coalesce(a.k, b.k)"), leftFirstOnly) == (2, 3))
    assert(compare(query("coalesce(b.k, a.k)"), leftFirstOnly) == (3, 3))
  }

  test("a downstream join on the reversed order does not shuffle the join output again") {
    def query(merged: String): String =
      s"""SELECT * FROM (
         |  SELECT $merged AS m, va, vb FROM a FULL OUTER JOIN b ON a.k = b.k
         |) x FULL OUTER JOIN c ON x.m = c.k""".stripMargin
    assert(compare(query("coalesce(a.k, b.k)")) == (3, 4))
    assert(compare(query("coalesce(b.k, a.k)")) == (3, 4))
    // With the left-first order only, the reversed order does not pair.
    assert(compare(query("coalesce(b.k, a.k)"), expandLimit -> "1") == (4, 4))
  }

  test("multi-column keys report the uniform orders first, up to the expand limit") {
    val join = "a FULL OUTER JOIN b ON a.k = b.k AND a.k2 = b.k2"
    def query(k: String, k2: String): String =
      s"SELECT $k AS c, $k2 AS c2, count(*) FROM $join GROUP BY 1, 2"
    val (lk, rk, lk2, rk2) = ("coalesce(a.k, b.k)", "coalesce(b.k, a.k)",
      "coalesce(a.k2, b.k2)", "coalesce(b.k2, a.k2)")
    // The default covers all four orders of two keys.
    Seq(query(lk, lk2), query(rk, rk2), query(lk, rk2), query(rk, lk2)).foreach { q =>
      assert(compare(q) == (2, 3), q)
    }
    // A limit of 2 keeps the uniform orders only.
    val uniformOnly = expandLimit -> "2"
    assert(compare(query(lk, lk2), uniformOnly) == (2, 3))
    assert(compare(query(rk, rk2), uniformOnly) == (2, 3))
    assert(compare(query(lk, rk2), uniformOnly) == (3, 3))
    withSQLConf(baseConfs :+ uniformOnly: _*) {
      assert(members(firstFullOuterJoin(sql(s"SELECT * FROM $join")).outputPartitioning)
        .size == 2)
    }
  }

  test("chained FULL OUTER JOIN USING shuffles only the new side") {
    val query =
      """SELECT k, count(*), sum(va), sum(vb), sum(vc)
        |FROM a FULL OUTER JOIN b USING (k) FULL OUTER JOIN c USING (k)
        |GROUP BY k""".stripMargin
    assert(compare(query) == (3, 5))
  }

  test("USING join on keys of different types") {
    // The analyzer casts `k` of `ia` to BIGINT on both the join key and the merged column.
    withTempView("ia") {
      sql("SELECT CAST(k AS INT) AS k, va FROM a").createOrReplaceTempView("ia")
      assert(compare(usingGroupBy.replace("FROM a ", "FROM ia ")) == (2, 3))
    }
  }

  test("window partitioned by the merged key does not shuffle again") {
    val query =
      """SELECT k, va, vb, row_number() OVER (PARTITION BY k ORDER BY va, vb) AS rn
        |FROM a FULL OUTER JOIN b USING (k)""".stripMargin
    assert(compare(query) == (2, 3))
  }

  test("multi-column USING join") {
    val query =
      "SELECT k, k2, count(*) FROM a FULL OUTER JOIN b USING (k, k2) GROUP BY k, k2"
    assert(compare(query) == (2, 3))
  }

  test("shuffled hash join") {
    val query =
      """SELECT /*+ SHUFFLE_HASH(b) */ k, count(*)
        |FROM a FULL OUTER JOIN b USING (k) GROUP BY k""".stripMargin
    assert(compare(query, SQLConf.PREFER_SORTMERGEJOIN.key -> "false") == (2, 3))
    withSQLConf(baseConfs :+ (SQLConf.PREFER_SORTMERGEJOIN.key -> "false"): _*) {
      assert(firstFullOuterJoin(sql(query)).isInstanceOf[ShuffledHashJoinExec])
    }
  }

  test("AQE coalesced partitions keep the claim") {
    val query = usingGroupBy
    assert(compare(query, aqeCoalescing: _*) == (2, 3))
    withSQLConf(baseConfs ++ aqeCoalescing: _*) {
      val df = sql(query)
      df.collect()
      assertCoalescedToSeveralPartitions(df.queryExecution.executedPlan)
      val ps = members(firstFullOuterJoin(df).outputPartitioning)
      assert(ps.size == 2 && ps.forall(_.isInstanceOf[CoalescedHashPartitioning]))
    }
  }

  test("a child PartitioningCollection pairs the member that matches the join keys") {
    Seq("a.k", "b.k").foreach { innerKey =>
      val join = s"a JOIN b ON a.k = b.k FULL OUTER JOIN c ON $innerKey = c.k"
      val query = s"SELECT coalesce($innerKey, c.k) AS g, count(*) FROM $join GROUP BY 1"
      assert(compare(query) == (3, 4), innerKey)
      withSQLConf(baseConfs: _*) {
        val fullOuter = firstFullOuterJoin(sql(s"SELECT * FROM $join"))
        val keys = members(fullOuter.outputPartitioning).map {
          case h: HashPartitioningLike => h.expressions.head.children.toSet
        }
        assert(keys.distinct.size == 1)
        assert(keys.head.size == 2)
        assert(keys.head.exists(_.semanticEquals(fullOuter.leftKeys.head)))
        assert(keys.head.exists(_.semanticEquals(fullOuter.rightKeys.head)))
      }
    }
  }

  test("every sound member pair is reported") {
    // The inner join reports Hash(ak) or Hash(bk), and with duplicated join keys both pair with
    // Hash(ck). Each coalesced claim holds on every row, since the inner join makes ak = bk.
    val base =
      """SELECT * FROM (SELECT a.k AS ak, b.k AS bk FROM a JOIN b ON a.k = b.k) x
        |FULL OUTER JOIN (SELECT k AS ck FROM c) y
        |ON x.ak = y.ck AND x.bk = y.ck""".stripMargin
    val coPartition = SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false"
    Seq("coalesce(ak, ck)", "coalesce(bk, ck)").foreach { merged =>
      val query = s"SELECT $merged AS g, count(*) FROM ($base) GROUP BY $merged"
      assert(compare(query, coPartition) == (3, 4), merged)
    }
    withSQLConf(baseConfs :+ coPartition: _*) {
      assert(members(firstFullOuterJoin(sql(base)).outputPartitioning).size == 4)
    }

    // The expand limit applies per pair: each pair keeps its left-first order.
    val leftFirstOnly = expandLimit -> "1"
    Seq("coalesce(ak, ck)", "coalesce(bk, ck)").foreach { merged =>
      val query = s"SELECT $merged AS g, count(*) FROM ($base) GROUP BY $merged"
      assert(compare(query, coPartition, leftFirstOnly) == (3, 4), merged)
    }
    withSQLConf(baseConfs ++ Seq(coPartition, leftFirstOnly): _*) {
      assert(members(firstFullOuterJoin(sql(base)).outputPartitioning).size == 2)
    }
  }

  test("GROUP BY one side's key still shuffles") {
    val query = "SELECT a.k, count(*) FROM a FULL OUTER JOIN b ON a.k = b.k GROUP BY a.k"
    assert(compare(query) == (3, 3))
  }

  test("null-aware children give a null-aware output") {
    val spread = SQLConf.SHUFFLE_SPREAD_NULL_JOIN_KEYS_ENABLED.key -> "true"
    // Spread NULL-keyed rows rule out a strict clustering, so the aggregate shuffles again ...
    val grouped = usingGroupBy
    assert(compare(grouped, spread) == (3, 3))
    // ... while a chained FULL OUTER join, which tolerates spread NULL keys, does not.
    val chained = "SELECT * FROM a FULL OUTER JOIN b USING (k) FULL OUTER JOIN c USING (k)"
    assert(compare(chained, spread) == (3, 4))

    withSQLConf(baseConfs :+ spread: _*) {
      assert(members(firstFullOuterJoin(sql(grouped)).outputPartitioning)
        .forall(_.isInstanceOf[NullAwareHashPartitioning]))
      checkAnswer(sql(s"SELECT count(*) FROM ($grouped) WHERE k IS NULL"), Row(1))
    }
  }

  test("a strict hash side next to a null-aware side gives a null-aware output") {
    val spread = SQLConf.SHUFFLE_SPREAD_NULL_JOIN_KEYS_ENABLED.key -> "true"
    mixedJoins.foreach { case (join, merged) =>
      // A chained FULL OUTER join tolerates spread NULL keys, so it reuses the null-aware output.
      assert(compare(s"SELECT * FROM $join FULL OUTER JOIN c ON $merged = c.k", spread) == (3, 4))
      withSQLConf(baseConfs :+ spread: _*) {
        val fullOuter = firstFullOuterJoin(sql(s"SELECT * FROM $join"))
        val sides = Seq(fullOuter.left, fullOuter.right).map(_.outputPartitioning)
        assert(sides.count(_.isInstanceOf[NullAwareHashPartitioning]) == 1, join)
        assert(sides.count(_.isInstanceOf[HashPartitioning]) == 1, join)
        assert(members(fullOuter.outputPartitioning)
          .forall(_.isInstanceOf[NullAwareHashPartitioning]), join)
      }
    }
  }

  test("a key that is not NULL on a NULL-padded row keeps the output partitioning unknown") {
    // `f` is 0 on a NULL-padded row, not NULL, yet nullable, so the optimizer keeps the
    // COALESCE below as written. The unmatched `y` row with key 5 sits on hash(5) but its
    // coalesced key is f(NULL) = 0, the key of the matched rows on hash(0). Claiming the
    // coalesced key would split group 0 across two partitions.
    def partitionOf(v: Long): Int =
      HashPartitioning(Seq(Literal(v)), numShufflePartitions).partitionIdExpression
        .eval().asInstanceOf[Int]
    assert(partitionOf(0L) != partitionOf(5L))

    def f(k: String): String = s"CASE WHEN $k IS NULL THEN 0 WHEN $k = 99 THEN NULL ELSE $k END"
    withTempView("x", "y") {
      spark.range(0, 2).selectExpr("CAST(IF(id = 0, 0, NULL) AS BIGINT) AS k")
        .createOrReplaceTempView("x")
      spark.range(0, 2).selectExpr("CAST(IF(id = 0, 5, NULL) AS BIGINT) AS k")
        .createOrReplaceTempView("y")
      val key = s"coalesce(${f("x.k")}, ${f("y.k")})"
      val query =
        s"""SELECT $key AS key, count(*)
           |FROM x FULL OUTER JOIN y ON ${f("x.k")} = ${f("y.k")}
           |GROUP BY $key""".stripMargin
      assert(compare(query) == (3, 3))
      withSQLConf(baseConfs: _*) {
        val df = sql(query)
        assert(firstFullOuterJoin(df).outputPartitioning.isInstanceOf[UnknownPartitioning])
        checkAnswer(df, Row(0L, 3L))
      }
    }
  }

  test("a key that fails the guard on one side keeps the output partitioning unknown") {
    // `coalesce(k, 0)` is 0 on a NULL-padded row. With the other side's key first, the merged key
    // below is 0 on an unmatched row of that side whose own key is NULL, which sits on
    // hash(NULL) rather than hash(0).
    def partitionOf(v: Any): Int =
      HashPartitioning(Seq(Literal.create(v, LongType)), numShufflePartitions)
        .partitionIdExpression.eval().asInstanceOf[Int]
    assert(partitionOf(0L) != partitionOf(null))

    withTempView("x", "y") {
      spark.range(0, 2).selectExpr("CAST(IF(id = 0, 0, NULL) AS BIGINT) AS k")
        .createOrReplaceTempView("x")
      spark.range(0, 2).selectExpr("CAST(IF(id = 0, 0, NULL) AS BIGINT) AS k")
        .createOrReplaceTempView("y")
      Seq(
        "x.k = coalesce(y.k, 0)" -> "coalesce(x.k, coalesce(y.k, 0))",
        "coalesce(x.k, 0) = y.k" -> "coalesce(y.k, coalesce(x.k, 0))").foreach {
        case (condition, merged) =>
          val join = s"x FULL OUTER JOIN y ON $condition"
          assert(compare(s"SELECT $merged AS g, count(*) FROM $join GROUP BY 1") == (3, 3),
            condition)
          withSQLConf(baseConfs: _*) {
            assert(firstFullOuterJoin(sql(s"SELECT * FROM $join")).outputPartitioning
              .isInstanceOf[UnknownPartitioning], condition)
          }
      }
    }
  }

  test("one key pair failing the guard keeps the output partitioning unknown") {
    val join = "a FULL OUTER JOIN b ON a.k = b.k AND a.k2 = coalesce(b.k2, 0)"
    val query =
      s"""SELECT coalesce(a.k, b.k) AS g, coalesce(a.k2, coalesce(b.k2, 0)) AS g2, count(*)
         |FROM $join GROUP BY 1, 2""".stripMargin
    assert(compare(query) == (3, 3))
    withSQLConf(baseConfs: _*) {
      assert(firstFullOuterJoin(sql(s"SELECT * FROM $join")).outputPartitioning
        .isInstanceOf[UnknownPartitioning])
    }
  }

  test("changing the expand limit after the plan is prepared keeps its answer") {
    /** Prepares `query`, sets `change`, then executes the prepared plan. */
    def checkChangeAfterPreparation(
        query: String, change: (String, String), confs: (String, String)*): Unit = {
      withSQLConf(baseConfs ++ confs: _*) {
        val expected = sql(query).collect().toSeq.map(_.toString).sorted
        val df = sql(query)
        df.queryExecution.executedPlan
        withSQLConf(change) {
          assert(df.collect().toSeq.map(_.toString).sorted == expected, change)
        }
      }
    }

    // A union of FULL OUTER legs is partitioning-aware, and the aggregate above it does not
    // shuffle, so both rely on the legs keeping their claim at execution.
    val union =
      """SELECT k, count(*) FROM (
        |  SELECT k FROM a FULL OUTER JOIN b USING (k) UNION ALL
        |  SELECT k FROM b FULL OUTER JOIN c USING (k)) GROUP BY k""".stripMargin
    assert(compare(union) == (3, 4))
    checkChangeAfterPreparation(union, expandLimit -> "0")
  }

  test("a null-safe equality keeps the output partitioning unknown") {
    // `a.k <=> b.k` plans as the keys (coalesce(k, 0), isnull(k)), which are 0 and true on a
    // NULL-padded row, so the coalesced key of an unmatched row is not its own key.
    val query =
      "SELECT coalesce(a.k, b.k), count(*) FROM a FULL OUTER JOIN b ON a.k <=> b.k GROUP BY 1"
    assert(compare(query) == (3, 3))
    withSQLConf(baseConfs: _*) {
      assert(firstFullOuterJoin(sql(query)).outputPartitioning.isInstanceOf[UnknownPartitioning])
    }
  }

  test("a normalized floating-point key keeps the output partitioning unknown") {
    // The planner wraps a floating-point key in `knownfloatingpointnormalized`, which is not
    // null-intolerant, so the guard falls back.
    withTempView("fa", "fb") {
      sql("SELECT * FROM VALUES (0.0D), (-0.0D), (CAST('NaN' AS DOUBLE)), (1.5D), (NULL) AS t(d)")
        .createOrReplaceTempView("fa")
      sql("SELECT * FROM VALUES (-0.0D), (CAST('NaN' AS DOUBLE)), (2.5D), (NULL) AS t(d)")
        .createOrReplaceTempView("fb")
      val query = "SELECT d, count(*) FROM fa FULL OUTER JOIN fb USING (d) GROUP BY d"
      assert(compare(query) == (3, 3))
      withSQLConf(baseConfs: _*) {
        assert(firstFullOuterJoin(sql(query)).outputPartitioning.isInstanceOf[UnknownPartitioning])
      }
    }
  }

  test("AQE: a strict hash side next to a null-aware side gives a coalesced null-aware output") {
    val confs = aqeCoalescing :+ (SQLConf.SHUFFLE_SPREAD_NULL_JOIN_KEYS_ENABLED.key -> "true")
    val (join, merged) = mixedJoins.head
    assert(compare(s"SELECT * FROM $join FULL OUTER JOIN c ON $merged = c.k", confs: _*) == (3, 4))
    withSQLConf(baseConfs ++ confs: _*) {
      val df = sql(s"SELECT * FROM $join FULL OUTER JOIN c ON $merged = c.k")
      df.collect()
      assertCoalescedToSeveralPartitions(df.queryExecution.executedPlan)
      // The lower join, over the strict and the null-aware side.
      val fullOuter = fullOuterJoins(df.queryExecution.executedPlan).last
      assert(fullOuter.left.outputPartitioning.isInstanceOf[CoalescedHashPartitioning])
      assert(fullOuter.right.outputPartitioning.isInstanceOf[CoalescedNullAwareHashPartitioning])
      assert(members(fullOuter.outputPartitioning).forall {
        case CoalescedNullAwareHashPartitioning(
            NullAwareHashPartitioning(_, 10), _) => true
        case _ => false
      })
    }
  }

  test("a null-intolerant key over an attribute keeps the claim") {
    withTempView("sa", "sb") {
      Seq("a" -> "va", "b" -> "vb").foreach { case (t, v) =>
        sql(s"SELECT named_struct('f', k) AS s, array(k) AS arr, k, $v FROM $t")
          .createOrReplaceTempView(s"s$t")
      }
      Seq("k + 1", "s.f", "arr[0]").foreach { key =>
        val merged = s"coalesce(sa.$key, sb.$key)"
        val query =
          s"""SELECT $merged AS key, count(*), sum(va), sum(vb)
             |FROM sa FULL OUTER JOIN sb ON sa.$key = sb.$key
             |GROUP BY $merged""".stripMargin
        assert(compare(query) == (2, 3), key)
        withSQLConf(baseConfs: _*) {
          assert(members(firstFullOuterJoin(sql(query)).outputPartitioning)
            .forall(_.isInstanceOf[HashPartitioning]), key)
        }
      }
    }
  }
}
