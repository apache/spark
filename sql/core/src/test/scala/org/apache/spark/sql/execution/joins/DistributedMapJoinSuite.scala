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

import org.apache.spark.internal.config.{EXECUTOR_MEMORY, SHARD_ENABLED}
import org.apache.spark.sql.{DataFrame, QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{DistributedMapJoinStrategy, JoinStrategyHint}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{
  AdaptiveSparkPlanHelper, DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite
}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ShardExchangeExec}
import org.apache.spark.sql.internal.SQLConf

abstract class DistributedMapJoinSuiteBase
    extends QueryTest
    with AdaptiveSparkPlanHelper {

  import testImplicits._

  protected var spark: SparkSession = _

  private val ensureReqs = EnsureRequirements()

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local-cluster[2,1,512]")
      .config(EXECUTOR_MEMORY.key, "512m")
      .config(SHARD_ENABLED.key, "true")
      .appName("dmj-testing")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      spark.stop()
      spark = null
    } finally {
      super.afterAll()
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    System.gc()
  }

  // prepare test data
  private def prepareTables(): Unit = {
    val dim: DataFrame = Seq[(Option[Int], String)](
      (Some(1), "A"),
      (Some(2), "B"),
      (Some(3), "C"),
      (Some(4), "D"),
      (Some(5), "E"),
      (Some(6), "F"),
      (Some(7), "G"),
      (None, "NULLK")).toDF("k", "v")

    val fact: DataFrame = Seq
      .tabulate(1000) { i =>
        val k: Option[Int] = if (i % 20 == 0) None else Some(i % 9)
        (k, s"r$i")
      }
      .toDF("k", "payload")

    dim.createOrReplaceTempView("dim")
    fact.createOrReplaceTempView("fact")

    val dim2: DataFrame = Seq[(Option[Int], String, String)](
      (Some(1), "X", "AX"),
      (Some(1), "Y", "AY"),
      (Some(2), "X", "BX"),
      (Some(3), "Z", "CZ"),
      (None, "X", "NX")).toDF("k", "cat", "val")

    val fact2: DataFrame = Seq(
      (Some(1), "X", "fx1"),
      (Some(1), "Y", "fy1"),
      (Some(1), null.asInstanceOf[String], "fnull1"),
      (Some(2), "Y", "fy2"),
      (Some(2), "X", "fx2"),
      (Some(3), "Z", "fz3"),
      (Some(4), "W", "fw4")).toDF("k", "cat", "payload")

    dim2.createOrReplaceTempView("dim2")
    fact2.createOrReplaceTempView("fact2")

    val dim3: DataFrame = Seq((1, "P"), (2, "Q"), (3, "R")).toDF("k", "tag")
    dim3.createOrReplaceTempView("dim3")

    val t: DataFrame = Seq.tabulate(20)(i => (i % 5, s"s$i")).toDF("k", "s")
    t.createOrReplaceTempView("t")
  }

  private def assertDMJPlan(plan: SparkPlan, expectBuildRight: Boolean = true): Unit = {
    // After EnsureRequirements, we should see ShardExchangeExec and DistributedMapJoinExec
    val p = ensureReqs.apply(plan)
    val hasShard = p.collect { case _: ShardExchangeExec => 1 }.nonEmpty
    val dmjOpt = p.collect { case j: DistributedMapJoinExec => j }.headOption
    assert(hasShard, s"Plan should contain ShardExchangeExec:\n$p")
    assert(dmjOpt.isDefined, s"Plan should contain DistributedMapJoinExec:\n$p")
    if (expectBuildRight) {
      assert(dmjOpt.get.buildSide == org.apache.spark.sql.catalyst.optimizer.BuildRight)
    } else {
      assert(dmjOpt.get.buildSide == org.apache.spark.sql.catalyst.optimizer.BuildLeft)
    }
  }

  private def checkDMJEquals(
      dmjSql: String,
      baselineSql: String,
      expectBuildRight: Boolean = true): Unit = {
    val df = sql(dmjSql)
    assertDMJPlan(df.queryExecution.sparkPlan, expectBuildRight)
    checkAnswer(df, sql(baselineSql))
  }

  private def assertDMJPlanCount(plan: SparkPlan, expect: Int): Unit = {
    val applied = ensureReqs.apply(plan)
    val dmjCount = applied.collect { case _: DistributedMapJoinExec => 1 }.sum
    val shardCount = applied.collect { case _: ShardExchangeExec => 1 }.sum
    assert(dmjCount == expect, s"Expected $expect DMJ, but found $dmjCount:\n$applied")
    assert(
      shardCount == expect,
      s"Expected $expect ShardExchange, but found $shardCount:\n$applied")
  }

  test("inner join build right via DMJ hint") {
    prepareTables()
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val dmj =
        """
          |SELECT /*+ DISTMAPJOIN(d(shard_count=3, replica_count=1)) */
          |  f.k, d.v, f.payload
          |FROM fact f JOIN dim d ON f.k = d.k
          |""".stripMargin
      val base =
        """SELECT f.k, d.v, f.payload FROM fact f JOIN dim d ON f.k = d.k"""
      checkDMJEquals(dmj, base, expectBuildRight = true)
    }
  }

  test("DMJ: inner join on two keys (build right)") {
    prepareTables()
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val dmj =
        """
          |SELECT /*+ DISTMAPJOIN(d(shard_count=3,replica_count=1)) */
          |  f.k, f.cat, d.val, f.payload
          |FROM fact2 f JOIN dim2 d
          |ON f.k = d.k AND f.cat = d.cat
          |""".stripMargin
      val base =
        """SELECT f.k, f.cat, d.val, f.payload FROM fact2 f JOIN dim2 d
          |ON f.k = d.k AND f.cat = d.cat""".stripMargin
      val df = sql(dmj)
      assertDMJPlanCount(df.queryExecution.sparkPlan, expect = 1)
      checkAnswer(df, sql(base))
    }
  }

  test("DMJ: two-key inner join with nulls on probe/build") {
    prepareTables()
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val dmj =
        """
          |SELECT /*+ DISTMAPJOIN(d) */
          |  f.k, f.cat, d.val
          |FROM fact2 f JOIN dim2 d
          |ON f.k = d.k AND f.cat = d.cat
          |""".stripMargin
      val base =
        """SELECT f.k, f.cat, d.val FROM fact2 f JOIN dim2 d
          |ON f.k = d.k AND f.cat = d.cat""".stripMargin
      val df = sql(dmj)
      assertDMJPlanCount(df.queryExecution.sparkPlan, expect = 1)
      checkAnswer(df, sql(base))
    }
  }

  test("left outer join build right via DMJ hint (AQE on)") {
    prepareTables()
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val dmj =
        """
          |SELECT /*+ DISTMAPJOIN(d(shard_count=4, replica_count=1)) */
          |  f.k, d.v
          |FROM fact f LEFT OUTER JOIN dim d ON f.k = d.k
          |""".stripMargin
      val base =
        """SELECT f.k, d.v FROM fact f LEFT OUTER JOIN dim d ON f.k = d.k"""
      checkDMJEquals(dmj, base, expectBuildRight = true)
    }
  }

  test("DMJ: left outer join with null keys on probe, null-supplied rows preserved") {
    prepareTables()
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val dmj =
        """
          |SELECT /*+ DISTMAPJOIN(d(shard_count=4,replica_count=1)) */
          |  f.k, d.v
          |FROM fact f LEFT OUTER JOIN dim d ON f.k = d.k
          |ORDER BY f.k NULLS FIRST, d.v NULLS FIRST
          |""".stripMargin
      val base =
        """SELECT f.k, d.v FROM fact f LEFT OUTER JOIN dim d ON f.k = d.k
          |ORDER BY f.k NULLS FIRST, d.v NULLS FIRST""".stripMargin
      val df = sql(dmj)
      assertDMJPlanCount(df.queryExecution.sparkPlan, expect = 1)
      checkAnswer(df, sql(base))
    }
  }

  test("right outer join build left via DMJ hint") {
    prepareTables()
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val dmj =
        """
          |SELECT /*+ DISTMAPJOIN(dleft(shard_count=2, replica_count=1)) */
          |  dleft.k, dleft.v, f.payload
          |FROM dim dleft RIGHT OUTER JOIN fact f ON dleft.k = f.k
          |""".stripMargin
      val base =
        """SELECT d.k, d.v, f.payload FROM dim d RIGHT OUTER JOIN fact f ON d.k = f.k"""
      val df = sql(dmj)
      assertDMJPlan(df.queryExecution.sparkPlan, expectBuildRight = false)
      checkAnswer(df, sql(base))
    }
  }

  test("left semi join build right via DMJ hint") {
    prepareTables()
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val dmj =
        """
          |SELECT /*+ DISTMAPJOIN(d(shard_count=3, replica_count=1)) */
          |  f.k
          |FROM fact f LEFT SEMI JOIN dim d ON f.k = d.k
          |""".stripMargin
      val base =
        """SELECT f.k FROM fact f LEFT SEMI JOIN dim d ON f.k = d.k"""
      checkDMJEquals(dmj, base, expectBuildRight = true)
    }
  }

  test("left anti join build right via DMJ hint") {
    prepareTables()
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val dmj =
        """
          |SELECT /*+ DISTMAPJOIN(d(shard_count=3, replica_count=1)) */
          |  f.k
          |FROM fact f LEFT ANTI JOIN dim d ON f.k = d.k
          |""".stripMargin
      val base =
        """SELECT f.k FROM fact f LEFT ANTI JOIN dim d ON f.k = d.k"""
      checkDMJEquals(dmj, base, expectBuildRight = true)
    }
  }

  test("DMJ: multi-table join chain (two DMJs in one query)") {
    prepareTables()
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val dmj =
        """
          |SELECT /*+ DISTMAPJOIN(d), DISTMAPJOIN(d3) */
          |  f.k, d.v, d3.tag, f.payload
          |FROM fact f
          |JOIN dim d ON f.k = d.k
          |JOIN dim3 d3 ON f.k = d3.k
          |""".stripMargin
      val base =
        """SELECT f.k, d.v, d3.tag, f.payload FROM fact f
          |JOIN dim d ON f.k = d.k
          |JOIN dim3 d3 ON f.k = d3.k""".stripMargin
      val df = sql(dmj)
      assertDMJPlanCount(df.queryExecution.sparkPlan, expect = 2)
      checkAnswer(df, sql(base))
    }
  }

  test("DMJ: self join with build left via hint") {
    prepareTables()
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val dmj =
        """
          |SELECT /*+ DISTMAPJOIN(a(shard_count=2,replica_count=1)) */
          |  a.k, a.s, b.s
          |FROM t a JOIN t b ON a.k = b.k
          |""".stripMargin
      val base =
        """SELECT a.k, a.s, b.s FROM t a JOIN t b ON a.k = b.k""".stripMargin
      val df = sql(dmj)
      val applied = ensureReqs.apply(df.queryExecution.sparkPlan)
      val dmjNode = applied.collect { case j: DistributedMapJoinExec => j }.head
      assert(
        dmjNode.buildSide == org.apache.spark.sql.catalyst.optimizer.BuildLeft,
        s"Expected build left, but got ${dmjNode.buildSide}")
      assertDMJPlanCount(df.queryExecution.sparkPlan, expect = 1)
      checkAnswer(df, sql(base))
    }
  }

  test("DMJ: subquery as build side with hint on alias") {
    prepareTables()
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val dmj =
        """
          |SELECT /*+ DISTMAPJOIN(ds) */
          |  f.k, ds.v
          |FROM fact f
          |JOIN (SELECT k, v FROM dim WHERE k IS NOT NULL) ds
          |ON f.k = ds.k
          |""".stripMargin
      val base =
        """SELECT f.k, ds.v FROM fact f
          |JOIN (SELECT k, v FROM dim WHERE k IS NOT NULL) ds
          |ON f.k = ds.k""".stripMargin
      val df = sql(dmj)
      assertDMJPlanCount(df.queryExecution.sparkPlan, expect = 1)
      checkAnswer(df, sql(base))
    }
  }

  test("DMJ: heterogeneous two-key join (int + string)") {
    prepareTables()
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val fhs = Seq((1, "X", "p1"), (2, "Y", "p2"), (3, "Z", "p3"), (3, "W", "p4")).toDF(
        "k",
        "cat",
        "payload")
      fhs.createOrReplaceTempView("facts")
      val dmj =
        """
          |SELECT /*+ DISTMAPJOIN(d) */
          |  f.k, f.cat, d.val
          |FROM facts f JOIN dim2 d ON f.k = d.k AND f.cat = d.cat
          |""".stripMargin
      val base =
        """SELECT f.k, f.cat, d.val FROM facts f JOIN dim2 d
          |ON f.k = d.k AND f.cat = d.cat""".stripMargin
      val df = sql(dmj)
      assertDMJPlanCount(df.queryExecution.sparkPlan, expect = 1)
      checkAnswer(df, sql(base))
    }
  }

  test("DMJ hint in SQL is applied to correct side") {
    withTempView("t", "u") {
      spark.range(10).createOrReplaceTempView("t")
      spark.range(10).createOrReplaceTempView("u")
      val plan1 = sql(
        "SELECT /*+ DISTMAPJOIN(t) */ * FROM t JOIN u ON t.id = u.id").queryExecution.optimizedPlan
        .asInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Join]
      val plan2 = sql(
        "SELECT /*+ DISTMAPJOIN(u) */ * FROM t JOIN u ON t.id = u.id").queryExecution.optimizedPlan
        .asInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Join]
      val plan3 = sql(
        "SELECT /*+ DISTMAPJOIN(v) */ * FROM t JOIN u ON t.id = u.id").queryExecution.optimizedPlan
        .asInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Join]
      assert(plan1.hint.leftHint.get.strategy.exists(isDistributedMapJoin))
      assert(plan1.hint.rightHint.isEmpty)
      assert(plan2.hint.leftHint.isEmpty)
      assert(plan2.hint.rightHint.get.strategy.exists(isDistributedMapJoin))
      assert(plan3.hint.leftHint.isEmpty && plan3.hint.rightHint.isEmpty)
    }
  }

  private def isDistributedMapJoin(s: JoinStrategyHint): Boolean =
    s.isInstanceOf[DistributedMapJoinStrategy]
}

// run with AQE disabled
class DistributedMapJoinSuite
    extends DistributedMapJoinSuiteBase
    with DisableAdaptiveExecutionSuite

// run with AQE enabled
class DistributedMapJoinSuiteAE
    extends DistributedMapJoinSuiteBase
    with EnableAdaptiveExecutionSuite
