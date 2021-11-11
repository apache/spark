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

import scala.reflect.ClassTag

import org.apache.spark.AccumulatorSuite
import org.apache.spark.sql.{Dataset, QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BitwiseAnd, BitwiseOr, Cast, Expression, Literal, ShiftLeft}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.BROADCAST
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, PartitioningCollection}
import org.apache.spark.sql.execution.{DummySparkPlan, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ShuffleExchangeExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.{LongType, ShortType}

/**
 * Test various broadcast join operators.
 *
 * Tests in this suite we need to run Spark in local-cluster mode. In particular, the use of
 * unsafe map in [[org.apache.spark.sql.execution.joins.UnsafeHashedRelation]] is not triggered
 * without serializing the hashed relation, which does not happen in local mode.
 */
abstract class BroadcastJoinSuiteBase extends QueryTest with SQLTestUtils
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  protected var spark: SparkSession = null

  private val EnsureRequirements = new EnsureRequirements()

  /**
   * Create a new [[SparkSession]] running in local-cluster mode with unsafe and codegen enabled.
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local-cluster[2,1,1024]")
      .appName("testing")
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

  /**
   * Test whether the specified broadcast join updates the peak execution memory accumulator.
   */
  private def testBroadcastJoinPeak[T: ClassTag](name: String, joinType: String): Unit = {
    AccumulatorSuite.verifyPeakExecutionMemorySet(spark.sparkContext, name) {
      val plan = testBroadcastJoin[T](joinType)
      plan.executeCollect()
    }
  }

  private def testBroadcastJoin[T: ClassTag](
      joinType: String,
      forceBroadcast: Boolean = false): SparkPlan = {
    val df1 = Seq((1, "4"), (2, "2")).toDF("key", "value")
    val df2 = Seq((1, "1"), (2, "2")).toDF("key", "value")

    // Comparison at the end is for broadcast left semi join
    val joinExpression = df1("key") === df2("key") && df1("value") > df2("value")
    val df3 = if (forceBroadcast) {
      df1.join(broadcast(df2), joinExpression, joinType)
    } else {
      df1.join(df2, joinExpression, joinType)
    }
    val plan = EnsureRequirements.apply(df3.queryExecution.sparkPlan)
    assert(plan.collect { case p: T => p }.size === 1)
    plan
  }

  test("unsafe broadcast hash join updates peak execution memory") {
    testBroadcastJoinPeak[BroadcastHashJoinExec]("unsafe broadcast hash join", "inner")
  }

  test("unsafe broadcast hash outer join updates peak execution memory") {
    testBroadcastJoinPeak[BroadcastHashJoinExec]("unsafe broadcast hash outer join", "left_outer")
  }

  test("unsafe broadcast left semi join updates peak execution memory") {
    testBroadcastJoinPeak[BroadcastHashJoinExec]("unsafe broadcast left semi join", "leftsemi")
  }

  test("broadcast hint isn't bothered by authBroadcastJoinThreshold set to low values") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
      testBroadcastJoin[BroadcastHashJoinExec]("inner", true)
    }
  }

  test("broadcast hint isn't bothered by a disabled authBroadcastJoinThreshold") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      testBroadcastJoin[BroadcastHashJoinExec]("inner", true)
    }
  }

  test("SPARK-23192: broadcast hint should be retained after using the cached data") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      try {
        val df1 = Seq((1, "4"), (2, "2")).toDF("key", "value")
        val df2 = Seq((1, "1"), (2, "2")).toDF("key", "value")
        df2.cache()
        val df3 = df1.join(broadcast(df2), Seq("key"), "inner")
        val numBroadCastHashJoin = collect(df3.queryExecution.executedPlan) {
          case b: BroadcastHashJoinExec => b
        }.size
        assert(numBroadCastHashJoin === 1)
      } finally {
        spark.catalog.clearCache()
      }
    }
  }

  test("SPARK-23214: cached data should not carry extra hint info") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      try {
        val df1 = Seq((1, "4"), (2, "2")).toDF("key", "value")
        val df2 = Seq((1, "1"), (2, "2")).toDF("key", "value")
        broadcast(df2).cache()

        val df3 = df1.join(df2, Seq("key"), "inner")
        val numCachedPlan = collect(df3.queryExecution.executedPlan) {
          case i: InMemoryTableScanExec => i
        }.size
        // df2 should be cached.
        assert(numCachedPlan === 1)

        val numBroadCastHashJoin = collect(df3.queryExecution.executedPlan) {
          case b: BroadcastHashJoinExec => b
        }.size
        // df2 should not be broadcasted.
        assert(numBroadCastHashJoin === 0)
      } finally {
        spark.catalog.clearCache()
      }
    }
  }

  test("broadcast hint isn't propagated after a join") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val df1 = Seq((1, "4"), (2, "2")).toDF("key", "value")
      val df2 = Seq((1, "1"), (2, "2")).toDF("key", "value")
      val df3 = df1.join(broadcast(df2), Seq("key"), "inner").drop(df2("key"))

      val df4 = Seq((1, "5"), (2, "5")).toDF("key", "value")
      val df5 = df4.join(df3, Seq("key"), "inner")

      val plan = EnsureRequirements.apply(df5.queryExecution.sparkPlan)

      assert(plan.collect { case p: BroadcastHashJoinExec => p }.size === 1)
      assert(plan.collect { case p: SortMergeJoinExec => p }.size === 1)
    }
  }

  private def assertBroadcastJoin(df : Dataset[Row]) : Unit = {
    val df1 = Seq((1, "4"), (2, "2")).toDF("key", "value")
    val joined = df1.join(df, Seq("key"), "inner")

    val plan = EnsureRequirements.apply(joined.queryExecution.sparkPlan)

    assert(plan.collect { case p: BroadcastHashJoinExec => p }.size === 1)
  }

  test("broadcast hint programming API") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val df2 = Seq((1, "1"), (2, "2"), (3, "2")).toDF("key", "value")
      val broadcasted = broadcast(df2)
      val df3 = Seq((2, "2"), (3, "3")).toDF("key", "value")

      val cases = Seq(
        broadcasted.limit(2),
        broadcasted.filter("value < 10"),
        broadcasted.sample(true, 0.5),
        broadcasted.distinct(),
        broadcasted.groupBy("value").agg(min($"key").as("key")),
        // except and intersect are semi/anti-joins which won't return more data then
        // their left argument, so the broadcast hint should be propagated here
        broadcasted.except(df3),
        broadcasted.intersect(df3))

      cases.foreach(assertBroadcastJoin)
    }
  }

  test("broadcast hint in SQL") {
    import org.apache.spark.sql.catalyst.plans.logical.Join
    withTempView("t", "u") {
      spark.range(10).createOrReplaceTempView("t")
      spark.range(10).createOrReplaceTempView("u")

      for (name <- Seq("BROADCAST", "BROADCASTJOIN", "MAPJOIN")) {
        val plan1 = sql(s"SELECT /*+ $name(t) */ * FROM t JOIN u ON t.id = u.id").queryExecution
          .optimizedPlan
        val plan2 = sql(s"SELECT /*+ $name(u) */ * FROM t JOIN u ON t.id = u.id").queryExecution
          .optimizedPlan
        val plan3 = sql(s"SELECT /*+ $name(v) */ * FROM t JOIN u ON t.id = u.id").queryExecution
          .optimizedPlan

        assert(plan1.asInstanceOf[Join].hint.leftHint.get.strategy.contains(BROADCAST))
        assert(plan1.asInstanceOf[Join].hint.rightHint.isEmpty)
        assert(plan2.asInstanceOf[Join].hint.leftHint.isEmpty)
        assert(plan2.asInstanceOf[Join].hint.rightHint.get.strategy.contains(BROADCAST))
        assert(plan3.asInstanceOf[Join].hint.leftHint.isEmpty)
        assert(plan3.asInstanceOf[Join].hint.rightHint.isEmpty)
      }
    }
  }

  test("join key rewritten") {
    val l = Literal(1L)
    val i = Literal(2)
    val s = Literal.create(3.toShort, ShortType)
    val ss = Literal("hello")

    assert(HashJoin.rewriteKeyExpr(l :: Nil) === l :: Nil)
    assert(HashJoin.rewriteKeyExpr(l :: l :: Nil) === l :: l :: Nil)
    assert(HashJoin.rewriteKeyExpr(l :: i :: Nil) === l :: i :: Nil)

    assert(HashJoin.rewriteKeyExpr(i :: Nil) ===
      Cast(i, LongType, Some(conf.sessionLocalTimeZone)) :: Nil)
    assert(HashJoin.rewriteKeyExpr(i :: l :: Nil) === i :: l :: Nil)
    assert(HashJoin.rewriteKeyExpr(i :: i :: Nil) ===
      BitwiseOr(ShiftLeft(Cast(i, LongType, Some(conf.sessionLocalTimeZone)), Literal(32)),
        BitwiseAnd(Cast(i, LongType, Some(conf.sessionLocalTimeZone)), Literal((1L << 32) - 1))) ::
        Nil)
    assert(HashJoin.rewriteKeyExpr(i :: i :: i :: Nil) === i :: i :: i :: Nil)

    assert(HashJoin.rewriteKeyExpr(s :: Nil) ===
      Cast(s, LongType, Some(conf.sessionLocalTimeZone)) :: Nil)
    assert(HashJoin.rewriteKeyExpr(s :: l :: Nil) === s :: l :: Nil)
    assert(HashJoin.rewriteKeyExpr(s :: s :: Nil) ===
      BitwiseOr(ShiftLeft(Cast(s, LongType, Some(conf.sessionLocalTimeZone)), Literal(16)),
        BitwiseAnd(Cast(s, LongType, Some(conf.sessionLocalTimeZone)), Literal((1L << 16) - 1))) ::
        Nil)
    assert(HashJoin.rewriteKeyExpr(s :: s :: s :: Nil) ===
      BitwiseOr(ShiftLeft(
        BitwiseOr(ShiftLeft(Cast(s, LongType, Some(conf.sessionLocalTimeZone)), Literal(16)),
          BitwiseAnd(Cast(s, LongType, Some(conf.sessionLocalTimeZone)), Literal((1L << 16) - 1))),
        Literal(16)),
        BitwiseAnd(Cast(s, LongType, Some(conf.sessionLocalTimeZone)), Literal((1L << 16) - 1))) ::
        Nil)
    assert(HashJoin.rewriteKeyExpr(s :: s :: s :: s :: Nil) ===
      BitwiseOr(ShiftLeft(
        BitwiseOr(ShiftLeft(
          BitwiseOr(ShiftLeft(Cast(s, LongType, Some(conf.sessionLocalTimeZone)), Literal(16)),
            BitwiseAnd(Cast(s, LongType, Some(conf.sessionLocalTimeZone)),
              Literal((1L << 16) - 1))),
          Literal(16)),
          BitwiseAnd(Cast(s, LongType, Some(conf.sessionLocalTimeZone)), Literal((1L << 16) - 1))),
        Literal(16)),
        BitwiseAnd(Cast(s, LongType, Some(conf.sessionLocalTimeZone)), Literal((1L << 16) - 1))) ::
        Nil)
    assert(HashJoin.rewriteKeyExpr(s :: s :: s :: s :: s :: Nil) ===
      s :: s :: s :: s :: s :: Nil)

    assert(HashJoin.rewriteKeyExpr(ss :: Nil) === ss :: Nil)
    assert(HashJoin.rewriteKeyExpr(l :: ss :: Nil) === l :: ss :: Nil)
    assert(HashJoin.rewriteKeyExpr(i :: ss :: Nil) === i :: ss :: Nil)
  }

  test("Shouldn't change broadcast join buildSide if user clearly specified") {
    withTempView("t1", "t2") {
      Seq((1, "4"), (2, "2")).toDF("key", "value").createTempView("t1")
      Seq((1, "1"), (2, "12.3"), (2, "123")).toDF("key", "value").createTempView("t2")

      val t1Size = spark.table("t1").queryExecution.analyzed.children.head.stats.sizeInBytes
      val t2Size = spark.table("t2").queryExecution.analyzed.children.head.stats.sizeInBytes
      assert(t1Size < t2Size)

      /* ######## test cases for equal join ######### */
      // INNER JOIN && t1Size < t2Size => BuildLeft
      assertJoinBuildSide(
        "SELECT /*+ MAPJOIN(t1, t2) */ * FROM t1 JOIN t2 ON t1.key = t2.key", bh, BuildLeft)
      // LEFT JOIN => BuildRight
      // broadcast hash join can not build left side for left join.
      assertJoinBuildSide(
        "SELECT /*+ MAPJOIN(t1, t2) */ * FROM t1 LEFT JOIN t2 ON t1.key = t2.key", bh, BuildRight)
      // RIGHT JOIN => BuildLeft
      // broadcast hash join can not build right side for right join.
      assertJoinBuildSide(
        "SELECT /*+ MAPJOIN(t1, t2) */ * FROM t1 RIGHT JOIN t2 ON t1.key = t2.key", bh, BuildLeft)
      // INNER JOIN && broadcast(t1) => BuildLeft
      assertJoinBuildSide(
        "SELECT /*+ MAPJOIN(t1) */ * FROM t1 JOIN t2 ON t1.key = t2.key", bh, BuildLeft)
      // INNER JOIN && broadcast(t2) => BuildRight
      assertJoinBuildSide(
        "SELECT /*+ MAPJOIN(t2) */ * FROM t1 JOIN t2 ON t1.key = t2.key", bh, BuildRight)

      /* ######## test cases for non-equal join ######### */
      withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
        // INNER JOIN && t1Size < t2Size => BuildLeft
        assertJoinBuildSide("SELECT /*+ MAPJOIN(t1, t2) */ * FROM t1 JOIN t2", bl, BuildLeft)
        // FULL JOIN && t1Size < t2Size => BuildLeft
        assertJoinBuildSide("SELECT /*+ MAPJOIN(t1, t2) */ * FROM t1 FULL JOIN t2", bl, BuildLeft)
        // FULL OUTER && t1Size < t2Size => BuildLeft
        assertJoinBuildSide("SELECT * FROM t1 FULL OUTER JOIN t2", bl, BuildLeft)
        // LEFT JOIN => BuildRight
        assertJoinBuildSide("SELECT /*+ MAPJOIN(t1, t2) */ * FROM t1 LEFT JOIN t2", bl, BuildRight)
        // RIGHT JOIN => BuildLeft
        assertJoinBuildSide("SELECT /*+ MAPJOIN(t1, t2) */ * FROM t1 RIGHT JOIN t2", bl, BuildLeft)

        /* #### test with broadcast hint #### */
        // INNER JOIN && broadcast(t1) => BuildLeft
        assertJoinBuildSide("SELECT /*+ MAPJOIN(t1) */ * FROM t1 JOIN t2", bl, BuildLeft)
        // INNER JOIN && broadcast(t2) => BuildRight
        assertJoinBuildSide("SELECT /*+ MAPJOIN(t2) */ * FROM t1 JOIN t2", bl, BuildRight)
        // FULL OUTER && broadcast(t1) => BuildLeft
        assertJoinBuildSide("SELECT /*+ MAPJOIN(t1) */ * FROM t1 FULL OUTER JOIN t2", bl, BuildLeft)
        // FULL OUTER && broadcast(t2) => BuildRight
        assertJoinBuildSide(
          "SELECT /*+ MAPJOIN(t2) */ * FROM t1 FULL OUTER JOIN t2", bl, BuildRight)
        // LEFT JOIN && broadcast(t1) => BuildLeft
        assertJoinBuildSide("SELECT /*+ MAPJOIN(t1) */ * FROM t1 LEFT JOIN t2", bl, BuildLeft)
        // RIGHT JOIN && broadcast(t2) => BuildRight
        assertJoinBuildSide("SELECT /*+ MAPJOIN(t2) */ * FROM t1 RIGHT JOIN t2", bl, BuildRight)
      }
    }
  }

  test("Shouldn't bias towards build right if user didn't specify") {

    withTempView("t1", "t2") {
      Seq((1, "4"), (2, "2")).toDF("key", "value").createTempView("t1")
      Seq((1, "1"), (2, "12.3"), (2, "123")).toDF("key", "value").createTempView("t2")

      val t1Size = spark.table("t1").queryExecution.analyzed.children.head.stats.sizeInBytes
      val t2Size = spark.table("t2").queryExecution.analyzed.children.head.stats.sizeInBytes
      assert(t1Size < t2Size)

      /* ######## test cases for equal join ######### */
      assertJoinBuildSide("SELECT * FROM t1 JOIN t2 ON t1.key = t2.key", bh, BuildLeft)
      assertJoinBuildSide("SELECT * FROM t2 JOIN t1 ON t1.key = t2.key", bh, BuildRight)

      assertJoinBuildSide("SELECT * FROM t1 LEFT JOIN t2 ON t1.key = t2.key", bh, BuildRight)
      assertJoinBuildSide("SELECT * FROM t2 LEFT JOIN t1 ON t1.key = t2.key", bh, BuildRight)

      assertJoinBuildSide("SELECT * FROM t1 RIGHT JOIN t2 ON t1.key = t2.key", bh, BuildLeft)
      assertJoinBuildSide("SELECT * FROM t2 RIGHT JOIN t1 ON t1.key = t2.key", bh, BuildLeft)

      /* ######## test cases for non-equal join ######### */
      withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
        // For full outer join, prefer to broadcast the smaller side.
        assertJoinBuildSide("SELECT * FROM t1 FULL OUTER JOIN t2", bl, BuildLeft)
        assertJoinBuildSide("SELECT * FROM t2 FULL OUTER JOIN t1", bl, BuildRight)

        // For inner join, prefer to broadcast the smaller side, if broadcast-able.
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> (t2Size + 1).toString()) {
          assertJoinBuildSide("SELECT * FROM t1 JOIN t2", bl, BuildLeft)
          assertJoinBuildSide("SELECT * FROM t2 JOIN t1", bl, BuildRight)
        }

        // For left join, prefer to broadcast the right side.
        assertJoinBuildSide("SELECT * FROM t1 LEFT JOIN t2", bl, BuildRight)
        assertJoinBuildSide("SELECT * FROM t2 LEFT JOIN t1", bl, BuildRight)

        // For right join, prefer to broadcast the left side.
        assertJoinBuildSide("SELECT * FROM t1 RIGHT JOIN t2", bl, BuildLeft)
        assertJoinBuildSide("SELECT * FROM t2 RIGHT JOIN t1", bl, BuildLeft)
      }
    }
  }

  private val bh = BroadcastHashJoinExec.toString
  private val bl = BroadcastNestedLoopJoinExec.toString

  private def assertJoinBuildSide(sqlStr: String, joinMethod: String, buildSide: BuildSide): Any = {
    val executedPlan = stripAQEPlan(sql(sqlStr).queryExecution.executedPlan)
    executedPlan match {
      case b: BroadcastNestedLoopJoinExec =>
        assert(b.getClass.getSimpleName === joinMethod)
        assert(b.buildSide === buildSide)
      case b: BroadcastHashJoinExec =>
        assert(b.getClass.getSimpleName === joinMethod)
        assert(b.buildSide === buildSide)
      case w: WholeStageCodegenExec =>
        assert(w.children.head.getClass.getSimpleName === joinMethod)
        if (w.children.head.isInstanceOf[BroadcastNestedLoopJoinExec]) {
          assert(
            w.children.head.asInstanceOf[BroadcastNestedLoopJoinExec].buildSide === buildSide)
        } else if (w.children.head.isInstanceOf[BroadcastHashJoinExec]) {
          assert(w.children.head.asInstanceOf[BroadcastHashJoinExec].buildSide === buildSide)
        } else {
          fail()
        }
    }
  }

  test("Broadcast timeout") {
    val timeout = 5
    val slowUDF = udf({ x: Int => Thread.sleep(timeout * 1000); x })
    val df1 = spark.range(10).select($"id" as 'a)
    val df2 = spark.range(5).select(slowUDF($"id") as 'a)
    val testDf = df1.join(broadcast(df2), "a")
    withSQLConf(SQLConf.BROADCAST_TIMEOUT.key -> timeout.toString) {
      if (!conf.adaptiveExecutionEnabled) {
        val e = intercept[Exception] {
          testDf.collect()
        }
        assert(e.getMessage.contains(s"Could not execute broadcast in $timeout secs."))
      }
    }
  }

  test("broadcast join where streamed side's output partitioning is HashPartitioning") {
    withTable("t1", "t3") {
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "500") {
        val df1 = (0 until 100).map(i => (i % 5, i % 13)).toDF("i1", "j1")
        val df2 = (0 until 20).map(i => (i % 7, i % 11)).toDF("i2", "j2")
        val df3 = (0 until 100).map(i => (i % 5, i % 13)).toDF("i3", "j3")
        df1.write.format("parquet").bucketBy(8, "i1", "j1").saveAsTable("t1")
        df3.write.format("parquet").bucketBy(8, "i3", "j3").saveAsTable("t3")
        val t1 = spark.table("t1")
        val t3 = spark.table("t3")

        // join1 is a broadcast join where df2 is broadcasted. Note that output partitioning on the
        // streamed side (t1) is HashPartitioning (bucketed files).
        val join1 = t1.join(df2, t1("i1") === df2("i2") && t1("j1") === df2("j2"))
        withSQLConf(SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key -> "false") {
          val plan1 = join1.queryExecution.executedPlan
          assert(collect(plan1) { case e: ShuffleExchangeExec => e }.isEmpty)
          val broadcastJoins = collect(plan1) { case b: BroadcastHashJoinExec => b }
          assert(broadcastJoins.size == 1)
          assert(broadcastJoins(0).outputPartitioning.isInstanceOf[PartitioningCollection])
          val p = broadcastJoins(0).outputPartitioning.asInstanceOf[PartitioningCollection]
          assert(p.partitionings.size == 4)
          // Verify all the combinations of output partitioning.
          Seq(Seq(t1("i1"), t1("j1")),
            Seq(t1("i1"), df2("j2")),
            Seq(df2("i2"), t1("j1")),
            Seq(df2("i2"), df2("j2"))).foreach { expected =>
            val expectedExpressions = expected.map(_.expr)
            assert(p.partitionings.exists {
              case h: HashPartitioning => expressionsEqual(h.expressions, expectedExpressions)
            })
          }
        }

        // Join on the column from the broadcasted side (i2, j2) and make sure output partitioning
        // is maintained by checking no shuffle exchange is introduced.
        val join2 = join1.join(t3, join1("i2") === t3("i3") && join1("j2") === t3("j3"))
        val plan2 = join2.queryExecution.executedPlan
        assert(collect(plan2) { case s: SortMergeJoinExec => s }.size == 1)
        assert(collect(plan2) { case b: BroadcastHashJoinExec => b }.size == 1)
        assert(collect(plan2) { case e: ShuffleExchangeExec => e }.isEmpty)

        // Validate the data with broadcast join off.
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          val df = join1.join(t3, join1("i2") === t3("i3") && join1("j2") === t3("j3"))
          checkAnswer(join2, df)
        }
      }
    }
  }

  test("broadcast join where streamed side's output partitioning is PartitioningCollection") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "500") {
      val t1 = (0 until 100).map(i => (i % 5, i % 13)).toDF("i1", "j1")
      val t2 = (0 until 100).map(i => (i % 5, i % 14)).toDF("i2", "j2")
      val t3 = (0 until 20).map(i => (i % 7, i % 11)).toDF("i3", "j3")
      val t4 = (0 until 100).map(i => (i % 5, i % 15)).toDF("i4", "j4")

      // join1 is a sort merge join (shuffle on the both sides).
      val join1 = t1.join(t2, t1("i1") === t2("i2"))
      val plan1 = join1.queryExecution.executedPlan
      assert(collect(plan1) { case s: SortMergeJoinExec => s }.size == 1)
      assert(collect(plan1) { case e: ShuffleExchangeExec => e }.size == 2)

      // join2 is a broadcast join where t3 is broadcasted. Note that output partitioning on the
      // streamed side (join1) is PartitioningCollection (sort merge join)
      val join2 = join1.join(t3, join1("i1") === t3("i3"))
      val plan2 = join2.queryExecution.executedPlan
      assert(collect(plan2) { case s: SortMergeJoinExec => s }.size == 1)
      assert(collect(plan2) { case e: ShuffleExchangeExec => e }.size == 2)
      val broadcastJoins = collect(plan2) { case b: BroadcastHashJoinExec => b }
      assert(broadcastJoins.size == 1)
      assert(broadcastJoins(0).outputPartitioning.isInstanceOf[PartitioningCollection])
      val p = broadcastJoins(0).outputPartitioning.asInstanceOf[PartitioningCollection]
      assert(p.partitionings.size == 3)
      // Verify all the combinations of output partitioning.
      Seq(Seq(t1("i1")), Seq(t2("i2")), Seq(t3("i3"))).foreach { expected =>
        val expectedExpressions = expected.map(_.expr)
        assert(p.partitionings.exists {
          case h: HashPartitioning => expressionsEqual(h.expressions, expectedExpressions)
        })
      }

      // Join on the column from the broadcasted side (i3) and make sure output partitioning
      // is maintained by checking no shuffle exchange is introduced. Note that one extra
      // ShuffleExchangeExec is from t4, not from join2.
      val join3 = join2.join(t4, join2("i3") === t4("i4"))
      val plan3 = join3.queryExecution.executedPlan
      assert(collect(plan3) { case s: SortMergeJoinExec => s }.size == 2)
      assert(collect(plan3) { case b: BroadcastHashJoinExec => b }.size == 1)
      assert(collect(plan3) { case e: ShuffleExchangeExec => e }.size == 3)

      // Validate the data with broadcast join off.
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        val df = join2.join(t4, join2("i3") === t4("i4"))
        checkAnswer(join3, df)
      }
    }
  }

  test("BroadcastHashJoinExec output partitioning scenarios for inner join") {
    val l1 = AttributeReference("l1", LongType)()
    val l2 = AttributeReference("l2", LongType)()
    val l3 = AttributeReference("l3", LongType)()
    val r1 = AttributeReference("r1", LongType)()
    val r2 = AttributeReference("r2", LongType)()
    val r3 = AttributeReference("r3", LongType)()

    // Streamed side has a HashPartitioning.
    var bhj = BroadcastHashJoinExec(
      leftKeys = Seq(l2, l3),
      rightKeys = Seq(r1, r2),
      Inner,
      BuildRight,
      None,
      left = DummySparkPlan(outputPartitioning = HashPartitioning(Seq(l1, l2, l3), 1)),
      right = DummySparkPlan())
    var expected = PartitioningCollection(Seq(
      HashPartitioning(Seq(l1, l2, l3), 1),
      HashPartitioning(Seq(l1, l2, r2), 1),
      HashPartitioning(Seq(l1, r1, l3), 1),
      HashPartitioning(Seq(l1, r1, r2), 1)))
    assert(bhj.outputPartitioning === expected)

    // Streamed side has a PartitioningCollection.
    bhj = BroadcastHashJoinExec(
      leftKeys = Seq(l1, l2, l3),
      rightKeys = Seq(r1, r2, r3),
      Inner,
      BuildRight,
      None,
      left = DummySparkPlan(outputPartitioning = PartitioningCollection(Seq(
        HashPartitioning(Seq(l1, l2), 1), HashPartitioning(Seq(l3), 1)))),
      right = DummySparkPlan())
    expected = PartitioningCollection(Seq(
      HashPartitioning(Seq(l1, l2), 1),
      HashPartitioning(Seq(l1, r2), 1),
      HashPartitioning(Seq(r1, l2), 1),
      HashPartitioning(Seq(r1, r2), 1),
      HashPartitioning(Seq(l3), 1),
      HashPartitioning(Seq(r3), 1)))
    assert(bhj.outputPartitioning === expected)

    // Streamed side has a nested PartitioningCollection.
    bhj = BroadcastHashJoinExec(
      leftKeys = Seq(l1, l2, l3),
      rightKeys = Seq(r1, r2, r3),
      Inner,
      BuildRight,
      None,
      left = DummySparkPlan(outputPartitioning = PartitioningCollection(Seq(
        PartitioningCollection(Seq(HashPartitioning(Seq(l1), 1), HashPartitioning(Seq(l2), 1))),
        HashPartitioning(Seq(l3), 1)))),
      right = DummySparkPlan())
    expected = PartitioningCollection(Seq(
      PartitioningCollection(Seq(
        HashPartitioning(Seq(l1), 1),
        HashPartitioning(Seq(r1), 1),
        HashPartitioning(Seq(l2), 1),
        HashPartitioning(Seq(r2), 1))),
      HashPartitioning(Seq(l3), 1),
      HashPartitioning(Seq(r3), 1)))
    assert(bhj.outputPartitioning === expected)

    // One-to-mapping case ("l1" = "r1" AND "l1" = "r2")
    bhj = BroadcastHashJoinExec(
      leftKeys = Seq(l1, l1),
      rightKeys = Seq(r1, r2),
      Inner,
      BuildRight,
      None,
      left = DummySparkPlan(outputPartitioning = HashPartitioning(Seq(l1, l2), 1)),
      right = DummySparkPlan())
    expected = PartitioningCollection(Seq(
      HashPartitioning(Seq(l1, l2), 1),
      HashPartitioning(Seq(r1, l2), 1),
      HashPartitioning(Seq(r2, l2), 1)))
    assert(bhj.outputPartitioning === expected)
  }

  test("BroadcastHashJoinExec output partitioning size should be limited with a config") {
    val l1 = AttributeReference("l1", LongType)()
    val l2 = AttributeReference("l2", LongType)()
    val r1 = AttributeReference("r1", LongType)()
    val r2 = AttributeReference("r2", LongType)()

    val expected = Seq(
      HashPartitioning(Seq(l1, l2), 1),
      HashPartitioning(Seq(l1, r2), 1),
      HashPartitioning(Seq(r1, l2), 1),
      HashPartitioning(Seq(r1, r2), 1))

    Seq(1, 2, 3, 4).foreach { limit =>
      withSQLConf(
        SQLConf.BROADCAST_HASH_JOIN_OUTPUT_PARTITIONING_EXPAND_LIMIT.key -> s"$limit") {
        val bhj = BroadcastHashJoinExec(
          leftKeys = Seq(l1, l2),
          rightKeys = Seq(r1, r2),
          Inner,
          BuildRight,
          None,
          left = DummySparkPlan(outputPartitioning = HashPartitioning(Seq(l1, l2), 1)),
          right = DummySparkPlan())
        assert(bhj.outputPartitioning === PartitioningCollection(expected.take(limit)))
      }
    }
  }

  private def expressionsEqual(l: Seq[Expression], r: Seq[Expression]): Boolean = {
    l.length == r.length && l.zip(r).forall { case (e1, e2) => e1.semanticEquals(e2) }
  }
}

class BroadcastJoinSuite extends BroadcastJoinSuiteBase with DisableAdaptiveExecutionSuite

class BroadcastJoinSuiteAE extends BroadcastJoinSuiteBase with EnableAdaptiveExecutionSuite
