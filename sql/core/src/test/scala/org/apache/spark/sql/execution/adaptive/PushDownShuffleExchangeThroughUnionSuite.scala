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

package org.apache.spark.sql.execution.adaptive

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Alias, Ascending, Attribute, AttributeReference, ExprId, GreaterThan, IsNotNull, Literal, Rand,
  RowNumber, SortOrder}
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.plans.physical.{
  ClusteredDistribution, CoalescedBoundary, CoalescedHashPartitioning, Distribution,
  HashPartitioning, NullAwareHashPartitioning, Partitioning, PartitioningCollection,
  RoundRobinPartitioning,
  UnknownPartitioning}
import org.apache.spark.sql.execution.{
  FilterExec, InputAdapter, LocalLimitExec, ProjectExec, QueryExecution, ScalarSubquery, SortExec,
  SparkPlan, StampUnionDecisions, SubqueryExec, UnaryExecNode, UnionConfSnapshot, UnionExec,
  WholeStageCodegenExec}
import org.apache.spark.sql.execution.exchange.{
  ENSURE_REQUIREMENTS, EnsureRequirements, REPARTITION_BY_COL, REPARTITION_BY_NUM,
  ShuffleExchangeExec, ValidateRequirements}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.window.{Final => FinalWindowGroupLimit, Partial => PartialWindowGroupLimit, WindowGroupLimitExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

class PushDownShuffleExchangeThroughUnionSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  // The suite exercises the rule itself, so it is on for every test whatever the default is.
  override protected def sparkConf =
    super.sparkConf
      .set(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ENABLED.key, "true")
      .set(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ALLOW_JOIN_INPUTS_ENABLED.key, "true")
      .set(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_JOIN_BRANCHES_ENABLED.key, "true")
      .set(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ALLOW_WITH_SUBQUERIES_ENABLED.key, "true")
      .set(SQLConf
        .PUSH_DOWN_SHUFFLE_THROUGH_UNION_ALLOW_PARTITION_KEYS_SUBSET_OF_CLUSTER_KEYS_ENABLED.key,
        "true")
      .set(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key, "false")

  private val query: String =
    """
      |SELECT id, v, row_number() OVER (PARTITION BY id ORDER BY v) AS rn
      |FROM (
      |  SELECT id, sum(v) AS v FROM t1 GROUP BY id
      |  UNION ALL
      |  SELECT id, v FROM t2
      |)
      |""".stripMargin

  // A shuffle that moves a union as a whole, not one the rewrite puts under one of its children to
  // lay that child out on the target.
  private def numShufflesOnUnion(plan: SparkPlan): Int =
    plan.collect { case s: ShuffleExchangeExec if isUnionPlan(s.child) => s }.size

  // The union may be wrapped in a `WholeStageCodegenExec` by `CollapseCodegenStages`, or in an
  // `InputAdapter` when it feeds a codegen'd parent, so look through both when checking whether
  // a shuffle's child is (effectively) a union.
  private def isUnionPlan(plan: SparkPlan): Boolean = plan match {
    case _: UnionExec => true
    case w: WholeStageCodegenExec => isUnionPlan(w.child)
    case i: InputAdapter => isUnionPlan(i.child)
    case _ => false
  }

  // A shuffle with a union anywhere below it, through the operators this rule can cross and the
  // codegen wrappers `CollapseCodegenStages` may have added.
  private def numShufflesOverUnion(plan: SparkPlan): Int =
    countShuffles(plan, underUnion = false) { s => reachesUnion(s.child) }

  private def countShuffles(
      plan: SparkPlan,
      underUnion: Boolean)(matches: ShuffleExchangeExec => Boolean): Int = {
    val here = plan match {
      case s: ShuffleExchangeExec if !underUnion && matches(s) => 1
      case _ => 0
    }
    here + plan.children.map(countShuffles(_, underUnion || plan.isInstanceOf[UnionExec])(matches))
      .sum
  }

  private def reachesUnion(plan: SparkPlan): Boolean = plan match {
    case _: UnionExec => true
    case unary: UnaryExecNode => reachesUnion(unary.child)
    case _ => false
  }

  private def withLookThrough(f: => Unit): Unit = withSQLConf(
    SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_LOOK_THROUGH_OPERATORS_ENABLED.key -> "true")(f)

  private def aqeInitialPlan(df: DataFrame): SparkPlan = df.queryExecution.executedPlan match {
    case a: AdaptiveSparkPlanExec => a.initialPlan
    case other => fail(s"expected an adaptive plan:\n$other")
  }

  private def pushDown(plan: SparkPlan, required: Option[Distribution] = None): SparkPlan = {
    val snapshot = UnionConfSnapshot(spark.sessionState.conf)
    PushDownShuffleExchangeThroughUnion(
      snapshot, EnsureRequirements(requiredDistribution = required),
      queryContainsSubqueries = false)(plan)
  }

  private def withPushDown(f: => Unit): Unit = withSQLConf(
      SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true",
      SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ENABLED.key -> "true",
      SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_NON_COALESCIBLE_BRANCHES_ENABLED.key ->
        "true")(f)

  private def attr(name: String): AttributeReference = AttributeReference(name, IntegerType)()

  private def leaf(
      output: Attribute,
      partitioning: Partitioning = UnknownPartitioning(0)): TestPartitioningPlan =
    TestPartitioningPlan(outputPartitioning = partitioning, output = Seq(output))

  private def hash(a: Attribute, n: Int): HashPartitioning = HashPartitioning(Seq(a), n)

  private def clustered(a: Attribute): ClusteredDistribution = ClusteredDistribution(Seq(a))

  private def ensureShuffle(partitioning: Partitioning, child: SparkPlan): ShuffleExchangeExec =
    ShuffleExchangeExec(partitioning, child, ENSURE_REQUIREMENTS)

  private def assertReshuffled(plan: SparkPlan, expected: Partitioning, child: SparkPlan): Unit = {
    plan match {
      case s: ShuffleExchangeExec =>
        assert(s.shuffleOrigin === ENSURE_REQUIREMENTS)
        assert(s.outputPartitioning === expected, s"unexpected target:\n$plan")
        assert(s.child.fastEquals(child), s"the shuffled child should be unchanged:\n$plan")
      case other => fail(s"expected a shuffle into $expected:\n$other")
    }
  }

  private def createTables(): Unit = {
    Seq((1, 10), (1, 20), (2, 5), (3, 7)).toDF("id", "v").createOrReplaceTempView("t1")
    Seq((1, 40), (2, 8)).toDF("id", "v").createOrReplaceTempView("t2")
  }

  private def expectedResult(sql: String): Seq[Row] =
    withSQLConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ENABLED.key -> "false") {
      spark.sql(sql).collect().toSeq
    }

  Seq("MERGE", "SHUFFLE_HASH").foreach { hint =>
    Seq("INNER", "FULL OUTER").foreach { joinType =>
      test(s"SPARK-60379: push down through union for $hint $joinType join") {
        withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
            SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
            SQLConf.SHUFFLE_PARTITIONS.key -> "4",
            SQLConf.SHUFFLE_SPREAD_NULL_JOIN_KEYS_ENABLED.key -> "false",
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          withTempView("t1", "t2") {
            Seq((Option(1), 10), (Option(1), 20), (None, 30)).toDF("id", "v")
              .createOrReplaceTempView("t1")
            Seq((Option(2), 40), (None, 50)).toDF("id", "v").createOrReplaceTempView("t2")
            val query = s"""SELECT /*+ $hint(u, r) */ u.id, u.v, r.v FROM (
               |SELECT /*+ REPARTITION(4, id) */ id, v FROM t1
               |UNION ALL SELECT id, v FROM t2) u
               |$joinType JOIN t1 r ON u.id = r.id""".stripMargin
            val expected = expectedResult(query)
            Seq(false, true).foreach { enabled =>
              withSQLConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ENABLED.key -> enabled.toString) {
                val df = spark.sql(query)
                checkAnswer(df, expected)
                val initial = aqeInitialPlan(df)
                assert(numShufflesOnUnion(initial) === (if (enabled) 0 else 1),
                  s"unexpected shuffle over the union:\n$initial")
                if (hint == "MERGE") {
                  assert(initial.exists(_.isInstanceOf[SortMergeJoinExec]),
                    s"expected a sort-merge join:\n$initial")
                } else {
                  assert(initial.exists(_.isInstanceOf[ShuffledHashJoinExec]),
                    s"expected a shuffled hash join:\n$initial")
                }
                assert(ValidateRequirements.validate(initial))
              }
            }
          }
        }
      }
    }
  }

  Seq(2, 8, 64).foreach { partitions =>
    test(s"SPARK-60379: the rebuilt union carries its own count with a " +
      s"$partitions-partition child") {
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
          SQLConf.SHUFFLE_PARTITIONS.key -> "8") {
        withTempView("t1", "t2") {
          createTables()
          val query = s"""SELECT id, v,
             |row_number() OVER (PARTITION BY id ORDER BY v) AS rn FROM (
             |SELECT /*+ REPARTITION($partitions, id) */ id, v FROM t1
             |UNION ALL SELECT id, v FROM t2)""".stripMargin
          val expected = expectedResult(query)
          Seq(false, true).foreach { enabled =>
            withSQLConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ENABLED.key -> enabled.toString) {
              val df = spark.sql(query)
              checkAnswer(df, expected)
              val initial = aqeInitialPlan(df)
              // A target below what the replaced exchange carried is not picked, since the
              // operator above would then run with fewer partitions than before.
              val pushedDown = enabled && partitions >= 8
              assert(numShufflesOnUnion(initial) === (if (pushedDown) 0 else 1),
                s"unexpected shuffle over the union:\n$initial")
              // The window runs with the child's count once the layout is pushed down, and with
              // `spark.sql.shuffle.partitions` while the exchange above the union is kept.
              val expectedPartitions = if (pushedDown) partitions else 8
              assert(initial.outputPartitioning.numPartitions === expectedPartitions,
                s"unexpected count:\n$initial")
              assert(df.rdd.getNumPartitions === expectedPartitions)
            }
          }
        }
      }
    }
  }

  test("SPARK-60379: do not push down a null-aware shuffle") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "4",
        SQLConf.SHUFFLE_SPREAD_NULL_JOIN_KEYS_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTempView("t1", "t2") {
        Seq((Option(1), 10), (Option(1), 20), (None, 30)).toDF("id", "v")
          .createOrReplaceTempView("t1")
        Seq((Option(2), 40), (None, 50)).toDF("id", "v").createOrReplaceTempView("t2")
        // A full outer join on nullable keys lets null keys spread, so its exchange is a
        // `NullAwareHashPartitioning`, which a child laid out on a plain layout cannot report.
        val query =
          """SELECT /*+ MERGE(u, r) */ u.id, u.v, r.v FROM (
            |SELECT /*+ REPARTITION(4, id) */ id, v FROM t1
            |UNION ALL SELECT id, v FROM t2) u
            |FULL OUTER JOIN t1 r ON u.id = r.id""".stripMargin
        val df = spark.sql(query)
        checkAnswer(df, expectedResult(query))

        val initial = aqeInitialPlan(df)
        assert(initial.collect {
          case s: ShuffleExchangeExec
            if s.outputPartitioning.isInstanceOf[NullAwareHashPartitioning] => s
        }.nonEmpty, s"the join should spread null keys:\n$initial")
        assert(numShufflesOnUnion(initial) === 1, s"the union shuffle must stay:\n$initial")
      }
    }
  }

  test("SPARK-60379: select compatible targets for two union sides") {
    withPushDown {
      val a = attr("a")
      val b = attr("b")
      val a1 = attr("a1")
      val b1 = attr("b1")
      val left = UnionExec(Seq(leaf(a, hash(a, 4)), leaf(a1)))
      val right = UnionExec(Seq(leaf(b, hash(b, 4)), leaf(b1)))
      val parent = TestPartitioningPlan(
        children = Seq(ensureShuffle(hash(a, 4), left), ensureShuffle(hash(b, 4), right)),
        required = Seq(clustered(a), clustered(b)))
      val result = pushDown(parent)
      assert(result.children.forall(_.isInstanceOf[UnionExec]))
      assert(ValidateRequirements.validate(result))
    }
  }

  test("SPARK-60379: shuffle push-down is not registered in classic preparations") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      assert(!QueryExecution.preparations(spark, subquery = false)
        .exists(_.isInstanceOf[PushDownShuffleExchangeThroughUnion]))
      withTempView("t1", "t2") {
        createTables()
        val df = spark.sql(query)
        checkAnswer(df, expectedResult(query))
        assert(numShufflesOnUnion(df.queryExecution.executedPlan) === 1)
      }
    }
  }

  test("SPARK-60379: push down shuffle through union") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withTempView("t1", "t2") {
        createTables()
        val df = spark.sql(query)

        // The rule only rewrites the physical plan, so the result must be unchanged.
        checkAnswer(df, expectedResult(query))

        val plan = aqeInitialPlan(df)
        assert(numShufflesOnUnion(plan) === 0,
          s"shuffle should be pushed down below the union:\n$plan")
      }
    }
  }

  test("SPARK-60379: do not push down when no child is co-located with the target") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withTempView("t1", "t2") {
        Seq((1, 10), (2, 5)).toDF("id", "v").createOrReplaceTempView("t1")
        Seq((1, 40), (2, 8)).toDF("id", "v").createOrReplaceTempView("t2")

        val df = spark.sql(
          """
            |SELECT id, v, row_number() OVER (PARTITION BY id ORDER BY v) AS rn
            |FROM (
            |  SELECT id, v FROM t1
            |  UNION ALL
            |  SELECT id, v FROM t2
            |)
            |""".stripMargin)

        // Pushing the shuffle down would turn one shuffle into two with no child saved.
        val plan = aqeInitialPlan(df)
        assert(numShufflesOnUnion(plan) === 1,
          s"shuffle on top of union should be kept when no child is co-located:\n$plan")
      }
    }
  }

  test("SPARK-60379: push down shuffle through union with AQE enabled") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withTempView("t1", "t2") {
        createTables()
        val df = spark.sql(query)
        checkAnswer(df, expectedResult(query))

        val initial = aqeInitialPlan(df)
        assert(numShufflesOnUnion(initial) === 0,
          s"shuffle should be pushed below the union in the initial plan:\n$initial")
        val union = initial.collectFirst { case u: UnionExec => u }.get
        assert(union.children.count(_.isInstanceOf[ShuffleExchangeExec]) === 1,
          s"only the scan child should be re-shuffled:\n$initial")
      }
    }
  }

  Seq(false, true).foreach { adaptive =>
    test(s"SPARK-60379: retain the user-requested root partition count with AQE=$adaptive") {
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
          SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
        withTempView("t1", "t2") {
          createTables()
          val df = spark.table("t1").repartition(8, $"id")
            .union(spark.table("t2"))
            .withColumn("rn", org.apache.spark.sql.functions.row_number().over(
              org.apache.spark.sql.expressions.Window.partitionBy("id").orderBy("v")))
            .repartition(5, $"id").select("id", "v", "rn")
          checkAnswer(df, Seq(
            Row(1, 10, 1), Row(1, 20, 2), Row(1, 40, 3), Row(2, 5, 1), Row(2, 8, 2),
            Row(3, 7, 1)))

          val plan = if (adaptive) {
            aqeInitialPlan(df)
          } else {
            df.queryExecution.executedPlan
          }
          if (adaptive) {
            // The 8-partition target passes the floor of 5, but not the elided root repartition's
            // explicit count of 5. Only the root check rejects this rewrite.
            assert(numShufflesOnUnion(plan) === 1,
              s"the root repartition's layout must be kept:\n$plan")
          }
          assert(df.rdd.getNumPartitions === 5, s"unexpected root:\n$plan")
        }
      }
    }
  }

  test("SPARK-60379: keep a union child that is a join on the picked layout") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      withTempView("t1", "t2", "t3", "t4") {
        Seq((1, 10), (2, 20)).toDF("id", "v").createOrReplaceTempView("t1")
        Seq((1, 100), (3, 300)).toDF("id", "v").createOrReplaceTempView("t2")
        Seq((1, 7), (3, 8)).toDF("id", "v").createOrReplaceTempView("t3")
        Seq((1, 1), (3, 3)).toDF("id", "v2").createOrReplaceTempView("t4")
        // The union's first child is an inner join whose sides are shuffled on the join key, so
        // it reports their layouts as a `PartitioningCollection`. That branch answers the target
        // and keeps its exchanges; only the scan branch is shuffled onto it.
        val sql =
          """SELECT u.id, u.v, r.v2 FROM (
            |  SELECT l.id AS id, l.v AS v FROM t1 l JOIN t2 r ON l.id = r.id
            |  UNION ALL
            |  SELECT id, v FROM t3
            |) u JOIN t4 r ON u.id = r.id""".stripMargin
        val df = spark.sql(sql)
        checkAnswer(df, Seq(Row(1, 10, 1), Row(1, 7, 1), Row(3, 8, 3)))

        val plan = aqeInitialPlan(df)
        assert(numShufflesOnUnion(plan) === 0,
          s"the union exchange should be pushed down:\n$plan")
        val union = plan.collectFirst { case u: UnionExec => u }.get
        assert(union.children.head.collect { case _: SortMergeJoinExec => true }.nonEmpty,
          s"the join branch should keep its layout:\n$plan")
        assert(union.children(1).isInstanceOf[ShuffleExchangeExec],
          s"only the scan branch should be shuffled:\n$plan")
      }
    }
  }

  test("SPARK-60379: push down through union below a sort-merge join") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTempView("t1", "t2") {
        createTables()
        // The local sort stays above the rebuilt union; the other side's layout constrains it.
        val sql =
          """
            |SELECT t1.id, u.v FROM t1 JOIN (
            |  SELECT id, v FROM (SELECT id, v FROM t1 DISTRIBUTE BY id)
            |  UNION ALL
            |  SELECT id, v FROM t2
            |) u ON t1.id = u.id
            |""".stripMargin
        val df = spark.sql(sql)
        checkAnswer(df, expectedResult(sql))
        val plan = aqeInitialPlan(df)
        assert(plan.collect { case j: SortMergeJoinExec => j }.nonEmpty, s"expected SMJ:\n$plan")
        assert(numShufflesOnUnion(plan) === 0,
          s"the join union shuffle should be pushed down:\n$plan")
      }
    }
  }

  test("SPARK-60379: push down shuffle with multiple partition key columns") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withTempView("t1", "t2") {
        Seq((1, "a", 10), (1, "a", 20), (2, "b", 5)).toDF("id", "grp", "v")
          .createOrReplaceTempView("t1")
        Seq((1, "a", 40), (2, "b", 8)).toDF("id", "grp", "v").createOrReplaceTempView("t2")

        val sql =
          """
            |SELECT id, grp, v, row_number() OVER (PARTITION BY id, grp ORDER BY v) AS rn
            |FROM (
            |  SELECT id, grp, sum(v) AS v FROM t1 GROUP BY id, grp
            |  UNION ALL
            |  SELECT id, grp, v FROM t2
            |)
            |""".stripMargin
        val df = spark.sql(sql)
        checkAnswer(df, expectedResult(sql))
        assert(numShufflesOnUnion(aqeInitialPlan(df)) === 0,
          s"shuffle should be pushed below the union:\n${df.queryExecution.executedPlan}")
      }
    }
  }

  test("SPARK-60379: push down through union with a nullability-adjusted co-located child") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withTempView("t1", "t2") {
        // `id` is nullable so that `IsNotNull` in the Filter actually tightens its nullability.
        Seq((Option(1), 10), (Option(1), 20), (Option(2), 5)).toDF("id", "v")
          .createOrReplaceTempView("t1")
        Seq((Option(3), 7)).toDF("id", "v").createOrReplaceTempView("t2")

        val sql =
          """
            |SELECT id, v, row_number() OVER (PARTITION BY id ORDER BY v) AS rn
            |FROM (
            |  SELECT id, v FROM (SELECT id, v FROM t1 DISTRIBUTE BY id) WHERE id IS NOT NULL
            |  UNION ALL
            |  SELECT id, v FROM t2
            |)
            |""".stripMargin
        val df = spark.sql(sql)
        checkAnswer(df, expectedResult(sql))

        // The DISTRIBUTE BY child is hash-partitioned on id and co-located with the union's
        // output despite the Filter tightening `id`'s nullability, so the shuffle for the window
        // should be pushed below the union and only the scan child re-shuffled.
        val plan = aqeInitialPlan(df)
        assert(numShufflesOnUnion(plan) === 0, s"shuffle should be pushed below the union:\n$plan")
      }
    }
  }

  test("SPARK-60379: push down shuffle through union with whole-stage codegen disabled") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
      withTempView("t1", "t2") {
        createTables()
        val df = spark.sql(query)
        checkAnswer(df, expectedResult(query))
        val finalPlan = df.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
          .executedPlan
        assert(!finalPlan.exists(_.isInstanceOf[WholeStageCodegenExec]))
        assert(numShufflesOnUnion(aqeInitialPlan(df)) === 0,
          s"shuffle should be pushed below the union:\n${df.queryExecution.executedPlan}")
      }
    }
  }

  test("SPARK-60379: relax the target to a child's coarser partitioning") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withTempView("t1", "t2") {
        Seq((1, "a", 10), (1, "a", 20), (2, "b", 5)).toDF("id", "grp", "v")
          .createOrReplaceTempView("t1")
        Seq((1, "a", 40), (2, "b", 8)).toDF("id", "grp", "v").createOrReplaceTempView("t2")

        // The window requires ClusteredDistribution([id, grp]) with requireAllClusterKeys = false,
        // so the DISTRIBUTE BY id child (HashPartitioning(id)) already satisfies it. The shuffle
        // should be pushed down and its target relaxed to HashPartitioning(id) rather than the
        // wider HashPartitioning(id, grp).
        val sql =
          """
            |SELECT id, grp, v, row_number() OVER (PARTITION BY id, grp ORDER BY v) AS rn
            |FROM (
            |  SELECT id, grp, v FROM (SELECT id, grp, v FROM t1 DISTRIBUTE BY id)
            |  UNION ALL
            |  SELECT id, grp, v FROM t2
            |)
            |""".stripMargin
        val df = spark.sql(sql)
        checkAnswer(df, expectedResult(sql))

        val union = aqeInitialPlan(df).collect { case u: UnionExec => u }.head
        assert(union.outputPartitioning.isInstanceOf[HashPartitioning],
          s"the union should propagate a hash partitioning after the push-down:\n" +
            df.queryExecution.executedPlan)
        assert(union.outputPartitioning.asInstanceOf[HashPartitioning].expressions.length === 1,
          s"the target should be relaxed to the single-column partitioning:\n" +
            df.queryExecution.executedPlan)
      }
    }
  }

  test("SPARK-60379: push down shuffle when children produce different partitionings") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withTempView("t1", "t2") {
        Seq((1, "a", 10), (1, "a", 20), (2, "b", 5), (3, "c", 6)).toDF("id", "grp", "v")
          .createOrReplaceTempView("t1")
        Seq((1, "a", 40), (2, "b", 8)).toDF("id", "grp", "v").createOrReplaceTempView("t2")

        // The two children are hash-partitioned on different key sets (`id` vs `id, grp`), both
        // of which satisfy the window's `ClusteredDistribution([id, grp])`. The rule should push
        // the shuffle down rather than giving up on an empty intersection.
        val sql =
          """
            |SELECT id, grp, v, row_number() OVER (PARTITION BY id, grp ORDER BY v) AS rn
            |FROM (
            |  SELECT id, grp, v FROM (SELECT id, grp, v FROM t1 DISTRIBUTE BY id)
            |  UNION ALL
            |  SELECT id, grp, v FROM (SELECT id, grp, v FROM t2 DISTRIBUTE BY id, grp)
            |)
            |""".stripMargin
        val df = spark.sql(sql)
        checkAnswer(df, expectedResult(sql))
        assert(numShufflesOnUnion(aqeInitialPlan(df)) === 0,
          s"shuffle should be pushed below the union:\n${df.queryExecution.executedPlan}")

        // The first child keeps more estimated bytes, so its single-key layout wins.
        val union = aqeInitialPlan(df).collect { case u: UnionExec => u }.head
        assert(union.outputPartitioning.isInstanceOf[HashPartitioning],
          s"the union should propagate a hash partitioning after the push-down:\n" +
            df.queryExecution.executedPlan)
        assert(union.outputPartitioning.asInstanceOf[HashPartitioning].expressions.length === 1,
          s"the larger child's partitioning should win the byte ranking:\n" +
            df.queryExecution.executedPlan)
      }
    }
  }

  test("SPARK-60379: disabling optional session configs clones only when necessary") {
    val conf = spark.sessionState.conf
    val entry = SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ENABLED
    val explicit = conf.getConfString(entry.key, null)
    try {
      Seq(None, Some(true), Some(false)).foreach { value =>
        value match {
          case Some(enabled) => conf.setConf(entry, Some(enabled))
          case None => conf.unsetConf(entry.key)
        }
        val configured = org.apache.spark.sql.classic.SparkSession.getOrCloneSessionWithConfigsOff(
          spark, Nil, Seq(entry))
        assert((configured eq spark) === value.contains(false))
        assert(configured.sessionState.conf.getConf(entry).contains(false))
        assert(conf.getConf(entry) === value)
      }
    } finally {
      if (explicit == null) { conf.unsetConf(entry.key) } else {
        conf.setConfString(entry.key, explicit)
      }
    }
  }

  test("SPARK-60379: custom evaluators require explicit push-down enablement") {
    withPushDown {
      val key = SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ENABLED.key
      val conf = spark.sessionState.conf
      val explicit = conf.getConfString(key, null)
      try {
        conf.unsetConf(key)
        withSQLConf(SQLConf.ADAPTIVE_CUSTOM_COST_EVALUATOR_CLASS.key -> "custom.Evaluator") {
          assert(!conf.pushDownShuffleThroughUnionEnabled)
          val a = attr("id")
          val union = UnionExec(Seq(leaf(a, hash(a, 2)), leaf(attr("id"))))
          val parent = TestPartitioningPlan(children = Seq(ensureShuffle(hash(a, 2), union)),
            required = Seq(clustered(a)), output = Seq(a))
          assert(pushDown(parent).fastEquals(parent))
          withSQLConf(key -> "true") {
            assert(conf.pushDownShuffleThroughUnionEnabled)
            assert(numShufflesOnUnion(pushDown(parent)) === 0)
          }
          withSQLConf(key -> "false") {
            assert(!conf.pushDownShuffleThroughUnionEnabled)
          }
        }
        assert(conf.pushDownShuffleThroughUnionEnabled)
      } finally {
        if (explicit == null) { conf.unsetConf(key) } else { conf.setConfString(key, explicit) }
      }
    }
  }

  test("SPARK-60379: equal offers from one child preserve collection member order") {
    withPushDown {
      val a = attr("a")
      val b = attr("b")
      Seq(false, true).foreach { reverse =>
        val members = Seq(hash(a, 2), hash(b, 2))
        val ordered = if (reverse) { members.reverse } else { members }
        val kept = TestPartitioningPlan(outputPartitioning =
          PartitioningCollection(ordered),
          output = Seq(a, b))
        val union = UnionExec(Seq(kept,
          TestPartitioningPlan(output = Seq(attr("a"), attr("b")))))
        val parent = TestPartitioningPlan(
          children = Seq(ensureShuffle(HashPartitioning(Seq(a, b), 2), union)),
          required = Seq(ClusteredDistribution(Seq(a, b))), output = Seq(a, b))
        val rebuilt = pushDown(parent).children.head.asInstanceOf[UnionExec]
        assert(rebuilt.outputPartitioning === ordered.head)
      }
    }
  }

  test("SPARK-60379: equal byte benefits preserve first-offered layout order") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false") {
      withTempView("t1", "t2") {
        Seq((1, 10, 100), (2, 20, 200)).toDF("a", "b", "v")
          .createOrReplaceTempView("t1")
        Seq((3, 30, 300), (4, 40, 400)).toDF("a", "b", "v")
          .createOrReplaceTempView("t2")
        Seq(false, true).foreach { reverse =>
          val branches = Seq("SELECT * FROM t1 DISTRIBUTE BY a",
            "SELECT * FROM t2 DISTRIBUTE BY b")
          val ordered = if (reverse) { branches.reverse } else { branches }
          val sql = s"""SELECT a, b, v, row_number() OVER
            | (PARTITION BY a, b ORDER BY v) rn FROM (
            | SELECT * FROM (${ordered.head})
            | UNION ALL SELECT * FROM (${ordered(1)}))""".stripMargin
          val df = spark.sql(sql)
          checkAnswer(df, expectedResult(sql))
          val union = aqeInitialPlan(df).collectFirst { case u: UnionExec => u }.get
          val target = union.outputPartitioning.asInstanceOf[HashPartitioning]
          val key = union.output(if (reverse) { 1 } else { 0 })
          assert(target.expressions.size === 1)
          assert(target.expressions.head.semanticEquals(key))
        }
      }
    }
  }

  test("SPARK-60379: rejected zero-move targets do not prevent empty-branch replanning") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "5",
        SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_JOIN_BRANCHES_ENABLED.key -> "false") {
      withTempView("t1", "t2", "empty_source") {
        spark.range(20).selectExpr("id % 7 k", "id v").createOrReplaceTempView("t1")
        spark.range(20, 40).selectExpr("id % 7 k", "id v").createOrReplaceTempView("t2")
        val never = org.apache.spark.sql.functions.udf((id: Long) => id < 0)
        spark.range(10).filter(never($"id")).createOrReplaceTempView("empty_source")
        val sql = """SELECT k, v, rank() OVER (PARTITION BY v ORDER BY k) rn FROM (
          | SELECT k, sum(v) v FROM t1 GROUP BY k
          | UNION ALL SELECT k, sum(v) v FROM t2 GROUP BY k
          | UNION ALL SELECT t.k, t.v FROM t1 t JOIN empty_source e ON t.k = e.id)""".stripMargin
        val expected = expectedResult(sql)
        Seq(false, true).foreach { enabled =>
          withSQLConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ENABLED.key -> enabled.toString) {
            val df = spark.sql(sql)
            checkAnswer(df, expected)
            def plans(p: SparkPlan): Seq[SparkPlan] = p match {
              case stage: QueryStageExec => stage +: plans(stage.plan)
              case other => other +: other.children.flatMap(plans)
            }
            val finalNodes = plans(df.queryExecution.executedPlan
              .asInstanceOf[AdaptiveSparkPlanExec].executedPlan)
            assert(!finalNodes.exists(_.isInstanceOf[SortMergeJoinExec]))
            val unions = finalNodes.collect { case union: UnionExec => union }
            assert(unions.size === 1)
            assert(unions.head.children.size === 2)
            assert(!AQEUtils.isPushedThroughUnion(unions.head))
          }
        }
      }
    }
  }

  test("SPARK-60379: rejected zero-move candidates leave the input union unmarked") {
    withPushDown {
      val a = attr("a")
      val b = attr("b")
      val a1 = attr("a")
      val b1 = attr("b")
      val original = UnionExec(Seq(
        TestPartitioningPlan(outputPartitioning = hash(a, 2), output = Seq(a, b)),
        TestPartitioningPlan(outputPartitioning = hash(a1, 2), output = Seq(a1, b1))))
      val shuffle = ensureShuffle(hash(b, 2), original)
      org.apache.spark.sql.execution.exchange.PartitioningOrigin.record(
        shuffle, hash(b, 2), REPARTITION_BY_COL)
      val parent = TestPartitioningPlan(children = Seq(shuffle),
        required = Seq(clustered(b)), output = Seq(a, b))
      assert(pushDown(parent).fastEquals(parent))
      assert(!AQEUtils.isPushedThroughUnion(original))
      assert(org.apache.spark.sql.execution.exchange.PartitioningOrigin.validateNode(original))
      assert(SimpleCostEvaluator(false, false, true).evaluateCost(parent) === SimpleCost(0, 1, 0))
      val accepted = TestPartitioningPlan(children = Seq(ensureShuffle(hash(a, 2), original)),
        required = Seq(clustered(a)), output = Seq(a, b))
      val result = pushDown(accepted)
      assert(result.children.head.isInstanceOf[UnionExec])
      assert(!AQEUtils.isPushedThroughUnion(result.children.head))
      assert(!AQEUtils.isPushedThroughUnion(original))
      assert(SimpleCostEvaluator(false, false, true).evaluateCost(result) === SimpleCost(0, 0, 0))
    }
  }

  test("SPARK-60379: rejected zero-move unary candidates preserve origin isolation") {
    withPushDown {
      withLookThrough {
        val a = attr("a")
        val b = attr("b")
        val a1 = attr("a")
        val b1 = attr("b")
        val union = UnionExec(Seq(
          TestPartitioningPlan(outputPartitioning = hash(a, 2), output = Seq(a, b)),
          TestPartitioningPlan(outputPartitioning = hash(a1, 2), output = Seq(a1, b1))))
        val wrappers: Seq[SparkPlan => SparkPlan] = Seq(
          child => ProjectExec(Seq(a, b), child),
          child => FilterExec(IsNotNull(a), child),
          child => SortExec(Seq(SortOrder(a, Ascending)), global = false, child),
          child => ProjectExec(Seq(a, b), FilterExec(IsNotNull(a), child)))
        wrappers.foreach { wrap =>
          val child = wrap(union)
          val shuffle = ensureShuffle(hash(b, 2), child)
          org.apache.spark.sql.execution.exchange.PartitioningOrigin.record(
            shuffle, hash(b, 2), REPARTITION_BY_COL)
          val parent = TestPartitioningPlan(children = Seq(shuffle),
            required = Seq(clustered(b)), output = Seq(a, b))
          assert(org.apache.spark.sql.execution.exchange.PartitioningOrigin.validateNode(child))
          assert(pushDown(parent).fastEquals(parent))
          assert(org.apache.spark.sql.execution.exchange.PartitioningOrigin.validateNode(child),
            s"a rejected candidate must not change ${child.nodeName}'s origin")
          val accepted = TestPartitioningPlan(
            children = Seq(ensureShuffle(hash(a, 2), child)),
            required = Seq(clustered(a)), output = Seq(a, b))
          assert(numShufflesOverUnion(pushDown(accepted)) === 0)
        }
      }
    }
  }

  test("SPARK-60379: prefer the layout that keeps the most bytes") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withTempView("t1", "t2", "t3") {
        // Two children share HashPartitioning(id); the third uses HashPartitioning(id, grp) and
        // holds most of the data, so its layout saves more shuffle bytes despite fewer offers.
        Seq((1, "a", 10)).toDF("id", "grp", "v").createOrReplaceTempView("t1")
        Seq((2, "b", 5)).toDF("id", "grp", "v").createOrReplaceTempView("t2")
        Seq((1, "a", 40), (2, "b", 8), (3, "c", 7), (4, "d", 6), (5, "e", 9))
          .toDF("id", "grp", "v").createOrReplaceTempView("t3")

        val sql =
          """
            |SELECT id, grp, v, row_number() OVER (PARTITION BY id, grp ORDER BY v) AS rn
            |FROM (
            |  SELECT id, grp, v FROM (SELECT id, grp, v FROM t1 DISTRIBUTE BY id)
            |  UNION ALL
            |  SELECT id, grp, v FROM (SELECT id, grp, v FROM t2 DISTRIBUTE BY id)
            |  UNION ALL
            |  SELECT id, grp, v FROM (SELECT id, grp, v FROM t3 DISTRIBUTE BY id, grp)
            |)
            |""".stripMargin
        val df = spark.sql(sql)
        checkAnswer(df, expectedResult(sql))
        assert(numShufflesOnUnion(aqeInitialPlan(df)) === 0,
          s"shuffle should be pushed below the union:\n${df.queryExecution.executedPlan}")

        val union = aqeInitialPlan(df).collect { case u: UnionExec => u }.head
        assert(union.outputPartitioning.isInstanceOf[HashPartitioning],
          s"the union should propagate a hash partitioning after the push-down:\n" +
            df.queryExecution.executedPlan)
        assert(union.outputPartitioning.asInstanceOf[HashPartitioning].expressions.length === 2,
          s"the target should keep the largest child's two-column layout:\n" +
            df.queryExecution.executedPlan)
      }
    }
  }

  test("SPARK-60379: rank a layout more children report above a finer one") {
    withPushDown {
      val a = attr("a")
      val a1 = attr("a")
      val a2 = attr("a")
      // hash(_, 2) is offered by two children, hash(_, 8) by one. Shuffling one child instead
      // of two comes first without size estimates, although the finer layout raises parallelism.
      val children = Seq(leaf(a, hash(a, 2)), leaf(a1, hash(a1, 2)), leaf(a2, hash(a2, 8)))
      val parent = TestPartitioningPlan(
        children = Seq(ensureShuffle(hash(a, 2), UnionExec(children))),
        required = Seq(clustered(a)),
        output = Seq(a))

      val union = pushDown(parent).children.head match {
        case u: UnionExec => u
        case other => fail(s"the shuffle should be pushed below the union:\n$other")
      }
      assert(union.outputPartitioning === hash(a, 2), s"unexpected target:\n$union")
      assert(union.children.head.fastEquals(children.head))
      assert(union.children(1).fastEquals(children(1)))
      assertReshuffled(union.children(2), hash(a2, 2), children(2))
    }
  }

  Seq(false, true).foreach { lookThrough =>
    test(s"SPARK-60379: cross a partial aggregate above a union with lookThrough=$lookThrough") {
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
          SQLConf.SHUFFLE_PARTITIONS.key -> "4",
          SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_LOOK_THROUGH_OPERATORS_ENABLED.key ->
            lookThrough.toString) {
        withTempView("t1", "t2") {
          createTables()
          // The final aggregate owns the exchange, so the union sits under the partial aggregate.
          val query =
            """SELECT id, sum(v) AS v FROM (
              |SELECT /*+ REPARTITION(4, id) */ id, v FROM t1
              |UNION ALL SELECT id, v FROM t2) GROUP BY id""".stripMargin
          val df = spark.sql(query)
          checkAnswer(df, expectedResult(query))

          val initial = aqeInitialPlan(df)
          assert(numShufflesOverUnion(initial) === (if (lookThrough) 0 else 1),
            s"unexpected shuffle over the union:\n$initial")
          assert(numShufflesOnUnion(initial) === 0, s"the union itself is not shuffled:\n$initial")
          assert(initial.outputPartitioning.numPartitions === 4, s"unexpected count:\n$initial")
        }
      }
    }
  }

  test("SPARK-60379: cross an operator between the shuffle and the union only when " +
      "look-through is enabled") {
    withPushDown {
      val a = attr("id")
      val a1 = attr("id")
      val filterUnion = UnionExec(Seq(leaf(a, hash(a, 2)), leaf(a1)))
      val filtered = TestPartitioningPlan(
        children = Seq(ensureShuffle(hash(a, 2), FilterExec(IsNotNull(a), filterUnion))),
        required = Seq(clustered(a)),
        output = Seq(a))

      // A filter drops rows on its way, so the child this rule shuffles is shuffled before the
      // reduction. That is the trade the conf weighs, not a question of correctness.
      assert(pushDown(filtered).fastEquals(filtered), "a filter must not be crossed by default")
      withLookThrough {
        val newPlan = pushDown(filtered)
        assert(numShufflesOverUnion(newPlan) === 0, s"the filter should be crossed:\n$newPlan")
        assert(newPlan.children.head.isInstanceOf[FilterExec],
          s"the filter should be rebuilt above the union:\n$newPlan")
        assert(ValidateRequirements.validate(newPlan), s"invalid plan:\n$newPlan")
      }

      // A partial window group limit stands on the local sort its own ordering requirement asks
      // for, so this crosses a chain two operators deep.
      val v = attr("v")
      val limitUnion = UnionExec(Seq(
        TestPartitioningPlan(outputPartitioning = hash(a, 2), output = Seq(a, v)),
        TestPartitioningPlan(output = Seq(attr("id"), attr("v")))))
      val limit = WindowGroupLimitExec(
        Seq(a), Seq(SortOrder(v, Ascending)), RowNumber(), 1, PartialWindowGroupLimit,
        SortExec(Seq(SortOrder(a, Ascending), SortOrder(v, Ascending)), global = false, limitUnion))
      val limited = TestPartitioningPlan(
        children = Seq(ensureShuffle(hash(a, 2), limit)),
        required = Seq(clustered(a)),
        output = Seq(a, v))
      assert(ValidateRequirements.validate(limited), s"invalid fixture:\n$limited")

      assert(pushDown(limited).fastEquals(limited),
        "a partial window group limit must not be crossed by default")
      withLookThrough {
        val newPlan = pushDown(limited)
        assert(numShufflesOverUnion(newPlan) === 0,
          s"the partial window group limit should be crossed:\n$newPlan")
        assert(newPlan.children.head.isInstanceOf[WindowGroupLimitExec],
          s"the limit should be rebuilt above the union:\n$newPlan")
        assert(ValidateRequirements.validate(newPlan), s"invalid plan:\n$newPlan")
      }
    }
  }

  test("SPARK-60379: push down through a project between the exchange and union") {
    withPushDown {
      withLookThrough {
        val a = attr("id")
        val a1 = attr("id")
        val union = UnionExec(Seq(leaf(a, hash(a, 2)), leaf(a1)))
        // The project aliases the partition key, so the layout the union reports only answers the
        // requirement after it has been projected through.
        val alias = Alias(a, "k")()
        val project = ProjectExec(Seq(alias), union)
        val k = alias.toAttribute
        val parent = TestPartitioningPlan(
          children = Seq(ensureShuffle(hash(k, 2), project)),
          required = Seq(clustered(k)),
          output = Seq(k))

        val newPlan = pushDown(parent)
        val newProject = newPlan.children.head
        assert(newProject.isInstanceOf[ProjectExec], s"the project should be kept:\n$newPlan")
        assert(newProject.outputPartitioning === hash(k, 2),
          s"the project should report the target through its alias:\n$newPlan")
        val newUnion = newProject.children.head.asInstanceOf[UnionExec]
        assert(newUnion.children.head.fastEquals(union.children.head))
        assertReshuffled(newUnion.children(1), hash(a1, 2), union.children(1))
        assert(ValidateRequirements.validate(newPlan), s"invalid plan:\n$newPlan")
      }
    }
  }

  test("SPARK-60379: keep a sibling shuffle as a fixed layout constraint") {
    withPushDown {
      val a = attr("a")
      val a1 = attr("a")
      val b = attr("b")
      val a2 = attr("a")
      val union = UnionExec(Seq(leaf(a, hash(a, 8)), leaf(a1, hash(a1, 8)),
        leaf(a2, hash(a2, 16))))
      val other = leaf(b)
      // The most-offered layout passes the floor, but only 16 partitions pair with the sibling.
      val sibling = SortExec(
        Seq(SortOrder(b, Ascending)), global = false, ensureShuffle(hash(b, 16), other))
      val parent = TestPartitioningPlan(
        children = Seq(ensureShuffle(hash(a, 16), union), sibling),
        required = Seq(clustered(a), clustered(b)),
        output = Seq(a, b))

      val newPlan = pushDown(parent)
      assert(newPlan.children.head.outputPartitioning.numPartitions === 16)
      assert(newPlan.children.head.isInstanceOf[UnionExec])
      assert(newPlan.children(1).fastEquals(sibling))

      val compatible = parent.withNewChildren(Seq(
        ensureShuffle(hash(a, 8), union),
        sibling.withNewChildren(Seq(ensureShuffle(hash(b, 8), other)))))
      val pushed = pushDown(compatible)
      val newUnion = pushed.children.head.asInstanceOf[UnionExec]
      assertReshuffled(newUnion.children(2), hash(a2, 8), union.children(2))
      assert(pushed.children(1).fastEquals(compatible.children(1)),
        s"the sibling shuffle and sort must remain unchanged:\n$pushed")
      assert(ValidateRequirements.validate(pushed), s"invalid plan:\n$pushed")
    }
  }

  test("SPARK-60379: a fixed sibling rules out an incompatible most-offered layout") {
    withPushDown {
      val a = attr("a")
      val a1 = attr("a")
      val a2 = attr("a")
      val b = attr("b")
      // Both candidates pass the floor; only the sibling-compatible count may be selected.
      val children = Seq(leaf(a, hash(a, 8)), leaf(a1, hash(a1, 8)), leaf(a2, hash(a2, 16)))

      def run(fixedPartitions: Int): UnionExec = {
        val parent = TestPartitioningPlan(
          children = Seq(
            ensureShuffle(hash(a, fixedPartitions), UnionExec(children)),
            leaf(b, hash(b, fixedPartitions))),
          required = Seq(clustered(a), clustered(b)),
          output = Seq(a, b))
        val newPlan = pushDown(parent)
        assert(ValidateRequirements.validate(newPlan), s"invalid plan:\n$newPlan")
        assert(newPlan.children(1).fastEquals(parent.children(1)),
          s"the fixed sibling must not change:\n$newPlan")
        newPlan.children.head match {
          case u: UnionExec => u
          case other => fail(s"the shuffle should be pushed below the union:\n$other")
        }
      }

      // Compatible with the fixed side: the most-offered layout wins.
      val most = run(fixedPartitions = 8)
      assert(most.children.head.fastEquals(children.head))
      assert(most.children(1).fastEquals(children(1)))
      assertReshuffled(most.children(2), hash(a2, 8), children(2))

      // Incompatible with the fixed side: fall back to the layout that pairs with it.
      val paired = run(fixedPartitions = 16)
      assertReshuffled(paired.children.head, hash(a, 16), children.head)
      assertReshuffled(paired.children(1), hash(a1, 16), children(1))
      assert(paired.children(2).fastEquals(children(2)))
    }
  }

  test("SPARK-60379: pick a lower-ranked layout when the most offered one does not " +
      "answer the requirement") {
    withPushDown {
      val a = attr("a")
      val b = attr("b")
      // Two children report hash(b) and one reports hash(a), so hash(b) is the most offered, but
      // only hash(a) answers what the child owes.
      def childOn(key: Int): TestPartitioningPlan = {
        val output = Seq(attr("a"), attr("b"))
        TestPartitioningPlan(outputPartitioning = hash(output(key), 2), output = output)
      }
      val unionChildren = Seq(
        TestPartitioningPlan(outputPartitioning = hash(b, 2), output = Seq(a, b)),
        childOn(1),
        childOn(0))
      val union = UnionExec(unionChildren)
      val parent = TestPartitioningPlan(
        children = Seq(ensureShuffle(hash(a, 2), union)),
        required = Seq(clustered(a)),
        output = Seq(a, b))

      val newUnion = pushDown(parent).children.head match {
        case u: UnionExec => u
        case other => fail(s"the shuffle should be pushed below the union:\n$other")
      }
      assert(newUnion.outputPartitioning === hash(a, 2),
        s"the union should be rebuilt on the layout that answers the requirement:\n$newUnion")
      assert(newUnion.children(2).fastEquals(unionChildren(2)), s"hash(a) is kept:\n$newUnion")
      assert(newUnion.children.take(2).forall(_.isInstanceOf[ShuffleExchangeExec]),
        s"the hash(b) children are re-shuffled:\n$newUnion")
    }
  }

  test("SPARK-60379: pick a lower-ranked layout when the most offered one does not pair " +
      "with a sibling") {
    withSQLConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
      withPushDown {
      val a = attr("a")
      val c = attr("c")
      val x = attr("x")
      val y = attr("y")
      // Two children report hash(a) and one reports hash(c). Both answer the child's own
      // distribution, but only hash(c) takes the same position in it that the sibling's layout
      // takes in its own, which is what pairs the two sides.
      def childOn(key: Int): TestPartitioningPlan = {
        val output = Seq(attr("a"), attr("c"))
        TestPartitioningPlan(outputPartitioning = hash(output(key), 2), output = output)
      }
      val unionChildren = Seq(
        TestPartitioningPlan(outputPartitioning = hash(a, 2), output = Seq(a, c)),
        childOn(0),
        childOn(1))
      val union = UnionExec(unionChildren)
      val sibling = TestPartitioningPlan(outputPartitioning = hash(y, 2), output = Seq(x, y))
      val parent = TestPartitioningPlan(
        children = Seq(ensureShuffle(hash(a, 2), union), sibling),
        required = Seq(ClusteredDistribution(Seq(a, c)), ClusteredDistribution(Seq(x, y))),
        output = Seq(a, c, x, y))

      val newPlan = pushDown(parent)
      val newUnion = newPlan.children.head match {
        case u: UnionExec => u
        case other => fail(s"the shuffle should be pushed below the union:\n$other")
      }
      assert(newUnion.outputPartitioning === hash(c, 2),
        s"the union should be rebuilt on the layout that pairs with the sibling:\n$newPlan")
      assert(newUnion.children(2).fastEquals(unionChildren(2)), s"hash(c) is kept:\n$newPlan")
      assert(newUnion.children.take(2).forall(_.isInstanceOf[ShuffleExchangeExec]),
        s"the hash(a) children are re-shuffled:\n$newPlan")
      assert(newPlan.children(1).fastEquals(sibling), s"the sibling must not change:\n$newPlan")
      assert(ValidateRequirements.validate(newPlan), s"invalid plan:\n$newPlan")
      }
    }
  }

  test("SPARK-60379: cross a partial window group limit above a union") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "4",
        SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_LOOK_THROUGH_OPERATORS_ENABLED.key -> "true") {
      withTempView("t1", "t2") {
        Seq((1, 10), (1, 20), (2, 5)).toDF("id", "v").createOrReplaceTempView("t1")
        Seq((1, 30), (3, 7)).toDF("id", "v").createOrReplaceTempView("t2")
        // Top-k plans a partial window group limit, and the local sort it needs, between the
        // exchange the final one gets and the union.
        val sql =
          """SELECT id, v FROM (
            |  SELECT id, v, row_number() OVER (PARTITION BY id ORDER BY v) AS rn
            |  FROM (
            |    SELECT id, v FROM (SELECT id, v FROM t1 DISTRIBUTE BY id)
            |    UNION ALL
            |    SELECT id, v FROM t2)
            |) WHERE rn <= 1""".stripMargin
        val df = spark.sql(sql)
        checkAnswer(df, Seq(Row(1, 10), Row(2, 5), Row(3, 7)))

        val plan = aqeInitialPlan(df)
        assert(numShufflesOverUnion(plan) === 0,
          s"the union exchange should be pushed down:\n$plan")
        val union = plan.collectFirst { case u: UnionExec => u }.get
        assert(union.children.head.isInstanceOf[ShuffleExchangeExec],
          s"the laid-out branch keeps its own shuffle:\n$plan")
        assert(union.children(1).isInstanceOf[ShuffleExchangeExec],
          s"the scan branch takes one:\n$plan")
      }
    }
  }

  test("SPARK-60379: push down nested sites under an ancestor with default requirements") {
    withPushDown {
      val i0 = attr("id")
      val i1 = attr("id")
      val o1 = attr("id")
      val innerUnion = UnionExec(Seq(leaf(i0, hash(i0, 2)), leaf(i1)))
      val innerParent = TestPartitioningPlan(
        children = Seq(ensureShuffle(hash(i0, 2), innerUnion)),
        outputPartitioning = hash(i0, 2),
        required = Seq(clustered(i0)),
        output = Seq(i0))
      val outerUnion = UnionExec(Seq(innerParent, leaf(o1)))
      val outerParent = TestPartitioningPlan(
        children = Seq(ensureShuffle(hash(i0, 2), outerUnion)),
        required = Seq(clustered(i0)),
        output = Seq(i0))
      val root = TestPartitioningPlan(children = Seq(outerParent), output = Seq(i0))

      val newPlan = pushDown(root)
      assert(numShufflesOnUnion(newPlan) === 0, s"both sites should be pushed down:\n$newPlan")
      val newOuter = newPlan.children.head.children.head.asInstanceOf[UnionExec]
      assertReshuffled(newOuter.children(1), hash(o1, 2), outerUnion.children(1))
      val newInner = newOuter.children.head.children.head.asInstanceOf[UnionExec]
      assert(newInner.children.head.fastEquals(innerUnion.children.head))
      assertReshuffled(newInner.children(1), hash(i1, 2), innerUnion.children(1))
      assert(ValidateRequirements.validate(newPlan), s"invalid plan:\n$newPlan")
    }
  }

  test("SPARK-60379: roll back when a higher operator's pairing breaks") {
    withPushDown {
      val a = attr("a")
      val b = attr("b")
      val x = attr("x")
      val y = attr("y")

      def plan(targetKey: Attribute): SparkPlan = {
        val kept = TestPartitioningPlan(
          outputPartitioning = hash(targetKey, 2), output = Seq(a, b))
        val plain = TestPartitioningPlan(output = Seq(attr("a"), attr("b")))
        val union = UnionExec(Seq(kept, plain))
        // Either target answers the site's requirement, but only `a` pairs with the ancestor.
        val site = PassThroughPlan(
          ensureShuffle(hash(a, 2), union), ClusteredDistribution(Seq(a, b)))
        val sibling = TestPartitioningPlan(
          outputPartitioning = hash(x, 2), output = Seq(x, y))
        TestPartitioningPlan(
          children = Seq(site, sibling),
          required = Seq(ClusteredDistribution(Seq(a, b)), ClusteredDistribution(Seq(x, y))),
          output = Seq(a, b, x, y))
      }

      val pushed = pushDown(plan(a))
      assert(numShufflesOnUnion(pushed) === 0, s"the push-down should be kept:\n$pushed")

      // Both layouts have two partitions, but `b` and `x` occupy different cluster-key positions.
      val incompatible = plan(b)
      val rolledBack = pushDown(incompatible)
      assert(rolledBack.fastEquals(incompatible),
        s"the push-down should be rolled back:\n$rolledBack")
    }
  }

  test("SPARK-60379: roll back when the root required distribution is no longer satisfied") {
    withPushDown {
      val a = attr("a")
      val a1 = attr("a")
      val union = UnionExec(Seq(leaf(a, hash(a, 2)), leaf(a1)))
      val root = PassThroughPlan(ensureShuffle(hash(a, 2), union), clustered(a))

      assert(numShufflesOnUnion(pushDown(root)) === 0, "pushed down without a root requirement")
      assert(numShufflesOnUnion(pushDown(root, Some(clustered(a)))) === 0,
        "pushed down when the root requirement still holds")

      val required = ClusteredDistribution(Seq(a), requiredNumPartitions = Some(3))
      val rolledBack = pushDown(root, Some(required))
      assert(rolledBack.fastEquals(root), s"the push-down should be rolled back:\n$rolledBack")
    }
  }

  test("SPARK-60379: do not push down through a union stamped as a plain concatenation") {
    withPushDown {
      val a = attr("id")
      val a1 = attr("id")
      val children = Seq(leaf(a, hash(a, 2)), leaf(a1))

      def parentOf(union: UnionExec): SparkPlan = TestPartitioningPlan(
        children = Seq(ensureShuffle(hash(a, 2), union)),
        required = Seq(clustered(a)),
        output = Seq(a))

      // Control: the same plan without a stamp is pushed down.
      assert(numShufflesOnUnion(pushDown(parentOf(UnionExec(children)))) === 0)

      val stamped = UnionExec(children)
      new StampUnionDecisions(UnionConfSnapshot(spark.sessionState.conf)).apply(stamped)
      assert(stamped.outputPartitioning.isInstanceOf[UnknownPartitioning])
      val parent = parentOf(stamped)
      val newPlan = pushDown(parent)
      assert(newPlan.fastEquals(parent),
        s"a union stamped plain must not take a new layout:\n$newPlan")
    }
  }

  test("SPARK-60379: do not push down through non-deterministic or partitioning-dependent " +
      "operators") {
    withPushDown {
      withLookThrough {
        val a = attr("id")
        val a1 = attr("id")
        type Wrap = SparkPlan => SparkPlan

        def pushed(between: Wrap, above: Wrap): Boolean = {
          val union = UnionExec(Seq(leaf(a, hash(a, 2)), leaf(a1)))
          val parent = TestPartitioningPlan(
            children = Seq(above(ensureShuffle(hash(a, 2), between(union)))),
            required = Seq(clustered(a)),
            output = Seq(a))
          !pushDown(parent).fastEquals(parent)
        }
        val none: Wrap = p => p
        val order = Seq(SortOrder(a, Ascending))

        // Controls: the operators this rule does cross, so the barriers below are turned away on
        // their own account rather than because nothing is crossed at all.
        assert(pushed((p: SparkPlan) => ProjectExec(Seq(a), p), none))
        assert(pushed((p: SparkPlan) => SortExec(order, global = false, p), none))
        assert(pushed((p: SparkPlan) => FilterExec(IsNotNull(a), p), none))
        assert(pushed(none, (p: SparkPlan) => SortExec(order, global = false, p)))

        val barriers: Seq[(String, Wrap, Wrap)] = Seq(
          ("global sort below", (p: SparkPlan) => SortExec(order, global = true, p), none),
          ("global sort above", none, (p: SparkPlan) => SortExec(order, global = true, p)),
          ("local limit below", (p: SparkPlan) => LocalLimitExec(1, p), none),
          ("local limit above", none, (p: SparkPlan) => LocalLimitExec(1, p)),
          ("final window group limit", (p: SparkPlan) => WindowGroupLimitExec(
            Seq(a), order, RowNumber(), 1, FinalWindowGroupLimit, p), none),
          ("non-deterministic project",
            (p: SparkPlan) => ProjectExec(Seq(a, Alias(Rand(Literal(0L)), "r")()), p), none),
          ("non-deterministic filter",
            (p: SparkPlan) => FilterExec(GreaterThan(Rand(Literal(0L)), Literal(0.5)), p), none),
          ("operator the rule does not know",
            (p: SparkPlan) => TestPartitioningPlan(children = Seq(p), output = p.output), none))
        barriers.foreach { case (name, between, above) =>
          assert(!pushed(between, above), s"$name must stop the push-down")
        }
      }
    }
  }

  test("SPARK-60379: retain the keys of a root repartition") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      withTempView("t1", "t2") {
        createTables()
        // The branch is laid out on `id` alone, which answers the window's two-key distribution
        // but not the root repartition the window's own exchange let `EnsureRequirements` elide.
        val df = spark.table("t1").repartition(5, $"id")
          .union(spark.table("t2"))
          .withColumn("rn", org.apache.spark.sql.functions.row_number().over(
            org.apache.spark.sql.expressions.Window.partitionBy("id", "v").orderBy("id")))
          .repartition(5, $"id", $"v").select("id", "v", "rn")
        checkAnswer(df, Seq(
          Row(1, 10, 1), Row(1, 20, 1), Row(1, 40, 1), Row(2, 5, 1), Row(2, 8, 1),
          Row(3, 7, 1)))

        val plan = aqeInitialPlan(df)
        assert(numShufflesOnUnion(plan) === 1,
          s"the root repartition's keys must be kept:\n$plan")
        assert(df.rdd.getNumPartitions === 5, s"unexpected root:\n$plan")
      }
    }
  }

  test("SPARK-60379: sort the rebuilt child back to the required ordering") {
    withPushDown {
      val a = attr("id")
      val a1 = attr("id")
      val ordering = Seq(SortOrder(a, Ascending))
      val parent = TestPartitioningPlan(
        children = Seq(ensureShuffle(hash(a, 2), UnionExec(Seq(leaf(a, hash(a, 2)), leaf(a1))))),
        required = Seq(clustered(a)),
        ordering = Seq(ordering),
        output = Seq(a))

      val newPlan = pushDown(parent)
      newPlan.children.head match {
        case sort: SortExec =>
          assert(!sort.global, s"the sort must stay local:\n$newPlan")
          assert(sort.sortOrder === ordering, s"unexpected ordering:\n$newPlan")
          assert(sort.child.isInstanceOf[UnionExec], s"the sort goes above the union:\n$newPlan")
        case other => fail(s"expected a local sort above the rebuilt union:\n$other")
      }
      assert(ValidateRequirements.validate(newPlan), s"invalid plan:\n$newPlan")
    }
  }

  test("SPARK-60379: do not lay out more children than the limit allows") {
    withPushDown {
      def planWith(moved: Int): SparkPlan = {
        val a = attr("id")
        val kept = leaf(a, hash(a, 2))
        val others = (0 until moved).map(_ => leaf(attr("id")))
        TestPartitioningPlan(
          children = Seq(ensureShuffle(hash(a, 2), UnionExec(kept +: others))),
          required = Seq(clustered(a)),
          output = Seq(a))
      }

      def pushed(moved: Int): Boolean = {
        val plan = planWith(moved)
        !pushDown(plan).fastEquals(plan)
      }

      // The limit is three by default: three children are laid out, a fourth is not.
      assert(pushed(3), "three moved children are within the limit")
      assert(!pushed(4), "four moved children are past the limit")

      withSQLConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_MAX_MOVED_BRANCHES.key -> "4") {
        assert(pushed(4), "a higher limit allows the fourth")
      }
      withSQLConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_MAX_MOVED_BRANCHES.key -> "0") {
        assert(!pushed(1), "a zero limit allows none")
      }
    }
  }

  test("SPARK-60379: a branch AQE cannot coalesce is not a target by default") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "200") {
      withTempView("t1", "t2", "t3") {
        Seq((1, 10), (2, 20)).toDF("id", "v").createOrReplaceTempView("t1")
        Seq((1, 30), (3, 7)).toDF("id", "v").createOrReplaceTempView("t2")
        Seq((1, 1), (3, 3)).toDF("id", "v2").createOrReplaceTempView("t3")
        // `REPARTITION(200, id)` shuffles on a fixed count, which coalescing cannot change, so
        // keeping that branch would cost the join's stages their coalescing.
        val sql =
          """SELECT u.id, u.v, r.v2 FROM (
            |  SELECT /*+ REPARTITION(200, id) */ id, v FROM t1
            |  UNION ALL
            |  SELECT id, v FROM t2
            |) u JOIN t3 r ON u.id = r.id""".stripMargin
        val expected = Seq(Row(1, 10, 1), Row(1, 30, 1), Row(3, 7, 3))

        Seq(
          "default" -> Map(
            SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_NON_COALESCIBLE_BRANCHES_ENABLED.key ->
              "false"),
          "allowed" -> Map(
            SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_NON_COALESCIBLE_BRANCHES_ENABLED.key ->
              "true"),
          "coalescing off" -> Map(
            SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false")).foreach { case (label, conf) =>
          withSQLConf(conf.toSeq: _*) {
            val df = spark.sql(sql)
            checkAnswer(df, expected)
            val pushedDown = label != "default"
            assert(numShufflesOnUnion(aqeInitialPlan(df)) === (if (pushedDown) 0 else 1),
              s"$label: unexpected union exchange:\n${aqeInitialPlan(df)}")
          }
        }
      }
    }
  }

  test("SPARK-60379: keep an exchange the plan reuses elsewhere") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "4") {
      withTempView("t1", "t2", "t3") {
        Seq((1, 10), (2, 20)).toDF("id", "v").createOrReplaceTempView("t1")
        Seq((1, 30), (3, 7)).toDF("id", "v").createOrReplaceTempView("t2")
        Seq((1, 1), (3, 3)).toDF("id", "v2").createOrReplaceTempView("t3")
        // EnsureRequirements builds two canonical twins; the rule keeps both exchanges and AQE
        // later reuses one stage. Moving them would execute the union's subtree per consumer.
        // The join branch is allowed as a target here, so only reuse blocks the rewrite.
        val sql =
          """WITH u AS (
            |  SELECT l.id AS id, l.v AS v FROM t1 l JOIN t3 r ON l.id = r.id
            |  UNION ALL
            |  SELECT id, v FROM t2)
            |SELECT * FROM u u1 JOIN u u2 ON u1.id = u2.id""".stripMargin
        val df = spark.sql(sql)
        checkAnswer(df, expectedResult(sql))

        val plan = aqeInitialPlan(df)
        assert(numShufflesOnUnion(plan) === 2,
          s"an exchange the plan reuses is not moved below the union:\n$plan")

        // With reuse off every exchange gets its own stage, so the rewrite goes ahead.
        withSQLConf(SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
          val noReuse = spark.sql(sql)
          checkAnswer(noReuse, expectedResult(sql))
          val noReusePlan = aqeInitialPlan(noReuse)
          assert(numShufflesOnUnion(noReusePlan) === 0,
            s"without exchange reuse there is no stage to protect:\n$noReusePlan")
        }
      }
    }
  }

  test("SPARK-60379: duplicated forms use pre-optimization stage keys and visible subqueries") {
    val leaf = spark.range(10).select($"id" as Symbol("k")).queryExecution.executedPlan
    val hp = HashPartitioning(leaf.output, 4)

    // A duplicate carried by a shuffle query stage, without materializing it.
    val staged = ShuffleExchangeExec(hp, leaf)
    val optimized = staged.copy(child = ProjectExec(leaf.output, leaf))
    val stage = ShuffleQueryStageExec(0, optimized, staged.canonicalized)
    val twin = ShuffleExchangeExec(hp, leaf)
    assert(AQEUtils.duplicatedShuffleForms(UnionExec(Seq(stage, twin))) ===
      Set(staged.canonicalized), "an exchange a stage holds is a duplicate")

    // A duplicate carried by a subquery plan.
    val outer = ShuffleExchangeExec(hp, leaf)
    val inner = ShuffleExchangeExec(hp, leaf)
    val subquery = ScalarSubquery(SubqueryExec("sub", inner), ExprId(0))
    val filter = FilterExec(GreaterThan(subquery, Literal(0)), leaf)
    assert(AQEUtils.duplicatedShuffleForms(UnionExec(Seq(outer, filter))) ===
      Set(outer.canonicalized), "an exchange a subquery plan holds is a duplicate")
  }

  test("SPARK-60379: a shuffle query stage with a supported origin may offer a target") {
    withPushDown {
      val a = attr("id")
      val staged = ensureShuffle(hash(a, 2), leaf(a))
      val stage = ShuffleQueryStageExec(0, staged, staged.canonicalized)
      val union = UnionExec(Seq(stage, leaf(attr("id"))))
      val parent = TestPartitioningPlan(
        children = Seq(ensureShuffle(hash(a, 2), union)),
        required = Seq(clustered(a)),
        output = Seq(a))

      val result = pushDown(parent)
      assert(numShufflesOnUnion(result) === 0,
        s"the stage reports a coalescible shuffle, so it keeps its layout:\n$result")
      val newUnion = result.children.head match {
        case u: UnionExec => u
        case other => fail(s"the shuffle should be pushed below the union:\n$other")
      }
      assert(newUnion.children.head.fastEquals(stage),
        s"the stage must not be shuffled:\n$result")
    }
  }

  test("SPARK-60379: all sources of a multi-input child must support coalescing") {
    withPushDown {
      withSQLConf(
          SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
          SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_NON_COALESCIBLE_BRANCHES_ENABLED.key ->
            "false") {
        val scan = spark.range(10).queryExecution.sparkPlan
        val a = scan.output.head
        val supported = ensureShuffle(hash(a, 2), scan)
        val fixed = ShuffleExchangeExec(hash(a, 2), scan, REPARTITION_BY_NUM)

        Seq(scan, fixed).foreach { unsupported =>
          Seq(Seq(supported, unsupported), Seq(unsupported, supported)).foreach { sources =>
            val child = TestPartitioningPlan(
              children = sources,
              outputPartitioning = hash(a, 2),
              output = Seq(a))
            val union = UnionExec(Seq(child, leaf(a.newInstance())))
            val parent = TestPartitioningPlan(
              children = Seq(ensureShuffle(hash(a, 2), union)),
              required = Seq(clustered(a)),
              output = Seq(a))

            assert(pushDown(parent).fastEquals(parent),
              s"an unsupported source must reject the target in either child order:\n$parent")
            withSQLConf(
                SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_NON_COALESCIBLE_BRANCHES_ENABLED.key ->
                  "true") {
              assert(numShufflesOnUnion(pushDown(parent)) === 0)
            }
          }
        }
      }
    }
  }

  test("SPARK-60379: source checks stop at supported shuffle boundaries") {
    withPushDown {
      withSQLConf(
          SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
          SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_NON_COALESCIBLE_BRANCHES_ENABLED.key ->
            "false") {
        val scan = spark.range(10).queryExecution.sparkPlan
        val a = scan.output.head
        val fixed = ShuffleExchangeExec(hash(a, 2), scan, REPARTITION_BY_NUM)
        Seq(scan, fixed).foreach { input =>
          val child = ensureShuffle(hash(a, 2), input)
          val union = UnionExec(Seq(child, leaf(a.newInstance())))
          val parent = TestPartitioningPlan(
            children = Seq(ensureShuffle(hash(a, 2), union)),
            required = Seq(clustered(a)),
            output = Seq(a))

          val result = pushDown(parent)
          assert(numShufflesOnUnion(result) === 0,
            s"sources below a supported shuffle must not reject its target:\n$result")
          assert(result.children.head.children.head.fastEquals(child))
        }
      }
    }
  }

  Seq("BROADCAST(b)", "BROADCAST(a)").foreach { hint =>
    test(s"SPARK-60379: broadcast build sources do not reject a target with $hint") {
      withSQLConf(
          SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_JOIN_BRANCHES_ENABLED.key -> "false",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
          SQLConf.SHUFFLE_PARTITIONS.key -> "4",
          SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_NON_COALESCIBLE_BRANCHES_ENABLED.key ->
            "false") {
        withTempView("t1", "t2", "t3") {
          Seq((1, 10), (1, 20), (2, 5)).toDF("id", "v").createOrReplaceTempView("t1")
          Seq(1, 2).toDF("id").createOrReplaceTempView("t2")
          Seq((1, 30), (3, 7)).toDF("id", "v").createOrReplaceTempView("t3")
          val inputs = if (hint == "BROADCAST(b)") {
            "(SELECT id, v FROM t1 DISTRIBUTE BY id) a JOIN t2 b ON a.id = b.id"
          } else {
            "t2 a JOIN (SELECT id, v FROM t1 DISTRIBUTE BY id) b ON a.id = b.id"
          }
          val columns = if (hint == "BROADCAST(b)") { "a.id, a.v" } else { "b.id, b.v" }
          val sql = s"""SELECT id, v, row_number() OVER (PARTITION BY id ORDER BY v) rn FROM (
            | SELECT /*+ $hint */ $columns FROM $inputs
            | UNION ALL SELECT id, v FROM t3)""".stripMargin
          Seq(false, true).foreach { enabled =>
            withSQLConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ENABLED.key -> enabled.toString) {
              val df = spark.sql(sql)
              checkAnswer(df, Seq(Row(1, 10, 1), Row(1, 20, 2), Row(1, 30, 3),
                Row(2, 5, 1), Row(3, 7, 1)))
              val initial = aqeInitialPlan(df)
              assert(initial.exists(_.isInstanceOf[BroadcastHashJoinExec]))
              assert(numShufflesOnUnion(initial) === (if (enabled) { 0 } else { 1 }),
                s"broadcast build sources must not reject the streamed layout:\n$initial")
            }
          }
        }
      }
    }
  }

  test("SPARK-60379: a cached child is not a target by default") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "4") {
      withTempView("t1", "t2", "t3", "cached") {
        Seq((1, 10), (2, 20)).toDF("id", "v").createOrReplaceTempView("t1")
        Seq((1, 30), (3, 7)).toDF("id", "v").createOrReplaceTempView("t2")
        Seq((1, 1), (3, 3)).toDF("id", "v2").createOrReplaceTempView("t3")
        // A cached relation is read through an in-memory scan, which reports the layout the cache
        // was built with: a leaf rather than a shuffle stage, so no group it feeds is coalesced.
        val cached = spark.table("t1").repartition(4, $"id").cache()
        try {
          cached.count()
          cached.createOrReplaceTempView("cached")
          val sql =
            """SELECT u.id, u.v, r.v2 FROM (
              |  SELECT id, v FROM cached
              |  UNION ALL
              |  SELECT id, v FROM t2
              |) u JOIN t3 r ON u.id = r.id""".stripMargin
          val expected = Seq(Row(1, 10, 1), Row(1, 30, 1), Row(3, 7, 3))

          Seq(
            "default" -> Map(
              SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_NON_COALESCIBLE_BRANCHES_ENABLED.key ->
                "false"),
            "allowed" -> Map(
              SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_NON_COALESCIBLE_BRANCHES_ENABLED.key ->
                "true")).foreach { case (label, conf) =>
            withSQLConf(conf.toSeq: _*) {
              val df = spark.sql(sql)
              checkAnswer(df, expected)
              val pushedDown = label != "default"
              assert(numShufflesOnUnion(aqeInitialPlan(df)) === (if (pushedDown) 0 else 1),
                s"$label: unexpected union exchange:\n${aqeInitialPlan(df)}")
            }
          }
        } finally {
          cached.unpersist()
        }
      }
    }
  }

  test("SPARK-60379: do not push down a shuffle not inserted by EnsureRequirements") {
    withPushDown {
      val a = attr("id")
      val a1 = attr("id")
      val union = UnionExec(Seq(leaf(a, hash(a, 2)), leaf(a1)))
      // A user-specified shuffle must stay in place even when it sits directly on the union.
      val shuffle = ShuffleExchangeExec(hash(a, 2), union, REPARTITION_BY_COL)
      val parent = TestPartitioningPlan(
        children = Seq(shuffle),
        required = Seq(clustered(a)),
        output = Seq(a))

      val newPlan = pushDown(parent)
      assert(newPlan.fastEquals(parent),
        s"a shuffle of origin ${REPARTITION_BY_COL} must not be pushed down:\n$newPlan")
    }
  }

  test("SPARK-60379: do not push down a shuffle whose partitioning is not hash partitioning") {
    withPushDown {
      val a = attr("id")
      val a1 = attr("id")
      val union = UnionExec(Seq(leaf(a, hash(a, 2)), leaf(a1)))
      val parent = TestPartitioningPlan(
        children = Seq(ensureShuffle(RoundRobinPartitioning(2), union)),
        required = Seq(clustered(a)),
        output = Seq(a))

      val newPlan = pushDown(parent)
      assert(newPlan.fastEquals(parent),
        s"a round-robin shuffle must not be pushed down:\n$newPlan")
    }
  }

  test("SPARK-60379: rebalance sources keep their boundary with push-down enabled") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "20",
        SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "800") {
      withTempView("t1", "t2") {
        spark.range(0, 1000, 1, 10).selectExpr("id % 2 id", "id v")
          .createOrReplaceTempView("t1")
        spark.range(20).selectExpr("id", "id v").createOrReplaceTempView("t2")
        val sql = """SELECT id, v, row_number() OVER (PARTITION BY id ORDER BY v) rn FROM (
          | SELECT /*+ REBALANCE(id) */ id, v FROM t1
          | UNION ALL SELECT id, v FROM t2)""".stripMargin
        val df = spark.sql(sql)
        checkAnswer(df, expectedResult(sql))
        assert(numShufflesOnUnion(aqeInitialPlan(df)) === 1)
      }
    }
  }

  Seq(false, true).foreach { force =>
    test(s"SPARK-60379: protected join branches preserve skew optimization with force=$force") {
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_JOIN_BRANCHES_ENABLED.key -> "false",
          SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN.key -> force.toString,
          SQLConf.SHUFFLE_PARTITIONS.key -> "20",
          SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "100",
          SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "100") {
        withTempView("t1", "t2", "t3") {
          spark.range(0, 300, 1, 10).selectExpr("id % 3 k", "id v")
            .createOrReplaceTempView("t1")
          spark.range(0, 30, 1, 10).selectExpr("id % 3 k", "id w")
            .createOrReplaceTempView("t2")
          spark.range(3).selectExpr("id k", "id v").createOrReplaceTempView("t3")
          val sql = """SELECT k, v, rank() OVER (PARTITION BY k ORDER BY v) rn FROM (
            | SELECT a.k, a.v FROM t1 a JOIN t2 b ON a.k = b.k
            | UNION ALL SELECT k, v FROM t3)""".stripMargin
          val df = spark.sql(sql)
          checkAnswer(df, expectedResult(sql))
          assert(numShufflesOnUnion(aqeInitialPlan(df)) === 1)
          def skewJoins(p: SparkPlan): Seq[SortMergeJoinExec] = p match {
            case stage: QueryStageExec => skewJoins(stage.plan)
            case other => other.collect { case j: SortMergeJoinExec if j.isSkewJoin => j } ++
              other.children.flatMap(skewJoins)
          }
          val finalPlan = df.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
            .executedPlan
          assert(skewJoins(finalPlan).nonEmpty,
            s"the protected inner join should split:\n$finalPlan")
        }
      }
    }
  }

  test("SPARK-60379: opted-in join branches restore a shuffle when skew optimization is forced") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_JOIN_BRANCHES_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN.key -> "true",
        SQLConf.SHUFFLE_PARTITIONS.key -> "20",
        SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "100",
        SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "100") {
      withTempView("t1", "t2", "t3") {
        spark.range(0, 300, 1, 10).selectExpr("id % 3 k", "id v")
          .createOrReplaceTempView("t1")
        spark.range(0, 30, 1, 10).selectExpr("id % 3 k", "id w")
          .createOrReplaceTempView("t2")
        spark.range(3).selectExpr("id k", "id v").createOrReplaceTempView("t3")
        val sql = """SELECT k, v, rank() OVER (PARTITION BY k ORDER BY v) rn FROM (
          | SELECT a.k, a.v FROM t1 a JOIN t2 b ON a.k = b.k
          | UNION ALL SELECT k, v FROM t3)""".stripMargin
        val df = spark.sql(sql)
        checkAnswer(df, expectedResult(sql))
        assert(numShufflesOnUnion(aqeInitialPlan(df)) === 0)
        def plans(p: SparkPlan): Seq[SparkPlan] = p match {
          case stage: QueryStageExec => stage +: plans(stage.plan)
          case other => other +: other.children.flatMap(plans)
        }
        val finalNodes = plans(df.queryExecution.executedPlan
          .asInstanceOf[AdaptiveSparkPlanExec].executedPlan)
        assert(finalNodes.exists {
          case join: SortMergeJoinExec => join.isSkewJoin
          case _ => false
        })
        assert(finalNodes.exists {
          case shuffle: ShuffleExchangeExec => isUnionPlan(shuffle.child)
          case _ => false
        }, "forced skew optimization must restore the union distribution")
      }
    }
  }

  test("SPARK-60379: join-input protection retains direct consumer skew splitting") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "20",
        SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "100",
        SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "100") {
      withTempView("agg_src", "skewed", "other") {
        spark.range(20).selectExpr("id % 3 k", "id v").createOrReplaceTempView("agg_src")
        spark.range(0, 300, 1, 10).selectExpr("id % 3 k", "id v")
          .createOrReplaceTempView("skewed")
        spark.range(0, 30, 1, 10).selectExpr("id % 3 k", "id w")
          .createOrReplaceTempView("other")
        val sql = """SELECT /*+ MERGE(u, o) */ u.k, u.v, o.w FROM (
          | SELECT k, sum(v) v FROM agg_src GROUP BY k
          | UNION ALL SELECT k, v FROM skewed) u JOIN other o ON u.k = o.k""".stripMargin
        val expected = expectedResult(sql)
        Seq(false, true).foreach { allow =>
          withSQLConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ALLOW_JOIN_INPUTS_ENABLED.key ->
              allow.toString) {
            val df = spark.sql(sql)
            checkAnswer(df, expected)
            assert(numShufflesOnUnion(aqeInitialPlan(df)) === (if (allow) { 0 } else { 1 }))
            def joins(p: SparkPlan): Seq[SortMergeJoinExec] = p match {
              case stage: QueryStageExec => joins(stage.plan)
              case join: SortMergeJoinExec => Seq(join)
              case other => other.children.flatMap(joins)
            }
            val finalJoins = joins(df.queryExecution.executedPlan
              .asInstanceOf[AdaptiveSparkPlanExec].executedPlan)
            assert(finalJoins.size === 1)
            assert(finalJoins.head.isSkewJoin === !allow)
          }
        }
      }
    }
  }

  test("SPARK-60379: cached plans respect the output partitioning policy") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      withTempView("t1", "t2") {
        createTables()
        Seq(false, true).foreach { canChange =>
          withSQLConf(
              SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING.key -> canChange.toString) {
            val cached = spark.sql(query).cache()
            try {
              cached.count()
              val relation = cached.queryExecution.withCachedData.collectFirst {
                case r: org.apache.spark.sql.execution.columnar.InMemoryRelation => r
              }.get
              val plan = relation.cacheBuilder.cachedPlan match {
                case a: AdaptiveSparkPlanExec => a.initialPlan
                case p => p
              }
              assert(numShufflesOnUnion(plan) === (if (canChange) { 0 } else { 1 }))
            } finally {
              cached.unpersist(blocking = true)
            }
          }
        }
      }
    }
  }

  test("SPARK-60379: union limits reject negative values") {
    Seq(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_MAX_UNION_CHILDREN.key,
      SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_MAX_MOVED_BRANCHES.key).foreach { key =>
      intercept[IllegalArgumentException] {
        withSQLConf(key -> "-1") {}
      }
    }
  }

  test("SPARK-60379: retarget an unshared branch exchange without stacking shuffles") {
    withPushDown {
      val a = attr("id")
      val b = attr("id")
      val c = attr("id")
      val moved = ShuffleExchangeExec(hash(c, 8), leaf(c), REPARTITION_BY_COL)
      val union = UnionExec(Seq(leaf(a, hash(a, 2)), leaf(b, hash(b, 2)), moved))
      val parent = TestPartitioningPlan(
        children = Seq(ensureShuffle(hash(a, 2), union)),
        required = Seq(clustered(a)), output = Seq(a))
      val result = pushDown(parent)
      val retargeted = result.children.head.children(2).asInstanceOf[ShuffleExchangeExec]
      assert(retargeted.outputPartitioning === hash(c, 2))
      assert(retargeted.shuffleOrigin === REPARTITION_BY_COL)
      assert(retargeted.child.fastEquals(moved.child))
      org.apache.spark.sql.execution.exchange.PartitioningOrigin.record(
        moved, hash(c, 8), REPARTITION_BY_COL)
      val protectedResult = pushDown(parent)
      val stacked = protectedResult.children.head.children(2).asInstanceOf[ShuffleExchangeExec]
      assert(stacked.child.fastEquals(moved))
      assert(ValidateRequirements.validate(protectedResult))
    }
  }

  test("SPARK-60379: column repartition retargeting preserves keys and their order") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      withTempView("t1", "t2") {
        spark.range(100).selectExpr("id % 7 a", "id % 3 b", "id v")
          .createOrReplaceTempView("t1")
        spark.range(100, 120).selectExpr("id % 7 a", "id % 3 b", "id v")
          .createOrReplaceTempView("t2")
        Seq("a, b", "b, a", "a", "b").foreach { keys =>
          val sql = s"""SELECT a, b, v,
            | row_number() OVER (PARTITION BY a, b ORDER BY v) rn FROM (
            | SELECT /*+ REPARTITION(8, a, b) */ * FROM t1
            | UNION ALL
            | SELECT * FROM (SELECT * FROM t2 DISTRIBUTE BY $keys))""".stripMargin
          val df = spark.sql(sql)
          checkAnswer(df, expectedResult(sql))
          val plan = aqeInitialPlan(df)
          assert(numShufflesOnUnion(plan) === 0, s"expected push-down:\n$plan")
          val union = plan.collectFirst { case u: UnionExec => u }.get
          val kept = union.children.head.asInstanceOf[ShuffleExchangeExec]
          assert(kept.shuffleOrigin === REPARTITION_BY_NUM)
          assert(kept.outputPartitioning.numPartitions === 8)
          val moved = union.children(1).asInstanceOf[ShuffleExchangeExec]
          val expected = HashPartitioning(Seq(moved.output.head, moved.output(1)), 8)
          assert(moved.outputPartitioning.asInstanceOf[HashPartitioning].semanticEquals(expected))
          if (keys == "a, b") {
            assert(moved.shuffleOrigin === REPARTITION_BY_COL)
            assert(!moved.child.isInstanceOf[ShuffleExchangeExec])
          } else {
            assert(moved.shuffleOrigin === ENSURE_REQUIREMENTS)
            val original = moved.child.asInstanceOf[ShuffleExchangeExec]
            assert(original.shuffleOrigin === REPARTITION_BY_COL)
            val expressions = keys.split(", ").map {
              case "a" => original.output.head
              case "b" => original.output(1)
            }.toSeq
            val originalLayout = HashPartitioning(expressions, 5)
            assert(original.outputPartitioning.asInstanceOf[HashPartitioning]
              .semanticEquals(originalLayout))
          }
          assert(AQEUtils.isPushedThroughUnion(union))
        }
      }
    }
  }

  test("SPARK-60379: reversed DISTRIBUTE BY keys retain the user exchange") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      withTempView("t1", "t2") {
        spark.range(100).selectExpr("id % 7 a", "id % 3 b", "id v")
          .createOrReplaceTempView("t1")
        spark.range(100, 120).selectExpr("id % 7 a", "id % 3 b", "id v")
          .createOrReplaceTempView("t2")
        val sql = """SELECT a, b, v,
          | row_number() OVER (PARTITION BY a, b ORDER BY v) rn FROM (
          | SELECT * FROM (SELECT * FROM t1 DISTRIBUTE BY a, b)
          | UNION ALL
          | SELECT * FROM (SELECT * FROM t2 DISTRIBUTE BY b, a))""".stripMargin
        val df = spark.sql(sql)
        checkAnswer(df, expectedResult(sql))
        val plan = aqeInitialPlan(df)
        assert(numShufflesOnUnion(plan) === 0, s"expected push-down:\n$plan")
        val union = plan.collectFirst { case u: UnionExec => u }.get
        val moved = union.children(1).asInstanceOf[ShuffleExchangeExec]
        assert(moved.shuffleOrigin === ENSURE_REQUIREMENTS)
        val original = moved.child.asInstanceOf[ShuffleExchangeExec]
        assert(original.shuffleOrigin === REPARTITION_BY_COL)
        val expected = HashPartitioning(Seq(original.output(1), original.output.head), 5)
        assert(original.outputPartitioning.asInstanceOf[HashPartitioning].semanticEquals(expected))
      }
    }
  }

  test("SPARK-60379: retargeting stops at projections, stages and fixed-count exchanges") {
    withPushDown {
      val a = attr("id")
      val b = attr("id")
      val c = attr("id")
      val exchange = ShuffleExchangeExec(hash(c, 8), leaf(c), REPARTITION_BY_COL)
      val stage = ShuffleQueryStageExec(0, exchange, exchange.canonicalized)
      val projected = ProjectExec(Seq(Alias(c, "id")()), exchange)
      val fixed = exchange.copy(shuffleOrigin = REPARTITION_BY_NUM)
      Seq(projected, stage, fixed).foreach { moved =>
        val union = UnionExec(Seq(leaf(a, hash(a, 2)), leaf(b, hash(b, 2)), moved))
        val parent = TestPartitioningPlan(
          children = Seq(ensureShuffle(hash(a, 2), union)),
          required = Seq(clustered(a)), output = Seq(a))
        val result = pushDown(parent)
        val outer = result.children.head.children(2).asInstanceOf[ShuffleExchangeExec]
        assert(outer.shuffleOrigin === ENSURE_REQUIREMENTS)
        assert(outer.child.fastEquals(moved))
      }
      val union = UnionExec(Seq(leaf(a, hash(a, 2)), leaf(b, hash(b, 2)), exchange))
      val parent = TestPartitioningPlan(children = Seq(ensureShuffle(hash(a, 2), union)),
        required = Seq(clustered(a)), output = Seq(a))
      val root = TestPartitioningPlan(children = Seq(parent, exchange.copy()))
      val result = pushDown(root)
      val outer = result.children.head.children.head.children(2).asInstanceOf[ShuffleExchangeExec]
      assert(outer.child.fastEquals(exchange), "a shared branch exchange must not be retargeted")
    }
  }

  test("SPARK-60379: origin validation distinguishes column and fixed-count repartitions") {
    def validateOrigin(plan: SparkPlan): Boolean = {
      org.apache.spark.sql.execution.exchange.PartitioningOrigin.validateNode(plan)
    }
    val a = attr("id")
    val original = hash(a, 4)
    val coalesced = CoalescedHashPartitioning(
      original, Seq(CoalescedBoundary(0, 2),
        CoalescedBoundary(2, 4)))
    Seq(REPARTITION_BY_COL, REPARTITION_BY_NUM, ENSURE_REQUIREMENTS).foreach { origin =>
      val plan = leaf(a, coalesced)
      org.apache.spark.sql.execution.exchange.PartitioningOrigin.record(plan, original, origin)
      assert(validateOrigin(plan) === (origin != REPARTITION_BY_NUM))
      val changed = leaf(a, hash(a, 8))
      org.apache.spark.sql.execution.exchange.PartitioningOrigin.record(changed, original, origin)
      assert(!validateOrigin(changed))
      assert(ValidateRequirements.validate(changed),
        "general validation must not enforce the push-down's recorded origin")
      val same = leaf(a, original)
      org.apache.spark.sql.execution.exchange.PartitioningOrigin.record(same, original, origin)
      assert(validateOrigin(same))
      val differentReducers = leaf(a, coalesced.copy(from = hash(a, 8)))
      org.apache.spark.sql.execution.exchange.PartitioningOrigin.record(
        differentReducers, original, origin)
      assert(validateOrigin(differentReducers) === (origin != REPARTITION_BY_NUM))
      val wrongKey = leaf(a, coalesced.copy(from = hash(attr("other"), 8)))
      org.apache.spark.sql.execution.exchange.PartitioningOrigin.record(wrongKey, original, origin)
      assert(!validateOrigin(wrongKey))
      val fixedCount = leaf(a,
        CoalescedHashPartitioning(
          hash(a, 8), (0 until 4).map { index =>
            CoalescedBoundary(index * 2, index * 2 + 2)
          }))
      org.apache.spark.sql.execution.exchange.PartitioningOrigin.record(
        fixedCount, original, origin)
      assert(validateOrigin(fixedCount))
      val b = attr("other")
      val twoKeys = HashPartitioning(Seq(a, b), 4)
      Seq(HashPartitioning(Seq(b, a), 8), hash(a, 8)).foreach { from =>
        val invalid = leaf(a, coalesced.copy(from = from))
        org.apache.spark.sql.execution.exchange.PartitioningOrigin.record(invalid, twoKeys, origin)
        assert(!validateOrigin(invalid))
      }
    }
  }

  test("SPARK-60379: elision contracts survive rebuilding a tagged node") {
    withPushDown {
      val a = attr("id")
      val child = PassThroughPlan(
        ensureShuffle(hash(a, 2), UnionExec(Seq(leaf(a, hash(a, 8)), leaf(attr("id"))))),
        clustered(a))
      val user = ShuffleExchangeExec(hash(a, 2), child, REPARTITION_BY_COL)
      val prepared = EnsureRequirements()(user)
      assert(prepared.fastEquals(child))
      assert(ValidateRequirements.validate(prepared))
      assert(pushDown(prepared).fastEquals(prepared))
      val id = attr("id")
      val unprotected = PassThroughPlan(ensureShuffle(hash(id, 2),
        UnionExec(Seq(leaf(id, hash(id, 8)), leaf(attr("id"))))), clustered(id))
      assert(numShufflesOnUnion(pushDown(unprotected)) === 0)
    }
  }

  test("SPARK-60379: preserve a user origin carried by the removed shuffle end to end") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      val previousStrategies = spark.experimental.extraStrategies
      // Force the carrier shape: a user exchange above an equal requirement exchange. SQL
      // normally plans only the user exchange here, so the strategy pins elision onto the carrier.
      spark.experimental.extraStrategies = Seq(new org.apache.spark.sql.execution.SparkStrategy {
        override def apply(
            plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Seq[SparkPlan] = {
          plan match {
            case repartition: org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression
                if repartition.optNumPartitions.contains(5) =>
              val required = ensureShuffle(repartition.partitioning, planLater(repartition.child))
              Seq(ShuffleExchangeExec(repartition.partitioning, required, REPARTITION_BY_NUM))
            case _ => Nil
          }
        }
      }) ++ previousStrategies
      try {
        val expected = (0L until 40L).map(i => (i % 7, i, 1))
        for (userRepartition <- Seq(false, true); enabled <- Seq(false, true)) {
          withSQLConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ENABLED.key -> enabled.toString) {
            val first = spark.range(20).selectExpr("id % 7 k", "id v").repartition(8, $"k")
            val second = spark.range(20, 40).selectExpr("id % 7 k", "id v")
            val union = first.union(second)
            val input = if (userRepartition) { union.repartition(5, $"k") } else { union }
            val result = input.withColumn("rn", org.apache.spark.sql.functions.row_number().over(
              org.apache.spark.sql.expressions.Window.partitionBy("k", "v").orderBy("v")))
              .as[(Long, Long, Int)].mapPartitions(rows => rows)
            assert(result.collect().sorted.toSeq === expected.sorted)
            val adaptive = result.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
            val initial = adaptive.initialPlan
            val expectedCount = if (enabled && !userRepartition) { 8 } else { 5 }
            assert(result.rdd.getNumPartitions === expectedCount)
            assert(numShufflesOnUnion(initial) ===
              (if (enabled && !userRepartition) { 0 } else { 1 }), s"unexpected plan:\n$initial")
            if (userRepartition) {
              assert(!initial.exists {
                case shuffle: ShuffleExchangeExec => shuffle.shuffleOrigin == REPARTITION_BY_NUM &&
                  shuffle.outputPartitioning.numPartitions == 5
                case _ => false
              }, s"the user exchange must be elided:\n$initial")
              val carrier = initial.collectFirst {
                case shuffle: ShuffleExchangeExec if isUnionPlan(shuffle.child) => shuffle
              }.get
              val changed = carrier.copy(outputPartitioning = HashPartitioning(
                carrier.outputPartitioning.asInstanceOf[HashPartitioning].expressions, 8))
              changed.copyTagsFrom(carrier)
              assert(!org.apache.spark.sql.execution.exchange.PartitioningOrigin
                .validateNode(changed),
                "the requirement exchange must carry the elided user's five-partition origin")
            }
          }
        }
      } finally {
        spark.experimental.extraStrategies = previousStrategies
      }
    }
  }

  test("SPARK-60379: preserve an elided repartition below mapPartitions") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      val first = spark.range(20).selectExpr("id % 3 a", "id b", "id v")
      val second = spark.range(20).selectExpr("id % 3 a", "id b", "id v")
      Seq(false, true).foreach { enabled =>
        withSQLConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ENABLED.key -> enabled.toString) {
          val branch = first.repartition(8, $"a", $"b").union(second)
            .withColumn("rn", org.apache.spark.sql.functions.row_number().over(
              org.apache.spark.sql.expressions.Window.partitionBy("a", "b").orderBy("v")))
          val other = second.withColumn("rn", org.apache.spark.sql.functions.row_number().over(
            org.apache.spark.sql.expressions.Window.partitionBy("a", "b").orderBy("v")))
          val result = branch.union(other).repartition($"a", $"b")
            .as[(Long, Long, Long, Int)]
            .mapPartitions(_.map(r => (r._1, r._2)).toVector.distinct.iterator)
          assert(result.collect().sorted.toSeq === (0L until 20L).map(i => (i % 3, i)).sorted)
        }
      }
    }
  }

  test("SPARK-60379: exploding join sources require non-coalescible permission") {
    withPushDown {
      val a = attr("id")
      val b = attr("id")
      val join = org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec(
        ensureShuffle(hash(a, 2), leaf(a)),
        org.apache.spark.sql.execution.exchange.BroadcastExchangeExec(
          org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode, leaf(b)),
        org.apache.spark.sql.catalyst.optimizer.BuildRight,
        org.apache.spark.sql.catalyst.plans.Inner, Some(GreaterThan(a, b)))
      val child = ProjectExec(Seq(a), join)
      val union = UnionExec(Seq(child, leaf(attr("id"))))
      val parent = TestPartitioningPlan(
        children = Seq(ensureShuffle(hash(a, 2), union)),
        required = Seq(clustered(a)), output = Seq(a))
      withSQLConf(
          SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_NON_COALESCIBLE_BRANCHES_ENABLED.key ->
            "false") {
        assert(pushDown(parent).fastEquals(parent))
      }
      assert(numShufflesOnUnion(pushDown(parent)) === 0)
    }
  }

  test("SPARK-60379: protected join branches retain runtime local reads") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "100000",
        SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_JOIN_BRANCHES_ENABLED.key -> "false",
        SQLConf.LOCAL_SHUFFLE_READER_ENABLED.key -> "true",
        SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      withTempView("t1", "t2", "t3") {
        spark.range(100).selectExpr("id % 7 k", "id v").createOrReplaceTempView("t1")
        spark.range(7).selectExpr("id k").createOrReplaceTempView("t2")
        spark.range(7).selectExpr("id k", "id v").createOrReplaceTempView("t3")
        val sql = """SELECT k, v, rank() OVER (PARTITION BY k ORDER BY v) rn FROM (
          | SELECT a.k, a.v FROM t1 a JOIN t2 b ON a.k = b.k
          | UNION ALL SELECT k, v FROM t3)""".stripMargin
        val df = spark.sql(sql)
        checkAnswer(df, expectedResult(sql))
        assert(aqeInitialPlan(df).exists(_.isInstanceOf[SortMergeJoinExec]))
        def nodes(p: SparkPlan): Seq[SparkPlan] = p match {
          case stage: QueryStageExec => stage +: nodes(stage.plan)
          case other => other +: other.children.flatMap(nodes)
        }
        val finalNodes = nodes(df.queryExecution.executedPlan
          .asInstanceOf[AdaptiveSparkPlanExec].executedPlan)
        val joins = finalNodes.collect { case join: BroadcastHashJoinExec => join }
        assert(joins.size === 1)
        val streamed = if (joins.head.buildSide == BuildLeft) {
          joins.head.right
        } else {
          joins.head.left
        }
        val probeReads = nodes(streamed).collect { case read: AQEShuffleReadExec => read }
        assert(probeReads.nonEmpty)
        assert(probeReads.forall(_.isLocalRead))
        assert(finalNodes.exists {
          case shuffle: ShuffleExchangeExec => isUnionPlan(shuffle.child)
          case _ => false
        }, "runtime broadcast conversion must retain the union exchange")
      }
    }
  }

  test("SPARK-60379: opted-in join layouts can roll back independent local shuffle reads") {
    withSQLConf(
        SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "100000",
        SQLConf.LOCAL_SHUFFLE_READER_ENABLED.key -> "true",
        SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      withTempView("t1", "t2", "t3", "t4", "t5") {
        spark.range(100).selectExpr("id % 7 k", "id v").createOrReplaceTempView("t1")
        spark.range(7).selectExpr("id k").createOrReplaceTempView("t2")
        spark.range(7).selectExpr("id k", "id v").createOrReplaceTempView("t3")
        spark.range(100).selectExpr("id % 7 k", "id v").createOrReplaceTempView("t4")
        spark.range(7).selectExpr("id k").createOrReplaceTempView("t5")
        val sql = """SELECT k, v, rank() OVER (PARTITION BY k ORDER BY v) rn FROM (
          | SELECT a.k, a.v FROM t1 a JOIN t2 b ON a.k = b.k
          | UNION ALL SELECT k, v FROM t3)
          | UNION ALL SELECT e.k, e.v, 0 FROM t4 e JOIN t5 f ON e.k = f.k""".stripMargin
        val expected = expectedResult(sql)
        Seq(false, true).foreach { enabled =>
          withSQLConf(
              SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ENABLED.key -> enabled.toString,
              SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_JOIN_BRANCHES_ENABLED.key -> "true") {
            val df = spark.sql(sql)
            checkAnswer(df, expected)
            assert(aqeInitialPlan(df).collect { case join: SortMergeJoinExec => join }.size === 2)
            def plans(p: SparkPlan): Seq[SparkPlan] = p match {
              case stage: QueryStageExec => stage +: plans(stage.plan)
              case other => other +: other.children.flatMap(plans)
            }
            val finalNodes = plans(df.queryExecution.executedPlan
              .asInstanceOf[AdaptiveSparkPlanExec].executedPlan)
            assert(finalNodes.count(_.isInstanceOf[BroadcastHashJoinExec]) === 2)
            val joins = finalNodes.collect { case join: BroadcastHashJoinExec => join }
            val probeReads = joins.flatMap { join =>
              val streamed = if (join.buildSide == BuildLeft) {
                join.right
              } else {
                join.left
              }
              plans(streamed).collect { case read: AQEShuffleReadExec => read }
            }
            assert(probeReads.size === 2)
            probeReads.foreach { read =>
              assert(read.isLocalRead === !enabled,
                s"enabled=$enabled final plan:\n${finalNodes.head}")
            }
          }
        }
      }
    }
  }

  test("SPARK-60379: planned writes preserve an elided repartition count") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      withTempPath { path =>
        val first = spark.range(100).selectExpr("id a", "id v")
        val second = spark.range(100, 200).selectExpr("id a", "id v")
        val window = first.repartition(8, $"a").union(second)
          .withColumn("rn", org.apache.spark.sql.functions.row_number().over(
            org.apache.spark.sql.expressions.Window.partitionBy("a").orderBy("v")))
        window.repartition($"a").write.parquet(path.getCanonicalPath)
        val files = path.listFiles().filter(_.getName.endsWith(".parquet"))
        assert(files.length === 5, "the user repartition must retain its five-partition layout")
        checkAnswer(spark.read.parquet(path.getCanonicalPath),
          (0L until 200L).map(id => Row(id, id, 1)))
      }
    }
  }

  test("SPARK-60379: full-key policy protects a join above relaxed windows") {
    withPushDown {
      val a = attr("a")
      val b = attr("b")
      val x = attr("x")
      val y = attr("y")
      def side(first: Attribute, second: Attribute): SparkPlan = {
        val union = UnionExec(Seq(TestPartitioningPlan(
          outputPartitioning = hash(first, 2), output = Seq(first, second)),
          TestPartitioningPlan(output = Seq(attr("a"), attr("b")))))
        PassThroughPlan(ensureShuffle(HashPartitioning(Seq(first, second), 2), union),
          ClusteredDistribution(Seq(first, second)))
      }
      val parent = TestPartitioningPlan(children = Seq(side(a, b), side(x, y)),
        required = Seq(ClusteredDistribution(Seq(a, b)), ClusteredDistribution(Seq(x, y))),
        output = Seq(a, b, x, y))
      withSQLConf(SQLConf
          .PUSH_DOWN_SHUFFLE_THROUGH_UNION_ALLOW_PARTITION_KEYS_SUBSET_OF_CLUSTER_KEYS_ENABLED
          .key -> "false") {
        assert(pushDown(parent).fastEquals(parent))
      }
      assert(numShufflesOnUnion(pushDown(parent)) === 0)
    }
  }

  test("SPARK-60379: two-key joins between unions respect both subset permissions") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "5",
        SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ALLOW_JOIN_INPUTS_ENABLED.key -> "true") {
      withTempView("t1", "t2", "t3", "t4") {
        Seq((1, 10), (1, 20), (2, 5)).toDF("a", "b").createOrReplaceTempView("t1")
        Seq((1, 20), (3, 7)).toDF("a", "b").createOrReplaceTempView("t2")
        Seq((1, 20), (2, 5)).toDF("x", "y").createOrReplaceTempView("t3")
        Seq((1, 20), (3, 7)).toDF("x", "y").createOrReplaceTempView("t4")
        val sql = """SELECT /*+ MERGE(u, w) */ u.a, u.b, w.x, w.y FROM (
          | SELECT a, max(b) b FROM t1 GROUP BY a UNION ALL SELECT a, b FROM t2) u
          | JOIN (SELECT x, max(y) y FROM t3 GROUP BY x UNION ALL SELECT x, y FROM t4) w
          | ON u.a = w.x AND u.b = w.y""".stripMargin
        val expected = expectedResult(sql)
        for (allow <- Seq(false, true); allKeys <- Seq(false, true)) {
          withSQLConf(SQLConf
              .PUSH_DOWN_SHUFFLE_THROUGH_UNION_ALLOW_PARTITION_KEYS_SUBSET_OF_CLUSTER_KEYS_ENABLED
              .key -> allow.toString,
              SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> allKeys.toString) {
            val df = spark.sql(sql)
            checkAnswer(df, expected)
            assert(numShufflesOnUnion(aqeInitialPlan(df)) ===
              (if (allow && !allKeys) { 0 } else { 2 }))
          }
        }
      }
    }
  }

  test("SPARK-60379: subset targets require both permissions") {
    withPushDown {
      val a = attr("a")
      val b = attr("b")
      val union = UnionExec(Seq(
        TestPartitioningPlan(outputPartitioning = hash(a, 2), output = Seq(a, b)),
        TestPartitioningPlan(output = Seq(attr("a"), attr("b")))))
      val parent = TestPartitioningPlan(
        children = Seq(ensureShuffle(HashPartitioning(Seq(a, b), 2), union)),
        required = Seq(ClusteredDistribution(Seq(a, b))), output = Seq(a, b))
      for (allow <- Seq(false, true); allKeys <- Seq(false, true)) {
        withSQLConf(
            SQLConf
              .PUSH_DOWN_SHUFFLE_THROUGH_UNION_ALLOW_PARTITION_KEYS_SUBSET_OF_CLUSTER_KEYS_ENABLED
              .key -> allow.toString,
            SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> allKeys.toString) {
          assert(!pushDown(parent).fastEquals(parent) === (allow && !allKeys))
        }
      }
    }
  }

  test("SPARK-60379: rebalance sources do not offer targets") {
    withPushDown {
      val a = attr("id")
      val shuffle = ShuffleExchangeExec(hash(a, 2), leaf(a),
        org.apache.spark.sql.execution.exchange.REBALANCE_PARTITIONS_BY_COL)
      val union = UnionExec(Seq(shuffle, leaf(attr("id"))))
      val parent = TestPartitioningPlan(
        children = Seq(ensureShuffle(hash(a, 2), union)),
        required = Seq(clustered(a)), output = Seq(a))
      assert(pushDown(parent).fastEquals(parent))
    }
  }

  test("SPARK-60379: join branches require explicit permission") {
    withPushDown {
      val a = attr("id")
      val b = attr("id")
      val join = SortMergeJoinExec(Seq(a), Seq(b),
        org.apache.spark.sql.catalyst.plans.Inner, None,
        SortExec(Seq(SortOrder(a, Ascending)), false, ensureShuffle(hash(a, 2), leaf(a))),
        SortExec(Seq(SortOrder(b, Ascending)), false, ensureShuffle(hash(b, 2), leaf(b))))
      val child = ProjectExec(Seq(a), join)
      val union = UnionExec(Seq(child, leaf(attr("id"))))
      val parent = TestPartitioningPlan(
        children = Seq(ensureShuffle(hash(a, 2), union)),
        required = Seq(clustered(a)), output = Seq(a))
      withSQLConf(
          SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_JOIN_BRANCHES_ENABLED.key -> "false") {
        assert(pushDown(parent).fastEquals(parent))
      }
      assert(numShufflesOnUnion(pushDown(parent)) === 0)
    }
  }

  test("SPARK-60379: join inputs require explicit permission") {
    withPushDown {
      val a = attr("id")
      val b = attr("id")
      val left = ensureShuffle(hash(a, 2), UnionExec(Seq(leaf(a, hash(a, 2)), leaf(attr("id")))))
      val right = ensureShuffle(hash(b, 2), leaf(b))
      val join = SortMergeJoinExec(Seq(a), Seq(b),
        org.apache.spark.sql.catalyst.plans.Inner, None, left, right)
      withSQLConf(
          SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ALLOW_JOIN_INPUTS_ENABLED.key -> "false") {
        assert(pushDown(join).fastEquals(join))
      }
      assert(numShufflesOnUnion(pushDown(join)) === 0)
    }
  }

  test("SPARK-60379: push-down cost preserves empty-branch replanning") {
    withSQLConf(
        SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_JOIN_BRANCHES_ENABLED.key -> "false",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "5",
        SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
      withTempView("t1", "t2", "empty_source") {
        spark.range(20).selectExpr("id % 7 k", "id v").createOrReplaceTempView("t1")
        spark.range(10).selectExpr("id k", "id v").createOrReplaceTempView("t2")
        val never = org.apache.spark.sql.functions.udf((id: Long) => id < 0)
        spark.range(10).filter(never($"id")).createOrReplaceTempView("empty_source")
        val sql = """SELECT k, v, rank() OVER (PARTITION BY k ORDER BY v) rn FROM (
          | SELECT k, sum(v) v FROM t1 GROUP BY k
          | UNION ALL SELECT k, v FROM t2
          | UNION ALL SELECT k, v + 1 FROM t2
          | UNION ALL SELECT k, v + 2 FROM t2
          | UNION ALL SELECT t.k, t.v FROM t2 t JOIN empty_source e ON t.k = e.id)""".stripMargin
        val df = spark.sql(sql)
        checkAnswer(df, expectedResult(sql))
        assert(numShufflesOnUnion(aqeInitialPlan(df)) === 1)
        def plans(p: SparkPlan): Seq[SparkPlan] = p match {
          case stage: QueryStageExec => stage +: plans(stage.plan)
          case other => other +: other.children.flatMap(plans)
        }
        val finalNodes = plans(df.queryExecution.executedPlan
          .asInstanceOf[AdaptiveSparkPlanExec].executedPlan)
        assert(finalNodes.exists {
          case union: UnionExec => union.children.length == 4
          case _ => false
        }, "the empty join branch must disappear")
        assert(!finalNodes.exists {
          case shuffle: ShuffleExchangeExec => isUnionPlan(shuffle.child)
          case _ => false
        }, "the new three-branch push-down must survive cost comparison")
      }
    }
  }

  test("SPARK-60379: shuffle cost counts pushed-through unions instead of added exchanges") {
    withPushDown {
      val a = attr("id")
      val children = leaf(a, hash(a, 2)) +: (0 until 3).map(_ => leaf(attr("id")))
      val union = UnionExec(children)
      val parent = TestPartitioningPlan(
        children = Seq(ensureShuffle(hash(a, 2), union)),
        required = Seq(clustered(a)), output = Seq(a))
      val rebuilt = pushDown(parent).children.head.asInstanceOf[UnionExec]
      val evaluator = SimpleCostEvaluator(false, false, pushDownShuffleThroughUnion = true)
      assert(evaluator.evaluateCost(rebuilt) === SimpleCost(0, 1, 0))
      val disabled = SimpleCostEvaluator(false, false, pushDownShuffleThroughUnion = false)
      assert(disabled.evaluateCost(rebuilt) === SimpleCost(0, 3, 0))
      assert(evaluator.evaluateCost(ensureShuffle(hash(a, 2), rebuilt)) === SimpleCost(0, 2, 0))
      val untagged = UnionExec(rebuilt.children)
      assert(evaluator.evaluateCost(untagged) === SimpleCost(0, 0, 0))
      val first = rebuilt.children(1).asInstanceOf[ShuffleExchangeExec]
      val second = rebuilt.children(2).asInstanceOf[ShuffleExchangeExec]
      val staged = rebuilt.withNewChildren(Seq(rebuilt.children.head,
        ShuffleQueryStageExec(0, first, first.canonicalized),
        ShuffleQueryStageExec(1, second, second.canonicalized), rebuilt.children(3)))
      assert(evaluator.evaluateCost(staged) === SimpleCost(0, 1, 0))
      val allStaged = staged.withNewChildren(staged.children.updated(3, {
        val last = rebuilt.children(3).asInstanceOf[ShuffleExchangeExec]
        ShuffleQueryStageExec(2, last, last.canonicalized)
      }))
      assert(evaluator.evaluateCost(allStaged) === SimpleCost(0, 1, 0))
      val plain = UnionExec(Seq(leaf(a), leaf(attr("id"))))
      plain.setTagValue(AQEUtils.PUSHED_THROUGH_UNION, true)
      assert(evaluator.evaluateCost(plain) === SimpleCost(0, 0, 0))
    }
  }

  test("SPARK-60379: runtime re-planning preserves staged union shuffle cost") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "5",
        SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_CUSTOM_COST_EVALUATOR_CLASS.key ->
          classOf[RecordingUnionCostEvaluator].getName) {
      withTempView("t1", "t2", "kept") {
        createTables()
        val kept = spark.table("t1").repartition(5, $"id").cache()
        kept.createOrReplaceTempView("kept")
        kept.count()
        RecordingUnionCostEvaluator.comparisons.clear()
        try {
          val sql = """SELECT id, v,
            | row_number() OVER (PARTITION BY id ORDER BY v) rn FROM (
            | SELECT * FROM kept
            | UNION ALL SELECT * FROM t2)""".stripMargin
          val expected = expectedResult(sql)
          RecordingUnionCostEvaluator.comparisons.clear()
          val df = spark.sql(sql)
          // checkAnswer also executes the RDD path, adding cost evaluations beyond this collect.
          // Collect once so the assertion below covers exactly this execution's comparisons.
          assert(df.collect().toSeq.sortBy(_.toString) === expected.sortBy(_.toString))
          val evaluations = RecordingUnionCostEvaluator.comparisons.toSeq
          val expectedEvaluation = Seq((true, true, SimpleCost(0, 1, 0)))
          // AQE may batch the cache and shuffle completion events into one re-planning round.
          assert(Set(2, 4).contains(evaluations.length), s"unexpected evaluations: $evaluations")
          assert(evaluations === Seq.fill(evaluations.length)(expectedEvaluation))
        } finally {
          kept.unpersist()
          RecordingUnionCostEvaluator.comparisons.clear()
        }
      }
    }
  }

  test("SPARK-60379: requirement stages restore union cost before all branches finish") {
    withPushDown {
      val a = attr("id")
      val b = attr("id")
      val c = attr("id")
      val kept = ShuffleExchangeExec(hash(a, 2), leaf(a), REPARTITION_BY_COL)
      val second = ShuffleExchangeExec(hash(b, 2), leaf(b), REPARTITION_BY_COL)
      val moved = ensureShuffle(hash(c, 2), leaf(c))
      val stage = ShuffleQueryStageExec(0, moved, moved.canonicalized)
      val evaluator = SimpleCostEvaluator(false, false, true)
      Seq(moved, stage).foreach { child =>
        val union = UnionExec(Seq(kept, second, child))
        val root = TestPartitioningPlan(children = Seq(union), output = Seq(a))
        val result = pushDown(root)
        assert(AQEUtils.isPushedThroughUnion(union))
        val shuffles = if (child eq moved) { 4 } else { 3 }
        assert(evaluator.evaluateCost(result) === SimpleCost(0, shuffles, 0))
        val extra = ensureShuffle(hash(a, 2), result)
        assert(evaluator.evaluateCost(extra) > evaluator.evaluateCost(result))
      }
      val allStaged = UnionExec(Seq(
        ShuffleQueryStageExec(1, kept, kept.canonicalized),
        ShuffleQueryStageExec(2, second, second.canonicalized), stage))
      val root = TestPartitioningPlan(children = Seq(allStaged), output = Seq(a))
      assert(evaluator.evaluateCost(pushDown(root)) === SimpleCost(0, 1, 0))
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
        val columnsOnly = UnionExec(Seq(kept, second))
        pushDown(TestPartitioningPlan(children = Seq(columnsOnly), output = Seq(a)))
        assert(!AQEUtils.isPushedThroughUnion(columnsOnly))
      }
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "8") {
        val retargeted = kept.copy(outputPartitioning = hash(a, 2))
        Seq(retargeted,
          ShuffleQueryStageExec(3, retargeted, retargeted.canonicalized)).foreach { child =>
          val union = UnionExec(Seq(child, second))
          pushDown(TestPartitioningPlan(children = Seq(union), output = Seq(a)))
          assert(AQEUtils.isPushedThroughUnion(union))
        }
        val fixed = kept.copy(shuffleOrigin = REPARTITION_BY_NUM)
        val union = UnionExec(Seq(fixed, fixed))
        pushDown(TestPartitioningPlan(children = Seq(union), output = Seq(a)))
        assert(!AQEUtils.isPushedThroughUnion(union))
      }
      val plain = UnionExec(Seq(kept, ensureShuffle(hash(c, 3), leaf(c))))
      pushDown(TestPartitioningPlan(children = Seq(plain), output = Seq(a)))
      assert(plain.isPlainUnion)
      assert(!AQEUtils.isPushedThroughUnion(plain))
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
        val wrapped = UnionExec(Seq(kept, ProjectExec(Seq(c), stage)))
        pushDown(TestPartitioningPlan(children = Seq(wrapped), output = Seq(a)))
        assert(!AQEUtils.isPushedThroughUnion(wrapped))
      }
    }
  }

  test("SPARK-60379: a non-exchange sibling permits non-coalescible union targets") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "4",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
        SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ALLOW_JOIN_INPUTS_ENABLED.key -> "true",
        SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_NON_COALESCIBLE_BRANCHES_ENABLED.key ->
          "false") {
      withTempView("t1", "t2", "t3", "cached_left", "cached_right") {
        Seq((1, 10), (1, 20), (2, 5)).toDF("id", "v").createOrReplaceTempView("t1")
        Seq((1, 30), (3, 7)).toDF("id", "v").createOrReplaceTempView("t2")
        Seq((1, 100), (3, 300)).toDF("id", "w").createOrReplaceTempView("t3")
        val left = spark.table("t1").repartition(4, $"id").cache()
        val right = spark.table("t3").repartition(4, $"id").cache()
        try {
          left.count()
          right.count()
          left.createOrReplaceTempView("cached_left")
          right.createOrReplaceTempView("cached_right")
          val expected = Seq(Row(1, 10, 100), Row(1, 20, 100),
            Row(1, 30, 100), Row(3, 7, 300))
          for (cachedSibling <- Seq(false, true); enabled <- Seq(false, true)) {
            withSQLConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ENABLED.key -> enabled.toString) {
              val sibling = if (cachedSibling) { "cached_right" } else { "t3" }
              val df = spark.sql(s"""SELECT /*+ MERGE(u, r) */ u.id, u.v, r.w FROM (
                | SELECT id, v FROM cached_left
                | UNION ALL SELECT id, v FROM t2) u JOIN $sibling r ON u.id = r.id""".stripMargin)
              checkAnswer(df, expected)
              val initial = aqeInitialPlan(df)
              val join = initial.collectFirst { case j: SortMergeJoinExec => j }.get
              assert(join.right.exists(_.isInstanceOf[ShuffleExchangeExec]) === !cachedSibling,
                s"only the uncached sibling should require an exchange:\n$initial")
              val pushed = enabled && cachedSibling
              assert(numShufflesOnUnion(initial) === (if (pushed) { 0 } else { 1 }),
                s"only an already non-coalescible consumer permits the cached target:\n$initial")
            }
          }
        } finally {
          left.unpersist(blocking = true)
          right.unpersist(blocking = true)
        }
      }
    }
  }

  test("SPARK-60379: a child without a target does not cancel its sibling") {
    withPushDown {
      val a = attr("id")
      val b = attr("id")
      val left = UnionExec(Seq(leaf(a, hash(a, 2)), leaf(attr("id"))))
      val right = UnionExec(leaf(b, hash(b, 2)) +:
        (0 until 4).map(_ => leaf(attr("id"))))
      val parent = TestPartitioningPlan(
        children = Seq(ensureShuffle(hash(a, 2), left), ensureShuffle(hash(b, 2), right)),
        required = Seq(clustered(a), clustered(b)), output = Seq(a, b))
      val result = pushDown(parent)
      assert(result.children.head.isInstanceOf[UnionExec])
      assert(result.children(1).fastEquals(parent.children(1)))
      assert(ValidateRequirements.validate(result))
    }
  }

  test("SPARK-60379: subquery policy applies to the entire adaptive query") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ALLOW_WITH_SUBQUERIES_ENABLED.key -> "false") {
      withTempView("t1", "t2") {
        createTables()
        val plain = spark.sql(query)
        checkAnswer(plain, expectedResult(query))
        val plainAdaptive = plain.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
        assert(!plainAdaptive.context.queryContainsSubqueries)
        assert(numShufflesOnUnion(plainAdaptive.initialPlan) === 0)

        val df = spark.sql(query + " WHERE v > (SELECT min(v) FROM t2)")
        checkAnswer(df, expectedResult(query + " WHERE v > (SELECT min(v) FROM t2)"))
        val adaptive = df.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
        assert(adaptive.context.queryContainsSubqueries)
        val subqueries = adaptive.initialPlan.subqueriesAll.flatMap(_.collect {
          case subquery: AdaptiveSparkPlanExec => subquery
        })
        assert(subqueries.nonEmpty)
        assert(subqueries.forall(_.context eq adaptive.context))
        assert(numShufflesOnUnion(adaptive.initialPlan) === 1)
      }
    }
  }

  test("SPARK-60379: total union child limit is independent of moved child limit") {
    withPushDown {
      val a = attr("id")
      def plan(size: Int): SparkPlan = {
        val children = (0 until size - 1).map { _ =>
          val id = attr("id")
          leaf(id, hash(id, 2))
        } :+ leaf(attr("id"))
        val union = UnionExec(children)
        TestPartitioningPlan(
          children = Seq(ensureShuffle(hash(union.output.head, 2), union)),
          required = Seq(clustered(union.output.head)), output = Seq(a))
      }
      assert(numShufflesOnUnion(pushDown(plan(10))) === 0)
      val tooMany = plan(11)
      assert(pushDown(tooMany).fastEquals(tooMany))
      withSQLConf(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_MAX_UNION_CHILDREN.key -> "11") {
        assert(numShufflesOnUnion(pushDown(tooMany)) === 0)
      }
    }
  }
}

/** Records the actual current/proposal cost evaluations without changing their cost policy. */
private class RecordingUnionCostEvaluator extends CostEvaluator {
  private val evaluator = SimpleCostEvaluator(false, false, true)

  override def evaluateCost(plan: SparkPlan): Cost = {
    val unions = plan.collect { case union: UnionExec =>
      (AQEUtils.isPushedThroughUnion(union),
        union.children.exists(_.isInstanceOf[ShuffleQueryStageExec]),
        evaluator.evaluateCost(union))
    }
    RecordingUnionCostEvaluator.comparisons += unions
    evaluator.evaluateCost(plan)
  }
}

private object RecordingUnionCostEvaluator {
  val comparisons = ArrayBuffer.empty[Seq[(Boolean, Boolean, Cost)]]
}

/** A node with a fixed output partitioning; empty requirements default to one per child. */
private case class TestPartitioningPlan(
    override val children: Seq[SparkPlan] = Nil,
    override val outputPartitioning: Partitioning = UnknownPartitioning(0),
    required: Seq[Distribution] = Nil,
    ordering: Seq[Seq[SortOrder]] = Nil,
    override val output: Seq[Attribute] = Seq.empty) extends SparkPlan {
  override def requiredChildDistribution: Seq[Distribution] = {
    if (required.isEmpty) {
      super.requiredChildDistribution
    } else {
      required
    }
  }
  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    if (ordering.isEmpty) {
      super.requiredChildOrdering
    } else {
      ordering
    }
  }
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("TestPartitioningPlan")
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    copy(children = newChildren)
}

/** A unary node that requires `required` and reports its child's partitioning. */
private case class PassThroughPlan(child: SparkPlan, required: Distribution)
  extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def requiredChildDistribution: Seq[Distribution] = Seq(required)
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("PassThroughPlan")
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
