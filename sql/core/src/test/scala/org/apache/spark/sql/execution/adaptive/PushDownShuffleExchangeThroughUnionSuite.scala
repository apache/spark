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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Alias, Ascending, Attribute, AttributeReference, ExprId, GreaterThan, IsNotNull, Literal, Rand,
  RowNumber, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{
  ClusteredDistribution, Distribution, HashPartitioning, NullAwareHashPartitioning, Partitioning,
  RoundRobinPartitioning, UnknownPartitioning}
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
    super.sparkConf.set(SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ENABLED.key, "true")

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
    countShuffles(plan, underUnion = false) { s => isUnionPlan(s.child) }

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
      snapshot, EnsureRequirements(requiredDistribution = required))(plan)
  }

  private def withPushDown(f: => Unit): Unit = withSQLConf(
      SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true",
      SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_ENABLED.key -> "true")(f)

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

        // Both partitionings tie on the number of children; the first one in child order wins.
        val union = aqeInitialPlan(df).collect { case u: UnionExec => u }.head
        assert(union.outputPartitioning.isInstanceOf[HashPartitioning],
          s"the union should propagate a hash partitioning after the push-down:\n" +
            df.queryExecution.executedPlan)
        assert(union.outputPartitioning.asInstanceOf[HashPartitioning].expressions.length === 1,
          s"the first child's partitioning should win the tie:\n" +
            df.queryExecution.executedPlan)
      }
    }
  }

  test("SPARK-60379: pick the partitioning produced by the most children") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withTempView("t1", "t2", "t3") {
        // Two children share HashPartitioning(id); the third uses HashPartitioning(id, grp) and
        // holds most of the data. The rule must pick the partitioning produced by the most
        // children, not the one produced by the most data.
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
        assert(union.outputPartitioning.asInstanceOf[HashPartitioning].expressions.length === 1,
          s"the target should be the single-column partitioning shared by two children:\n" +
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
      // of two comes first, even though the finer layout would hand the operator above the
      // parallelism the pushed shuffle had.
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
      val union = UnionExec(Seq(leaf(a, hash(a, 2)), leaf(a1)))
      val other = leaf(b)
      // The sibling is a shuffle (under a local sort) with no union below it.
      val sibling = SortExec(
        Seq(SortOrder(b, Ascending)), global = false, ensureShuffle(hash(b, 5), other))
      val parent = TestPartitioningPlan(
        children = Seq(ensureShuffle(hash(a, 5), union), sibling),
        required = Seq(clustered(a), clustered(b)),
        output = Seq(a, b))

      val newPlan = pushDown(parent)
      assert(newPlan.fastEquals(parent), s"incompatible sibling must prevent push-down:\n$newPlan")

      val compatible = parent.withNewChildren(Seq(
        ensureShuffle(hash(a, 2), union),
        sibling.withNewChildren(Seq(ensureShuffle(hash(b, 2), other)))))
      val pushed = pushDown(compatible)
      val newUnion = pushed.children.head.asInstanceOf[UnionExec]
      assertReshuffled(newUnion.children(1), hash(a1, 2), union.children(1))
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
      // hash(_, 2) is offered by two children, hash(_, 3) by one.
      val children = Seq(leaf(a, hash(a, 2)), leaf(a1, hash(a1, 2)), leaf(a2, hash(a2, 3)))

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
      val most = run(fixedPartitions = 2)
      assert(most.children.head.fastEquals(children.head))
      assert(most.children(1).fastEquals(children(1)))
      assertReshuffled(most.children(2), hash(a2, 2), children(2))

      // Incompatible with the fixed side: fall back to the layout that pairs with it.
      val paired = run(fixedPartitions = 3)
      assertReshuffled(paired.children.head, hash(a, 3), children.head)
      assertReshuffled(paired.children(1), hash(a1, 3), children(1))
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
        // The union is consumed twice on the same key, so `EnsureRequirements` builds one
        // exchange and AQE reuses its stage. Moving it below the union would run the union's
        // subtree once per consumer. The join branch is allowed as a target here, so only the
        // reuse blocks the rewrite.
        val sql =
          """WITH u AS (
            |  SELECT l.id AS id, l.v AS v FROM t1 l JOIN t3 r ON l.id = r.id
            |  UNION ALL
            |  SELECT id, v FROM t2)
            |SELECT * FROM u u1 JOIN u u2 ON u1.id = u2.id""".stripMargin
        val df = spark.sql(sql)
        df.collect()

        val plan = aqeInitialPlan(df)
        assert(numShufflesOnUnion(plan) === 2,
          s"an exchange the plan reuses is not moved below the union:\n$plan")

        // With reuse off every exchange gets its own stage, so the rewrite goes ahead.
        withSQLConf(SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
          val noReuse = spark.sql(sql)
          noReuse.collect()
          val noReusePlan = aqeInitialPlan(noReuse)
          assert(numShufflesOnUnion(noReusePlan) === 0,
            s"without exchange reuse there is no stage to protect:\n$noReusePlan")
        }
      }
    }
  }

  test("SPARK-60379: duplicated shuffle forms include a shuffle query stage and a " +
      "subquery plan") {
    import testImplicits._
    val leaf = spark.range(10).select($"id" as Symbol("k")).queryExecution.executedPlan
    val hp = HashPartitioning(leaf.output, 4)

    // A duplicate carried by a shuffle query stage, without materializing it.
    val staged = ShuffleExchangeExec(hp, leaf)
    val stage = ShuffleQueryStageExec(0, staged, staged.canonicalized)
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
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
          SQLConf.SHUFFLE_PARTITIONS.key -> "4",
          SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_KEEP_NON_COALESCIBLE_BRANCHES_ENABLED.key ->
            "false") {
        withTempView("t1", "t2", "t3") {
          Seq((1, 10), (1, 20), (2, 5)).toDF("id", "v").createOrReplaceTempView("t1")
          Seq(1, 2).toDF("id").createOrReplaceTempView("t2")
          Seq((1, 30), (3, 7)).toDF("id", "v").createOrReplaceTempView("t3")
          val sql = s"""SELECT id, v, row_number() OVER (PARTITION BY id ORDER BY v) rn FROM (
            | SELECT /*+ $hint */ a.id, a.v FROM
            | (SELECT id, v FROM t1 DISTRIBUTE BY id) a JOIN
            | (SELECT id FROM t2 DISTRIBUTE BY id) b ON a.id = b.id
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
