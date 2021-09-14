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

package org.apache.spark.sql

import org.apache.log4j.Level

import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide, EliminateResolvedHint}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class JoinHintSuite extends PlanTest with SharedSparkSession with AdaptiveSparkPlanHelper {
  import testImplicits._

  lazy val df = spark.range(10)
  lazy val df1 = df.selectExpr("id as a1", "id as a2")
  lazy val df2 = df.selectExpr("id as b1", "id as b2")
  lazy val df3 = df.selectExpr("id as c1", "id as c2")

  def msgNoHintRelationFound(relation: String, hint: String): String =
    s"Count not find relation '$relation' specified in hint '$hint'."

  def msgNoJoinForJoinHint(strategy: String): String =
    s"A join hint (strategy=$strategy) is specified but it is not part of a join relation."

  def msgJoinHintOverridden(strategy: String): String =
    s"Hint (strategy=$strategy) is overridden by another hint and will not take effect."

  def verifyJoinHintWithWarnings(
      df: => DataFrame,
      expectedHints: Seq[JoinHint],
      warnings: Seq[String]): Unit = {
    val logAppender = new LogAppender("join hints")
    withLogAppender(logAppender) {
      verifyJoinHint(df, expectedHints)
    }
    val warningMessages = logAppender.loggingEvents
      .filter(_.getLevel == Level.WARN)
      .map(_.getRenderedMessage)
      .filter(_.contains("hint"))
    assert(warningMessages.size == warnings.size)
    warnings.foreach { w =>
      assert(warningMessages.contains(w))
    }
  }

  def verifyJoinHint(df: DataFrame, expectedHints: Seq[JoinHint]): Unit = {
    val optimized = df.queryExecution.optimizedPlan
    val joinHints = optimized collect {
      case Join(_, _, _, _, hint) => hint
      case _: ResolvedHint => fail("ResolvedHint should not appear after optimize.")
    }
    assert(joinHints == expectedHints)
  }

  test("single join") {
    verifyJoinHint(
      df.hint("broadcast").join(df, "id"),
      JoinHint(
        Some(HintInfo(strategy = Some(BROADCAST))),
        None) :: Nil
    )
    verifyJoinHint(
      df.join(df.hint("broadcast"), "id"),
      JoinHint(
        None,
        Some(HintInfo(strategy = Some(BROADCAST)))) :: Nil
    )
  }

  test("multiple joins") {
    verifyJoinHint(
      df1.join(df2.hint("broadcast").join(df3, $"b1" === $"c1").hint("broadcast"), $"a1" === $"c1"),
      JoinHint(
        None,
        Some(HintInfo(strategy = Some(BROADCAST)))) ::
        JoinHint(
          Some(HintInfo(strategy = Some(BROADCAST))),
          None) :: Nil
    )
    verifyJoinHint(
      df1.hint("broadcast").join(df2, $"a1" === $"b1").hint("broadcast").join(df3, $"a1" === $"c1"),
      JoinHint(
        Some(HintInfo(strategy = Some(BROADCAST))),
        None) ::
        JoinHint(
          Some(HintInfo(strategy = Some(BROADCAST))),
          None) :: Nil
    )
  }

  test("hint scope") {
    withTempView("a", "b", "c") {
      df1.createOrReplaceTempView("a")
      df2.createOrReplaceTempView("b")
      verifyJoinHint(
        sql(
          """
            |select /*+ broadcast(a, b)*/ * from (
            |  select /*+ broadcast(b)*/ * from a join b on a.a1 = b.b1
            |) a join (
            |  select /*+ broadcast(a)*/ * from a join b on a.a1 = b.b1
            |) b on a.a1 = b.b1
          """.stripMargin),
        JoinHint(
          Some(HintInfo(strategy = Some(BROADCAST))),
          Some(HintInfo(strategy = Some(BROADCAST)))) ::
          JoinHint(
            None,
            Some(HintInfo(strategy = Some(BROADCAST)))) ::
          JoinHint(
            Some(HintInfo(strategy = Some(BROADCAST))),
            None) :: Nil
      )
    }
  }

  test("hints prevent join reorder") {
    withSQLConf(SQLConf.CBO_ENABLED.key -> "true", SQLConf.JOIN_REORDER_ENABLED.key -> "true") {
      withTempView("a", "b", "c") {
        df1.createOrReplaceTempView("a")
        df2.createOrReplaceTempView("b")
        df3.createOrReplaceTempView("c")
        verifyJoinHint(
          sql("select /*+ broadcast(a, c)*/ * from a, b, c " +
            "where a.a1 = b.b1 and b.b1 = c.c1"),
          JoinHint(
            None,
            Some(HintInfo(strategy = Some(BROADCAST)))) ::
            JoinHint(
              Some(HintInfo(strategy = Some(BROADCAST))),
              None) :: Nil
        )
        verifyJoinHint(
          sql("select /*+ broadcast(a, c)*/ * from a, c, b " +
            "where a.a1 = b.b1 and b.b1 = c.c1"),
          JoinHint.NONE ::
            JoinHint(
              Some(HintInfo(strategy = Some(BROADCAST))),
              Some(HintInfo(strategy = Some(BROADCAST)))) :: Nil
        )
        verifyJoinHint(
          sql("select /*+ broadcast(b, c)*/ * from a, c, b " +
            "where a.a1 = b.b1 and b.b1 = c.c1"),
          JoinHint(
            None,
            Some(HintInfo(strategy = Some(BROADCAST)))) ::
            JoinHint(
              None,
              Some(HintInfo(strategy = Some(BROADCAST)))) :: Nil
        )

        verifyJoinHint(
          df1.join(df2, $"a1" === $"b1" && $"a1" > 5).hint("broadcast")
            .join(df3, $"b1" === $"c1" && $"a1" < 10),
          JoinHint(
            Some(HintInfo(strategy = Some(BROADCAST))),
            None) ::
            JoinHint.NONE :: Nil
        )

        verifyJoinHint(
          df1.join(df2, $"a1" === $"b1" && $"a1" > 5).hint("broadcast")
            .join(df3, $"b1" === $"c1" && $"a1" < 10)
            .join(df, $"b1" === $"id"),
          JoinHint.NONE ::
            JoinHint(
              Some(HintInfo(strategy = Some(BROADCAST))),
              None) ::
            JoinHint.NONE :: Nil
        )
      }
    }
  }

  test("intersect/except") {
    val dfSub = spark.range(2)
    verifyJoinHint(
      df.hint("broadcast").except(dfSub).join(df, "id"),
      JoinHint(
        Some(HintInfo(strategy = Some(BROADCAST))),
        None) ::
        JoinHint.NONE :: Nil
    )
    verifyJoinHint(
      df.join(df.hint("broadcast").intersect(dfSub), "id"),
      JoinHint(
        None,
        Some(HintInfo(strategy = Some(BROADCAST)))) ::
        JoinHint.NONE :: Nil
    )
  }

  test("hint merge") {
    verifyJoinHintWithWarnings(
      df.hint("broadcast").filter($"id" > 2).hint("broadcast").join(df, "id"),
      JoinHint(
        Some(HintInfo(strategy = Some(BROADCAST))),
        None) :: Nil,
      Nil
    )
    verifyJoinHintWithWarnings(
      df.join(df.hint("broadcast").limit(2).hint("broadcast"), "id"),
      JoinHint(
        None,
        Some(HintInfo(strategy = Some(BROADCAST)))) :: Nil,
      Nil
    )
    verifyJoinHintWithWarnings(
      df.hint("merge").filter($"id" > 2).hint("shuffle_hash").join(df, "id").hint("broadcast"),
      JoinHint(
        Some(HintInfo(strategy = Some(SHUFFLE_HASH))),
        None) :: Nil,
      msgJoinHintOverridden("merge") ::
        msgNoJoinForJoinHint("broadcast") :: Nil
    )
    verifyJoinHintWithWarnings(
      df.join(df.hint("broadcast").limit(2).hint("merge"), "id")
        .hint("shuffle_hash")
        .hint("shuffle_replicate_nl")
        .join(df, "id"),
      JoinHint(
        Some(HintInfo(strategy = Some(SHUFFLE_REPLICATE_NL))),
        None) ::
        JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_MERGE)))) :: Nil,
      msgJoinHintOverridden("broadcast") ::
        msgJoinHintOverridden("shuffle_hash") :: Nil
    )
  }

  test("hint merge - SQL") {
    withTempView("a", "b", "c") {
      df1.createOrReplaceTempView("a")
      df2.createOrReplaceTempView("b")
      df3.createOrReplaceTempView("c")
      verifyJoinHintWithWarnings(
        sql("select /*+ shuffle_hash merge(a, c) broadcast(a, b)*/ * from a, b, c " +
          "where a.a1 = b.b1 and b.b1 = c.c1"),
        JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_MERGE)))) ::
          JoinHint(
            Some(HintInfo(strategy = Some(SHUFFLE_MERGE))),
            Some(HintInfo(strategy = Some(BROADCAST)))) :: Nil,
        msgNoJoinForJoinHint("shuffle_hash") ::
          msgJoinHintOverridden("broadcast") :: Nil
      )
      verifyJoinHintWithWarnings(
        sql("select /*+ shuffle_hash(a, b) merge(b, d) broadcast(b)*/ * from a, b, c " +
          "where a.a1 = b.b1 and b.b1 = c.c1"),
        JoinHint.NONE ::
          JoinHint(
            Some(HintInfo(strategy = Some(SHUFFLE_HASH))),
            Some(HintInfo(strategy = Some(SHUFFLE_HASH)))) :: Nil,
        msgNoHintRelationFound("d", "merge(b, d)") ::
          msgJoinHintOverridden("broadcast") ::
          msgJoinHintOverridden("merge") :: Nil
      )
      verifyJoinHintWithWarnings(
        sql(
          """
            |select /*+ broadcast(a, c) merge(a, d)*/ * from a
            |join (
            |  select /*+ shuffle_hash(c) shuffle_replicate_nl(b, c)*/ * from b
            |  join c on b.b1 = c.c1
            |) as d
            |on a.a2 = d.b2
          """.stripMargin),
        JoinHint(
          Some(HintInfo(strategy = Some(BROADCAST))),
          Some(HintInfo(strategy = Some(SHUFFLE_MERGE)))) ::
          JoinHint(
            Some(HintInfo(strategy = Some(SHUFFLE_REPLICATE_NL))),
            Some(HintInfo(strategy = Some(SHUFFLE_HASH)))) :: Nil,
        msgNoHintRelationFound("c", "broadcast(a, c)") ::
          msgJoinHintOverridden("merge") ::
          msgJoinHintOverridden("shuffle_replicate_nl") :: Nil
      )
    }
  }

  test("nested hint") {
    verifyJoinHint(
      df.hint("broadcast").hint("broadcast").filter($"id" > 2).join(df, "id"),
      JoinHint(
        Some(HintInfo(strategy = Some(BROADCAST))),
        None) :: Nil
    )
    verifyJoinHint(
      df.hint("shuffle_hash").hint("broadcast").hint("merge").filter($"id" > 2).join(df, "id"),
      JoinHint(
        Some(HintInfo(strategy = Some(SHUFFLE_MERGE))),
        None) :: Nil
    )
  }

  test("hints prevent cost-based join reorder") {
    withSQLConf(SQLConf.CBO_ENABLED.key -> "true", SQLConf.JOIN_REORDER_ENABLED.key -> "true") {
      val join = df.join(df, "id")
      val broadcasted = join.hint("broadcast")
      verifyJoinHint(
        join.join(broadcasted, "id").join(broadcasted, "id"),
        JoinHint(
          None,
          Some(HintInfo(strategy = Some(BROADCAST)))) ::
          JoinHint(
            None,
            Some(HintInfo(strategy = Some(BROADCAST)))) ::
          JoinHint.NONE :: JoinHint.NONE :: JoinHint.NONE :: Nil
      )
    }
  }

  def equiJoinQueryWithHint(hints: Seq[String], joinType: String = "INNER"): String =
    hints.map("/*+ " + _ + " */").mkString(
      "SELECT ", " ", s" * FROM t1 $joinType JOIN t2 ON t1.key = t2.key")

  def nonEquiJoinQueryWithHint(hints: Seq[String], joinType: String = "INNER"): String =
    hints.map("/*+ " + _ + " */").mkString(
      "SELECT ", " ", s" * FROM t1 $joinType JOIN t2 ON t1.key > t2.key")

  private def assertBroadcastHashJoin(df: DataFrame, buildSide: BuildSide): Unit = {
    val executedPlan = df.queryExecution.executedPlan
    val broadcastHashJoins = collect(executedPlan) {
      case b: BroadcastHashJoinExec => b
    }
    assert(broadcastHashJoins.size == 1)
    assert(broadcastHashJoins.head.buildSide == buildSide)
  }

  private def assertBroadcastNLJoin(df: DataFrame, buildSide: BuildSide): Unit = {
    val executedPlan = df.queryExecution.executedPlan
    val broadcastNLJoins = collect(executedPlan) {
      case b: BroadcastNestedLoopJoinExec => b
    }
    assert(broadcastNLJoins.size == 1)
    assert(broadcastNLJoins.head.buildSide == buildSide)
  }

  private def assertShuffleHashJoin(df: DataFrame, buildSide: BuildSide): Unit = {
    val executedPlan = df.queryExecution.executedPlan
    val shuffleHashJoins = collect(executedPlan) {
      case s: ShuffledHashJoinExec => s
    }
    assert(shuffleHashJoins.size == 1)
    assert(shuffleHashJoins.head.buildSide == buildSide)
  }

  private def assertShuffleMergeJoin(df: DataFrame): Unit = {
    val executedPlan = df.queryExecution.executedPlan
    val shuffleMergeJoins = collect(executedPlan) {
      case s: SortMergeJoinExec => s
    }
    assert(shuffleMergeJoins.size == 1)
  }

  private def assertShuffleReplicateNLJoin(df: DataFrame): Unit = {
    val executedPlan = df.queryExecution.executedPlan
    val shuffleReplicateNLJoins = collect(executedPlan) {
      case c: CartesianProductExec => c
    }
    assert(shuffleReplicateNLJoins.size == 1)
  }

  test("join strategy hint - broadcast") {
    withTempView("t1", "t2") {
      Seq((1, "4"), (2, "2")).toDF("key", "value").createTempView("t1")
      Seq((1, "1"), (2, "12.3"), (2, "123")).toDF("key", "value").createTempView("t2")

      val t1Size = spark.table("t1").queryExecution.analyzed.children.head.stats.sizeInBytes
      val t2Size = spark.table("t2").queryExecution.analyzed.children.head.stats.sizeInBytes
      assert(t1Size < t2Size)

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        // Broadcast hint specified on one side
        assertBroadcastHashJoin(
          sql(equiJoinQueryWithHint("BROADCAST(t1)" :: Nil)), BuildLeft)
        assertBroadcastNLJoin(
          sql(nonEquiJoinQueryWithHint("BROADCAST(t2)" :: Nil)), BuildRight)

        // Determine build side based on the join type and child relation sizes
        assertBroadcastHashJoin(
          sql(equiJoinQueryWithHint("BROADCAST(t1, t2)" :: Nil)), BuildLeft)
        assertBroadcastNLJoin(
          sql(nonEquiJoinQueryWithHint("BROADCAST(t1, t2)" :: Nil, "left")), BuildRight)
        assertBroadcastNLJoin(
          sql(nonEquiJoinQueryWithHint("BROADCAST(t1, t2)" :: Nil, "right")), BuildLeft)

        // Use broadcast-hash join if hinted "broadcast" and equi-join
        assertBroadcastHashJoin(
          sql(equiJoinQueryWithHint("BROADCAST(t2)" :: "SHUFFLE_HASH(t1)" :: Nil)), BuildRight)
        assertBroadcastHashJoin(
          sql(equiJoinQueryWithHint("BROADCAST(t1)" :: "MERGE(t1, t2)" :: Nil)), BuildLeft)
        assertBroadcastHashJoin(
          sql(equiJoinQueryWithHint("BROADCAST(t1)" :: "SHUFFLE_REPLICATE_NL(t2)" :: Nil)),
          BuildLeft)

        // Use broadcast-nl join if hinted "broadcast" and non-equi-join
        assertBroadcastNLJoin(
          sql(nonEquiJoinQueryWithHint("SHUFFLE_HASH(t2)" :: "BROADCAST(t1)" :: Nil)), BuildLeft)
        assertBroadcastNLJoin(
          sql(nonEquiJoinQueryWithHint("MERGE(t1)" :: "BROADCAST(t2)" :: Nil)), BuildRight)
        assertBroadcastNLJoin(
          sql(nonEquiJoinQueryWithHint("SHUFFLE_REPLICATE_NL(t1)" :: "BROADCAST(t2)" :: Nil)),
          BuildRight)

        // Broadcast hint specified but not doable
        assertShuffleMergeJoin(
          sql(equiJoinQueryWithHint("BROADCAST(t1)" :: Nil, "left")))
        assertShuffleMergeJoin(
          sql(equiJoinQueryWithHint("BROADCAST(t2)" :: Nil, "right")))
      }
    }
  }

  test("join strategy hint - shuffle-merge") {
    withTempView("t1", "t2") {
      Seq((1, "4"), (2, "2")).toDF("key", "value").createTempView("t1")
      Seq((1, "1"), (2, "12.3"), (2, "123")).toDF("key", "value").createTempView("t2")

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> Int.MaxValue.toString) {
        // Shuffle-merge hint specified on one side
        assertShuffleMergeJoin(
          sql(equiJoinQueryWithHint("SHUFFLE_MERGE(t1)" :: Nil)))
        assertShuffleMergeJoin(
          sql(equiJoinQueryWithHint("MERGEJOIN(t2)" :: Nil)))

        // Shuffle-merge hint specified on both sides
        assertShuffleMergeJoin(
          sql(equiJoinQueryWithHint("MERGE(t1, t2)" :: Nil)))

        // Shuffle-merge hint prioritized over shuffle-hash hint and shuffle-replicate-nl hint
        assertShuffleMergeJoin(
          sql(equiJoinQueryWithHint("SHUFFLE_REPLICATE_NL(t2)" :: "MERGE(t1)" :: Nil, "left")))
        assertShuffleMergeJoin(
          sql(equiJoinQueryWithHint("MERGE(t2)" :: "SHUFFLE_HASH(t1)" :: Nil, "right")))

        // Broadcast hint prioritized over shuffle-merge hint, but broadcast hint is not applicable
        assertShuffleMergeJoin(
          sql(equiJoinQueryWithHint("BROADCAST(t1)" :: "MERGE(t2)" :: Nil, "left")))
        assertShuffleMergeJoin(
          sql(equiJoinQueryWithHint("BROADCAST(t2)" :: "MERGE(t1)" :: Nil, "right")))

        // Shuffle-merge hint specified but not doable
        assertBroadcastNLJoin(
          sql(nonEquiJoinQueryWithHint("MERGE(t1, t2)" :: Nil, "left")), BuildRight)
      }
    }
  }

  test("join strategy hint - shuffle-hash") {
    withTempView("t1", "t2") {
      Seq((1, "4"), (2, "2")).toDF("key", "value").createTempView("t1")
      Seq((1, "1"), (2, "12.3"), (2, "123")).toDF("key", "value").createTempView("t2")

      val t1Size = spark.table("t1").queryExecution.analyzed.children.head.stats.sizeInBytes
      val t2Size = spark.table("t2").queryExecution.analyzed.children.head.stats.sizeInBytes
      assert(t1Size < t2Size)

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> Int.MaxValue.toString) {
        // Shuffle-hash hint specified on one side
        assertShuffleHashJoin(
          sql(equiJoinQueryWithHint("SHUFFLE_HASH(t1)" :: Nil)), BuildLeft)
        assertShuffleHashJoin(
          sql(equiJoinQueryWithHint("SHUFFLE_HASH(t2)" :: Nil)), BuildRight)

        // Determine build side based on the join type and child relation sizes
        assertShuffleHashJoin(
          sql(equiJoinQueryWithHint("SHUFFLE_HASH(t1, t2)" :: Nil)), BuildLeft)
        assertShuffleHashJoin(
          sql(equiJoinQueryWithHint("SHUFFLE_HASH(t1, t2)" :: Nil, "left")), BuildRight)
        assertShuffleHashJoin(
          sql(equiJoinQueryWithHint("SHUFFLE_HASH(t1, t2)" :: Nil, "right")), BuildLeft)

        // Shuffle-hash hint prioritized over shuffle-replicate-nl hint
        assertShuffleHashJoin(
          sql(equiJoinQueryWithHint("SHUFFLE_REPLICATE_NL(t2)" :: "SHUFFLE_HASH(t1)" :: Nil)),
          BuildLeft)

        // Broadcast hint prioritized over shuffle-hash hint, but broadcast hint is not applicable
        assertShuffleHashJoin(
          sql(equiJoinQueryWithHint("BROADCAST(t1)" :: "SHUFFLE_HASH(t2)" :: Nil, "left")),
          BuildRight)
        assertShuffleHashJoin(
          sql(equiJoinQueryWithHint("BROADCAST(t2)" :: "SHUFFLE_HASH(t1)" :: Nil, "right")),
          BuildLeft)

        // Shuffle-hash hint specified but not doable
        assertBroadcastHashJoin(
          sql(equiJoinQueryWithHint("SHUFFLE_HASH(t1)" :: Nil, "left")), BuildRight)
        assertBroadcastNLJoin(
          sql(nonEquiJoinQueryWithHint("SHUFFLE_HASH(t1)" :: Nil)), BuildLeft)
      }
    }
  }

  test("join strategy hint - shuffle-replicate-nl") {
    withTempView("t1", "t2") {
      Seq((1, "4"), (2, "2")).toDF("key", "value").createTempView("t1")
      Seq((1, "1"), (2, "12.3"), (2, "123")).toDF("key", "value").createTempView("t2")

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> Int.MaxValue.toString) {
        // Shuffle-replicate-nl hint specified on one side
        assertShuffleReplicateNLJoin(
          sql(equiJoinQueryWithHint("SHUFFLE_REPLICATE_NL(t1)" :: Nil)))
        assertShuffleReplicateNLJoin(
          sql(equiJoinQueryWithHint("SHUFFLE_REPLICATE_NL(t2)" :: Nil)))

        // Shuffle-replicate-nl hint specified on both sides
        assertShuffleReplicateNLJoin(
          sql(equiJoinQueryWithHint("SHUFFLE_REPLICATE_NL(t1, t2)" :: Nil)))

        // Shuffle-merge hint prioritized over shuffle-replicate-nl hint, but shuffle-merge hint
        // is not applicable
        assertShuffleReplicateNLJoin(
          sql(nonEquiJoinQueryWithHint("MERGE(t1)" :: "SHUFFLE_REPLICATE_NL(t2)" :: Nil)))

        // Shuffle-hash hint prioritized over shuffle-replicate-nl hint, but shuffle-hash hint is
        // not applicable
        assertShuffleReplicateNLJoin(
          sql(nonEquiJoinQueryWithHint("SHUFFLE_HASH(t2)" :: "SHUFFLE_REPLICATE_NL(t1)" :: Nil)))

        // Shuffle-replicate-nl hint specified but not doable
        assertBroadcastHashJoin(
          sql(equiJoinQueryWithHint("SHUFFLE_REPLICATE_NL(t1, t2)" :: Nil, "left")), BuildRight)
        assertBroadcastNLJoin(
          sql(nonEquiJoinQueryWithHint("SHUFFLE_REPLICATE_NL(t1, t2)" :: Nil, "right")), BuildLeft)
      }
    }
  }

  test("Verify that the EliminatedResolvedHint rule is idempotent") {
    withTempView("t1", "t2") {
      Seq((1, "4"), (2, "2")).toDF("key", "value").createTempView("t1")
      Seq((1, "1"), (2, "12.3"), (2, "123")).toDF("key", "value").createTempView("t2")
      val df = sql("SELECT /*+ broadcast(t2) */ * from t1 join t2 ON t1.key = t2.key")
      val optimize = new RuleExecutor[LogicalPlan] {
        val batches = Batch("EliminateResolvedHint", FixedPoint(10), EliminateResolvedHint) :: Nil
      }
      val optimized = optimize.execute(df.logicalPlan)
      val expectedHints =
        JoinHint(
          None,
          Some(HintInfo(strategy = Some(BROADCAST)))) :: Nil
      val joinHints = optimized collect {
        case Join(_, _, _, _, hint) => hint
        case _: ResolvedHint => fail("ResolvedHint should not appear after optimize.")
      }
      assert(joinHints == expectedHints)
    }
  }

  test("SPARK-32220: Non Cartesian Product Join Result Correct with SHUFFLE_REPLICATE_NL hint") {
    withTempView("t1", "t2") {
      Seq((1, "4"), (2, "2")).toDF("key", "value").createTempView("t1")
      Seq((1, "1"), (2, "12.3"), (2, "123")).toDF("key", "value").createTempView("t2")
      val df1 = sql("SELECT /*+ shuffle_replicate_nl(t1) */ * from t1 join t2 ON t1.key = t2.key")
      val df2 = sql("SELECT * from t1 join t2 ON t1.key = t2.key")
      assert(df1.collect().size == df2.collect().size)

      val df3 = sql("SELECT /*+ shuffle_replicate_nl(t1) */ * from t1 join t2")
      val df4 = sql("SELECT * from t1 join t2")
      assert(df3.collect().size == df4.collect().size)

      val df5 = sql("SELECT /*+ shuffle_replicate_nl(t1) */ * from t1 join t2 ON t1.key < t2.key")
      val df6 = sql("SELECT * from t1 join t2 ON t1.key < t2.key")
      assert(df5.collect().size == df6.collect().size)

      val df7 = sql("SELECT /*+ shuffle_replicate_nl(t1) */ * from t1 join t2 ON t1.key < 2")
      val df8 = sql("SELECT * from t1 join t2 ON t1.key < 2")
      assert(df7.collect().size == df8.collect().size)


      val df9 = sql("SELECT /*+ shuffle_replicate_nl(t1) */ * from t1 join t2 ON t2.key < 2")
      val df10 = sql("SELECT * from t1 join t2 ON t2.key < 2")
      assert(df9.collect().size == df10.collect().size)
    }
  }

  test("SPARK-35221: Add join hint build side check") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "true") {
      Seq("left_outer", "left_semi", "left_anti").foreach { joinType =>
        val hintAppender = new LogAppender(s"join hint build side check for $joinType")
        withLogAppender(hintAppender, level = Some(Level.WARN)) {
          assertShuffleMergeJoin(
            df1.hint("BROADCAST").join(df2, $"a1" === $"b1", joinType))
          assertShuffleMergeJoin(
            df1.hint("SHUFFLE_HASH").join(df2, $"a1" === $"b1", joinType))
        }

        val logs = hintAppender.loggingEvents.map(_.getRenderedMessage)
          .filter(_.contains("is not supported in the query:"))
        assert(logs.size == 2)
        logs.forall(_.contains(s"build left for ${joinType.split("_").mkString(" ")} join."))
      }

      Seq("left_outer", "left_semi", "left_anti").foreach { joinType =>
        val hintAppender = new LogAppender(s"join hint build side check for $joinType")
        withLogAppender(hintAppender, level = Some(Level.WARN)) {
          assertBroadcastHashJoin(
            df1.join(df2.hint("BROADCAST"), $"a1" === $"b1", joinType), BuildRight)
          assertShuffleHashJoin(
            df1.join(df2.hint("SHUFFLE_HASH"), $"a1" === $"b1", joinType), BuildRight)
        }

        val logs = hintAppender.loggingEvents.map(_.getRenderedMessage)
          .filter(_.contains("is not supported in the query:"))
        assert(logs.isEmpty)
      }

      Seq("right_outer").foreach { joinType =>
        val hintAppender = new LogAppender(s"join hint build side check for $joinType")
        withLogAppender(hintAppender, level = Some(Level.WARN)) {
          assertShuffleMergeJoin(
            df1.join(df2.hint("BROADCAST"), $"a1" === $"b1", joinType))
          assertShuffleMergeJoin(
            df1.join(df2.hint("SHUFFLE_HASH"), $"a1" === $"b1", joinType))
        }
        val logs = hintAppender.loggingEvents.map(_.getRenderedMessage)
          .filter(_.contains("is not supported in the query:"))
        assert(logs.size == 2)
        logs.forall(_.contains(s"build right for ${joinType.split("_").mkString(" ")} join."))
      }

      Seq("right_outer").foreach { joinType =>
        val hintAppender = new LogAppender(s"join hint build side check for $joinType")
        withLogAppender(hintAppender, level = Some(Level.WARN)) {
          assertBroadcastHashJoin(
            df1.hint("BROADCAST").join(df2, $"a1" === $"b1", joinType), BuildLeft)
          assertShuffleHashJoin(
            df1.hint("SHUFFLE_HASH").join(df2, $"a1" === $"b1", joinType), BuildLeft)
        }
        val logs = hintAppender.loggingEvents.map(_.getRenderedMessage)
          .filter(_.contains("is not supported in the query:"))
        assert(logs.isEmpty)
      }

      Seq("inner", "cross").foreach { joinType =>
        val hintAppender = new LogAppender(s"join hint build side check for $joinType")
        withLogAppender(hintAppender, level = Some(Level.WARN)) {
          assertBroadcastHashJoin(
            df1.hint("BROADCAST").join(df2, $"a1" === $"b1", joinType), BuildLeft)
          assertBroadcastHashJoin(
            df1.join(df2.hint("BROADCAST"), $"a1" === $"b1", joinType), BuildRight)

          assertShuffleHashJoin(
            df1.hint("SHUFFLE_HASH").join(df2, $"a1" === $"b1", joinType), BuildLeft)
          assertShuffleHashJoin(
            df1.join(df2.hint("SHUFFLE_HASH"), $"a1" === $"b1", joinType), BuildRight)
        }
        val logs = hintAppender.loggingEvents.map(_.getRenderedMessage)
          .filter(_.contains("is not supported in the query:"))
        assert(logs.isEmpty)
      }
    }
  }

  test("SPARK-35221: Add join hint non equi-join check") {
    val hintAppender = new LogAppender(s"join hint check for equi-join")
    withLogAppender(hintAppender, level = Some(Level.WARN)) {
      assertBroadcastNLJoin(
        df1.hint("SHUFFLE_HASH").join(df2, $"a1" !== $"b1"), BuildRight)
    }
    withLogAppender(hintAppender, level = Some(Level.WARN)) {
      assertBroadcastNLJoin(
        df1.join(df2.hint("MERGE"), $"a1" !== $"b1"), BuildRight)
    }
    val logs = hintAppender.loggingEvents.map(_.getRenderedMessage)
      .filter(_.contains("is not supported in the query:"))
    assert(logs.size == 2)
    logs.forall(_.contains("no equi-join keys"))
  }

  test("SPARK-36652: AQE dynamic join selection should not apply to non-equi join") {
    val hintAppender = new LogAppender(s"join hint check for equi-join")
    withLogAppender(hintAppender, level = Some(Level.WARN)) {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_MAX_SHUFFLE_HASH_JOIN_LOCAL_MAP_THRESHOLD.key -> "64MB") {
        df1.join(df2.repartition($"b1"), $"a1" =!= $"b1").collect()
      }
      val logs = hintAppender.loggingEvents.map(_.getRenderedMessage)
        .filter(_.contains("is not supported in the query: no equi-join keys"))
      assert(logs.isEmpty)
    }
  }
}
