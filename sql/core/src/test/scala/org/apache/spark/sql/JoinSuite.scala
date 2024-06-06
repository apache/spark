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

import java.util.Locale

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.TestUtils.{assertNotSpilled, assertSpilled}
import org.apache.spark.internal.config.SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Ascending, GenericRow, SortOrder}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, HintInfo, Join, JoinHint, NO_BROADCAST_AND_REPLICATION}
import org.apache.spark.sql.execution.{BinaryExecNode, FilterExec, ProjectExec, SortExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.{ShuffleExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python.BatchEvalPythonExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
class JoinSuite extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper
  with JoinSelectionHelper {
  import testImplicits._

  setupTestData()

  def statisticSizeInByte(df: DataFrame): BigInt = {
    df.queryExecution.optimizedPlan.stats.sizeInBytes
  }

  test("equi-join is hash-join") {
    val x = testData2.as("x")
    val y = testData2.as("y")
    val join = x.join(y, $"x.a" === $"y.a", "inner").queryExecution.optimizedPlan
    val planned = spark.sessionState.planner.JoinSelection(join)
    assert(planned.size === 1)
  }

  def assertJoin(pair: (String, Class[_ <: BinaryExecNode])): Any = {
    val sqlString = pair._1
    val c = pair._2
    val df = sql(sqlString)
    val optimized = df.queryExecution.optimizedPlan
    val physical = df.queryExecution.sparkPlan
    val operators = physical.collect {
      case j: BroadcastHashJoinExec => j
      case j: ShuffledHashJoinExec => j
      case j: CartesianProductExec => j
      case j: BroadcastNestedLoopJoinExec => j
      case j: SortMergeJoinExec => j
    }

    assert(operators.size === 1)
    if (operators.head.getClass != c) {
      fail(s"$sqlString expected operator: $c, but got ${operators.head}\n physical: \n$physical")
    }
    assert(
      canPlanAsBroadcastHashJoin(optimized.asInstanceOf[Join], conf) ===
        operators.head.isInstanceOf[BroadcastHashJoinExec],
      "canPlanAsBroadcastHashJoin not in sync with join selection codepath!")
    operators.head
  }

  test("NO_BROADCAST_AND_REPLICATION hint is respected in cross joins") {
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      val noBroadcastAndReplicationHint = JoinHint(
        leftHint = None,
        rightHint = Some(HintInfo(Some(NO_BROADCAST_AND_REPLICATION))))

      val join = testData.crossJoin(testData2).queryExecution.optimizedPlan.asInstanceOf[Join]
      val joinWithHint = join.copy(hint = noBroadcastAndReplicationHint)

      val planned = spark.sessionState.planner.JoinSelection(join)
      assert(planned.size == 1)
      assert(planned.head.isInstanceOf[CartesianProductExec])
      assert(!canPlanAsBroadcastHashJoin(join, conf))

      val plannedWithHint = spark.sessionState.planner.JoinSelection(joinWithHint)
      assert(plannedWithHint.size == 1)
      assert(plannedWithHint.head.isInstanceOf[BroadcastNestedLoopJoinExec])
      assert(plannedWithHint.head.asInstanceOf[BroadcastNestedLoopJoinExec].buildSide == BuildLeft)
      assert(!canPlanAsBroadcastHashJoin(joinWithHint, conf))
    }
  }

  test("NO_BROADCAST_AND_REPLICATION hint disables broadcast hash joins") {
    sql("CACHE TABLE testData")
    sql("CACHE TABLE testData2")

    val noBroadcastAndReplicationHint = JoinHint(
      leftHint = Some(HintInfo(Some(NO_BROADCAST_AND_REPLICATION))),
      rightHint = Some(HintInfo(Some(NO_BROADCAST_AND_REPLICATION))))

    val ds = sql("SELECT * FROM testData JOIN testData2 ON key = a")
    val join = ds.queryExecution.optimizedPlan.asInstanceOf[Join]
    val joinWithHint = join.copy(hint = noBroadcastAndReplicationHint)

    val planned = spark.sessionState.planner.JoinSelection(join)
    assert(planned.size == 1)
    assert(planned.head.isInstanceOf[BroadcastHashJoinExec])
    assert(canPlanAsBroadcastHashJoin(join, conf))

    val plannedWithHint = spark.sessionState.planner.JoinSelection(joinWithHint)
    assert(plannedWithHint.size == 1)
    assert(plannedWithHint.head.isInstanceOf[SortMergeJoinExec])
    assert(!canPlanAsBroadcastHashJoin(joinWithHint, conf))
  }

  test("NO_BROADCAST_AND_REPLICATION controls build side in BNLJ") {
    val noBroadcastAndReplicationHint = JoinHint(
      leftHint = None,
      rightHint = Some(HintInfo(Some(NO_BROADCAST_AND_REPLICATION))))

    val ds = testData.join(testData2, $"key" === 1, "left_outer")
    val join = ds.queryExecution.optimizedPlan.asInstanceOf[Join]
    val joinWithHint = join.copy(hint = noBroadcastAndReplicationHint)

    val planned = spark.sessionState.planner.JoinSelection(join)
    assert(planned.size == 1)
    assert(planned.head.isInstanceOf[BroadcastNestedLoopJoinExec])
    assert(planned.head.asInstanceOf[BroadcastNestedLoopJoinExec].buildSide == BuildRight)
    assert(!canPlanAsBroadcastHashJoin(join, conf))

    val plannedWithHint = spark.sessionState.planner.JoinSelection(joinWithHint)
    assert(plannedWithHint.size == 1)
    assert(plannedWithHint.head.isInstanceOf[BroadcastNestedLoopJoinExec])
    assert(plannedWithHint.head.asInstanceOf[BroadcastNestedLoopJoinExec].buildSide == BuildLeft)
    assert(!canPlanAsBroadcastHashJoin(joinWithHint, conf))
  }

  test("join operator selection") {
    spark.sharedState.cacheManager.clearCache()

    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0",
      SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      Seq(
        ("SELECT * FROM testData LEFT SEMI JOIN testData2 ON key = a",
          classOf[SortMergeJoinExec]),
        ("SELECT * FROM testData LEFT SEMI JOIN testData2", classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData JOIN testData2", classOf[CartesianProductExec]),
        ("SELECT * FROM testData JOIN testData2 WHERE key = 2", classOf[CartesianProductExec]),
        ("SELECT * FROM testData LEFT JOIN testData2", classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData RIGHT JOIN testData2", classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData FULL OUTER JOIN testData2", classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData LEFT JOIN testData2 WHERE key = 2",
          classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData RIGHT JOIN testData2 WHERE key = 2",
          classOf[CartesianProductExec]),
        ("SELECT * FROM testData FULL OUTER JOIN testData2 WHERE key = 2",
          classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData JOIN testData2 WHERE key > a", classOf[CartesianProductExec]),
        ("SELECT * FROM testData FULL OUTER JOIN testData2 WHERE key > a",
          classOf[CartesianProductExec]),
        ("SELECT * FROM testData JOIN testData2 ON key = a", classOf[SortMergeJoinExec]),
        ("SELECT * FROM testData JOIN testData2 ON key = a and key = 2",
          classOf[SortMergeJoinExec]),
        ("SELECT * FROM testData JOIN testData2 ON key = a where key = 2",
          classOf[SortMergeJoinExec]),
        ("SELECT * FROM testData LEFT JOIN testData2 ON key = a", classOf[SortMergeJoinExec]),
        ("SELECT * FROM testData RIGHT JOIN testData2 ON key = a where key = 2",
          classOf[SortMergeJoinExec]),
        ("SELECT * FROM testData right join testData2 ON key = a and key = 2",
          classOf[SortMergeJoinExec]),
        ("SELECT * FROM testData full outer join testData2 ON key = a",
          classOf[SortMergeJoinExec]),
        ("SELECT * FROM testData left JOIN testData2 ON (key * a != key + a)",
          classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData right JOIN testData2 ON (key * a != key + a)",
          classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData full JOIN testData2 ON (key * a != key + a)",
          classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData ANTI JOIN testData2 ON key = a", classOf[SortMergeJoinExec]),
        ("SELECT * FROM testData LEFT ANTI JOIN testData2", classOf[BroadcastNestedLoopJoinExec])
      ).foreach(assertJoin)
    }
  }

//  ignore("SortMergeJoin shouldn't work on unsortable columns") {
//    Seq(
//      ("SELECT * FROM arrayData JOIN complexData ON data = a", classOf[ShuffledHashJoin])
//    ).foreach { case (query, joinClass) => assertJoin(query, joinClass) }
//  }

  test("broadcastable join with shuffle join hint") {
    spark.sharedState.cacheManager.clearCache()
    sql("CACHE TABLE testData")
    // Make sure it's planned as broadcast join without the hint.
    assertJoin("SELECT * FROM testData JOIN testData2 ON key = a",
      classOf[BroadcastHashJoinExec])
    assertJoin("SELECT /*+ SHUFFLE_HASH(testData) */ * FROM testData JOIN testData2 ON key = a",
      classOf[ShuffledHashJoinExec])
  }

  test("broadcasted hash join operator selection") {
    spark.sharedState.cacheManager.clearCache()
    sql("CACHE TABLE testData")
    Seq(
      ("SELECT * FROM testData join testData2 ON key = a",
        classOf[BroadcastHashJoinExec]),
      ("SELECT * FROM testData join testData2 ON key = a and key = 2",
        classOf[BroadcastHashJoinExec]),
      ("SELECT * FROM testData join testData2 ON key = a where key = 2",
        classOf[BroadcastHashJoinExec])
    ).foreach(assertJoin)
  }

  test("broadcasted hash outer join operator selection") {
    spark.sharedState.cacheManager.clearCache()
    sql("CACHE TABLE testData")
    sql("CACHE TABLE testData2")
    Seq(
      ("SELECT * FROM testData LEFT JOIN testData2 ON key = a",
        classOf[BroadcastHashJoinExec]),
      ("SELECT * FROM testData RIGHT JOIN testData2 ON key = a where key = 2",
        classOf[BroadcastHashJoinExec]),
      ("SELECT * FROM testData right join testData2 ON key = a and key = 2",
        classOf[BroadcastHashJoinExec])
    ).foreach(assertJoin)
  }

  test("multiple-key equi-join is hash-join") {
    val x = testData2.as("x")
    val y = testData2.as("y")
    val join = x.join(y, ($"x.a" === $"y.a") && ($"x.b" === $"y.b")).queryExecution.optimizedPlan
    val planned = spark.sessionState.planner.JoinSelection(join)
    assert(planned.size === 1)
  }

  test("inner join where, one match per row") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkAnswer(
        upperCaseData.join(lowerCaseData).where($"n" === $"N"),
        Seq(
          Row(1, "A", 1, "a"),
          Row(2, "B", 2, "b"),
          Row(3, "C", 3, "c"),
          Row(4, "D", 4, "d")
        ))
    }
  }

  test("inner join ON, one match per row") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkAnswer(
        upperCaseData.join(lowerCaseData, $"n" === $"N"),
        Seq(
          Row(1, "A", 1, "a"),
          Row(2, "B", 2, "b"),
          Row(3, "C", 3, "c"),
          Row(4, "D", 4, "d")
        ))
    }
  }

  test("inner join, where, multiple matches") {
    val x = testData2.where($"a" === 1).as("x")
    val y = testData2.where($"a" === 1).as("y")
    checkAnswer(
      x.join(y).where($"x.a" === $"y.a"),
      Row(1, 1, 1, 1) ::
      Row(1, 1, 1, 2) ::
      Row(1, 2, 1, 1) ::
      Row(1, 2, 1, 2) :: Nil
    )
  }

  test("inner join, no matches") {
    val x = testData2.where($"a" === 1).as("x")
    val y = testData2.where($"a" === 2).as("y")
    checkAnswer(
      x.join(y).where($"x.a" === $"y.a"),
      Nil)
  }

  test("SPARK-22141: Propagate empty relation before checking Cartesian products") {
    Seq("inner", "left", "right", "left_outer", "right_outer", "full_outer").foreach { joinType =>
      val x = testData2.where($"a" === 2 && !($"a" === 2)).as("x")
      val y = testData2.where($"a" === 1 && !($"a" === 1)).as("y")
      checkAnswer(x.join(y, Seq.empty, joinType), Nil)
    }
  }

  test("big inner join, 4 matches per row") {
    val bigData = testData.union(testData).union(testData).union(testData)
    val bigDataX = bigData.as("x")
    val bigDataY = bigData.as("y")

    checkAnswer(
      bigDataX.join(bigDataY).where($"x.key" === $"y.key"),
      testData.rdd.flatMap { row =>
        Seq.fill(16)(new GenericRow(Seq(row, row).flatMap(_.toSeq).toArray))
      }.collect().toSeq)
  }

  test("cartesian product join") {
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      checkAnswer(
        testData3.join(testData3),
        Row(1, null, 1, null) ::
          Row(1, null, 2, 2) ::
          Row(2, 2, 1, null) ::
          Row(2, 2, 2, 2) :: Nil)
      checkAnswer(
        testData3.as("x").join(testData3.as("y"), $"x.a" > $"y.a"),
        Row(2, 2, 1, null) :: Nil)
    }
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "false") {
      val e = intercept[Exception] {
        checkAnswer(
          testData3.join(testData3),
          Row(1, null, 1, null) ::
            Row(1, null, 2, 2) ::
            Row(2, 2, 1, null) ::
            Row(2, 2, 2, 2) :: Nil)
      }
      assert(e.getMessage.contains("Detected implicit cartesian product for INNER join " +
        "between logical plans"))
    }
  }

  test("left outer join") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkAnswer(
        upperCaseData.join(lowerCaseData, $"n" === $"N", "left"),
        Row(1, "A", 1, "a") ::
          Row(2, "B", 2, "b") ::
          Row(3, "C", 3, "c") ::
          Row(4, "D", 4, "d") ::
          Row(5, "E", null, null) ::
          Row(6, "F", null, null) :: Nil)

      checkAnswer(
        upperCaseData.join(lowerCaseData, $"n" === $"N" && $"n" > 1, "left"),
        Row(1, "A", null, null) ::
          Row(2, "B", 2, "b") ::
          Row(3, "C", 3, "c") ::
          Row(4, "D", 4, "d") ::
          Row(5, "E", null, null) ::
          Row(6, "F", null, null) :: Nil)

      checkAnswer(
        upperCaseData.join(lowerCaseData, $"n" === $"N" && $"N" > 1, "left"),
        Row(1, "A", null, null) ::
          Row(2, "B", 2, "b") ::
          Row(3, "C", 3, "c") ::
          Row(4, "D", 4, "d") ::
          Row(5, "E", null, null) ::
          Row(6, "F", null, null) :: Nil)

      checkAnswer(
        upperCaseData.join(lowerCaseData, $"n" === $"N" && $"l" > $"L", "left"),
        Row(1, "A", 1, "a") ::
          Row(2, "B", 2, "b") ::
          Row(3, "C", 3, "c") ::
          Row(4, "D", 4, "d") ::
          Row(5, "E", null, null) ::
          Row(6, "F", null, null) :: Nil)

      // Make sure we are choosing left.outputPartitioning as the
      // outputPartitioning for the outer join operator.
      checkAnswer(
        sql(
          """
          |SELECT l.N, count(*)
          |FROM uppercasedata l LEFT OUTER JOIN allnulls r ON (l.N = r.a)
          |GROUP BY l.N
          """.stripMargin),
      Row(
        1, 1) ::
        Row(2, 1) ::
        Row(3, 1) ::
        Row(4, 1) ::
        Row(5, 1) ::
        Row(6, 1) :: Nil)

      checkAnswer(
        sql(
          """
            |SELECT r.a, count(*)
            |FROM uppercasedata l LEFT OUTER JOIN allnulls r ON (l.N = r.a)
            |GROUP BY r.a
          """.stripMargin),
        Row(null, 6) :: Nil)
    }
  }

  test("right outer join") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkAnswer(
        lowerCaseData.join(upperCaseData, $"n" === $"N", "right"),
        Row(1, "a", 1, "A") ::
          Row(2, "b", 2, "B") ::
          Row(3, "c", 3, "C") ::
          Row(4, "d", 4, "D") ::
          Row(null, null, 5, "E") ::
          Row(null, null, 6, "F") :: Nil)
      checkAnswer(
        lowerCaseData.join(upperCaseData, $"n" === $"N" && $"n" > 1, "right"),
        Row(null, null, 1, "A") ::
          Row(2, "b", 2, "B") ::
          Row(3, "c", 3, "C") ::
          Row(4, "d", 4, "D") ::
          Row(null, null, 5, "E") ::
          Row(null, null, 6, "F") :: Nil)
      checkAnswer(
        lowerCaseData.join(upperCaseData, $"n" === $"N" && $"N" > 1, "right"),
        Row(null, null, 1, "A") ::
          Row(2, "b", 2, "B") ::
          Row(3, "c", 3, "C") ::
          Row(4, "d", 4, "D") ::
          Row(null, null, 5, "E") ::
          Row(null, null, 6, "F") :: Nil)
      checkAnswer(
        lowerCaseData.join(upperCaseData, $"n" === $"N" && $"l" > $"L", "right"),
        Row(1, "a", 1, "A") ::
          Row(2, "b", 2, "B") ::
          Row(3, "c", 3, "C") ::
          Row(4, "d", 4, "D") ::
          Row(null, null, 5, "E") ::
          Row(null, null, 6, "F") :: Nil)

      // Make sure we are choosing right.outputPartitioning as the
      // outputPartitioning for the outer join operator.
      checkAnswer(
        sql(
          """
            |SELECT l.a, count(*)
            |FROM allnulls l RIGHT OUTER JOIN uppercasedata r ON (l.a = r.N)
            |GROUP BY l.a
          """.stripMargin),
        Row(null,
          6))

      checkAnswer(
        sql(
          """
            |SELECT r.N, count(*)
            |FROM allnulls l RIGHT OUTER JOIN uppercasedata r ON (l.a = r.N)
            |GROUP BY r.N
          """.stripMargin),
        Row(1
          , 1) ::
          Row(2, 1) ::
          Row(3, 1) ::
          Row(4, 1) ::
          Row(5, 1) ::
          Row(6, 1) :: Nil)
    }
  }

  test("full outer join") {
    withTempView("`left`", "`right`") {
      upperCaseData.where($"N" <= 4).createOrReplaceTempView("`left`")
      upperCaseData.where($"N" >= 3).createOrReplaceTempView("`right`")

      val left = UnresolvedRelation(TableIdentifier("left"))
      val right = UnresolvedRelation(TableIdentifier("right"))

      checkAnswer(
        left.join(right, $"left.N" === $"right.N", "full"),
        Row(1, "A", null, null) ::
          Row(2, "B", null, null) ::
          Row(3, "C", 3, "C") ::
          Row(4, "D", 4, "D") ::
          Row(null, null, 5, "E") ::
          Row(null, null, 6, "F") :: Nil)

      checkAnswer(
        left.join(right, ($"left.N" === $"right.N") && ($"left.N" =!= 3), "full"),
        Row(1, "A", null, null) ::
          Row(2, "B", null, null) ::
          Row(3, "C", null, null) ::
          Row(null, null, 3, "C") ::
          Row(4, "D", 4, "D") ::
          Row(null, null, 5, "E") ::
          Row(null, null, 6, "F") :: Nil)

      checkAnswer(
        left.join(right, ($"left.N" === $"right.N") && ($"right.N" =!= 3), "full"),
        Row(1, "A", null, null) ::
          Row(2, "B", null, null) ::
          Row(3, "C", null, null) ::
          Row(null, null, 3, "C") ::
          Row(4, "D", 4, "D") ::
          Row(null, null, 5, "E") ::
          Row(null, null, 6, "F") :: Nil)

      // Make sure we are UnknownPartitioning as the outputPartitioning for the outer join
      // operator.
      checkAnswer(
        sql(
          """
          |SELECT l.a, count(*)
          |FROM allNulls l FULL OUTER JOIN upperCaseData r ON (l.a = r.N)
          |GROUP BY l.a
        """.
            stripMargin),
        Row(null, 10))

      checkAnswer(
        sql(
          """
            |SELECT r.N, count(*)
            |FROM allNulls l FULL OUTER JOIN upperCaseData r ON (l.a = r.N)
            |GROUP BY r.N
          """.stripMargin),
        Row
        (1, 1) ::
          Row(2, 1) ::
          Row(3, 1) ::
          Row(4, 1) ::
          Row(5, 1) ::
          Row(6, 1) ::
          Row(null, 4) :: Nil)

      checkAnswer(
        sql(
          """
            |SELECT l.N, count(*)
            |FROM upperCaseData l FULL OUTER JOIN allNulls r ON (l.N = r.a)
            |GROUP BY l.N
          """.stripMargin),
        Row(1
          , 1) ::
          Row(2, 1) ::
          Row(3, 1) ::
          Row(4, 1) ::
          Row(5, 1) ::
          Row(6, 1) ::
          Row(null, 4) :: Nil)

      checkAnswer(
        sql(
          """
          |SELECT r.a, count(*)
          |FROM upperCaseData l FULL OUTER JOIN allNulls r ON (l.N = r.a)
          |GROUP BY r.a
        """.
            stripMargin),
        Row(null, 10))
    }
  }

  test("broadcasted existence join operator selection") {
    spark.sharedState.cacheManager.clearCache()
    sql("CACHE TABLE testData")

    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> Long.MaxValue.toString) {
      Seq(
        ("SELECT * FROM testData LEFT SEMI JOIN testData2 ON key = a",
          classOf[BroadcastHashJoinExec]),
        ("SELECT * FROM testData ANT JOIN testData2 ON key = a", classOf[BroadcastHashJoinExec])
      ).foreach(assertJoin)
    }

    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      Seq(
        ("SELECT * FROM testData LEFT SEMI JOIN testData2 ON key = a",
          classOf[SortMergeJoinExec]),
        ("SELECT * FROM testData LEFT ANTI JOIN testData2 ON key = a",
          classOf[SortMergeJoinExec])
      ).foreach(assertJoin)
    }

  }

  test("cross join with broadcast") {
    sql("CACHE TABLE testData")

    val sizeInByteOfTestData = statisticSizeInByte(spark.table("testData"))

    // we set the threshold is greater than statistic of the cached table testData
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> (sizeInByteOfTestData + 1).toString(),
      SQLConf.CROSS_JOINS_ENABLED.key -> "true") {

      assert(statisticSizeInByte(spark.table("testData2")) >
        spark.conf.get[Long](SQLConf.AUTO_BROADCASTJOIN_THRESHOLD))

      assert(statisticSizeInByte(spark.table("testData")) <
        spark.conf.get[Long](SQLConf.AUTO_BROADCASTJOIN_THRESHOLD))

      Seq(
        ("SELECT * FROM testData LEFT SEMI JOIN testData2 ON key = a",
          classOf[SortMergeJoinExec]),
        ("SELECT * FROM testData LEFT SEMI JOIN testData2",
          classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData JOIN testData2",
          classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData JOIN testData2 WHERE key = 2",
          classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData LEFT JOIN testData2",
          classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData RIGHT JOIN testData2",
          classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData FULL OUTER JOIN testData2",
          classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData LEFT JOIN testData2 WHERE key = 2",
          classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData RIGHT JOIN testData2 WHERE key = 2",
          classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData FULL OUTER JOIN testData2 WHERE key = 2",
          classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData JOIN testData2 WHERE key > a",
          classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData FULL OUTER JOIN testData2 WHERE key > a",
          classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData left JOIN testData2 WHERE (key * a != key + a)",
          classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData right JOIN testData2 WHERE (key * a != key + a)",
          classOf[BroadcastNestedLoopJoinExec]),
        ("SELECT * FROM testData full JOIN testData2 WHERE (key * a != key + a)",
          classOf[BroadcastNestedLoopJoinExec])
      ).foreach(assertJoin)

      checkAnswer(
        sql(
          """
            SELECT x.value, y.a, y.b FROM testData x JOIN testData2 y WHERE x.key = 2
          """.stripMargin),
        Row("2", 1, 1) ::
        Row("2", 1, 2) ::
        Row("2", 2, 1) ::
        Row("2", 2, 2) ::
        Row("2", 3, 1) ::
        Row("2", 3, 2) :: Nil)

      checkAnswer(
        sql(
          """
            SELECT x.value, y.a, y.b FROM testData x JOIN testData2 y WHERE x.key < y.a
          """.stripMargin),
        Row("1", 2, 1) ::
        Row("1", 2, 2) ::
        Row("1", 3, 1) ::
        Row("1", 3, 2) ::
        Row("2", 3, 1) ::
        Row("2", 3, 2) :: Nil)

      checkAnswer(
        sql(
          """
            SELECT x.value, y.a, y.b FROM testData x JOIN testData2 y ON x.key < y.a
          """.stripMargin),
        Row("1", 2, 1) ::
          Row("1", 2, 2) ::
          Row("1", 3, 1) ::
          Row("1", 3, 2) ::
          Row("2", 3, 1) ::
          Row("2", 3, 2) :: Nil)
    }

  }

  test("left semi join") {
    val df = sql("SELECT * FROM testData2 LEFT SEMI JOIN testData ON key = a")
    checkAnswer(df,
      Row(1, 1) ::
        Row(1, 2) ::
        Row(2, 1) ::
        Row(2, 2) ::
        Row(3, 1) ::
        Row(3, 2) :: Nil)
  }

  test("cross join detection") {
    withTempView("A", "B", "C", "D") {
      testData.createOrReplaceTempView("A")
      testData.createOrReplaceTempView("B")
      testData2.createOrReplaceTempView("C")
      testData3.createOrReplaceTempView("D")
      upperCaseData.where($"N" >= 3).createOrReplaceTempView("`right`")
      val cartesianQueries = Seq(
        /** The following should error out since there is no explicit cross join */
        "SELECT * FROM testData inner join testData2",
        "SELECT * FROM testData left outer join testData2",
        "SELECT * FROM testData right outer join testData2",
        "SELECT * FROM testData full outer join testData2",
        "SELECT * FROM testData, testData2",
        "SELECT * FROM testData, testData2 where testData.key = 1 and testData2.a = 22",
        /** The following should fail because after reordering there are cartesian products */
        "select * from (A join B on (A.key = B.key)) join D on (A.key=D.a) join C",
        "select * from ((A join B on (A.key = B.key)) join C) join D on (A.key = D.a)",
        /** Cartesian product involving C, which is not involved in a CROSS join */
        "select * from ((A join B on (A.key = B.key)) cross join D) join C on (A.key = D.a)");

      def checkCartesianDetection(query: String): Unit = {
        val e = intercept[Exception] {
          checkAnswer(sql(query), Nil);
        }
        assert(e.getMessage.contains("Detected implicit cartesian product"))
      }

      withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "false") {
        cartesianQueries.foreach(checkCartesianDetection)
      }

      // Check that left_semi, left_anti, existence joins without conditions do not throw
      // an exception if cross joins are disabled
      withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "false") {
        checkAnswer(
          sql("SELECT * FROM testData3 LEFT SEMI JOIN testData2"),
          Row(1, null) :: Row (2, 2) :: Nil)
        checkAnswer(
          sql("SELECT * FROM testData3 LEFT ANTI JOIN testData2"),
          Nil)
        checkAnswer(
          sql(
            """
              |SELECT a FROM testData3
              |WHERE
              |  EXISTS (SELECT * FROM testData)
              |OR
              |  EXISTS (SELECT * FROM testData2)""".stripMargin),
          Row(1) :: Row(2) :: Nil)
        checkAnswer(
          sql(
            """
              |SELECT key FROM testData
              |WHERE
              |  key IN (SELECT a FROM testData2)
              |OR
              |  key IN (SELECT a FROM testData3)""".stripMargin),
          Row(1) :: Row(2) :: Row(3) :: Nil)
      }
    }
  }

  test("test SortMergeJoin (without spill)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1",
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD.key -> Int.MaxValue.toString) {

      assertNotSpilled(sparkContext, "inner join") {
        checkAnswer(
          sql("SELECT * FROM testData JOIN testData2 ON key = a where key = 2"),
          Row(2, "2", 2, 1) :: Row(2, "2", 2, 2) :: Nil
        )
      }

      val expected = new ListBuffer[Row]()
      expected.appendAll(Seq(
        Row(1, "1", 1, 1), Row(1, "1", 1, 2),
        Row(2, "2", 2, 1), Row(2, "2", 2, 2),
        Row(3, "3", 3, 1), Row(3, "3", 3, 2))
      )
      for (i <- 4 to 100) {
        expected.append(Row(i, i.toString, null, null))
      }

      assertNotSpilled(sparkContext, "left outer join") {
        checkAnswer(
          sql(
            """
              |SELECT
              |  big.key, big.value, small.a, small.b
              |FROM
              |  testData big
              |LEFT OUTER JOIN
              |  testData2 small
              |ON
              |  big.key = small.a
            """.stripMargin),
          expected.toSeq
        )
      }

      assertNotSpilled(sparkContext, "right outer join") {
        checkAnswer(
          sql(
            """
              |SELECT
              |  big.key, big.value, small.a, small.b
              |FROM
              |  testData2 small
              |RIGHT OUTER JOIN
              |  testData big
              |ON
              |  big.key = small.a
            """.stripMargin),
          expected.toSeq
        )
      }
    }
  }

  test("test SortMergeJoin (with spill)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1",
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "0",
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD.key -> "1") {

      assertSpilled(sparkContext, "inner join") {
        checkAnswer(
          sql("SELECT * FROM testData JOIN testData2 ON key = a where key = 2"),
          Row(2, "2", 2, 1) :: Row(2, "2", 2, 2) :: Nil
        )
      }

      // LEFT SEMI JOIN without bound condition does not spill
      assertNotSpilled(sparkContext, "left semi join") {
        checkAnswer(
          sql("SELECT * FROM testData LEFT SEMI JOIN testData2 ON key = a WHERE key = 2"),
          Row(2, "2") :: Nil
        )
      }

      // LEFT ANTI JOIN without bound condition does not spill
      assertNotSpilled(sparkContext, "left anti join") {
        checkAnswer(
          sql("SELECT * FROM testData LEFT ANTI JOIN testData2 ON key = a WHERE key = 2"),
          Nil
        )
      }

      val expected = new ListBuffer[Row]()
      expected.appendAll(Seq(
        Row(1, "1", 1, 1), Row(1, "1", 1, 2),
        Row(2, "2", 2, 1), Row(2, "2", 2, 2),
        Row(3, "3", 3, 1), Row(3, "3", 3, 2))
      )
      for (i <- 4 to 100) {
        expected.append(Row(i, i.toString, null, null))
      }

      assertSpilled(sparkContext, "left outer join") {
        checkAnswer(
          sql(
            """
              |SELECT
              |  big.key, big.value, small.a, small.b
              |FROM
              |  testData big
              |LEFT OUTER JOIN
              |  testData2 small
              |ON
              |  big.key = small.a
            """.stripMargin),
          expected.toSeq
        )
      }

      assertSpilled(sparkContext, "right outer join") {
        checkAnswer(
          sql(
            """
              |SELECT
              |  big.key, big.value, small.a, small.b
              |FROM
              |  testData2 small
              |RIGHT OUTER JOIN
              |  testData big
              |ON
              |  big.key = small.a
            """.stripMargin),
          expected.toSeq
        )
      }

      // FULL OUTER JOIN still does not use [[ExternalAppendOnlyUnsafeRowArray]]
      // so should not cause any spill
      assertNotSpilled(sparkContext, "full outer join") {
        checkAnswer(
          sql(
            """
              |SELECT
              |  big.key, big.value, small.a, small.b
              |FROM
              |  testData2 small
              |FULL OUTER JOIN
              |  testData big
              |ON
              |  big.key = small.a
            """.stripMargin),
          expected.toSeq
        )
      }
    }
  }

  test("outer broadcast hash join should not throw NPE") {
    withTempView("v1", "v2") {
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
        Seq(2 -> 2).toDF("x", "y").createTempView("v1")

        spark.createDataFrame(
          Seq(Row(1, "a")).asJava,
          new StructType().add("i", "int", nullable = false).add("j", "string", nullable = false)
        ).createTempView("v2")

        checkAnswer(
          sql("select x, y, i, j from v1 left join v2 on x = i and y < length(j)"),
          Row(2, 2, null, null)
        )
      }
    }
  }

  test("test SortMergeJoin output ordering") {
    val joinQueries = Seq(
      "SELECT * FROM testData JOIN testData2 ON key = a",
      "SELECT * FROM testData t1 JOIN " +
        "testData2 t2 ON t1.key = t2.a JOIN testData3 t3 ON t2.a = t3.a",
      "SELECT * FROM testData t1 JOIN " +
        "testData2 t2 ON t1.key = t2.a JOIN " +
        "testData3 t3 ON t2.a = t3.a JOIN " +
        "testData t4 ON t1.key = t4.key")

    def assertJoinOrdering(sqlString: String): Unit = {
      val df = sql(sqlString)
      val physical = df.queryExecution.sparkPlan
      val physicalJoins = physical.collect {
        case j: SortMergeJoinExec => j
      }
      val executed = df.queryExecution.executedPlan
      val executedJoins = collect(executed) {
        case j: SortMergeJoinExec => j
      }
      // This only applies to the above tested queries, in which a child SortMergeJoin always
      // contains the SortOrder required by its parent SortMergeJoin. Thus, SortExec should never
      // appear as parent of SortMergeJoin.
      executed.foreach {
        case s: SortExec => s.foreach {
          case j: SortMergeJoinExec => fail(
            s"No extra sort should be added since $j already satisfies the required ordering"
          )
          case _ =>
        }
        case _ =>
      }
      val joinPairs = physicalJoins.zip(executedJoins)
      val numOfJoins = sqlString.split(" ").count(_.toUpperCase(Locale.ROOT) == "JOIN")
      assert(joinPairs.size == numOfJoins)

      joinPairs.foreach {
        case(join1, join2) =>
          val leftKeys = join1.leftKeys
          val rightKeys = join1.rightKeys
          val outputOrderingPhysical = join1.outputOrdering
          val outputOrderingExecuted = join2.outputOrdering

          // outputOrdering should always contain join keys
          assert(
            SortOrder.orderingSatisfies(
              outputOrderingPhysical, leftKeys.map(SortOrder(_, Ascending))))
          assert(
            SortOrder.orderingSatisfies(
              outputOrderingPhysical, rightKeys.map(SortOrder(_, Ascending))))
          // outputOrdering should be consistent between physical plan and executed plan
          assert(outputOrderingPhysical == outputOrderingExecuted,
            s"Operator $join1 did not have the same output ordering in the physical plan as in " +
            s"the executed plan.")
      }
    }

    joinQueries.foreach(assertJoinOrdering)
  }

  test("SPARK-22445 Respect stream-side child's needCopyResult in BroadcastHashJoin") {
    val df1 = Seq((2, 3), (2, 5), (2, 2), (3, 8), (2, 1)).toDF("k", "v1")
    val df2 = Seq((2, 8), (3, 7), (3, 4), (1, 2)).toDF("k", "v2")
    val df3 = Seq((1, 1), (3, 2), (4, 3), (5, 1)).toDF("k", "v3")

    withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.JOIN_REORDER_ENABLED.key -> "false") {
      val df = df1.join(df2, "k").join(functions.broadcast(df3), "k")
      val plan = df.queryExecution.sparkPlan

      // Check if `needCopyResult` in `BroadcastHashJoin` is correct when smj->bhj
      val joins = new collection.mutable.ArrayBuffer[BinaryExecNode]()
      plan.foreachUp {
        case j: BroadcastHashJoinExec => joins += j
        case j: SortMergeJoinExec => joins += j
        case _ =>
      }
      assert(joins.size == 2)
      assert(joins(0).isInstanceOf[SortMergeJoinExec])
      assert(joins(1).isInstanceOf[BroadcastHashJoinExec])
      checkAnswer(df, Row(3, 8, 7, 2) :: Row(3, 8, 4, 2) :: Nil)
    }
  }

  test("SPARK-24495: Join may return wrong result when having duplicated equal-join keys") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1",
      SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val df1 = spark.range(0, 100, 1, 2)
      val df2 = spark.range(100).select($"id".as("b1"), (- $"id").as("b2"))
      val res = df1.join(df2, $"id" === $"b1" && $"id" === $"b2").select($"b1", $"b2", $"id")
      checkAnswer(res, Row(0, 0, 0))
    }
  }

  test("SPARK-27485: EnsureRequirements should not fail join with duplicate keys") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val tbl_a = spark.range(40)
        .select($"id" as "x", $"id" % 10 as "y")
        .repartition(2, $"x", $"y", $"x")
        .as("tbl_a")

      val tbl_b = spark.range(20)
        .select($"id" as "x", $"id" % 2 as "y1", $"id" % 20 as "y2")
        .as("tbl_b")

      val res = tbl_a
        .join(tbl_b,
          $"tbl_a.x" === $"tbl_b.x" && $"tbl_a.y" === $"tbl_b.y1" && $"tbl_a.y" === $"tbl_b.y2")
        .select($"tbl_a.x")
      checkAnswer(res, Row(0L) :: Row(1L) :: Nil)
    }
  }

  test("SPARK-26352: join reordering should not change the order of columns") {
    withTable("tab1", "tab2", "tab3") {
      spark.sql("select 1 as x, 100 as y").write.saveAsTable("tab1")
      spark.sql("select 42 as i, 200 as j").write.saveAsTable("tab2")
      spark.sql("select 1 as a, 42 as b").write.saveAsTable("tab3")

      val df = spark.sql("""
        with tmp as (select * from tab1 cross join tab2)
        select * from tmp join tab3 on a = x and b = i
      """)
      checkAnswer(df, Row(1, 100, 42, 200, 1, 42))
    }
  }

  test("NaN and -0.0 in join keys") {
    withTempView("v1", "v2", "v3", "v4") {
      Seq(Float.NaN -> Double.NaN, 0.0f -> 0.0, -0.0f -> -0.0).toDF("f", "d").createTempView("v1")
      Seq(Float.NaN -> Double.NaN, 0.0f -> 0.0, -0.0f -> -0.0).toDF("f", "d").createTempView("v2")

      checkAnswer(
        sql(
          """
            |SELECT v1.f, v1.d, v2.f, v2.d
            |FROM v1 JOIN v2
            |ON v1.f = v2.f AND v1.d = v2.d
          """.stripMargin),
        Seq(
          Row(Float.NaN, Double.NaN, Float.NaN, Double.NaN),
          Row(0.0f, 0.0, 0.0f, 0.0),
          Row(0.0f, 0.0, -0.0f, -0.0),
          Row(-0.0f, -0.0, 0.0f, 0.0),
          Row(-0.0f, -0.0, -0.0f, -0.0)))

      // test with complicated join keys.
      checkAnswer(
        sql(
          """
            |SELECT v1.f, v1.d, v2.f, v2.d
            |FROM v1 JOIN v2
            |ON
            |  array(v1.f) = array(v2.f) AND
            |  struct(v1.d) = struct(v2.d) AND
            |  array(struct(v1.f, v1.d)) = array(struct(v2.f, v2.d)) AND
            |  struct(array(v1.f), array(v1.d)) = struct(array(v2.f), array(v2.d))
          """.stripMargin),
        Seq(
          Row(Float.NaN, Double.NaN, Float.NaN, Double.NaN),
          Row(0.0f, 0.0, 0.0f, 0.0),
          Row(0.0f, 0.0, -0.0f, -0.0),
          Row(-0.0f, -0.0, 0.0f, 0.0),
          Row(-0.0f, -0.0, -0.0f, -0.0)))

      // test with tables with complicated-type columns.
      Seq((Array(-0.0f, 0.0f), Tuple2(-0.0d, Double.NaN), Seq(Tuple2(-0.0d, Double.NaN))))
        .toDF("arr", "stru", "arrOfStru").createTempView("v3")
      Seq((Array(0.0f, -0.0f), Tuple2(0.0d, 0.0/0.0), Seq(Tuple2(0.0d, 0.0/0.0))))
        .toDF("arr", "stru", "arrOfStru").createTempView("v4")
      checkAnswer(
        sql(
          """
            |SELECT v3.arr, v3.stru, v3.arrOfStru, v4.arr, v4.stru, v4.arrOfStru
            |FROM v3 JOIN v4
            |ON v3.arr = v4.arr AND v3.stru = v4.stru AND v3.arrOfStru = v4.arrOfStru
          """.stripMargin),
        Seq(Row(
          Seq(-0.0f, 0.0f),
          Row(-0.0d, Double.NaN),
          Seq(Row(-0.0d, Double.NaN)),
          Seq(0.0f, -0.0f),
          Row(0.0d, 0.0/0.0),
          Seq(Row(0.0d, 0.0/0.0)))))
    }
  }

  test("SPARK-28323: PythonUDF should be able to use in join condition") {
    import IntegratedUDFTestUtils._

    assume(shouldTestPythonUDFs)

    val pythonTestUDF = TestPythonUDF(name = "udf")

    val left = Seq((1, 2), (2, 3)).toDF("a", "b")
    val right = Seq((1, 2), (3, 4)).toDF("c", "d")
    val df = left.join(right, pythonTestUDF(left("a")) === pythonTestUDF(right.col("c")))

    val joinNode = find(df.queryExecution.executedPlan)(_.isInstanceOf[BroadcastHashJoinExec])
    assert(joinNode.isDefined)

    // There are two PythonUDFs which use attribute from left and right of join, individually.
    // So two PythonUDFs should be evaluated before the join operator, at left and right side.
    val pythonEvals = collect(joinNode.get) {
      case p: BatchEvalPythonExec => p
    }
    assert(pythonEvals.size == 2)

    checkAnswer(df, Row(1, 2, 1, 2) :: Nil)
  }

  test("SPARK-28345: PythonUDF predicate should be able to pushdown to join") {
    import IntegratedUDFTestUtils._

    assume(shouldTestPythonUDFs)

    val pythonTestUDF = TestPythonUDF(name = "udf")

    val left = Seq((1, 2), (2, 3)).toDF("a", "b")
    val right = Seq((1, 2), (3, 4)).toDF("c", "d")
    val df = left.crossJoin(right).where(pythonTestUDF(left("a")) === right.col("c"))

    // Before optimization, there is a logical Filter operator.
    val filterInAnalysis = df.queryExecution.analyzed.exists(_.isInstanceOf[Filter])
    assert(filterInAnalysis)

    // Filter predicate was pushdown as join condition. So there is no Filter exec operator.
    val filterExec = find(df.queryExecution.executedPlan)(_.isInstanceOf[FilterExec])
    assert(filterExec.isEmpty)

    checkAnswer(df, Row(1, 2, 1, 2) :: Nil)
  }

  test("SPARK-21492: cleanupResource without code generation") {
    withSQLConf(
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val df1 = spark.range(0, 10, 1, 2)
      val df2 = spark.range(10).select($"id".as("b1"), (- $"id").as("b2"))
      val res = df1.join(df2, $"id" === $"b1" && $"id" === $"b2").select($"b1", $"b2", $"id")
      checkAnswer(res, Row(0, 0, 0))
    }
  }

  test("SPARK-29850: sort-merge-join an empty table should not memory leak") {
    val df1 = spark.range(10).select($"id", $"id" % 3 as Symbol("p"))
      .repartition($"id").groupBy($"id").agg(Map("p" -> "max"))
    val df2 = spark.range(0)
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      assert(df2.join(df1, "id").collect().isEmpty)
    }
  }

  test("SPARK-32330: Preserve shuffled hash join build side partitioning") {
    val df1 = spark.range(10).select($"id".as("k1"))
    val df2 = spark.range(30).select($"id".as("k2"))
    Seq("inner", "cross").foreach(joinType => {
      val plan = df1.join(df2.hint("SHUFFLE_HASH"), $"k1" === $"k2", joinType)
        .groupBy($"k1").count()
        .queryExecution.executedPlan
      assert(collect(plan) { case _: ShuffledHashJoinExec => true }.size === 1)
      // No extra shuffle before aggregate
      assert(collect(plan) { case _: ShuffleExchangeExec => true }.size === 2)
    })
  }

  test("SPARK-32383: Preserve hash join (BHJ and SHJ) stream side ordering") {
    val df1 = spark.range(100).select($"id".as("k1"))
    val df2 = spark.range(100).select($"id".as("k2"))
    val df3 = spark.range(3).select($"id".as("k3"))
    val df4 = spark.range(100).select($"id".as("k4"))

    // Test broadcast hash join
    Seq("inner", "left_outer").foreach(joinType => {
      val plan = df1.join(df2.hint("SHUFFLE_MERGE"), $"k1" === $"k2", joinType)
        .join(df3.hint("BROADCAST"), $"k1" === $"k3", joinType)
        .join(df4.hint("SHUFFLE_MERGE"), $"k1" === $"k4", joinType)
        .queryExecution
        .executedPlan
      assert(collect(plan) { case _: SortMergeJoinExec => true }.size === 2)
      assert(collect(plan) { case _: BroadcastHashJoinExec => true }.size === 1)
      // No extra sort before last sort merge join
      assert(collect(plan) { case _: SortExec => true }.size === 3)
    })

    // Test shuffled hash join
    Seq("inner", "left_outer").foreach(joinType => {
      val plan = df1.join(df2.hint("SHUFFLE_MERGE"), $"k1" === $"k2", joinType)
        .join(df3.hint("SHUFFLE_HASH"), $"k1" === $"k3", joinType)
        .join(df4.hint("SHUFFLE_MERGE"), $"k1" === $"k4", joinType)
        .queryExecution
        .executedPlan
      assert(collect(plan) { case _: SortMergeJoinExec => true }.size === 2)
      assert(collect(plan) { case _: ShuffledHashJoinExec => true }.size === 1)
      // No extra sort before last sort merge join
      assert(collect(plan) { case _: SortExec => true }.size === 3)
    })
  }

  test("SPARK-32290: SingleColumn Null Aware Anti Join Optimize") {
    withSQLConf(SQLConf.OPTIMIZE_NULL_AWARE_ANTI_JOIN.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> Long.MaxValue.toString) {
      // positive not in subquery case
      var joinExec = assertJoin((
        "select * from testData where key not in (select a from testData2)",
        classOf[BroadcastHashJoinExec]))
      assert(joinExec.asInstanceOf[BroadcastHashJoinExec].isNullAwareAntiJoin)

      // negative not in subquery case since multi-column is not supported
      assertJoin((
        "select * from testData where (key, key + 1) not in (select * from testData2)",
        classOf[BroadcastNestedLoopJoinExec]))

      // positive hand-written left anti join
      // testData.key nullable false
      // testData3.b nullable true
      joinExec = assertJoin((
        "select * from testData left anti join testData3 ON key = b or isnull(key = b)",
        classOf[BroadcastHashJoinExec]))
      assert(joinExec.asInstanceOf[BroadcastHashJoinExec].isNullAwareAntiJoin)

      // negative hand-written left anti join
      // testData.key nullable false
      // testData2.a nullable false
      // isnull(key = a) will be optimized to true literal and removed
      joinExec = assertJoin((
        "select * from testData left anti join testData2 ON key = a or isnull(key = a)",
        classOf[BroadcastHashJoinExec]))
      assert(!joinExec.asInstanceOf[BroadcastHashJoinExec].isNullAwareAntiJoin)

      // negative hand-written left anti join
      // not match pattern Or(EqualTo(a=b), IsNull(EqualTo(a=b))
      assertJoin((
        "select * from testData2 left anti join testData3 ON testData2.a = testData3.b or " +
          "isnull(testData2.b = testData3.b)",
        classOf[BroadcastNestedLoopJoinExec]))
    }
  }

  test("SPARK-32399: Full outer shuffled hash join") {
    val inputDFs = Seq(
      // Test unique join key
      (spark.range(10).selectExpr("id as k1"),
        spark.range(30).selectExpr("id as k2"),
        $"k1" === $"k2"),
      // Test non-unique join key
      (spark.range(10).selectExpr("id % 5 as k1"),
        spark.range(30).selectExpr("id % 5 as k2"),
        $"k1" === $"k2"),
      // Test empty build side
      (spark.range(10).selectExpr("id as k1").filter("k1 < -1"),
        spark.range(30).selectExpr("id as k2"),
        $"k1" === $"k2"),
      // Test empty stream side
      (spark.range(10).selectExpr("id as k1"),
        spark.range(30).selectExpr("id as k2").filter("k2 < -1"),
        $"k1" === $"k2"),
      // Test empty build and stream side
      (spark.range(10).selectExpr("id as k1").filter("k1 < -1"),
        spark.range(30).selectExpr("id as k2").filter("k2 < -1"),
        $"k1" === $"k2"),
      // Test string join key
      (spark.range(10).selectExpr("cast(id * 3 as string) as k1"),
        spark.range(30).selectExpr("cast(id as string) as k2"),
        $"k1" === $"k2"),
      // Test build side at right
      (spark.range(30).selectExpr("cast(id / 3 as string) as k1"),
        spark.range(10).selectExpr("cast(id as string) as k2"),
        $"k1" === $"k2"),
      // Test NULL join key
      (spark.range(10).map(i => if (i % 2 == 0) i else null).selectExpr("value as k1"),
        spark.range(30).map(i => if (i % 4 == 0) i else null).selectExpr("value as k2"),
        $"k1" === $"k2"),
      (spark.range(10).map(i => if (i % 3 == 0) i else null).selectExpr("value as k1"),
        spark.range(30).map(i => if (i % 5 == 0) i else null).selectExpr("value as k2"),
        $"k1" === $"k2"),
      // Test multiple join keys
      (spark.range(10).map(i => if (i % 2 == 0) i else null).selectExpr(
        "value as k1", "cast(value % 5 as short) as k2", "cast(value * 3 as long) as k3"),
        spark.range(30).map(i => if (i % 3 == 0) i else null).selectExpr(
          "value as k4", "cast(value % 5 as short) as k5", "cast(value * 3 as long) as k6"),
        $"k1" === $"k4" && $"k2" === $"k5" && $"k3" === $"k6")
    )
    inputDFs.foreach { case (df1, df2, joinExprs) =>
      val smjDF = df1.join(df2.hint("SHUFFLE_MERGE"), joinExprs, "full")
      assert(collect(smjDF.queryExecution.executedPlan) {
        case _: SortMergeJoinExec => true }.size === 1)
      val smjResult = smjDF.collect()

      val shjDF = df1.join(df2.hint("SHUFFLE_HASH"), joinExprs, "full")
      assert(collect(shjDF.queryExecution.executedPlan) {
        case _: ShuffledHashJoinExec => true }.size === 1)
      // Same result between shuffled hash join and sort merge join
      checkAnswer(shjDF, smjResult)
    }
  }

  test("SPARK-36612: Support left outer join build left or right outer join build right in " +
    "shuffled hash join") {
    Seq("true", "false").foreach{ codegen =>
      withSQLConf(SQLConf.ENABLE_BUILD_SIDE_OUTER_SHUFFLED_HASH_JOIN_CODEGEN.key -> codegen) {
        val inputDFs = Seq(
          // Test unique join key
          (spark.range(10).selectExpr("id as k1"),
            spark.range(30).selectExpr("id as k2"),
            $"k1" === $"k2"),
          // Test non-unique join key
          (spark.range(10).selectExpr("id % 5 as k1"),
            spark.range(30).selectExpr("id % 5 as k2"),
            $"k1" === $"k2"),
          // Test empty build side
          (spark.range(10).selectExpr("id as k1").filter("k1 < -1"),
            spark.range(30).selectExpr("id as k2"),
            $"k1" === $"k2"),
          // Test empty stream side
          (spark.range(10).selectExpr("id as k1"),
            spark.range(30).selectExpr("id as k2").filter("k2 < -1"),
            $"k1" === $"k2"),
          // Test empty build and stream side
          (spark.range(10).selectExpr("id as k1").filter("k1 < -1"),
            spark.range(30).selectExpr("id as k2").filter("k2 < -1"),
            $"k1" === $"k2"),
          // Test string join key
          (spark.range(10).selectExpr("cast(id * 3 as string) as k1"),
            spark.range(30).selectExpr("cast(id as string) as k2"),
            $"k1" === $"k2"),
          // Test build side at right
          (spark.range(30).selectExpr("cast(id / 3 as string) as k1"),
            spark.range(10).selectExpr("cast(id as string) as k2"),
            $"k1" === $"k2"),
          // Test NULL join key
          (spark.range(10).map(i => if (i % 2 == 0) i else null).selectExpr("value as k1"),
            spark.range(30).map(i => if (i % 4 == 0) i else null).selectExpr("value as k2"),
            $"k1" === $"k2"),
          (spark.range(10).map(i => if (i % 3 == 0) i else null).selectExpr("value as k1"),
            spark.range(30).map(i => if (i % 5 == 0) i else null).selectExpr("value as k2"),
            $"k1" === $"k2"),
          // Test multiple join keys
          (spark.range(10).map(i => if (i % 2 == 0) i else null).selectExpr(
            "value as k1", "cast(value % 5 as short) as k2", "cast(value * 3 as long) as k3"),
            spark.range(30).map(i => if (i % 3 == 0) i else null).selectExpr(
              "value as k4", "cast(value % 5 as short) as k5", "cast(value * 3 as long) as k6"),
            $"k1" === $"k4" && $"k2" === $"k5" && $"k3" === $"k6")
        )

        // test left outer with left side build
        inputDFs.foreach { case (df1, df2, joinExprs) =>
          val smjDF = df1.hint("SHUFFLE_MERGE").join(df2, joinExprs, "leftouter")
          assert(collect(smjDF.queryExecution.executedPlan) {
            case _: SortMergeJoinExec => true
          }.size === 1)
          val smjResult = smjDF.collect()

          val shjDF = df1.hint("SHUFFLE_HASH").join(df2, joinExprs, "leftouter")
          assert(collect(shjDF.queryExecution.executedPlan) {
            case _: ShuffledHashJoinExec => true
          }.size === 1)
          // Same result between shuffled hash join and sort merge join
          checkAnswer(shjDF, smjResult)
        }

        // test right outer with right side build
        inputDFs.foreach { case (df2, df1, joinExprs) =>
          val smjDF = df2.join(df1.hint("SHUFFLE_MERGE"), joinExprs, "rightouter")
          assert(collect(smjDF.queryExecution.executedPlan) {
            case _: SortMergeJoinExec => true
          }.size === 1)
          val smjResult = smjDF.collect()

          val shjDF = df2.join(df1.hint("SHUFFLE_HASH"), joinExprs, "rightouter")
          assert(collect(shjDF.queryExecution.executedPlan) {
            case _: ShuffledHashJoinExec => true
          }.size === 1)
          // Same result between shuffled hash join and sort merge join
          checkAnswer(shjDF, smjResult)
        }
      }
    }
  }

  test("SPARK-32649: Optimize BHJ/SHJ inner/semi join with empty hashed relation") {
    val inputDFs = Seq(
      // Test empty build side for inner join
      (spark.range(30).selectExpr("id as k1"),
        spark.range(10).selectExpr("id as k2").filter("k2 < -1"),
        "inner"),
      // Test empty build side for semi join
      (spark.range(30).selectExpr("id as k1"),
        spark.range(10).selectExpr("id as k2").filter("k2 < -1"),
        "semi")
    )
    inputDFs.foreach { case (df1, df2, joinType) =>
      // Test broadcast hash join
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val bhjCodegenDF = df1.join(df2.hint("BROADCAST"), $"k1" === $"k2", joinType)
        assert(bhjCodegenDF.queryExecution.executedPlan.collect {
          case WholeStageCodegenExec(_ : BroadcastHashJoinExec) => true
          case WholeStageCodegenExec(ProjectExec(_, _ : BroadcastHashJoinExec)) => true
        }.size === 1)
        checkAnswer(bhjCodegenDF, Seq.empty)

        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
          val bhjNonCodegenDF = df1.join(df2, $"k1" === $"k2", joinType)
          assert(bhjNonCodegenDF.queryExecution.executedPlan.collect {
            case _: BroadcastHashJoinExec => true }.size === 1)
          checkAnswer(bhjNonCodegenDF, Seq.empty)
        }
      }

      // Test shuffled hash join
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val shjCodegenDF = df1.join(df2.hint("SHUFFLE_HASH"), $"k1" === $"k2", joinType)
        assert(shjCodegenDF.queryExecution.executedPlan.collect {
          case WholeStageCodegenExec(_ : ShuffledHashJoinExec) => true
          case WholeStageCodegenExec(ProjectExec(_, _ : ShuffledHashJoinExec)) => true
        }.size === 1)
        checkAnswer(shjCodegenDF, Seq.empty)

        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
          val shjNonCodegenDF = df1.join(df2.hint("SHUFFLE_HASH"), $"k1" === $"k2", joinType)
          assert(shjNonCodegenDF.queryExecution.executedPlan.collect {
            case _: ShuffledHashJoinExec => true }.size === 1)
          checkAnswer(shjNonCodegenDF, Seq.empty)
        }
      }
    }
  }

  test("SPARK-34593: Preserve broadcast nested loop join partitioning and ordering") {
    withTable("t1", "t2", "t3", "t4", "t5") {
      spark.range(15).toDF("k").write.bucketBy(4, "k").saveAsTable("t1")
      spark.range(6).toDF("k").write.bucketBy(4, "k").saveAsTable("t2")
      spark.range(8).toDF("k").write.saveAsTable("t3")
      spark.range(9).toDF("k").write.saveAsTable("t4")
      spark.range(11).toDF("k").write.saveAsTable("t5")

      def getAggQuery(selectExpr: String, joinType: String): String = {
        s"""
           |SELECT k, COUNT(*)
           |FROM (SELECT $selectExpr FROM t1 $joinType JOIN t2)
           |GROUP BY k
         """.stripMargin
      }

      // Test output partitioning is preserved
      Seq("INNER", "LEFT OUTER", "RIGHT OUTER", "LEFT SEMI", "LEFT ANTI").foreach {
        joinType =>
          val selectExpr = if (joinType == "RIGHT OUTER") {
            "/*+ BROADCAST(t1) */ t2.k AS k"
          } else {
            "/*+ BROADCAST(t2) */ t1.k as k"
          }
          val plan = sql(getAggQuery(selectExpr, joinType)).queryExecution.executedPlan
          assert(collect(plan) { case _: BroadcastNestedLoopJoinExec => true }.size === 1)
          // No extra shuffle before aggregation
          assert(collect(plan) { case _: ShuffleExchangeExec => true }.size === 0)
      }

      // Test output partitioning is not preserved
      Seq("LEFT OUTER", "RIGHT OUTER", "LEFT SEMI", "LEFT ANTI", "FULL OUTER").foreach {
        joinType =>
          val selectExpr = if (joinType == "RIGHT OUTER") {
            "/*+ BROADCAST(t2) */ t1.k AS k"
          } else {
            "/*+ BROADCAST(t1) */ t1.k as k"
          }
          val plan = sql(getAggQuery(selectExpr, joinType)).queryExecution.executedPlan
          assert(collect(plan) { case _: BroadcastNestedLoopJoinExec => true }.size === 1)
          // Have shuffle before aggregation
          assert(collect(plan) { case _: ShuffleExchangeExec => true }.size === 1)
      }

      def getJoinQuery(selectExpr: String, joinType: String): String = {
        s"""
           |SELECT /*+ MERGE(t3) */ t3.k
           |FROM
           |(
           |  SELECT $selectExpr
           |  FROM
           |    (SELECT /*+ MERGE(t4) */ t1.k AS k1 FROM t1 JOIN t4 ON t1.k = t4.k) AS left_t
           |  $joinType JOIN
           |    (SELECT /*+ MERGE(t5) */ t2.k AS k2 FROM t2 JOIN t5 ON t2.k = t5.k) AS right_t
           |)
           |JOIN t3
           |ON t3.k = k0
         """.stripMargin
      }

      // Test output ordering is preserved
      Seq("INNER", "LEFT OUTER", "RIGHT OUTER", "LEFT SEMI", "LEFT ANTI").foreach {
        joinType =>
          val selectExpr = if (joinType == "RIGHT OUTER") {
            "/*+ BROADCAST(left_t) */ k2 AS k0"
          } else {
            "/*+ BROADCAST(right_t) */ k1 as k0"
          }
          val plan = sql(getJoinQuery(selectExpr, joinType)).queryExecution.executedPlan
          assert(collect(plan) { case _: BroadcastNestedLoopJoinExec => true }.size === 1)
          assert(collect(plan) { case _: SortMergeJoinExec => true }.size === 3)
          // No extra sort on left side before last sort merge join
          assert(collect(plan) { case _: SortExec => true }.size === 5)
      }

      // Test output ordering is not preserved
      Seq("LEFT OUTER", "FULL OUTER").foreach {
        joinType =>
          val selectExpr = "/*+ BROADCAST(left_t) */ k1 as k0"
          val plan = sql(getJoinQuery(selectExpr, joinType)).queryExecution.executedPlan
          assert(collect(plan) { case _: BroadcastNestedLoopJoinExec => true }.size === 1)
          assert(collect(plan) { case _: SortMergeJoinExec => true }.size === 3)
          // Have sort on left side before last sort merge join
          assert(collect(plan) { case _: SortExec => true }.size === 6)
      }

      // Test singe partition
      val fullJoinDF = sql(
        s"""
           |SELECT /*+ BROADCAST(t1) */ COUNT(*)
           |FROM range(0, 10, 1, 1) t1 FULL OUTER JOIN range(0, 10, 1, 1) t2
           |""".stripMargin)
      val plan = fullJoinDF.queryExecution.executedPlan
      assert(collect(plan) { case _: ShuffleExchangeExec => true}.size == 1)
      checkAnswer(fullJoinDF, Row(100))
    }
  }

  test("SPARK-35984: Config to force applying shuffled hash join") {
    val sql = "SELECT * FROM testData JOIN testData2 ON key = a"
    assertJoin(sql, classOf[SortMergeJoinExec])
    withSQLConf("spark.sql.join.forceApplyShuffledHashJoin" -> "true") {
      assertJoin(sql, classOf[ShuffledHashJoinExec])
    }
  }

  test("SPARK-36794: Ignore duplicated key when building relation for semi/anti hash join") {
    withTable("t1", "t2") {
      spark.range(10).map(i => (i.toString, i + 1)).toDF("c1", "c2").write.saveAsTable("t1")
      spark.range(10).map(i => ((i % 5).toString, i % 3)).toDF("c1", "c2").write.saveAsTable("t2")

      val semiJoinQueries = Seq(
        // No join condition, ignore duplicated key.
        (s"SELECT /*+ SHUFFLE_HASH(t2) */ t1.c1 FROM t1 LEFT SEMI JOIN t2 ON t1.c1 = t2.c1",
          true),
        // Have join condition on build join key only, ignore duplicated key.
        (s"""
            |SELECT /*+ SHUFFLE_HASH(t2) */ t1.c1 FROM t1 LEFT SEMI JOIN t2
            |ON t1.c1 = t2.c1 AND CAST(t1.c2 * 2 AS STRING) != t2.c1
          """.stripMargin,
          true),
        // Have join condition on other build attribute beside join key, do not ignore
        // duplicated key.
        (s"""
            |SELECT /*+ SHUFFLE_HASH(t2) */ t1.c1 FROM t1 LEFT SEMI JOIN t2
            |ON t1.c1 = t2.c1 AND t1.c2 * 100 != t2.c2
          """.stripMargin,
          false)
      )
      semiJoinQueries.foreach {
        case (query, ignoreDuplicatedKey) =>
          val semiJoinDF = sql(query)
          val antiJoinDF = sql(query.replaceAll("SEMI", "ANTI"))
          checkAnswer(semiJoinDF, Seq(Row("0"), Row("1"), Row("2"), Row("3"), Row("4")))
          checkAnswer(antiJoinDF, Seq(Row("5"), Row("6"), Row("7"), Row("8"), Row("9")))
          Seq(semiJoinDF, antiJoinDF).foreach { df =>
            assert(collect(df.queryExecution.executedPlan) {
              case j: ShuffledHashJoinExec if j.ignoreDuplicatedKey == ignoreDuplicatedKey => true
            }.size == 1)
          }
      }
    }
  }

  test("SPARK-40487: Make defaultJoin in BroadcastNestedLoopJoinExec running in parallel") {
    withTable("t1", "t2") {
      spark.range(5, 15).toDF("k").write.saveAsTable("t1")
      spark.range(4, 8).toDF("k").write.saveAsTable("t2")

      val queryBuildLeft = "SELECT /*+ BROADCAST(t1) */ *  FROM t1 LEFT JOIN t2 ON t1.k < t2.k"
      val result1 = sql(queryBuildLeft)

      val queryBuildRight = "SELECT /*+ BROADCAST(t2) */ *  FROM t1 LEFT JOIN t2 ON t1.k < t2.k"
      val result2 = sql(queryBuildRight)

      checkAnswer(result1, result2)
    }
  }

  def dupStreamSideColTest(hint: String, check: SparkPlan => Unit): Unit = {
    val query =
      s"""select /*+ ${hint}(r) */ *
         |from testData2 l
         |full outer join testData3 r
         |on l.a = r.a
         |and l.b < (r.b + 1)
         |and l.b < (r.a + 1)""".stripMargin
    val df = sql(query)
    val plan = df.queryExecution.executedPlan
    check(plan)
    val expected = Row(1, 1, null, null) ::
      Row(1, 2, null, null) ::
      Row(null, null, 1, null) ::
      Row(2, 1, 2, 2) ::
      Row(2, 2, 2, 2) ::
      Row(3, 1, null, null) ::
      Row(3, 2, null, null) :: Nil
    checkAnswer(df, expected)
  }

  test("SPARK-43113: Full outer join with duplicate stream-side references in condition (SMJ)") {
    def check(plan: SparkPlan): Unit = {
      assert(collect(plan) { case _: SortMergeJoinExec => true }.size === 1)
    }
    dupStreamSideColTest("MERGE", check)
  }

  test("SPARK-43113: Full outer join with duplicate stream-side references in condition (SHJ)") {
    def check(plan: SparkPlan): Unit = {
      assert(collect(plan) { case _: ShuffledHashJoinExec => true }.size === 1)
    }
    dupStreamSideColTest("SHUFFLE_HASH", check)
  }

  test("SPARK-43718: USING with references to key columns: Full Outer") {
    withTempView("t1", "t2") {
      sql("create or replace temp view t1 as values (1), (2), (3) as (c1)")
      sql("create or replace temp view t2 as values (2), (3), (4) as (c1)")

      val query =
        """select explode(array(t1.c1, t2.c1)) as x1
          |from t1
          |full outer join t2
          |using (c1)
          |""".stripMargin

      val expected = Seq(Row(1), Row(2), Row(2), Row(3), Row(3), Row(4), Row(null), Row(null))

      checkAnswer(sql(query), expected)
    }
  }

  test("SPARK-43718: USING with references to key columns: Left Outer") {
    withTempView("t1", "t2") {
      sql("create or replace temp view t1 as values (1), (2), (3) as (c1)")
      sql("create or replace temp view t2 as values (2), (3), (4) as (c1)")

      val query =
        """select explode(array(t1.c1, t2.c1)) as x1
          |from t1
          |left outer join t2
          |using (c1)
          |""".stripMargin

      val expected = Seq(Row(1), Row(2), Row(2), Row(3), Row(3), Row(null))

      checkAnswer(sql(query), expected)
    }
  }

  test("SPARK-43718: USING with references to key columns: Right Outer") {
    withTempView("t1", "t2") {
      sql("create or replace temp view t1 as values (1), (2), (3) as (c1)")
      sql("create or replace temp view t2 as values (2), (3), (4) as (c1)")

      val query =
        """select explode(array(t1.c1, t2.c1)) as x1
          |from t1
          |right outer join t2
          |using (c1)
          |""".stripMargin

      val expected = Seq(Row(2), Row(2), Row(3), Row(3), Row(4), Row(null))

      checkAnswer(sql(query), expected)
    }
  }

  test("SPARK-44251: Full outer USING join with null key value") {
    withTempView("v1", "v2") {
      sql("create or replace temp view v1 as values (1, 2), (null, 7) as (c1, c2)")
      sql("create or replace temp view v2 as values (2, 3) as (c1, c2)")

      val query =
        """select explode(array(c1)) as x
          |from v1
          |full outer join v2
          |using (c1)
          |""".stripMargin

      val expected = Seq(Row(null), Row(1), Row(2))

      checkAnswer(sql(query), expected)
    }
  }

  test("SPARK-44132: FULL OUTER JOIN by streamed column name fails with NPE") {
    val dsA = Seq((1, "a")).toDF("id", "c1")
    val dsB = Seq((2, "b")).toDF("id", "c2")
    val dsC = Seq((3, "c")).toDF("id", "c3")
    val joined = dsA.join(dsB, LazyList("id"), "full_outer").join(dsC, LazyList("id"), "full_outer")

    val expected = Seq(Row(1, "a", null, null), Row(2, null, "b", null), Row(3, null, null, "c"))

    checkAnswer(joined, expected)
  }

  test("SPARK-44132: FULL OUTER JOIN by streamed column name fails with invalid access") {
    val ds = Seq((1, "a")).toDF("id", "c1")
    val joined = ds.join(ds, LazyList("id"), "full_outer").join(ds, LazyList("id"), "full_outer")

    val expected = Seq(Row(1, "a", "a", "a"))

    checkAnswer(joined, expected)
  }

  test("SPARK-45882: BroadcastHashJoinExec propagate partitioning should respect " +
    "CoalescedHashPartitioning") {
    val cached = spark.sql(
      """
        |select /*+ broadcast(testData) */ key, value, a
        |from testData join (
        | select a from testData2 group by a
        |)tmp on key = a
        |""".stripMargin).cache()
    try {
      val df = cached.groupBy("key").count()
      val expected = Seq(Row(1, 1), Row(2, 1), Row(3, 1))
      assert(find(df.queryExecution.executedPlan) {
        case _: ShuffleExchangeLike => true
        case _ => false
      }.size == 1, df.queryExecution)
      checkAnswer(df, expected)
      assert(find(df.queryExecution.executedPlan) {
        case _: ShuffleExchangeLike => true
        case _ => false
      }.isEmpty, df.queryExecution)
    } finally {
      cached.unpersist()
    }
  }
}

class ThreadLeakInSortMergeJoinSuite
  extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {

  setupTestData()
  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    new TestSparkSession(
      sparkConf.set(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD, 20))
  }

  test("SPARK-47146: thread leak when doing SortMergeJoin (with spill)") {

    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1") {

      assertSpilled(sparkContext, "inner join") {
        sql("SELECT * FROM testData JOIN testData2 ON key = a").collect()
      }

      val readAheadThread = Thread.getAllStackTraces.keySet().asScala
        .find {
          _.getName.startsWith("read-ahead")
        }
      assert(readAheadThread.isEmpty)
    }
  }
}
