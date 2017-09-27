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

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.language.existentials

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.TestUtils.{assertNotSpilled, assertSpilled}

class JoinSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  setupTestData()

  def statisticSizeInByte(df: DataFrame): BigInt = {
    df.queryExecution.optimizedPlan.stats(sqlConf).sizeInBytes
  }

  test("equi-join is hash-join") {
    val x = testData2.as("x")
    val y = testData2.as("y")
    val join = x.join(y, $"x.a" === $"y.a", "inner").queryExecution.optimizedPlan
    val planned = spark.sessionState.planner.JoinSelection(join)
    assert(planned.size === 1)
  }

  def assertJoin(pair: (String, Class[_])): Any = {
    val (sqlString, c) = pair
    val df = sql(sqlString)
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
  }

  test("join operator selection") {
    spark.sharedState.cacheManager.clearCache()

    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "0",
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
    sql("UNCACHE TABLE testData")
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
    sql("UNCACHE TABLE testData")
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
        upperCaseData.join(lowerCaseData).where('n === 'N),
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
      testData.rdd.flatMap(row => Seq.fill(16)(Row.merge(row, row))).collect().toSeq)
  }

  test("cartesian product join") {
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      checkAnswer(
        testData3.join(testData3),
        Row(1, null, 1, null) ::
          Row(1, null, 2, 2) ::
          Row(2, 2, 1, null) ::
          Row(2, 2, 2, 2) :: Nil)
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
      assert(e.getMessage.contains("Detected cartesian product for INNER join " +
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
    upperCaseData.where('N <= 4).createOrReplaceTempView("`left`")
    upperCaseData.where('N >= 3).createOrReplaceTempView("`right`")

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

    sql("UNCACHE TABLE testData")
  }

  test("cross join with broadcast") {
    sql("CACHE TABLE testData")

    val sizeInByteOfTestData = statisticSizeInByte(spark.table("testData"))

    // we set the threshold is greater than statistic of the cached table testData
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> (sizeInByteOfTestData + 1).toString(),
      SQLConf.CROSS_JOINS_ENABLED.key -> "true") {

      assert(statisticSizeInByte(spark.table("testData2")) >
        spark.conf.get(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD))

      assert(statisticSizeInByte(spark.table("testData")) <
        spark.conf.get(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD))

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

    sql("UNCACHE TABLE testData")
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
    testData.createOrReplaceTempView("A")
    testData.createOrReplaceTempView("B")
    testData2.createOrReplaceTempView("C")
    testData3.createOrReplaceTempView("D")
    upperCaseData.where('N >= 3).createOrReplaceTempView("`right`")
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
      assert(e.getMessage.contains("Detected cartesian product"))
    }

    cartesianQueries.foreach(checkCartesianDetection)
  }

  test("test SortMergeJoin (without spill)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1",
      "spark.sql.sortMergeJoinExec.buffer.spill.threshold" -> Int.MaxValue.toString) {

      assertNotSpilled(sparkContext, "inner join") {
        checkAnswer(
          sql("SELECT * FROM testData JOIN testData2 ON key = a where key = 2"),
          Row(2, "2", 2, 1) :: Row(2, "2", 2, 2) :: Nil
        )
      }

      val expected = new ListBuffer[Row]()
      expected.append(
        Row(1, "1", 1, 1), Row(1, "1", 1, 2),
        Row(2, "2", 2, 1), Row(2, "2", 2, 2),
        Row(3, "3", 3, 1), Row(3, "3", 3, 2)
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
          expected
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
          expected
        )
      }
    }
  }

  test("test SortMergeJoin (with spill)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1",
      "spark.sql.sortMergeJoinExec.buffer.in.memory.threshold" -> "0",
      "spark.sql.sortMergeJoinExec.buffer.spill.threshold" -> "1") {

      assertSpilled(sparkContext, "inner join") {
        checkAnswer(
          sql("SELECT * FROM testData JOIN testData2 ON key = a where key = 2"),
          Row(2, "2", 2, 1) :: Row(2, "2", 2, 2) :: Nil
        )
      }

      val expected = new ListBuffer[Row]()
      expected.append(
        Row(1, "1", 1, 1), Row(1, "1", 1, 2),
        Row(2, "2", 2, 1), Row(2, "2", 2, 2),
        Row(3, "3", 3, 1), Row(3, "3", 3, 2)
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
          expected
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
          expected
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
          expected
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
}
