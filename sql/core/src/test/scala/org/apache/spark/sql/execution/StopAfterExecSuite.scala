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

package org.apache.spark.sql.execution

import java.lang.{Integer => JavaInteger}

import org.apache.spark.sql.{Column, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.StopAfterExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

class StopAfterExecSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  private def checkIfStopAfterExists(df: DataFrame, shouldBeStopAfter: Boolean): Unit = {
    val stopAfter = df.queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }
    if (shouldBeStopAfter) {
      assert(stopAfter.nonEmpty)
    } else {
      assert(stopAfter.isEmpty)
    }
  }

  test("Filter on sorted data: StopAfter should not change filter results") {
    val N = 10L << 5
    // Prevent filter has been pushdown through sort, so persist sort result.
    val sortedDF = spark.range(N).sort("id").persist()

    val schema = new StructType()
      .add("a", IntegerType, nullable = false)
      .add("b", IntegerType, nullable = false)
    val inputData = Seq(1, 2, 3).flatMap(i => (1 to 10).map(j => Row(i, j)))
    val sortedDF2 = spark.createDataFrame(sparkContext.parallelize(inputData, 10), schema)
      .sort(Column("a").asc, Column("b").asc).persist()

    Seq("true", "false").map { codeGen =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codeGen) {
        val df2 = sortedDF.filter("id <= 2").toDF()
        checkIfStopAfterExists(df2, shouldBeStopAfter = true)
        checkAnswer(
          df2,
          Row(0) :: Row(1) :: Row(2) :: Nil)

        val df3 = sortedDF.filter("id < 10").groupBy().sum().toDF()
        checkIfStopAfterExists(df3, shouldBeStopAfter = true)
        checkAnswer(
          df3,
          Row(45) :: Nil)

        val df4 = sortedDF2.filter("a < 3 AND b < 5")
        checkIfStopAfterExists(df4, shouldBeStopAfter = true)
        checkAnswer(
          df4,
          Row(1, 1) :: Row(1, 2) :: Row(1, 3) :: Row(1, 4) ::
          Row(2, 1) :: Row(2, 2) :: Row(2, 3) :: Row(2, 4) :: Nil)
      }
    }
    sortedDF.unpersist()
    sortedDF2.unpersist()
  }

  test("Only do StopAfter if filtering predicates are on the first sort order expression") {
    //   a    b    c    d
    //   1    2    3    4
    //   2    3    4    5
    //   3    4    5    6
    // Only the filtering predicates depending on attribute "a" can be stopped early.
    val df = Seq(1, 2, 3).map(i => (i, i + 1, i + 2, i + 3)).toDF("a", "b", "c", "d")
      .sort($"a", $"c", $"b" + 1, $"d").persist()

    Seq("true", "false").map { codeGen =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codeGen) {
        // filter on "a": StopAfter
        val df2 = df.filter("a <= 2").toDF()
        checkIfStopAfterExists(df2, shouldBeStopAfter = true)
        checkAnswer(
          df2,
          Row(1, 2, 3, 4) :: Row(2, 3, 4, 5) :: Nil)

        // filter on "b": no StopAfter
        val df3 = df.filter("b <= 2").toDF()
        checkIfStopAfterExists(df3, shouldBeStopAfter = false)
        checkAnswer(
          df3,
          Row(1, 2, 3, 4) :: Nil)

        // filter on "c": no StopAfter
        val df4 = df.filter("c <= 4").toDF()
        checkIfStopAfterExists(df4, shouldBeStopAfter = false)
        checkAnswer(
          df4,
          Row(1, 2, 3, 4) :: Row(2, 3, 4, 5) :: Nil)

        // filter on "d": no StopAfter
        val df5 = df.filter("d <= 6").toDF()
        checkIfStopAfterExists(df5, shouldBeStopAfter = false)
        checkAnswer(
          df5,
          Row(1, 2, 3, 4) :: Row(2, 3, 4, 5) :: Row(3, 4, 5, 6) :: Nil)

        // filter on "a" and "b": StopAfter
        val df6 = df.filter("a <= 2 AND b < 3").toDF()
        checkIfStopAfterExists(df6, shouldBeStopAfter = true)
        checkAnswer(
          df6,
          Row(1, 2, 3, 4) :: Nil)

        // filter on "a" and "c": StopAfter
        val df7 = df.filter("a <= 2 AND c <= 5").toDF()
        checkIfStopAfterExists(df7, shouldBeStopAfter = true)
        checkAnswer(
          df7,
          Row(1, 2, 3, 4) :: Row(2, 3, 4, 5) :: Nil)
      }
    }
    df.unpersist()
  }

  test("No StopAfter for non-determinstic predicate") {
    val N = 10L << 5
    val df = spark.range(N).sort("id").persist()
    val df2 = df.filter("id < rand()").toDF()
    checkIfStopAfterExists(df2, shouldBeStopAfter = false)
    df.unpersist()
  }

  test("No StopAfter if predicates only contain non sort-order attribute") {
    val df = Seq(1, 2, 3).map(i => (i, i + 1)).toDF("a", "b").sort("a").persist()
    val df2 = df.filter("a <= 2")
    checkIfStopAfterExists(df2, shouldBeStopAfter = true)

    val df3 = df.filter("b <= 2")
    checkIfStopAfterExists(df3, shouldBeStopAfter = false)

    val df4 = df.filter("a <= 2 AND b <= 2")
    checkIfStopAfterExists(df4, shouldBeStopAfter = true)

    df.unpersist()
  }

  test("StopAfter if child plan is sorted on non-attribute expressions") {
    val df = Seq(1, 2, 3).map(i => (i, i + 1)).toDF("a", "b").sort($"a" + 1).persist()
    val df2 = df.filter("a <= 2")
    checkIfStopAfterExists(df2, shouldBeStopAfter = false)

    val df3 = df.filter("a + 1 <= 2")
    checkIfStopAfterExists(df3, shouldBeStopAfter = true)

    df.unpersist()
  }

  test("No StopAfter if predicate is not binary comparison between attribute and literal") {
    val df = Seq(1, 2, 3).map(i => (i, i + 1)).toDF("a", "b").sort("a", "b").persist()
    // TODO: Once we can normalize "a + 1 <= 2" to "a <= 1" for StopAfter node,
    //       We can change the following test to positive one.
    val df2 = df.filter("a + 1 <= 2")
    checkIfStopAfterExists(df2, shouldBeStopAfter = false)

    val df3 = df.filter("a < 2 AND a + 1 < 5")
    checkIfStopAfterExists(df3, shouldBeStopAfter = true)

    val df4 = df.filter("a is null")
    checkIfStopAfterExists(df4, shouldBeStopAfter = false)

    val df5 = df.filter("a < b")
    checkIfStopAfterExists(df5, shouldBeStopAfter = false)

    df.unpersist()
  }

  test("No StopAfter for disjunctive predicates") {
    val df = Seq(1, 2, 3).map(i => (i, i + 1)).toDF("a", "b").sort("a", "b").persist()

    val df2 = df.filter("a < 2 OR a < 0")
    checkIfStopAfterExists(df2, shouldBeStopAfter = false)

    val df3 = df.filter("(a < 2 AND b < 2) OR a < 0")
    checkIfStopAfterExists(df3, shouldBeStopAfter = false)

    val df4 = df.filter("a < 2 AND (b < 2 OR a < 0)")
    checkIfStopAfterExists(df4, shouldBeStopAfter = true)

    df.unpersist()
  }

  test("No StopAfter if data contains null") {
    val df = Seq((new JavaInteger(1), 2), (null, 3))
      .toDF("a", "b").sort("a", "b").persist()

    val output = df.queryExecution.executedPlan.output

    // attribute "a" is nullable. Can't apply StopAfter with predicate on it.
    assert(output(0).name.equalsIgnoreCase("a"))
    assert(output(0).nullable == true)

    checkIfStopAfterExists(df.filter("a < 2"), shouldBeStopAfter = false)
    checkIfStopAfterExists(df.filter("a < 2 AND b < 2"), shouldBeStopAfter = false)

    df.unpersist()
  }

  test("Only do StopAfter if predicate direction matching sort direction") {
    // Sort in ascending way. Matching LessThan and LessThanOrEqual.
    val df = Seq(1, 2, 3).map(i => (i, i + 1)).toDF("a", "b").sort("a").persist()

    // LessThan
    checkIfStopAfterExists(df.filter("a < 2"), shouldBeStopAfter = true)

    // LessThanOrEqual
    checkIfStopAfterExists(df.filter("a <= 2"), shouldBeStopAfter = true)

    // Sort in descending way. Matching GreaterThan and GreaterThanOrEqual.
    val df2 = Seq(1, 2, 3).map(i => (i, i + 1)).toDF("a", "b").sort($"a".desc).persist()

    // GreaterThan
    checkIfStopAfterExists(df2.filter("a > 2"), shouldBeStopAfter = true)

    // GreaterThanOrEqual
    checkIfStopAfterExists(df2.filter("a >= 2"), shouldBeStopAfter = true)

    // Equal doesn't match ascending or descending ways.
    checkIfStopAfterExists(df.filter("a = 2"), shouldBeStopAfter = false)
    checkIfStopAfterExists(df2.filter("a = 2"), shouldBeStopAfter = false)

    df.unpersist()
    df2.unpersist()
  }

  test("No StopAfter if predicates are not the same direction") {
    val df = Seq(1, 2, 3).map(i => (i, i + 1)).toDF("a", "b").sort("a", "b").persist()

    Seq("true", "false").map { codeGen =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codeGen) {
        // "a <= 1" && "3 > a" can be normalized as "a <= 1" && "a < 3".
        val df2 = df.filter("(a <= 1) AND (3 > a)")
        checkIfStopAfterExists(df2, shouldBeStopAfter = true)
        checkAnswer(
          df2,
          Row(1, 2) :: Nil)
      }
    }

    // "a <= 1" && "3 < a" can be normalized as "a <= 1" && "a > 3".
    val df3 = df.filter("(a <= 1) AND (3 < a)")
    checkIfStopAfterExists(df3, shouldBeStopAfter = false)

    df.unpersist()
  }
}
