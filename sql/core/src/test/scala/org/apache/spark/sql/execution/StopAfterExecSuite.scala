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

import org.apache.spark.sql.{Column, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.StopAfterExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

class StopAfterExecSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

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
        assert(df2.queryExecution.executedPlan.collect {
          case _: StopAfterExec => true
        }.nonEmpty)
        checkAnswer(
          df2,
          Row(0) :: Row(1) :: Row(2) :: Nil)

        val df3 = sortedDF.filter("id < 10").groupBy().sum().toDF()
        assert(df3.queryExecution.executedPlan.collect {
          case _: StopAfterExec => true
        }.nonEmpty)
        checkAnswer(
          df3,
          Row(45) :: Nil)

        val df4 = sortedDF2.filter("a < 3 AND b < 5")
        assert(df4.queryExecution.executedPlan.collect {
          case _: StopAfterExec => true
        }.nonEmpty)
        checkAnswer(
          df4,
          Row(1, 1) :: Row(1, 2) :: Row(1, 3) :: Row(1, 4) ::
          Row(2, 1) :: Row(2, 2) :: Row(2, 3) :: Row(2, 4) :: Nil)
      }
    }
    sortedDF.unpersist()
    sortedDF2.unpersist()
  }

  test("Only do StopAfter if predicates are on the prefix sort by attributes") {
    // int int2 int3 int4
    //   1    2    3    4
    //   2    3    4    5
    //   3    4    5    6
    val df = Seq(1, 2, 3).map(i => (i, i + 1, i + 2, i + 3)).toDF("int", "int2", "int3", "int4")
      .sort($"int", $"int3", $"int2" + 1, $"int4").persist()

    Seq("true", "false").map { codeGen =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codeGen) {
        // filter on "int": StopAfter
        val df2 = df.filter("int <= 2").toDF()
        assert(df2.queryExecution.executedPlan.collect {
          case _: StopAfterExec => true
        }.nonEmpty)
        checkAnswer(
          df2,
          Row(1, 2, 3, 4) :: Row(2, 3, 4, 5) :: Nil)

        // filter on "int2": no StopAfter
        val df3 = df.filter("int2 <= 2").toDF()
        assert(df3.queryExecution.executedPlan.collect {
          case _: StopAfterExec => true
        }.isEmpty)
        checkAnswer(
          df3,
          Row(1, 2, 3, 4) :: Nil)

        // filter on "int3": no StopAfter
        val df4 = df.filter("int3 <= 4").toDF()
        assert(df4.queryExecution.executedPlan.collect {
          case _: StopAfterExec => true
        }.isEmpty)
        checkAnswer(
          df4,
          Row(1, 2, 3, 4) :: Row(2, 3, 4, 5) :: Nil)

        // filter on "int4": no StopAfter
        val df5 = df.filter("int4 <= 6").toDF()
        assert(df5.queryExecution.executedPlan.collect {
          case _: StopAfterExec => true
        }.isEmpty)
        checkAnswer(
          df5,
          Row(1, 2, 3, 4) :: Row(2, 3, 4, 5) :: Row(3, 4, 5, 6) :: Nil)

        // filter on "int" and "int2": StopAfter
        val df6 = df.filter("int <= 2 AND int2 < 3").toDF()
        assert(df6.queryExecution.executedPlan.collect {
          case _: StopAfterExec => true
        }.nonEmpty)
        checkAnswer(
          df6,
          Row(1, 2, 3, 4) :: Nil)

        // filter on "int" and "int3": StopAfter
        val df7 = df.filter("int <= 2 AND int3 <= 5").toDF()
        assert(df7.queryExecution.executedPlan.collect {
          case _: StopAfterExec => true
        }.nonEmpty)
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
    assert(df2.queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.isEmpty)
    df.unpersist()
  }

  test("No StopAfter if predicates contain non sort-order attribute") {
    val df = Seq(1, 2, 3).map(i => (i, i + 1)).toDF("int", "int2").sort("int").persist()
    val df2 = df.filter("int <= 2")
    assert(df2.queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.nonEmpty)

    val df3 = df.filter("int2 <= 2")
    assert(df3.queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.isEmpty)

    val df4 = df.filter("int <= 2 AND int2 <= 2")
    assert(df4.queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.nonEmpty)

    df.unpersist()
  }

  test("StopAfter if child plan is sorted on non-attribute expressions") {
    val df = Seq(1, 2, 3).map(i => (i, i + 1)).toDF("int", "int2").sort($"int" + 1).persist()
    val df2 = df.filter("int <= 2")
    assert(df2.queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.isEmpty)

    val df3 = df.filter("int + 1 <= 2")
    assert(df3.queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.nonEmpty)

    df.unpersist()
  }

  test("No StopAfter if predicate is not binary comparison between attribute and literal") {
    val df = Seq(1, 2, 3).map(i => (i, i + 1)).toDF("int", "int2").sort("int", "int2").persist()
    // TODO: Once we can normalize "int + 1 <= 2" to "int <= 1" for StopAfter node,
    //       We can change the following test to positive one.
    val df2 = df.filter("int + 1 <= 2")
    assert(df2.queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.isEmpty)

    val df3 = df.filter("int < 2 AND int + 1 < 5")
    assert(df3.queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.nonEmpty)

    val df4 = df.filter("int is null")
    assert(df4.queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.isEmpty)

    val df5 = df.filter("int < int2")
    assert(df5.queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.isEmpty)

    df.unpersist()
  }

  test("No StopAfter for disjunctive predicates") {
    val df = Seq(1, 2, 3).map(i => (i, i + 1)).toDF("int", "int2").sort("int", "int2").persist()

    val df1 = df.filter("int < 2 OR int < 0")
    assert(df1.queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.isEmpty)

    val df2 = df.filter("(int < 2 AND int2 < 2) OR int < 0")
    assert(df2.queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.isEmpty)

    val df3 = df.filter("int < 2 AND (int2 < 2 OR int < 0)")
    assert(df3.queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.nonEmpty)

    df.unpersist()
  }

  test("No StopAfter if data contains null") {
    val df = Seq((new JavaInteger(1), 2), (null, 3))
      .toDF("int", "int2").sort("int", "int2").persist()
    val output = df.queryExecution.executedPlan.output

    // attribute "int" is nullable. Can't apply StopAfter with predicate on it.
    assert(output(0).name.equalsIgnoreCase("int"))
    assert(output(0).nullable == true)
    assert(df.filter("int < 2").queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.isEmpty)

    assert(df.filter("int < 2 AND int2 < 2").queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.isEmpty)

    df.unpersist()
  }

  test("Only do StopAfter if predicate direction matching sort direction") {
    // Sort in ascending way. Matching LessThan and LessThanOrEqual.
    val df = Seq(1, 2, 3).map(i => (i, i + 1)).toDF("int", "int2").sort("int").persist()

    // LessThan
    assert(df.filter("int < 2").queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.nonEmpty)

    // LessThanOrEqual
    assert(df.filter("int <= 2").queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.nonEmpty)

    // Sort in descending way. Matching GreaterThan and GreaterThanOrEqual.
    val df2 = Seq(1, 2, 3).map(i => (i, i + 1)).toDF("int", "int2").sort($"int".desc).persist()

    // GreaterThan
    assert(df2.filter("int > 2").queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.nonEmpty)

    // GreaterThanOrEqual
    assert(df2.filter("int >= 2").queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.nonEmpty)

    // Equal doesn't match ascending or descending ways.
    assert(df.filter("int = 2").queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.isEmpty)

    assert(df2.filter("int = 2").queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.isEmpty)

    df.unpersist()
    df2.unpersist()
  }

  test("No StopAfter if predicates are not the same direction") {
    val df = Seq(1, 2, 3).map(i => (i, i + 1)).toDF("int", "int2").sort("int", "int2").persist()

    Seq("true", "false").map { codeGen =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codeGen) {
        // "int <= 1" && "3 > int" can be normalized as "int <= 1" && "int < 3".
        val df2 = df.filter("(int <= 1) AND (3 > int)")
        assert(df2.queryExecution.executedPlan.collect {
          case _: StopAfterExec => true
        }.nonEmpty)
        checkAnswer(
          df2,
          Row(1, 2) :: Nil)
      }
    }

    // "int <= 1" && "3 < int" can be normalized as "int <= 1" && "int > 3".
    val df3 = df.filter("(int <= 1) AND (3 < int)")
    assert(df3.queryExecution.executedPlan.collect {
      case _: StopAfterExec => true
    }.isEmpty)

    df.unpersist()
  }
}
