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

package org.apache.spark.sql.execution.python

import scala.jdk.CollectionConverters._

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.{Column, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, Expression, Multiply, Subtract, TranspiledPythonUDF, TranspiledUDFParameter}
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.functions.{col, rand}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, DoubleType, LongType}

/**
 * Tests how `UserDefinedPythonFunction.builder` marks the `_udf_param_N` placeholders it fills in
 * (SPARK-58626). The builder owns this because it is the last place that still sees the
 * placeholders: filling them in is what erases which copy came from which parameter, and the
 * indexes it stamps on are what tell `ConvertToCatalyst` which copies share one evaluation.
 *
 * Plus the two operator shapes whose plans the Python suites do not reach: a call used as an
 * [[org.apache.spark.sql.catalyst.plans.logical.Aggregate]]'s grouping key, and one under a
 * [[org.apache.spark.sql.catalyst.plans.logical.Filter]], which is the case where
 * `RewriteWithExpression` has to add a Project on each side of the operator. Everything else
 * end-to-end lives in `pyspark.sql.tests.test_udf_transpile_unit`, which asserts on the optimized
 * plan directly and would only be duplicated here.
 */
class TranspiledUDFParameterSuite extends QueryTest with SharedSparkSession {

  private val argA = AttributeReference("a", LongType)()
  private val argB = AttributeReference("b", LongType)()

  private def param(index: Int): Expression = UnresolvedAttribute(s"_udf_param_$index")

  /**
   * A transpiled UDF of `arity` numeric parameters carrying the given option body. `func` is null
   * because a call that transpiles never reaches Python -- so a fallback shows up as an NPE rather
   * than as a quietly passing test.
   */
  private def udfWith(
      option: Expression,
      arity: Int = 2,
      returnType: DataType = LongType): UserDefinedPythonFunction =
    UserDefinedPythonFunction(
      "udf",
      null,
      returnType,
      PythonEvalType.SQL_BATCHED_UDF,
      udfDeterministic = true,
      List(Column(option)).asJava,
      List(List.fill(arity)("numeric").asJava).asJava)

  /** The indexes of the markers in the single option the builder produced, in tree order. */
  private def markerIndexes(option: Expression, args: Expression*): Seq[Int] =
    udfWith(option).builder(args) match {
      case t: TranspiledPythonUDF =>
        t.transpiledOptions.head.collect { case p: TranspiledUDFParameter => p.index }
      case other => fail(s"Expected a TranspiledPythonUDF, got: $other")
    }

  private def transpileOn(f: => Unit): Unit = withSQLConf(
    SQLConf.ATTEMPT_TRANSPILATION_OF_PYTHON_UDFS.key -> "true",
    SQLConf.ANSI_ENABLED.key -> "true")(f)

  test("marks every copy of every parameter with the parameter it came from") {
    // `lambda a, b: a * a + b`: `a` twice and `b` once, all marked -- the Python eval operator this
    // replaces computes a column per argument whether the body reads it once or twice. Binding both
    // parameters to the same argument leaves three identical copies, so the indexes are then the
    // only thing saying which two belong to the repeated parameter.
    val option = Add(Multiply(param(0), param(0)), param(1))
    Seq(argB, argA).foreach { second =>
      assert(markerIndexes(option, argA, second) == Seq(0, 0, 1),
        s"Expected a, a, b marked when b is bound to $second")
    }
  }

  test("drops a parameter the body never uses") {
    // `lambda a, b: a + a`: b never reaches the option, so nothing evaluates it.
    val option = Add(param(0), param(0))
    assert(markerIndexes(option, argA, argB) == Seq(0, 0))
  }

  test("shares an argument that appears in both a grouping and a result expression") {
    transpileOn {
      // `groupBy(f(x)).count()` puts one call in both the grouping and the result expressions, and
      // ConvertToCatalyst rewrites one top-level expression at a time, so each gets its own `With`
      // with its own ids. PhysicalAggregation pairs them by semanticEquals, which only lines up
      // because `With.canonicalized` renumbers ids -- otherwise the Aggregate fails to bind.
      // `id % 3` rather than a bare column so the argument is not cheap enough to be inlined.
      val square = udfWith(Multiply(param(0), param(0)), arity = 1)
      val df = spark.range(0, 6).groupBy(square(col("id") % 3).as("sq")).count()
      checkAnswer(df, Seq(Row(0L, 2L), Row(1L, 2L), Row(4L, 2L)))
    }
  }

  test("shares an argument under a Filter") {
    transpileOn {
      // `lambda a: a - a` over `rand()` again, because a deterministic argument would give the same
      // answer shared or not and the test would pass with sharing switched off entirely. Every row
      // survives the filter only if the two uses read one draw.
      val diff = udfWith(Subtract(param(0), param(0)), arity = 1, returnType = DoubleType)
      val df = spark.range(0, 30).filter(diff(rand(seed = 2L)) === 0.0d)
      assert(df.count() === 30)
    }
  }
}
