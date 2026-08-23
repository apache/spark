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
import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, Expression, Multiply, TranspiledPythonUDF, TranspiledUDFParameter}
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

/**
 * Tests how `UserDefinedPythonFunction.builder` marks the `_udf_param_N` placeholders it fills in
 * (SPARK-58626). The builder owns this because it is the last place that still sees the
 * placeholders: filling them in is what erases which copy came from which parameter, and the
 * indexes it stamps on are what tell `ConvertToCatalyst` which copies share one evaluation.
 *
 * Plus two operator shapes the Python suite does not assert plans for: a call as a grouping key,
 * which does share, and one in a predicate, which does not. Both assert on the `_common_expr`
 * column, since a deterministic argument gives the same answer either way.
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
  private def udfWith(option: Expression, arity: Int = 2): UserDefinedPythonFunction =
    UserDefinedPythonFunction(
      "udf",
      null,
      LongType,
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
      val plan = df.queryExecution.optimizedPlan.toString
      assert(plan.contains("_common_expr"), s"Expected a shared argument column:\n$plan")
      checkAnswer(df, Seq(Row(0L, 2L), Row(1L, 2L), Row(4L, 2L)))
    }
  }

  test("puts a deterministic argument in a predicate back inline") {
    transpileOn {
      // `PushPredicateThroughNonJoin` pushes a Filter through a Project only when every field is
      // deterministic, so a deterministic argument's column is substituted back into the condition
      // and evaluated at both use sites. Only extra work: same rows either way. A nondeterministic
      // argument blocks that push and keeps its column -- covered in the Python suite, where a draw
      // count is observable.
      val square = udfWith(Multiply(param(0), param(0)), arity = 1)
      val df = spark.range(0, 6).filter(square(col("id") % 3) > 1)
      val plan = df.queryExecution.optimizedPlan.toString
      assert(!plan.contains("_common_expr"), s"Expected the argument inlined:\n$plan")
      checkAnswer(df.select("id"), Seq(Row(2L), Row(5L)))
    }
  }
}
