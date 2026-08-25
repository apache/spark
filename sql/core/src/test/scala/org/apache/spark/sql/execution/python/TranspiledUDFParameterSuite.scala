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
 * Tests how `UserDefinedPythonFunction.builder` turns the `_udf_param_N` placeholders into
 * references to the UDF's arguments (SPARK-58626). The builder owns this because it is the last
 * place that still sees the placeholders.
 *
 * Plus the two operator shapes that decide whether an argument is pre-evaluated at all, asserted
 * on the plan rather than the values, since a deterministic argument gives the same answer either
 * way: a Project, which does get a column, and an Aggregate, which does not.
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

  /** The argument indexes the single option the builder produced refers to, in tree order. */
  private def parameterIndexes(option: Expression, args: Expression*): Seq[Int] =
    udfWith(option).builder(args) match {
      case t: TranspiledPythonUDF =>
        t.transpiledOptions.head.collect { case p: TranspiledUDFParameter => p.index }
      case other => fail(s"Expected a TranspiledPythonUDF, got: $other")
    }

  private def transpileOn(f: => Unit): Unit = withSQLConf(
    SQLConf.ATTEMPT_TRANSPILATION_OF_PYTHON_UDFS.key -> "true",
    SQLConf.ANSI_ENABLED.key -> "true")(f)

  test("refers to every parameter the body reads, once per read") {
    // `lambda a, b: a * a + b`: two references to `a` and one to `b`. Binding both parameters to
    // the same argument changes nothing here -- the option refers to parameters, not to arguments,
    // so which argument each stands for is settled later.
    val option = Add(Multiply(param(0), param(0)), param(1))
    Seq(argB, argA).foreach { second =>
      assert(parameterIndexes(option, argA, second) == Seq(0, 0, 1),
        s"Expected a, a, b referenced when b is bound to $second")
    }
  }

  test("drops a parameter the body never uses") {
    // `lambda a, b: a + a`: b never reaches the option, so nothing evaluates it.
    val option = Add(param(0), param(0))
    assert(parameterIndexes(option, argA, argB) == Seq(0, 0))
  }

  test("pre-evaluates an argument two references read") {
    transpileOn {
      // `id % 3` rather than a bare column so the argument is not cheap enough for CollapseProject
      // to fold the pre-evaluating Project back into its parent. Count the column *definitions*:
      // one `AS _udf_param_0` read twice by the body is the shape we want.
      val square = udfWith(Multiply(param(0), param(0)), arity = 1)
      val df = spark.range(0, 6).select(square(col("id") % 3).as("sq"))
      val plan = df.queryExecution.optimizedPlan.toString
      assert("AS _udf_param_0".r.findAllIn(plan).length == 1,
        s"Expected exactly one pre-evaluated argument column:\n$plan")
      checkAnswer(df, Seq(Row(0L), Row(1L), Row(4L), Row(0L), Row(1L), Row(4L)))
    }
  }

  test("leaves an Aggregate's arguments at their use sites") {
    transpileOn {
      // An Aggregate gets no pre-evaluated column: a result expression no aggregate function wraps
      // has to be built from the grouping expressions, and reading a column instead is not.
      //
      // The grouping key has to be the bare argument, NOT the call. With the call as the key, the
      // grouping and the result expressions would both read one column -- the dedup is shared
      // across the whole operator -- and bind fine, so that shape proves nothing. Here `id % 3` is
      // the key and only the result would read a column, which is what fails MISSING_AGGREGATION.
      // And `id % 3` rather than a bare column, so the argument is not left inline merely for being
      // cheap, which would make the assertion pass for the wrong reason.
      val square = udfWith(Multiply(param(0), param(0)), arity = 1)
      val df = spark.range(0, 6).groupBy(col("id") % 3).agg(square(col("id") % 3).as("sq"))
      val plan = df.queryExecution.optimizedPlan.toString
      assert(!plan.contains("_udf_param_"), s"Expected no column under an Aggregate:\n$plan")
      checkAnswer(df, Seq(Row(0L, 0L), Row(1L, 1L), Row(2L, 4L)))
    }
  }
}
