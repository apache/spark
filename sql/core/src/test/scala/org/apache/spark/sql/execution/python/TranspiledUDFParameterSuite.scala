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
import org.apache.spark.sql.Column
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, Expression, Literal, Multiply, TranspiledPythonUDF, TranspiledUDFParameter}
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

/**
 * Tests which `_udf_param_N` placeholders `UserDefinedPythonFunction.builder` marks with a
 * [[TranspiledUDFParameter]] (SPARK-58626).
 *
 * The builder owns this decision because it is the last place that still sees the placeholders:
 * resolving them into the bound arguments is what erases which copy came from which parameter. So
 * what the builder marks is what `PreEvaluateTranspiledUDFInputs` can pre-evaluate, and the ids it
 * hands out are what say which copies read the same column.
 */
class TranspiledUDFParameterSuite extends QueryTest with SharedSparkSession {

  private val argA = AttributeReference("a", LongType)()
  private val argB = AttributeReference("b", LongType)()

  private def param(index: Int): Expression = UnresolvedAttribute(s"_udf_param_$index")

  /** A two-parameter transpiled UDF carrying the given option bodies. */
  private def udfWith(options: Expression*): UserDefinedPythonFunction =
    UserDefinedPythonFunction(
      "udf",
      null,
      LongType,
      PythonEvalType.SQL_BATCHED_UDF,
      udfDeterministic = true,
      options.map(Column(_)).asJava,
      options.map(_ => List("numeric", "numeric").asJava).asJava)

  /** The single option the builder produced, or a failure if it declined to transpile. */
  private def builtOption(udf: UserDefinedPythonFunction, args: Expression*): Expression =
    udf.builder(args) match {
      case t: TranspiledPythonUDF => t.transpiledOptions.head
      case other => fail(s"Expected a TranspiledPythonUDF, got: $other")
    }

  private def markers(e: Expression): Seq[TranspiledUDFParameter] =
    e.collect { case p: TranspiledUDFParameter => p }

  private def markerIndexes(e: Expression): Seq[Int] = markers(e).map(_.index)

  test("marks every copy of every parameter, with one id per parameter") {
    // `lambda a, b: a * a + b`: `a` twice and `b` once, all marked -- the Python eval operator
    // this replaces computes a column per argument whether the body reads it once or twice. Binding
    // both parameters to the same argument leaves three identical copies, so the ids are then the
    // only thing saying which two belong to the repeated parameter.
    val option = Add(Multiply(param(0), param(0)), param(1))
    Seq(argB, argA).foreach { second =>
      val built = builtOption(udfWith(option), argA, second)
      assert(markerIndexes(built) == Seq(0, 0, 1), s"Expected a, a, b marked, got: $built")
      val ids = markers(built).map(_.id)
      assert(ids(0) == ids(1) && ids(0) != ids(2),
        s"Expected the ids to follow the parameter, not the argument, got: $built")
    }
  }

  test("leaves a foldable argument bare") {
    // Constant folding collapses a literal at each use site, which beats reading a column.
    val option = Multiply(param(0), param(0))
    val built = builtOption(udfWith(option), Literal(3L), argB)
    assert(markerIndexes(built).isEmpty, s"Expected no marker for a literal, got: $built")
    assert(built == Multiply(Literal(3L), Literal(3L)), s"Unexpected option shape: $built")
  }

  test("drops a parameter the body never uses") {
    // `lambda a, b: a + a`: b never reaches the option, so nothing evaluates it.
    val option = Add(param(0), param(0))
    val built = builtOption(udfWith(option), argA, argB)
    assert(!built.exists(_ == argB), s"Unused argument survived in: $built")
    assert(markerIndexes(built) == Seq(0, 0), s"Expected both copies of a marked, got: $built")
  }

  test("gives the options of one call the same ids per parameter") {
    // Two options for the same call: only one survives option pruning, and either way a parameter's
    // id is the parameter's, so the same parameter has the same id in every option.
    val repeats = Multiply(param(0), param(0))
    val single = Add(param(0), param(1))
    val udf = udfWith(repeats, single)
    udf.builder(Seq(argA, argB)) match {
      case t: TranspiledPythonUDF =>
        val first = markers(t.transpiledOptions.head)
        assert(first.map(_.index) == Seq(0, 0), s"Expected a marked twice, got: $first")
        assert(first.map(_.id).distinct.length == 1, s"Expected one id for a, got: $first")
        val second = markers(t.transpiledOptions(1))
        assert(second.map(_.index) == Seq(0, 1), s"Expected a and b marked, got: $second")
        assert(second.head.id == first.head.id,
          s"Expected parameter a to keep its id across options, got: $first / $second")
      case other => fail(s"Expected a TranspiledPythonUDF, got: $other")
    }
  }
}
