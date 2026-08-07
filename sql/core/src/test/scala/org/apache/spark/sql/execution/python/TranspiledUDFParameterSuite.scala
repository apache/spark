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
 * Tests which `_udf_param_N` placeholders `UserDefinedPythonFunction.builder` tags with a
 * [[TranspiledUDFParameter]] (SPARK-58626).
 *
 * The builder owns this decision because it is the last place that still sees the placeholders: it
 * resolves them into the bound arguments, after which nothing says which copy came from which
 * parameter. `ConvertToCatalyst` gives every tagged parameter a single evaluation, so what the
 * builder tags is what ends up shared.
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

  private def tagIndexes(e: Expression): Seq[Int] =
    e.collect { case p: TranspiledUDFParameter => p.index }

  test("tags every copy of a parameter used more than once") {
    // `lambda a, b: a * a + b`: only `a` repeats, so only its copies are tagged.
    val option = Add(Multiply(param(0), param(0)), param(1))
    val built = builtOption(udfWith(option), argA, argB)
    assert(tagIndexes(built) == Seq(0, 0), s"Expected both copies of a tagged, got: $built")
    assert(built.exists(_ == argB), s"Expected b spliced in bare, got: $built")
    assert(built == Add(Multiply(TranspiledUDFParameter(argA, 0), TranspiledUDFParameter(argA, 0)),
      argB), s"Unexpected option shape: $built")
  }

  test("leaves a parameter used once bare") {
    // `lambda a, b: a + b`: nothing repeats, so the option is plain substitution as before.
    val option = Add(param(0), param(1))
    val built = builtOption(udfWith(option), argA, argB)
    assert(tagIndexes(built).isEmpty, s"Expected no tags, got: $built")
    assert(built == Add(argA, argB), s"Unexpected option shape: $built")
  }

  test("leaves a foldable argument bare even when its parameter repeats") {
    // Constant folding collapses a literal at each use site, which beats a shared column.
    val option = Multiply(param(0), param(0))
    val built = builtOption(udfWith(option), Literal(3L), argB)
    assert(tagIndexes(built).isEmpty, s"Expected no tags for a literal, got: $built")
    assert(built == Multiply(Literal(3L), Literal(3L)), s"Unexpected option shape: $built")
  }

  test("tags parameters bound to structurally equal arguments separately") {
    // f(a, a) with `lambda x, y: x * x + y`: substitution leaves three identical copies, so the
    // tags are the only thing saying which two belong to the repeated parameter.
    val option = Add(Multiply(param(0), param(0)), param(1))
    val built = builtOption(udfWith(option), argA, argA)
    assert(tagIndexes(built) == Seq(0, 0), s"Expected only parameter 0 tagged, got: $built")
  }

  test("drops a parameter the body never uses") {
    // `lambda a, b: a`: b never reaches the option, so nothing evaluates it.
    val option = Add(param(0), param(0))
    val built = builtOption(udfWith(option), argA, argB)
    assert(!built.exists(_ == argB), s"Unused argument survived in: $built")
    assert(tagIndexes(built) == Seq(0, 0), s"Expected both copies of a tagged, got: $built")
  }

  test("counts uses per option rather than across them") {
    // Two options for the same call: only the one that repeats a parameter gets tags.
    val repeats = Multiply(param(0), param(0))
    val single = Add(param(0), param(1))
    val udf = udfWith(repeats, single)
    udf.builder(Seq(argA, argB)) match {
      case t: TranspiledPythonUDF =>
        assert(tagIndexes(t.transpiledOptions.head) == Seq(0, 0),
          s"Expected the repeating option tagged, got: ${t.transpiledOptions.head}")
        assert(tagIndexes(t.transpiledOptions(1)).isEmpty,
          s"Expected the single-use option untagged, got: ${t.transpiledOptions(1)}")
      case other => fail(s"Expected a TranspiledPythonUDF, got: $other")
    }
  }
}
