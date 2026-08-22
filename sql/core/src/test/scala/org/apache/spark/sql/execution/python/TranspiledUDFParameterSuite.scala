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
import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, Expression, Multiply, TranspiledPythonUDF, TranspiledUDFParameter}
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

/**
 * Tests how `UserDefinedPythonFunction.builder` marks the `_udf_param_N` placeholders it fills in
 * (SPARK-58626). The builder owns this because it is the last place that still sees the
 * placeholders: filling them in is what erases which copy came from which parameter, and the
 * indexes it stamps on are what tell `ConvertToCatalyst` which copies share one evaluation.
 */
class TranspiledUDFParameterSuite extends QueryTest with SharedSparkSession {

  private val argA = AttributeReference("a", LongType)()
  private val argB = AttributeReference("b", LongType)()

  private def param(index: Int): Expression = UnresolvedAttribute(s"_udf_param_$index")

  /** A two-parameter transpiled UDF carrying the given option body. */
  private def udfWith(option: Expression): UserDefinedPythonFunction =
    UserDefinedPythonFunction(
      "udf",
      null,
      LongType,
      PythonEvalType.SQL_BATCHED_UDF,
      udfDeterministic = true,
      List(Column(option)).asJava,
      List(List("numeric", "numeric").asJava).asJava)

  /** The indexes of the markers in the single option the builder produced, in tree order. */
  private def markerIndexes(option: Expression, args: Expression*): Seq[Int] =
    udfWith(option).builder(args) match {
      case t: TranspiledPythonUDF =>
        t.transpiledOptions.head.collect { case p: TranspiledUDFParameter => p.index }
      case other => fail(s"Expected a TranspiledPythonUDF, got: $other")
    }

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
}
