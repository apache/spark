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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, NamedArgumentExpression}
import org.apache.spark.sql.types.{DataType, StringType}

class NamedArgumentFunctionSuite extends SparkFunSuite {

  private def createMyFunction(
      params: Seq[String],
      expressions: Seq[Expression]): NamedArgumentFunction = {
    case class MyFunction() extends NamedArgumentFunction {
      case class MyParam(name: String) extends Param {
        override val dataType: DataType = StringType
        override val required: Boolean = false
        override val default: Option[Any] = None
      }
      override def parameters: Seq[Param] = params.map(MyParam)
      override def inputExpressions: Seq[Expression] = expressions
    }
    MyFunction()
  }

  test("Named function happy path") {
    val myFunction = createMyFunction(
      Seq("name", "age"),
      Seq(Literal("jack"), NamedArgumentExpression("age", Literal("18"))))
    assert(myFunction.positionalArguments == Seq(Literal("jack")))
    assert(myFunction.namedArguments == Seq(NamedArgumentExpression("age", Literal("18"))))
  }

  test("An unnamed argument after a named argument") {
    val myFunction = createMyFunction(
      Seq("name", "age"),
      Seq(NamedArgumentExpression("name", Literal("jack")), Literal("18")))
    checkError(
      exception = intercept[AnalysisException](myFunction.positionalArguments),
      errorClass = "TABLE_FUNCTION_UNEXPECTED_ARGUMENT",
      parameters = Map("expression" -> "18", "pos" -> "1")
    )
  }

  test("User passes duplicated arguments") {
    val myFunction = createMyFunction(
      Seq("name", "age"),
      Seq(
        Literal("18"),
        NamedArgumentExpression("name", Literal("jack")),
        NamedArgumentExpression("name", Literal("lily"))
      ))
    checkError(
      exception = intercept[AnalysisException](myFunction.positionalArguments),
      errorClass = "TABLE_FUNCTION_DUPLICATED_NAMED_ARGUMENTS",
      parameters = Map("name" -> "name", "pos" -> "2")
    )
  }

  test("Define named argument function with duplicated params") {
    val e = intercept[AssertionError] {
      createMyFunction(Seq("name", "name"), Seq.empty[Expression])
    }
    assert(e.getMessage.contains(
      "Cannot define duplicated parameters in named argument function."))
  }
}
