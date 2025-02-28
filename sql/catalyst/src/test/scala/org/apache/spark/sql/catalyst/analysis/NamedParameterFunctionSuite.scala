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
package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, NamedArgumentExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.{FunctionSignature, InputParameter, NamedParametersSupport}
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLId
import org.apache.spark.sql.types.DataType


case class DummyExpression(
    k1: Expression,
    k2: Expression,
    k3: Expression,
    k4: Expression) extends Expression {
  override def nullable: Boolean = false
  override def eval(input: InternalRow): Any = None
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = null
  override def dataType: DataType = null
  override def children: Seq[Expression] = Nil
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = null
}

object DummyExpressionBuilder extends ExpressionBuilder  {

  def defaultFunctionSignature: FunctionSignature = {
    FunctionSignature(Seq(InputParameter("k1"),
      InputParameter("k2"),
      InputParameter("k3"),
      InputParameter("k4")))
  }

  override def functionSignature: Option[FunctionSignature] =
    Some(defaultFunctionSignature)

  override def build(funcName: String, expressions: Seq[Expression]): Expression =
    DummyExpression(expressions(0), expressions(1), expressions(2), expressions(3))
}

class NamedParameterFunctionSuite extends AnalysisTest {

  final val k1Arg = Literal("v1")
  final val k2Arg = NamedArgumentExpression("k2", Literal("v2"))
  final val k3Arg = NamedArgumentExpression("k3", Literal("v3"))
  final val k4Arg = NamedArgumentExpression("k4", Literal("v4"))
  final val namedK1Arg = NamedArgumentExpression("k1", Literal("v1-2"))
  final val args = Seq(k1Arg, k4Arg, k2Arg, k3Arg)

  final val expectedSeq = Seq(Literal("v1"), Literal("v2"), Literal("v3"), Literal("v4"))
  final val signature = DummyExpressionBuilder.defaultFunctionSignature
  final val illegalSignature = FunctionSignature(Seq(
    InputParameter("k1"), InputParameter("k2", Option(Literal("v2"))), InputParameter("k3")))

  test("Check rearrangement of expressions") {
    val rearrangedArgs = NamedParametersSupport.defaultRearrange(
      signature, args, "function")
    for ((returnedArg, expectedArg) <- rearrangedArgs.zip(expectedSeq)) {
      assert(returnedArg == expectedArg)
    }
    val rearrangedArgsWithBuilder =
      FunctionRegistry.rearrangeExpressions("function", DummyExpressionBuilder, args)
    for ((returnedArg, expectedArg) <- rearrangedArgsWithBuilder.zip(expectedSeq)) {
      assert(returnedArg == expectedArg)
    }
  }

  private def parseRearrangeException(functionSignature: FunctionSignature,
                                      expressions: Seq[Expression],
                                      functionName: String = "function"): SparkThrowable = {
    intercept[SparkThrowable](
      NamedParametersSupport.defaultRearrange(functionSignature, expressions, functionName))
  }

  test("DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT") {
    val condition =
      "DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.BOTH_POSITIONAL_AND_NAMED"
    checkError(
      exception = parseRearrangeException(
        signature, Seq(k1Arg, k2Arg, k3Arg, k4Arg, namedK1Arg), "foo"),
      condition = condition,
      parameters = Map("routineName" -> toSQLId("foo"), "parameterName" -> toSQLId("k1"))
    )
    checkError(
      exception = parseRearrangeException(
        signature, Seq(k1Arg, k2Arg, k3Arg, k4Arg, k4Arg), "foo"),
      condition = "DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE",
      parameters = Map("routineName" -> toSQLId("foo"), "parameterName" -> toSQLId("k4"))
    )
  }

  test("REQUIRED_PARAMETER_NOT_FOUND") {
    checkError(
      exception = parseRearrangeException(signature, Seq(k1Arg, k2Arg, k3Arg), "foo"),
      condition = "REQUIRED_PARAMETER_NOT_FOUND",
      parameters = Map(
        "routineName" -> toSQLId("foo"), "parameterName" -> toSQLId("k4"), "index" -> "2"))
  }

  test("UNRECOGNIZED_PARAMETER_NAME") {
    checkError(
      exception = parseRearrangeException(signature,
        Seq(k1Arg, k2Arg, k3Arg, k4Arg, NamedArgumentExpression("k5", Literal("k5"))), "foo"),
      condition = "UNRECOGNIZED_PARAMETER_NAME",
      parameters = Map("routineName" -> toSQLId("foo"), "argumentName" -> toSQLId("k5"),
        "proposal" -> (toSQLId("k1") + " " + toSQLId("k2") + " " + toSQLId("k3")))
    )
  }

  test("UNEXPECTED_POSITIONAL_ARGUMENT") {
    checkError(
      exception = parseRearrangeException(signature,
        Seq(k2Arg, k3Arg, k1Arg, k4Arg), "foo"),
      condition = "UNEXPECTED_POSITIONAL_ARGUMENT",
      parameters = Map("routineName" -> toSQLId("foo"), "parameterName" -> toSQLId("k3"))
    )
  }

  test("INTERNAL_ERROR: Enforce optional arguments after required arguments") {
    val errorMessage = s"Routine ${toSQLId("foo")} has an unexpected required argument for" +
      s" the provided routine signature ${illegalSignature.parameters.mkString("[", ", ", "]")}." +
      s" All required arguments should come before optional arguments."
    checkError(
      exception = parseRearrangeException(illegalSignature, args, "foo"),
      condition = "INTERNAL_ERROR",
      parameters = Map("message" -> errorMessage)
    )
  }
}
