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
import org.apache.spark.sql.catalyst.plans.logical.{FixedArgumentType, FunctionSignature, NamedArgument, SupportsNamedArguments}
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLId
import org.apache.spark.sql.types.{DataType, StringType}


case class DummyExpression() extends Expression {
  override def nullable: Boolean = false
  override def eval(input: InternalRow): Any = None
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = null
  override def dataType: DataType = null
  override def children: Seq[Expression] = Nil
  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): Expression = null
}

object DummyExpression extends SupportsNamedArguments {
  def defaultFunctionSignature: FunctionSignature = {
    FunctionSignature(Seq(NamedArgument("k1", FixedArgumentType(StringType)),
      NamedArgument("k2", FixedArgumentType(StringType)),
      NamedArgument("k3", FixedArgumentType(StringType)),
      NamedArgument("k4", FixedArgumentType(StringType))))
  }
  override def functionSignatures: Seq[FunctionSignature] = {
    Seq(defaultFunctionSignature)
  }
}

class NamedArgumentFunctionSuite extends AnalysisTest {

  final val k1Arg = Literal("v1")
  final val k2Arg = NamedArgumentExpression("k2", Literal("v2"))
  final val k3Arg = NamedArgumentExpression("k3", Literal("v3"))
  final val k4Arg = NamedArgumentExpression("k4", Literal("v4"))
  final val args = Seq(k1Arg, k4Arg, k2Arg, k3Arg)
  final val expectedSeq = Seq(Literal("v1"), Literal("v2"), Literal("v3"), Literal("v4"))

  test("Check rearrangement of expressions") {
    val rearrangedArgs = SupportsNamedArguments.defaultRearrange(
      DummyExpression.defaultFunctionSignature, args, "function")
    for ((returnedArg, expectedArg) <- rearrangedArgs.zip(expectedSeq)) {
      assert(returnedArg == expectedArg)
    }
  }

  private def parseRearrangeException(functionSignature: FunctionSignature,
                                      expressions: Seq[Expression],
                                      functionName: String = "function"): SparkThrowable = {
    intercept[SparkThrowable](
      SupportsNamedArguments.defaultRearrange(functionSignature, expressions, functionName))
  }

  test("DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT") {
    checkError(
      exception = parseRearrangeException(DummyExpression.defaultFunctionSignature,
        Seq(k1Arg, k2Arg, k3Arg, k4Arg, k4Arg), "foo"),
      errorClass = "DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT",
      parameters = Map("functionName" -> toSQLId("foo"), "parameterName" -> toSQLId("k4"))
    )
  }

  test("REQUIRED_PARAMETER_NOT_FOUND") {
    checkError(
      exception = parseRearrangeException(DummyExpression.defaultFunctionSignature,
        Seq(k1Arg, k2Arg, k3Arg), "foo"),
      errorClass = "REQUIRED_PARAMETER_NOT_FOUND",
      parameters = Map("functionName" -> toSQLId("foo"), "parameterName" -> toSQLId("k4"))
    )
  }

  test("UNRECOGNIZED_PARAMETER_NAME") {
    checkError(
      exception = parseRearrangeException(DummyExpression.defaultFunctionSignature,
        Seq(k1Arg, k2Arg, k3Arg, k4Arg, NamedArgumentExpression("k5", Literal("k5"))), "foo"),
      errorClass = "UNRECOGNIZED_PARAMETER_NAME",
      parameters = Map("functionName" -> toSQLId("foo"), "argumentName" -> toSQLId("k5"),
        "proposal" -> (toSQLId("k1") + " " + toSQLId("k2") + " " + toSQLId("k3") + " "))
    )
  }

  test("UNEXPECTED_POSITIONAL_ARGUMENT") {
    checkError(
      exception = parseRearrangeException(DummyExpression.defaultFunctionSignature,
        Seq(k2Arg, k3Arg, k1Arg, k4Arg), "foo"),
      errorClass = "UNEXPECTED_POSITIONAL_ARGUMENT",
      parameters = Map("functionName" -> toSQLId("foo"))
    )
  }
}
