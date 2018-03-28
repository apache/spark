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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkException
import org.apache.spark.api.java.function._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.types.DataType


private[sql] case class JavaUDF (
    function: AnyRef,
    dataType: DataType,
    children: Seq[Expression],
    inputTypes: Seq[DataType],
    udfName: Option[String] = None,
    nullable: Boolean = true,
    udfDeterministic: Boolean = true)
  extends Expression with ImplicitCastInputTypes with NonSQLExpression with UserDefinedExpression {

  override lazy val deterministic: Boolean = udfDeterministic && children.forall(_.deterministic)

  override def toString: String =
    s"${udfName.map(name => s"UDF:$name").getOrElse("UDF")}(${children.mkString(", ")})"

  // TODO: generate this method by script
  private[this] val f = function match {
    case func: Function0[Any] =>
      (input: InternalRow) => {
        func.call()
      }
    case func: Function[Any, Any] =>
      (input: InternalRow) => {
        func.call(children(0).eval(input))
      }
    case func: Function[Any, Any, Any] =>
      (input: InternalRow) => {
        func.call(children(0).eval(input), children(1).eval(input))
      }
    case _ => throw new IllegalArgumentException("unknown function type.")
  }

  lazy val udfErrorMessage = {
    val funcCls = function.getClass.getSimpleName
    val inputTypes = children.map(_.dataType.simpleString).mkString(", ")
    s"Failed to execute user defined function($funcCls: ($inputTypes) => ${dataType.simpleString})"
  }

  override def doGenCode(
      ctx: CodegenContext,
      ev: ExprCode): ExprCode = {

    val errorMsgTerm = ctx.addReferenceObj("errMsg", udfErrorMessage)
    val resultTerm = ctx.freshName("result")

    // codegen for children expressions
    val evals = children.map(_.genCode(ctx))

    // Generate the codes for expressions and calling user-defined function
    // We need to get the boxedType of dataType's javaType here. Because for the dataType
    // such as IntegerType, its javaType is `int` and the returned type of user-defined
    // function is Object. Trying to convert an Object to `int` will cause casting exception.
    val evalCode = evals.map(_.code).mkString("\n")
    val (funcArgs, initArgs) = evals.zipWithIndex.map { case (eval, i) =>
      val argTerm = ctx.freshName("arg")
      val initArg = s"Object $argTerm = ${eval.isNull} ? null : ${eval.value};"
      (argTerm, initArg)
    }.unzip

    val udf = ctx.addReferenceObj("udf", function, s"scala.Function${children.length}")
    val getFuncResult = s"$udf.apply(${funcArgs.mkString(", ")})"
    val boxedType = CodeGenerator.boxedType(dataType)
    val callFunc =
      s"""
         |$boxedType $resultTerm = null;
         |try {
         |  $resultTerm = ($boxedType)$getFuncResult;
         |} catch (Exception e) {
         |  throw new org.apache.spark.SparkException($errorMsgTerm, e);
         |}
       """.stripMargin

    ev.copy(code =
      s"""
         |$evalCode
         |${initArgs.mkString("\n")}
         |$callFunc
         |
         |boolean ${ev.isNull} = $resultTerm == null;
         |${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
         |if (!${ev.isNull}) {
         |  ${ev.value} = $resultTerm;
         |}
       """.stripMargin)
  }


  override def eval(input: InternalRow): Any = {
    val result = try {
      f(input)
    } catch {
      case e: Exception =>
        throw new SparkException(udfErrorMessage, e)
    }
    result
  }

}
