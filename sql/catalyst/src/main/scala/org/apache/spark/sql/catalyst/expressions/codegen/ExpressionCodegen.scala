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

package org.apache.spark.sql.catalyst.expressions.codegen

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions._

/**
 * Defines APIs used in expression code generation.
 */
object ExpressionCodegen {

  /**
   * Given an expression, returns the all necessary parameters to evaluate it, so the generated
   * code of this expression can be split in a function.
   * The 1st string in returned tuple is the parameter strings used to call the function.
   * The 2nd string in returned tuple is the parameter strings used to declare the function.
   *
   * Returns `None` if it can't produce valid parameters.
   *
   * Params to include:
   * 1. Evaluated columns referred by this, children or deferred expressions.
   * 2. Rows referred by this, children or deferred expressions.
   * 3. Eliminated subexpressions referred bu children expressions.
   */
  def getExpressionInputParams(
      ctx: CodegenContext,
      expr: Expression): Option[(Seq[String], Seq[String])] = {
    val (inputAttrs, inputVars) = getInputVarsForChildren(ctx, expr)
    val paramsFromColumns = prepareFunctionParams(ctx, inputAttrs, inputVars)

    val subExprs = getSubExprInChildren(ctx, expr)
    val subExprCodes = getSubExprCodes(ctx, subExprs)
    val paramsFromSubExprs = prepareFunctionParams(ctx, subExprs, subExprCodes)

    val inputRows = ctx.INPUT_ROW +: getInputRowsForChildren(ctx, expr)
    val paramsFromRows = inputRows.distinct.filter(_ != null).map { row =>
      (row, s"InternalRow $row")
    }

    val paramsLength = getParamLength(ctx, inputAttrs, subExprs) + paramsFromRows.length
    // Maximum allowed parameter number for Java's method descriptor.
    if (paramsLength > 255) {
      None
    } else {
      val allParams = (paramsFromRows ++ paramsFromColumns ++ paramsFromSubExprs).unzip
      val callParams = allParams._1.distinct
      val declParams = allParams._2.distinct
      Some((callParams, declParams))
    }
  }

  /**
   * Returns the eliminated subexpressions in the children expressions.
   */
  def getSubExprInChildren(ctx: CodegenContext, expr: Expression): Seq[Expression] = {
    expr.children.flatMap { child =>
      child.collect {
        case e if ctx.subExprEliminationExprs.contains(e) => e
      }
    }.distinct
  }

  /**
   * A small helper function to return `ExprCode`s that represent subexpressions.
   */
  def getSubExprCodes(ctx: CodegenContext, subExprs: Seq[Expression]): Seq[ExprCode] = {
    subExprs.map { subExpr =>
      val state = ctx.subExprEliminationExprs(subExpr)
      ExprCode(code = "", value = state.value, isNull = state.isNull)
    }
  }

  /**
   * Retrieves previous input rows referred by children and deferred expressions.
   */
  def getInputRowsForChildren(ctx: CodegenContext, expr: Expression): Seq[String] = {
    expr.children.flatMap(getInputRows(ctx, _)).distinct
  }

  /**
   * Given a child expression, retrieves previous input rows referred by it or deferred expressions
   * which are needed to evaluate it.
   */
  def getInputRows(ctx: CodegenContext, child: Expression): Seq[String] = {
    child.flatMap {
      // An expression directly evaluates on current input row.
      case BoundReference(ordinal, _, _) if ctx.currentVars == null ||
          ctx.currentVars(ordinal) == null =>
        Seq(ctx.INPUT_ROW)

      // An expression which is not evaluated yet. Tracks down to find input rows.
      case BoundReference(ordinal, _, _) if ctx.currentVars(ordinal).code != "" =>
        trackDownRow(ctx, ctx.currentVars(ordinal))

      case _ => Seq.empty
    }.distinct
  }

  /**
   * Tracks down input rows referred by the generated code snippet.
   */
  def trackDownRow(ctx: CodegenContext, exprCode: ExprCode): Seq[String] = {
    val exprCodes = mutable.Queue[ExprCode](exprCode)
    val inputRows = mutable.ArrayBuffer.empty[String]

    while (exprCodes.nonEmpty) {
      val curExprCode = exprCodes.dequeue()
      if (curExprCode.inputRow != null) {
        inputRows += curExprCode.inputRow
      }
      curExprCode.inputVars.foreach { inputVar =>
        if (inputVar.exprCode.code != "") {
          exprCodes.enqueue(inputVar.exprCode)
        }
      }
    }
    inputRows
  }

  /**
   * Retrieves previously evaluated columns referred by children and deferred expressions.
   * Returned tuple contains the list of expressions and the list of generated codes.
   */
  def getInputVarsForChildren(
      ctx: CodegenContext,
      expr: Expression): (Seq[Expression], Seq[ExprCode]) = {
    expr.children.flatMap(getInputVars(ctx, _)).distinct.unzip
  }

  /**
   * Given a child expression, retrieves previously evaluated columns referred by it or
   * deferred expressions which are needed to evaluate it.
   */
  def getInputVars(ctx: CodegenContext, child: Expression): Seq[(Expression, ExprCode)] = {
    if (ctx.currentVars == null) {
      return Seq.empty
    }

    child.flatMap {
      // An evaluated variable.
      case b @ BoundReference(ordinal, _, _) if ctx.currentVars(ordinal) != null &&
          ctx.currentVars(ordinal).code == "" =>
        Seq((b, ctx.currentVars(ordinal)))

      // An input variable which is not evaluated yet. Tracks down to find any evaluated variables
      // in the expression path.
      // E.g., if this expression is "d = c + 1" and "c" is not evaluated. We need to track to
      // "c = a + b" and see if "a" and "b" are evaluated. If they are, we need to return them so
      // to include them into parameters, if not, we track down further.
      case BoundReference(ordinal, _, _) if ctx.currentVars(ordinal) != null =>
        trackDownVar(ctx, ctx.currentVars(ordinal))

      case _ => Seq.empty
    }.distinct
  }

  /**
   * Tracks down previously evaluated columns referred by the generated code snippet.
   */
  def trackDownVar(ctx: CodegenContext, exprCode: ExprCode): Seq[(Expression, ExprCode)] = {
    val exprCodes = mutable.Queue[ExprCode](exprCode)
    val inputVars = mutable.ArrayBuffer.empty[(Expression, ExprCode)]

    while (exprCodes.nonEmpty) {
      exprCodes.dequeue().inputVars.foreach { inputVar =>
        if (inputVar.exprCode.code == "") {
          inputVars += ((inputVar.expr, inputVar.exprCode))
        } else {
          exprCodes.enqueue(inputVar.exprCode)
        }
      }
    }
    inputVars
  }

  /**
   * Helper function to calculate the size of an expression as function parameter.
   */
  def calculateParamLength(ctx: CodegenContext, input: Expression): Int = {
    ctx.javaType(input.dataType) match {
      case (ctx.JAVA_LONG | ctx.JAVA_DOUBLE) if !input.nullable => 2
      case ctx.JAVA_LONG | ctx.JAVA_DOUBLE => 3
      case _ if !input.nullable => 1
      case _ => 2
    }
  }

  /**
   * In Java, a method descriptor is valid only if it represents method parameters with a total
   * length of 255 or less. `this` contributes one unit and a parameter of type long or double
   * contributes two units.
   */
  def getParamLength(
      ctx: CodegenContext,
      inputs: Seq[Expression],
      subExprs: Seq[Expression]): Int = {
    // Initial value is 1 for `this`.
    1 + (inputs ++ subExprs).distinct.map(calculateParamLength(ctx, _)).sum
  }

  /**
   * Given the lists of input attributes and variables to this expression, returns the strings of
   * funtion parameters. The first is the variable names used to call the function, the second is
   * the parameters used to declare the function in generated code.
   */
  def prepareFunctionParams(
      ctx: CodegenContext,
      inputAttrs: Seq[Expression],
      inputVars: Seq[ExprCode]): Seq[(String, String)] = {
    inputAttrs.zip(inputVars).flatMap { case (input, ev) =>
      val argType = ctx.javaType(input.dataType)

      if (!input.nullable || ev.isNull == "true" || ev.isNull == "false") {
        Seq((ev.value, s"$argType ${ev.value}"))
      } else {
        Seq((ev.value, s"$argType ${ev.value}"), (ev.isNull, s"boolean ${ev.isNull}"))
      }
    }.distinct
  }
}
