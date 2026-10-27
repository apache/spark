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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.expressions.{BindReferences, BoundReference, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan}

/**
 * An interface for those join physical operators that support codegen.
 */
trait JoinCodegenSupport extends CodegenSupport with BaseJoinExec {

  /**
   * Generate the (non-equi) condition used to filter joined rows.
   * This is used in Inner, Left Semi, Left Anti and Full Outer joins.
   *
   * @return Tuple of variable name for row of build side, generated code for condition,
   *         and generated code for variables of build side.
   */
  protected def getJoinCondition(
      ctx: CodegenContext,
      streamVars: Seq[ExprCode],
      streamPlan: SparkPlan,
      buildPlan: SparkPlan,
      buildRow: Option[String] = None): (String, String, Seq[ExprCode]) = {
    if (condition.isDefined) {
      getJoinCondition(ctx, condition.get, streamVars, streamPlan, buildPlan, buildRow)
    } else {
      val buildSideRow = buildRow.getOrElse(ctx.freshName("buildRow"))
      val buildVars = genOneSideJoinVars(ctx, buildSideRow, buildPlan, setDefaultValue = false)
      (buildSideRow, "", buildVars)
    }
  }

  /**
   * Generate the (non-equi) condition used to filter joined rows from an explicit expression.
   */
  protected def getJoinCondition(
      ctx: CodegenContext,
      conditionExpr: Expression,
      streamVars: Seq[ExprCode],
      streamPlan: SparkPlan,
      buildPlan: SparkPlan,
      buildRow: Option[String]): (String, String, Seq[ExprCode]) = {
    val buildSideRow = buildRow.getOrElse(ctx.freshName("buildRow"))
    val buildVars = genOneSideJoinVars(ctx, buildSideRow, buildPlan, setDefaultValue = false)
    // We want to evaluate the passed streamVars. However, evaluation modifies the contained
    // ExprCode instances, which may surprise the caller to this method (in particular,
    // full outer join will want to evaluate streamVars in a different scope than the
    // condition check). Because of this, we first make a copy.
    val streamVars2 = streamVars.map(_.copy())
    // evaluate the variables that are used by the condition
    val eval = evaluateRequiredVariables(streamPlan.output ++ buildPlan.output,
      streamVars2 ++ buildVars, conditionExpr.references)

    // filter the output via condition
    ctx.currentVars = streamVars2 ++ buildVars
    val ev = BindReferences.bindReference(
      conditionExpr, streamPlan.output ++ buildPlan.output).genCode(ctx)
    val skipRow = s"${ev.isNull} || !${ev.value}"
    val checkCondition =
      s"""
         |$eval
         |${ev.code}
         |if (!($skipRow))
       """.stripMargin
    (buildSideRow, checkCondition, buildVars)
  }

  /**
   * Generates the code for variables of one child side of join.
   */
  protected def genOneSideJoinVars(
      ctx: CodegenContext,
      row: String,
      plan: SparkPlan,
      setDefaultValue: Boolean): Seq[ExprCode] = {
    ctx.currentVars = null
    ctx.INPUT_ROW = row
    plan.output.toIndexedSeq.zipWithIndex.map { case (a, i) =>
      val ev = BoundReference(i, a.dataType, a.nullable).genCode(ctx)
      if (setDefaultValue) {
        // the variables are needed even there is no matched rows
        val isNull = ctx.freshName("isNull")
        val value = ctx.freshName("value")
        val javaType = CodeGenerator.javaType(a.dataType)
        val code = code"""
            |boolean $isNull = true;
            |$javaType $value = ${CodeGenerator.defaultValue(a.dataType)};
            |if ($row != null) {
            |  ${ev.code}
            |  $isNull = ${ev.isNull};
            |  $value = ${ev.value};
            |}
          """.stripMargin
        ExprCode(code, JavaCode.isNullVariable(isNull), JavaCode.variable(value, a.dataType))
      } else {
        ev
      }
    }
  }

  /**
   * Like [[genOneSideJoinVars]] but stores extracted column values in class-level mutable fields
   * instead of local variables. This allows the extraction code to be split across helper methods
   * via [[CodegenContext.splitExpressions]], avoiding the HotSpot 8K JIT limit on wide schemas.
   *
   * Returns (exprCodes, extractionCode) where exprCodes reference the class fields and
   * extractionCode is the (possibly split) code that populates them from `row`.
   */
  protected def genOneSideJoinVarsAsFields(
      ctx: CodegenContext,
      row: String,
      plan: SparkPlan,
      setDefaultValue: Boolean): (Seq[ExprCode], String) = {
    ctx.currentVars = null
    ctx.INPUT_ROW = row
    val javaType = CodeGenerator.javaType _
    val defaultValue: org.apache.spark.sql.types.DataType => String =
      dt => CodeGenerator.defaultValue(dt)

    val varsAndCode = plan.output.toIndexedSeq.zipWithIndex.map { case (a, i) =>
      val ev = BoundReference(i, a.dataType, a.nullable).genCode(ctx)
      val value = ctx.addMutableState(javaType(a.dataType), "joinVar",
        v => s"$v = ${defaultValue(a.dataType)};", forceInline = true)
      // Outer joins (setDefaultValue=true) need an isNull field even for non-nullable columns,
      // because the column becomes nullable when there is no match on the other side.
      val needsIsNull = a.nullable || setDefaultValue
      if (needsIsNull) {
        val isNull = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "joinVarIsNull",
          v => s"$v = true;", forceInline = true)
        val extractCode = if (setDefaultValue) {
          s"""
             |$isNull = true;
             |$value = ${defaultValue(a.dataType)};
             |if ($row != null) {
             |  ${ev.code}
             |  $isNull = ${ev.isNull};
             |  $value = ${ev.value};
             |}
           """.stripMargin
        } else {
          s"""
             |${ev.code}
             |$isNull = ${ev.isNull};
             |$value = ${ev.value};
           """.stripMargin
        }
        val exprCode = ExprCode(
          code"",
          JavaCode.isNullVariable(isNull),
          JavaCode.variable(value, a.dataType))
        (exprCode, extractCode)
      } else {
        val extractCode =
          s"""
             |${ev.code}
             |$value = ${ev.value};
           """.stripMargin
        val exprCode = ExprCode(code"", FalseLiteral, JavaCode.variable(value, a.dataType))
        (exprCode, extractCode)
      }
    }

    val (exprCodes, extractionSnippets) = varsAndCode.unzip

    // Use splitExpressions to batch extraction code into helper methods when the total
    // exceeds the JIT threshold. The row parameter makes it accessible inside each method.
    val splitCode = ctx.splitExpressions(
      expressions = extractionSnippets,
      funcName = "extractJoinVars",
      arguments = Seq(("InternalRow", row)))

    (exprCodes, splitCode)
  }
}
