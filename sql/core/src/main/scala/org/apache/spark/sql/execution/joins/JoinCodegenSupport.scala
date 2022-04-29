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

import org.apache.spark.sql.catalyst.expressions.{BindReferences, BoundReference}
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
    val buildSideRow = buildRow.getOrElse(ctx.freshName("buildRow"))
    val buildVars = genOneSideJoinVars(ctx, buildSideRow, buildPlan, setDefaultValue = false)
    val checkCondition = if (condition.isDefined) {
      val expr = condition.get
      // evaluate the variables from build side that used by condition
      val eval = evaluateRequiredVariables(buildPlan.output, buildVars, expr.references)

      // filter the output via condition
      ctx.currentVars = streamVars ++ buildVars
      val ev =
        BindReferences.bindReference(expr, streamPlan.output ++ buildPlan.output).genCode(ctx)
      val skipRow = s"${ev.isNull} || !${ev.value}"
      s"""
         |$eval
         |${ev.code}
         |if (!($skipRow))
       """.stripMargin
    } else {
      ""
    }
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
    plan.output.zipWithIndex.map { case (a, i) =>
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
}
