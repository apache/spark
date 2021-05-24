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
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, InnerLike, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan}

/**
 * An interface for those join physical operators that support codegen.
 */
trait JoinCodegenSupport extends CodegenSupport with BaseJoinExec {

  /**
   * Generate the (non-equi) condition used to filter joined rows.
   * This is used in Inner, Left Semi and Left Anti joins.
   *
   * @return Tuple of variable name for row of build side, generated code for condition,
   *         and generated code for variables of build side.
   */
  protected def getJoinCondition(
      ctx: CodegenContext,
      streamVars: Seq[ExprCode],
      streamPlan: SparkPlan,
      buildPlan: SparkPlan): (String, String, Seq[ExprCode]) = {
    val buildRow = ctx.freshName("buildRow")
    val buildVars = genBuildSideVars(ctx, buildRow, buildPlan)
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
    (buildRow, checkCondition, buildVars)
  }

  /**
   * Generates the code for variables of build side.
   */
  protected def genBuildSideVars(
      ctx: CodegenContext,
      buildRow: String,
      buildPlan: SparkPlan): Seq[ExprCode] = {
    ctx.currentVars = null
    ctx.INPUT_ROW = buildRow
    buildPlan.output.zipWithIndex.map { case (a, i) =>
      val ev = BoundReference(i, a.dataType, a.nullable).genCode(ctx)
      joinType match {
        case _: InnerLike | LeftSemi | LeftAnti | _: ExistenceJoin =>
          ev
        case LeftOuter | RightOuter =>
          // the variables are needed even there is no matched rows
          val isNull = ctx.freshName("isNull")
          val value = ctx.freshName("value")
          val javaType = CodeGenerator.javaType(a.dataType)
          val code = code"""
            |boolean $isNull = true;
            |$javaType $value = ${CodeGenerator.defaultValue(a.dataType)};
            |if ($buildRow != null) {
            |  ${ev.code}
            |  $isNull = ${ev.isNull};
            |  $value = ${ev.value};
            |}
          """.stripMargin
          ExprCode(code, JavaCode.isNullVariable(isNull), JavaCode.variable(value, a.dataType))
        case _ =>
          throw new IllegalArgumentException(
            s"JoinCodegenSupport.genBuildSideVars should not take $joinType as the JoinType")
      }
    }
  }
}
