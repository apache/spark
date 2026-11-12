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

import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, BoundReference, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan}

/**
 * An interface for those join physical operators that support codegen.
 */
trait JoinCodegenSupport extends CodegenSupport with BaseJoinExec {

  /** Generates the code for join keys `keys` read from `row`, whose columns are `input`. */
  protected def createJoinKey(
      ctx: CodegenContext,
      row: String,
      keys: Seq[Expression],
      input: Seq[Attribute]): Seq[ExprCode] = {
    ctx.INPUT_ROW = row
    ctx.currentVars = null
    BindReferences.bindReferences(keys, input).map(_.genCode(ctx))
  }

  /** Copies join keys into class members, so they can be used in a later function call. */
  protected def copyKeys(ctx: CodegenContext, vars: Seq[ExprCode]): Seq[ExprCode] = {
    vars.zipWithIndex.map { case (ev, i) =>
      ctx.addBufferedState(leftKeys(i).dataType, "value", ev.value)
    }
  }

  /** Generates the code that sets `comp` to the order of the join keys `a` and `b`. */
  protected def genComparison(ctx: CodegenContext, a: Seq[ExprCode], b: Seq[ExprCode]): String = {
    // The first key compare always runs, so emit it unguarded. Each subsequent key compare runs
    // only when previous keys were equal (comp == 0).
    val pairs = a.zip(b).zipWithIndex
    val firstCompare = pairs.headOption.map { case ((l, r), i) =>
      s"comp = ${ctx.genComp(leftKeys(i).dataType, l.value, r.value)};"
    }.getOrElse("comp = 0;")
    val restCompares = pairs.drop(1).map { case ((l, r), i) =>
      s"""
         |if (comp == 0) {
         |  comp = ${ctx.genComp(leftKeys(i).dataType, l.value, r.value)};
         |}
       """.stripMargin.trim
    }
    s"""
       |$firstCompare
       |${restCompares.mkString("\n")}
     """.stripMargin
  }

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
}
