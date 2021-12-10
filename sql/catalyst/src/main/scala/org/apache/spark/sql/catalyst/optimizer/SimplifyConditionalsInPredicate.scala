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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{And, CaseWhen, Coalesce, Expression, If, Literal, Not, Or}
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CASE_WHEN, IF}
import org.apache.spark.sql.types.BooleanType

/**
 * A rule that converts conditional expressions to predicate expressions, if possible, in the
 * search condition of the WHERE/HAVING/ON(JOIN) clauses, which contain an implicit Boolean operator
 * "(search condition) = TRUE". After this converting, we can potentially push the filter down to
 * the data source. This rule is null-safe.
 *
 * Supported cases are:
 * - IF(cond, trueVal, false)                   => AND(cond, trueVal)
 * - IF(cond, trueVal, true)                    => OR(NOT(cond), trueVal)
 * - IF(cond, false, falseVal)                  => AND(NOT(cond), falseVal)
 * - IF(cond, true, falseVal)                   => OR(cond, falseVal)
 * - CASE WHEN cond THEN trueVal ELSE false END => AND(cond, trueVal)
 * - CASE WHEN cond THEN trueVal END            => AND(cond, trueVal)
 * - CASE WHEN cond THEN trueVal ELSE null END  => AND(cond, trueVal)
 * - CASE WHEN cond THEN trueVal ELSE true END  => OR(NOT(cond), trueVal)
 * - CASE WHEN cond THEN false ELSE elseVal END => AND(NOT(cond), elseVal)
 * - CASE WHEN cond THEN true ELSE elseVal END  => OR(cond, elseVal)
 */
object SimplifyConditionalsInPredicate extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAnyPattern(CASE_WHEN, IF), ruleId) {
    case f @ Filter(cond, _) => f.copy(condition = simplifyConditional(cond))
    case j @ Join(_, _, _, Some(cond), _) => j.copy(condition = Some(simplifyConditional(cond)))
    case d @ DeleteFromTable(_, Some(cond)) => d.copy(condition = Some(simplifyConditional(cond)))
    case u @ UpdateTable(_, _, Some(cond)) => u.copy(condition = Some(simplifyConditional(cond)))
  }

  private def simplifyConditional(e: Expression): Expression = e match {
    case And(left, right) => And(simplifyConditional(left), simplifyConditional(right))
    case Or(left, right) => Or(simplifyConditional(left), simplifyConditional(right))
    case If(cond, trueValue, FalseLiteral) => And(cond, trueValue)
    case If(cond, trueValue, TrueLiteral) => Or(Not(Coalesce(Seq(cond, FalseLiteral))), trueValue)
    case If(cond, FalseLiteral, falseValue) =>
      And(Not(Coalesce(Seq(cond, FalseLiteral))), falseValue)
    case If(cond, TrueLiteral, falseValue) => Or(cond, falseValue)
    case CaseWhen(Seq((cond, trueValue)),
        Some(FalseLiteral) | Some(Literal(null, BooleanType)) | None) =>
      And(cond, trueValue)
    case CaseWhen(Seq((cond, trueValue)), Some(TrueLiteral)) =>
      Or(Not(Coalesce(Seq(cond, FalseLiteral))), trueValue)
    case CaseWhen(Seq((cond, FalseLiteral)), Some(elseValue)) =>
      And(Not(Coalesce(Seq(cond, FalseLiteral))), elseValue)
    case CaseWhen(Seq((cond, TrueLiteral)), Some(elseValue)) =>
      Or(cond, elseValue)
    case e if e.dataType == BooleanType => e
    case e =>
      assert(e.dataType != BooleanType,
      "Expected a Boolean type expression in SimplifyConditionalsInPredicate, " +
        s"but got the type `${e.dataType.catalogString}` in `${e.sql}`.")
      e
  }
}
