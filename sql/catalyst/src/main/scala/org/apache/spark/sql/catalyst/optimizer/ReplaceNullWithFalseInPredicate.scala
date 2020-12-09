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

import org.apache.spark.sql.catalyst.expressions.{And, ArrayExists, ArrayFilter, CaseWhen, Expression, If}
import org.apache.spark.sql.catalyst.expressions.{LambdaFunction, Literal, MapFilter, Or}
import org.apache.spark.sql.catalyst.expressions.Literal.FalseLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.util.Utils


/**
 * A rule that replaces `Literal(null, BooleanType)` with `FalseLiteral`, if possible, in the search
 * condition of the WHERE/HAVING/ON(JOIN) clauses, which contain an implicit Boolean operator
 * "(search condition) = TRUE". The replacement is only valid when `Literal(null, BooleanType)` is
 * semantically equivalent to `FalseLiteral` when evaluating the whole search condition.
 *
 * Please note that FALSE and NULL are not exchangeable in most cases, when the search condition
 * contains NOT and NULL-tolerant expressions. Thus, the rule is very conservative and applicable
 * in very limited cases.
 *
 * For example, `Filter(Literal(null, BooleanType))` is equal to `Filter(FalseLiteral)`.
 *
 * Another example containing branches is `Filter(If(cond, FalseLiteral, Literal(null, _)))`;
 * this can be optimized to `Filter(If(cond, FalseLiteral, FalseLiteral))`, and eventually
 * `Filter(FalseLiteral)`.
 *
 * Moreover, this rule also transforms predicates in all [[If]] expressions as well as branch
 * conditions in all [[CaseWhen]] expressions, even if they are not part of the search conditions.
 *
 * For example, `Project(If(And(cond, Literal(null)), Literal(1), Literal(2)))` can be simplified
 * into `Project(Literal(2))`.
 */
object ReplaceNullWithFalseInPredicate extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case f @ Filter(cond, _) => f.copy(condition = replaceNullWithFalse(cond))
    case j @ Join(_, _, _, Some(cond), _) => j.copy(condition = Some(replaceNullWithFalse(cond)))
    case p: LogicalPlan => p transformExpressions {
      case i @ If(pred, _, _) => i.copy(predicate = replaceNullWithFalse(pred))
      case cw @ CaseWhen(branches, _) =>
        val newBranches = branches.map { case (cond, value) =>
          replaceNullWithFalse(cond) -> value
        }
        cw.copy(branches = newBranches)
      case af @ ArrayFilter(_, lf @ LambdaFunction(func, _, _)) =>
        val newLambda = lf.copy(function = replaceNullWithFalse(func))
        af.copy(function = newLambda)
      case ae @ ArrayExists(_, lf @ LambdaFunction(func, _, _), false) =>
        val newLambda = lf.copy(function = replaceNullWithFalse(func))
        ae.copy(function = newLambda)
      case mf @ MapFilter(_, lf @ LambdaFunction(func, _, _)) =>
        val newLambda = lf.copy(function = replaceNullWithFalse(func))
        mf.copy(function = newLambda)
    }
  }

  /**
   * Recursively traverse the Boolean-type expression to replace
   * `Literal(null, BooleanType)` with `FalseLiteral`, if possible.
   *
   * Note that `transformExpressionsDown` can not be used here as we must stop as soon as we hit
   * an expression that is not [[CaseWhen]], [[If]], [[And]], [[Or]] or
   * `Literal(null, BooleanType)`.
   */
  private def replaceNullWithFalse(e: Expression): Expression = e match {
    case Literal(null, BooleanType) =>
      FalseLiteral
    case And(left, right) =>
      And(replaceNullWithFalse(left), replaceNullWithFalse(right))
    case Or(left, right) =>
      Or(replaceNullWithFalse(left), replaceNullWithFalse(right))
    case cw: CaseWhen if cw.dataType == BooleanType =>
      val newBranches = cw.branches.map { case (cond, value) =>
        replaceNullWithFalse(cond) -> replaceNullWithFalse(value)
      }
      val newElseValue = cw.elseValue.map(replaceNullWithFalse)
      CaseWhen(newBranches, newElseValue)
    case i @ If(pred, trueVal, falseVal) if i.dataType == BooleanType =>
      If(replaceNullWithFalse(pred), replaceNullWithFalse(trueVal), replaceNullWithFalse(falseVal))
    case e if e.dataType == BooleanType =>
      e
    case e =>
      val message = "Expected a Boolean type expression in replaceNullWithFalse, " +
        s"but got the type `${e.dataType.catalogString}` in `${e.sql}`."
      if (Utils.isTesting) {
        throw new IllegalArgumentException(message)
      } else {
        logWarning(message)
        e
      }
  }
}
