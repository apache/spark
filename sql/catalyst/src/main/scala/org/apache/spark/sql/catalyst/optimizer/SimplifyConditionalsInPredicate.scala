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

import org.apache.spark.sql.catalyst.expressions.{And, ArrayExists, ArrayFilter, CaseWhen, Expression, If, LambdaFunction, Literal, MapFilter, Not, Or}
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.util.Utils


object SimplifyConditionalsInPredicate extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case f @ Filter(cond, _) => f.copy(condition = simplifyConditional(cond))
    case j @ Join(_, _, _, Some(cond), _) => j.copy(condition = Some(simplifyConditional(cond)))
    case d @ DeleteFromTable(_, Some(cond)) => d.copy(condition = Some(simplifyConditional(cond)))
    case u @ UpdateTable(_, _, Some(cond)) => u.copy(condition = Some(simplifyConditional(cond)))
    case p: LogicalPlan => p transformExpressions {
      case i @ If(pred, _, _) => i.copy(predicate = simplifyConditional(pred))
      case cw @ CaseWhen(branches, _) =>
        val newBranches = branches.map { case (cond, value) =>
          simplifyConditional(cond) -> value
        }
        cw.copy(branches = newBranches)
      case af @ ArrayFilter(_, lf @ LambdaFunction(func, _, _)) =>
        val newLambda = lf.copy(function = simplifyConditional(func))
        af.copy(function = newLambda)
      case ae @ ArrayExists(_, lf @ LambdaFunction(func, _, _), false) =>
        val newLambda = lf.copy(function = simplifyConditional(func))
        ae.copy(function = newLambda)
      case mf @ MapFilter(_, lf @ LambdaFunction(func, _, _)) =>
        val newLambda = lf.copy(function = simplifyConditional(func))
        mf.copy(function = newLambda)
    }
  }

  private def simplifyConditional(e: Expression): Expression = e match {
    case Literal(null, BooleanType) => FalseLiteral
    case And(left, right) => And(simplifyConditional(left), simplifyConditional(right))
    case Or(left, right) => Or(simplifyConditional(left), simplifyConditional(right))
    case If(cond, t, FalseLiteral) => And(cond, t)
    case If(cond, t, TrueLiteral) => Or(Not(cond), t)
    case If(cond, FalseLiteral, f) => And(Not(cond), f)
    case If(cond, TrueLiteral, f) => Or(cond, f)
    case CaseWhen(Seq((cond, trueValue)),
        Some(FalseLiteral) | Some(Literal(null, BooleanType)) | None) =>
      And(cond, trueValue)
    case CaseWhen(Seq((cond, trueValue)), Some(TrueLiteral)) =>
      Or(Not(cond), trueValue)
    case CaseWhen(Seq((cond, FalseLiteral)), elseValue) =>
      And(Not(cond), elseValue.getOrElse(Literal(null, BooleanType)))
    case CaseWhen(Seq((cond, TrueLiteral)), elseValue) =>
      Or(cond, elseValue.getOrElse(Literal(null, BooleanType)))
    case e if e.dataType == BooleanType => e
    case e =>
      val message = "Expected a Boolean type expression in simplifyConditional, " +
        s"but got the type `${e.dataType.catalogString}` in `${e.sql}`."
      if (Utils.isTesting) {
        throw new IllegalArgumentException(message)
      } else {
        logWarning(message)
        e
      }
  }
}
