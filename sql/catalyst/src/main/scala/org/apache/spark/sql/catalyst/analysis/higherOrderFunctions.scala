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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._

/**
 * Resolve the lambda variables exposed by a higher order functions.
 *
 * This rule works in two steps:
 * [1]. Bind the anonymous variables exposed by the higher order function to the lambda function's
 *      arguments; this creates named and typed lambda variables. The argument names are checked
 *      for duplicates and the number of arguments are checked during this step.
 * [2]. Resolve the used lambda variables used in the lambda function's function expression tree.
 *      Note that we allow the use of variables from outside the current lambda, this can either
 *      be a lambda function defined in an outer scope, or a attribute in produced by the plan's
 *      child. If names are duplicate, the name defined in the most inner scope is used.
 */
object ResolveLambdaVariables extends Rule[LogicalPlan] {

  type LambdaVariableMap = Map[String, NamedExpression]

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsWithPruning(
      _.containsAnyPattern(HIGH_ORDER_FUNCTION, LAMBDA_FUNCTION, LAMBDA_VARIABLE), ruleId) {
      case q: LogicalPlan =>
        q.mapExpressions(resolve(_, Map.empty))
    }
  }

  /**
   * Resolve lambda variables in the expression subtree, using the passed lambda variable registry.
   */
  private def resolve(e: Expression, parentLambdaMap: LambdaVariableMap): Expression = e match {
    case _ if e.resolved => e

    case h: HigherOrderFunction if h.argumentsResolved && h.checkArgumentDataTypes().isSuccess =>
      SubqueryExpressionInLambdaOrHigherOrderFunctionValidator(e)
      h.bind(LambdaBinder(_, _)).mapChildren(resolve(_, parentLambdaMap))

    case l: LambdaFunction if !l.bound =>
      SubqueryExpressionInLambdaOrHigherOrderFunctionValidator(e)
      // Do not resolve an unbound lambda function. If we see such a lambda function this means
      // that either the higher order function has yet to be resolved, or that we are seeing
      // dangling lambda function.
      l

    case l: LambdaFunction if !l.hidden =>
      val lambdaMap = l.arguments.map(v => conf.canonicalize(v.name) -> v).toMap
      l.mapChildren(resolve(_, parentLambdaMap ++ lambdaMap))

    case u @ UnresolvedNamedLambdaVariable(name +: nestedFields) =>
      parentLambdaMap.get(conf.canonicalize(name)) match {
        case Some(lambda) =>
          nestedFields.foldLeft(lambda: Expression) { (expr, fieldName) =>
            ExtractValue(expr, Literal(fieldName), conf.resolver)
          }
        case None =>
          UnresolvedAttribute(u.nameParts)
      }

    case _ =>
      e.mapChildren(resolve(_, parentLambdaMap))
  }
}
