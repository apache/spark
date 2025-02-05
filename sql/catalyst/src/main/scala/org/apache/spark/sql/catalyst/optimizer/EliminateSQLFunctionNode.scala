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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.{SQLFunctionExpression, SQLFunctionNode, SQLScalarFunction, SQLTableFunction}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * This rule removes [[SQLScalarFunction]] and [[SQLFunctionNode]] wrapper. They are respected
 * till the end of analysis stage because we want to see which part of an analyzed logical
 * plan is generated from a SQL function and also perform ACL checks.
 */
object EliminateSQLFunctionNode extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Include subqueries when eliminating SQL function expressions otherwise we might miss
    // expressions in subqueries which can be inlined by the rule `OptimizeOneRowRelationSubquery`.
    plan.transformWithSubqueries {
      case SQLFunctionNode(_, child) => child
      case f: SQLTableFunction =>
        throw SparkException.internalError(
          s"SQL table function plan should be rewritten during analysis: $f")
      case p: LogicalPlan => p.transformExpressions {
        case f: SQLScalarFunction => f.child
        case f: SQLFunctionExpression =>
          throw SparkException.internalError(
            s"SQL function expression should be rewritten during analysis: $f")
      }
    }
  }
}
