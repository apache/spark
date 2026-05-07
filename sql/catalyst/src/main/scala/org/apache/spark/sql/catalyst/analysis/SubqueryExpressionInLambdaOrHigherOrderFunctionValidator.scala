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

import org.apache.spark.sql.catalyst.analysis.ResolveLambdaVariables.conf
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * Object used to validate whether there are subquery expressions in [[LambdaFunction]] or
 * [[HigherOrderFunction]].
 */
object SubqueryExpressionInLambdaOrHigherOrderFunctionValidator {

  /**
   * SPARK-47509: There is a correctness bug when subquery expressions appear within lambdas or
   * higher-order functions. Here we check for that case and return an error if the corresponding
   * configuration indicates to do so.
   */
  def apply(expression: Expression): Unit = {
    if (expression.containsPattern(PLAN_EXPRESSION) &&
      !conf.getConf(SQLConf.ALLOW_SUBQUERY_EXPRESSIONS_IN_LAMBDAS_AND_HIGHER_ORDER_FUNCTIONS)) {
      throw QueryCompilationErrors.subqueryExpressionInLambdaOrHigherOrderFunctionNotAllowedError()
    }
  }
}
