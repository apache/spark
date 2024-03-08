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

import org.apache.spark.sql.catalyst.expressions.{BinaryComparison, DoubleLiteral, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Rand}
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{BINARY_COMPARISON, EXPRESSION_WITH_RANDOM_SEED, LITERAL}

/**
 * Rand() generates a random column with i.i.d. uniformly distributed values in [0, 1), so
 * compare double literal value with 1.0 or 0.0 could eliminate Rand() in binary comparison.
 *
 * 1. Converts the binary comparison to true literal when the comparison value must be true.
 * 2. Converts the binary comparison to false literal when the comparison value must be false.
 */
object OptimizeRand extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformAllExpressionsWithPruning(_.containsAllPatterns(
      EXPRESSION_WITH_RANDOM_SEED, LITERAL, BINARY_COMPARISON), ruleId) {
      case op @ BinaryComparison(DoubleLiteral(_), _: Rand) => eliminateRand(swapComparison(op))
      case op @ BinaryComparison(_: Rand, DoubleLiteral(_)) => eliminateRand(op)
  }

  /**
   * Swaps the left and right sides of some binary comparisons. e.g., transform "a < b" to "b > a"
   */
  private def swapComparison(comparison: BinaryComparison): BinaryComparison = comparison match {
    case a LessThan b => GreaterThan(b, a)
    case a LessThanOrEqual b => GreaterThanOrEqual(b, a)
    case a GreaterThan b => LessThan(b, a)
    case a GreaterThanOrEqual b => LessThanOrEqual(b, a)
    case o => o
  }

  private def eliminateRand(op: BinaryComparison): Expression = op match {
    case GreaterThan(_: Rand, DoubleLiteral(value)) =>
      if (value < 0.0) TrueLiteral else if (value >= 1.0) FalseLiteral else op
    case GreaterThanOrEqual(_: Rand, DoubleLiteral(value)) =>
      if (value <= 0.0) TrueLiteral else if (value >= 1.0) FalseLiteral else op
    case LessThan(_: Rand, DoubleLiteral(value)) =>
      if (value >= 1.0) TrueLiteral else if (value <= 0.0) FalseLiteral else op
    case LessThanOrEqual(_: Rand, DoubleLiteral(value)) =>
      if (value >= 1.0) TrueLiteral else if (value < 0.0) FalseLiteral else op
    case other => other
  }
}
