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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.plans.logical.Aggregate

/**
 * Validates [[Aggregate]] operators in the single-pass resolver. Delegates to
 * [[ExprUtils.assertValidAggregation]] with canonicalized expression equality since
 * non-deterministic expressions are still not extracted (done in [[PullOutNondeterministic]] post
 * hoc rule).
 *
 * TODO: [[ExprUtils.assertValidAggregation]] does a post-traversal. This is discouraged in the
 * single-pass Analyzer.
 */
object AggregationValidator {

  /**
   * Applies [[ExprUtils.assertValidAggregation]] on a given [[Aggregate]].
   *
   * @param skipGroupingExprChecks When true, skips grouping-expression checks (type-is-orderable,
   *   no nested aggregates). Only set to true when unexpanded BaseGroupingSets are present and
   *   validation will be performed post-expansion.
   */
  def apply(aggregate: Aggregate, skipGroupingExprChecks: Boolean = false): Unit = {
    ExprUtils.assertValidAggregation(
      aggregate,
      (groupingExpression, checkedExpression) =>
        groupingExpression.canonicalized == checkedExpression.canonicalized,
      skipGroupingExprChecks
    )
  }
}
