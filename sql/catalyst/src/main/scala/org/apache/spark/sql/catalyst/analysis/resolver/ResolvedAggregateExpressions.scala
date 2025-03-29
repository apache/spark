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

import java.util.HashSet

import org.apache.spark.sql.catalyst.expressions.NamedExpression

/**
 * [[ResolvedAggregateExpressions]] is used by the
 * [[ExpressionResolver.resolveAggregateExpressions]] to return resolution results.
 *  - expressions: The resolved expressions. They are resolved using the
 *      `resolveExpressionTreeInOperator`.
 *  - resolvedExpressionsWithoutAggregates: List of resolved aggregate expressions that don't have
 *      [[AggregateExpression]]s in their subtrees.
 *  - hasAttributeOutsideOfAggregateExpressions: True if `expressions` list contains any attributes
 *      that are not under an [[AggregateExpression]].
 *  - hasStar: True if there is a star (`*`) in aggregate expressions list
 *  - expressionIndexesWithAggregateFunctions: Indices of expressions in aggregate expressions list
 *      that have aggregate functions in their subtrees.
 */
case class ResolvedAggregateExpressions(
    expressions: Seq[NamedExpression],
    resolvedExpressionsWithoutAggregates: Seq[NamedExpression],
    hasAttributeOutsideOfAggregateExpressions: Boolean,
    hasStar: Boolean,
    expressionIndexesWithAggregateFunctions: HashSet[Int]
)
