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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * The result of [[SubqueryExpression.plan]] resolution. This is used internally in
 * [[SubqueryExpressionResolver]].
 *
 * @param plan The resolved plan of the subquery.
 * @param output Plan output. We don't use [[LogicalPlan.output]] in the single-pass Analyzer,
 *   because this method is often recursive.
 * @param outerExpressions The outer expressions that are references in the plan. [[OuterReference]]
 *   wrapper is stripped away. These can be either actual leaf [[AttributeReference]]s or
 *   [[AggregateExpression]]s with outer references inside.
 */
case class ResolvedSubqueryExpressionPlan(
    plan: LogicalPlan,
    output: Seq[Attribute],
    outerExpressions: Seq[Expression])
