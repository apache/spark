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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, CollationKey}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.types.StringType

/**
 * This rule rewrites Aggregates to ensure that all aggregations containing non-binary collated
 * strings are performed with respect to collation keys. This is necessary because hash aggregation
 * is generally evaluated using binary equality, which does not work correctly for non-binary
 * collated strings. However, by injecting CollationKey expressions into the corresponding grouping
 * expressions, we allow HashAggregate to work properly on this type of data.
 */
object RewriteGroupByCollation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUpWithNewOutput {
    case a: Aggregate if canRewriteAggregateCollation(a) =>

      val newGroupingExpressions = a.groupingExpressions.map {
        case attr: AttributeReference if attr.dataType.isInstanceOf[StringType] &&
          !CollationFactory.fetchCollation(
            attr.dataType.asInstanceOf[StringType].collationId).supportsBinaryEquality =>
          CollationKey(attr)
        case other => other
      }

      val newAggregate = a.copy(
        groupingExpressions = newGroupingExpressions,
        aggregateExpressions = a.aggregateExpressions
      )

      if (!newAggregate.fastEquals(a)) {
        (newAggregate, a.output.zip(newAggregate.output))
      } else {
        (a, a.output.zip(a.output))
      }
  }

  private def canRewriteAggregateCollation(aggregate: Aggregate): Boolean = {
    // This rewrite rule is used to enabled hash aggregation on collated string columns. However,
    // hash aggregation is currently only supported for grouping aggregations - this means that no
    // string type can be found in the aggregate expressions, so we avoid rewrite in this case.
    !aggregate.aggregateExpressions.exists(e => e.dataType.isInstanceOf[StringType])
  }

}
