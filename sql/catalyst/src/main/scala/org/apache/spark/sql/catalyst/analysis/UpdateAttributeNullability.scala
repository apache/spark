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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Updates nullability of Attributes in a resolved LogicalPlan by using the nullability of
 * corresponding Attributes of its children output Attributes. This step is needed because
 * users can use a resolved AttributeReference in the Dataset API and outer joins
 * can change the nullability of an AttribtueReference. Without this rule, a nullable column's
 * nullable field can be actually set as non-nullable, which cause illegal optimization
 * (e.g., NULL propagation) and wrong answers.
 * See SPARK-13484 and SPARK-13801 for the concrete queries of this case.
 */
object UpdateAttributeNullability extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
    // Skip unresolved nodes.
    case p if !p.resolved => p
    // Skip leaf node, as it has no child and no need to update nullability.
    case p: LeafNode => p
    case p: LogicalPlan =>
      val nullabilities = p.children.flatMap(c => c.output).groupBy(_.exprId).map {
        // If there are multiple Attributes having the same ExprId, we need to resolve
        // the conflict of nullable field. We do not really expect this to happen.
        case (exprId, attributes) => exprId -> attributes.exists(_.nullable)
      }
      // For an Attribute used by the current LogicalPlan, if it is from its children,
      // we fix the nullable field by using the nullability setting of the corresponding
      // output Attribute from the children.
      p.transformExpressions {
        case attr: Attribute if nullabilities.contains(attr.exprId) =>
          attr.withNullability(nullabilities(attr.exprId))
      }
  }
}
