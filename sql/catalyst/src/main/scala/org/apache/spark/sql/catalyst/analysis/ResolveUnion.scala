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

import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.UNION

/**
 * Resolves different children of Union to a common set of columns.
 */
object ResolveUnion extends Rule[LogicalPlan] {
  private def unionTwoSides(
      left: LogicalPlan,
      right: LogicalPlan,
      allowMissingCol: Boolean): LogicalPlan = {
    val alignedChildren = UnionByNameTransformer.alignChildrenByName(
      left = left,
      leftOutput = left.output,
      right = right,
      rightOutput = right.output,
      allowMissingCol = allowMissingCol,
      newAlias = (child, name) => Alias(child, name)())
    Union(alignedChildren.left, alignedChildren.right)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    _.containsPattern(UNION), ruleId) {
    case e if !e.childrenResolved => e

    case Union(children, byName, allowMissingCol) if byName =>
      children.reduceLeft { (left, right) =>
        unionTwoSides(left, right, allowMissingCol)
      }
  }
}
