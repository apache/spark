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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.ExpressionSet
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}

/**
 * A visitor pattern for traversing a [[LogicalPlan]] tree and propagate the distinct attributes.
 */
object DistinctAttributesVisitor extends LogicalPlanVisitor[ExpressionSet] {
  override def default(p: LogicalPlan): ExpressionSet = {
    ExpressionSet()
  }

  override def visitAggregate(p: Aggregate): ExpressionSet = {
    if (p.groupOnly) ExpressionSet(p.output) else default(p)
  }

  override def visitDistinct(p: Distinct): ExpressionSet = ExpressionSet(p.output)

  override def visitExcept(p: Except): ExpressionSet =
    if (!p.isAll) ExpressionSet(p.output) else default(p)

  override def visitExpand(p: Expand): ExpressionSet = default(p)

  override def visitFilter(p: Filter): ExpressionSet = p.child.distinctAttributes

  override def visitGenerate(p: Generate): ExpressionSet = default(p)

  override def visitGlobalLimit(p: GlobalLimit): ExpressionSet = default(p)

  override def visitIntersect(p: Intersect): ExpressionSet = {
    if (!p.isAll) ExpressionSet(p.output) else default(p)
  }

  override def visitJoin(p: Join): ExpressionSet = {
    p.joinType match {
      case LeftSemi | LeftAnti => p.left.distinctAttributes
      case _ => default(p)
    }
  }

  override def visitLocalLimit(p: LocalLimit): ExpressionSet = default(p)

  override def visitPivot(p: Pivot): ExpressionSet = default(p)

  override def visitProject(p: Project): ExpressionSet = {
    if (p.output.forall(p.child.distinctAttributes.contains)) {
      ExpressionSet(p.output)
    } else {
      default(p)
    }
  }

  override def visitRepartition(p: Repartition): ExpressionSet = p.child.distinctAttributes

  override def visitRepartitionByExpr(p: RepartitionByExpression): ExpressionSet =
    p.child.distinctAttributes

  override def visitSample(p: Sample): ExpressionSet = default(p)

  override def visitScriptTransform(p: ScriptTransformation): ExpressionSet = default(p)

  override def visitUnion(p: Union): ExpressionSet = default(p)

  override def visitWindow(p: Window): ExpressionSet = default(p)

  override def visitTail(p: Tail): ExpressionSet = default(p)

  override def visitSort(sort: Sort): ExpressionSet = sort.child.distinctAttributes
}
