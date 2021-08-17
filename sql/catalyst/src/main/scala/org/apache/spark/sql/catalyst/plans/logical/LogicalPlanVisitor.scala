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

/**
 * A visitor pattern for traversing a [[LogicalPlan]] tree and computing some properties.
 */
trait LogicalPlanVisitor[T] {

  def visit(p: LogicalPlan): T = p match {
    case p: Aggregate => visitAggregate(p)
    case p: Distinct => visitDistinct(p)
    case p: Except => visitExcept(p)
    case p: Expand => visitExpand(p)
    case p: Filter => visitFilter(p)
    case p: Generate => visitGenerate(p)
    case p: GlobalLimit => visitGlobalLimit(p)
    case p: Intersect => visitIntersect(p)
    case p: Join => visitJoin(p)
    case p: LocalLimit => visitLocalLimit(p)
    case p: Pivot => visitPivot(p)
    case p: Project => visitProject(p)
    case p: Repartition => visitRepartition(p)
    case p: RepartitionByExpression => visitRepartitionByExpr(p)
    case p: Sample => visitSample(p)
    case p: ScriptTransformation => visitScriptTransform(p)
    case p: Union => visitUnion(p)
    case p: Window => visitWindow(p)
    case p: Tail => visitTail(p)
    case p: Sort => visitSort(p)
    case p: WithCTE => visitWithCTE(p)
    case p: LogicalPlan => default(p)
  }

  def default(p: LogicalPlan): T

  def visitAggregate(p: Aggregate): T

  def visitDistinct(p: Distinct): T

  def visitExcept(p: Except): T

  def visitExpand(p: Expand): T

  def visitFilter(p: Filter): T

  def visitGenerate(p: Generate): T

  def visitGlobalLimit(p: GlobalLimit): T

  def visitIntersect(p: Intersect): T

  def visitJoin(p: Join): T

  def visitLocalLimit(p: LocalLimit): T

  def visitPivot(p: Pivot): T

  def visitProject(p: Project): T

  def visitRepartition(p: Repartition): T

  def visitRepartitionByExpr(p: RepartitionByExpression): T

  def visitSample(p: Sample): T

  def visitScriptTransform(p: ScriptTransformation): T

  def visitUnion(p: Union): T

  def visitWindow(p: Window): T

  def visitTail(p: Tail): T

  def visitSort(sort: Sort): T

  def visitWithCTE(p: WithCTE): T
}
