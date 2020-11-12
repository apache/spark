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

package org.apache.spark.sql.catalyst.plans.logical.statsEstimation

import org.apache.spark.sql.catalyst.plans.logical._

/**
 * A [[LogicalPlanVisitor]] that computes the statistics for the cost-based optimizer.
 */
object BasicStatsPlanVisitor extends LogicalPlanVisitor[Statistics] {

  /** Falls back to the estimation computed by [[SizeInBytesOnlyStatsPlanVisitor]]. */
  private def fallback(p: LogicalPlan): Statistics = SizeInBytesOnlyStatsPlanVisitor.visit(p)

  override def default(p: LogicalPlan): Statistics = fallback(p)

  override def visitAggregate(p: Aggregate): Statistics = {
    AggregateEstimation.estimate(p).getOrElse(fallback(p))
  }

  override def visitDistinct(p: Distinct): Statistics = fallback(p)

  override def visitExcept(p: Except): Statistics = fallback(p)

  override def visitExpand(p: Expand): Statistics = fallback(p)

  override def visitFilter(p: Filter): Statistics = {
    FilterEstimation(p).estimate.getOrElse(fallback(p))
  }

  override def visitGenerate(p: Generate): Statistics = fallback(p)

  override def visitGlobalLimit(p: GlobalLimit): Statistics = fallback(p)

  override def visitIntersect(p: Intersect): Statistics = fallback(p)

  override def visitJoin(p: Join): Statistics = {
    JoinEstimation(p).estimate.getOrElse(fallback(p))
  }

  override def visitLocalLimit(p: LocalLimit): Statistics = fallback(p)

  override def visitPivot(p: Pivot): Statistics = fallback(p)

  override def visitProject(p: Project): Statistics = {
    ProjectEstimation.estimate(p).getOrElse(fallback(p))
  }

  override def visitRepartition(p: Repartition): Statistics = fallback(p)

  override def visitRepartitionByExpr(p: RepartitionByExpression): Statistics = fallback(p)

  override def visitSample(p: Sample): Statistics = fallback(p)

  override def visitScriptTransform(p: ScriptTransformation): Statistics = fallback(p)

  override def visitUnion(p: Union): Statistics = fallback(p)

  override def visitWindow(p: Window): Statistics = fallback(p)
}
