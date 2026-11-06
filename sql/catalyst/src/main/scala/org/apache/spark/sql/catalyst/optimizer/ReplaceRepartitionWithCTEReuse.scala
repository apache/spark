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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.REPARTITION_OPERATION
import org.apache.spark.sql.internal.SQLConf

/**
 * Converts plan-reuse repartitions into [[CTEReuseRelation]] nodes for guaranteed exchange reuse
 * in AQE.
 *
 * By the time this runs, [[ReplaceCTERefWithRepartition]] has already turned CTE references into
 * plan-reuse repartitions (a [[Repartition]] / [[RepartitionByExpression]] carrying a non-zero
 * `repartitionId`). This rule seals every plan-reuse
 * repartition -- the ones from CTE references as well as any pre-existing ones from decorrelation,
 * multi-distinct aggregate, or subplan reuse -- into a [[CTEReuseRelation]].
 *
 * A CTE/Subplan with same `repartitionId` referenced only once is directly inlined.
 *
 * The rule runs only on the main query plan: it counts a `repartitionId`'s references across the
 * whole plan (descending into subqueries) to decide reuse, so it must not run on a subquery
 * optimized on its own, where that count would be subquery-local and wrong.
 *
 * This rule is required for guaranteed reuse and is gated on
 * [[SQLConf.REPLACE_CTE_REF_WITH_CTE_REUSE]].
 */
object ReplaceRepartitionWithCTEReuse extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Skip a subquery optimized on its own (root is a Subquery); only the main query pass sees
    // every reference of a shared repartitionId. See the class doc.
    if (plan.isInstanceOf[Subquery] ||
        !conf.getConf(SQLConf.REPLACE_CTE_REF_WITH_CTE_REUSE) ||
        !plan.containsPattern(REPARTITION_OPERATION)) {
      return plan
    }

    // Count plan-reuse repartitions by id before converting; keep only the ids with a second
    // consumer. Counting by `repartitionId` is correct even though `deduplicatePlan` may have
    // produced copies with fresh exprIds -- the id is preserved across copies.
    val refCount = mutable.HashMap.empty[Long, Int]
    plan.foreachWithSubqueriesAndPruning(_.containsPattern(REPARTITION_OPERATION)) {
      case r: PlanReusableRepartition if r.isForPlanReuse =>
        refCount(r.repartitionId) = refCount.getOrElse(r.repartitionId, 0) + 1
      case _ =>
    }
    val reusedIds = refCount.collect { case (id, count) if count > 1 => id }.toSet
    if (reusedIds.isEmpty) {
      return plan
    }

    val result = plan.transformUpWithSubqueriesAndPruning(
        _.containsPattern(REPARTITION_OPERATION)) {
      case r: PlanReusableRepartition if r.isForPlanReuse && reusedIds.contains(r.repartitionId) =>
        CTEReuseRelation(
          cteId = r.repartitionId,
          partitioning = r.partitioning,
          sharedSubplan = r)
    }

    LogicalPlanIntegrity.validateCTEReuseRelations(result) match {
      case Some(msg) =>
        // Leave the plan-reuse repartitions unconverted rather than sealing an invalid plan. They
        // still plan and execute correctly (as LOCAL_SHUFFLE_FOR_CTE shuffles); only guaranteed
        // reuse is skipped for this query.
        logWarning(s"CTEReuseRelation validation failed, leaving plan-reuse repartitions " +
          s"unconverted:\n$msg")
        plan
      case None => result
    }
  }
}
