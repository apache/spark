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
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_REUSE_REPARTITION
import org.apache.spark.sql.internal.SQLConf

/**
 * Converts plan-reuse repartitions into [[CTEReuseRelation]] nodes for guaranteed exchange reuse.
 *
 * By the time this runs, [[ReplaceCTERefWithRepartition]] has turned the references of each
 * `forceSkipInline` definition that a batch query holds two or more copies of, of each
 * definition whose deterministic body equals such a sealed body, and of each definition that the
 * body of such a sealed, unpinned forced definition only selects and that the plan also reads
 * elsewhere, into plan-reuse [[RepartitionByExpression]]s: copies of the lowered body that all
 * carry the same nonzero `repartitionId`. No other rule assigns such ids. This rule seals every id
 * that still has two or more copies into [[CTEReuseRelation]]s sharing that id as their `cteId`.
 * With AQE on, the body is then materialized once, by one inner adaptive plan that every reference
 * reads. With AQE off, the copies are deduplicated by exchange reuse, which is best effort: a
 * miss, for example with `spark.sql.exchange.reuse` off, is only detected by `VerifyCTEReuse`,
 * which logs it or, under [[SQLConf.FAIL_ON_CTE_REUSE_WITHOUT_AQE]], throws.
 *
 * [[ReplaceCTERefWithRepartition]] assigns an id only to a body the plan holds two or more copies
 * of, so every id is expected to have two or more copies here. As a defensive
 * fallback, an id with a single copy is not sealed, and its id is reset to zero so the repartition
 * plans as an ordinary shuffle. If the sealed plan fails
 * [[LogicalPlanIntegrity.validateCTEReuseRelations]], every id is reset instead and nothing is
 * sealed.
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
        !plan.containsPattern(PLAN_REUSE_REPARTITION)) {
      return plan
    }

    // Count plan-reuse repartitions by id before converting; keep only the ids with a second
    // consumer. Counting by `repartitionId` is correct even though
    // `DeduplicateRelations.deduplicateRight`, which `ReplaceCTERefWithRepartition` uses to copy
    // each reference's body, may have produced copies with fresh exprIds -- the id is preserved
    // across copies.
    val refCount = mutable.HashMap.empty[Long, Int]
    plan.foreachWithSubqueriesAndPruning(_.containsPattern(PLAN_REUSE_REPARTITION)) {
      case r: PlanReusableRepartition if r.isForPlanReuse =>
        refCount(r.repartitionId) = refCount.getOrElse(r.repartitionId, 0) + 1
      case _ =>
    }
    if (refCount.isEmpty) {
      return plan
    }
    val reusedIds = refCount.collect { case (id, count) if count > 1 => id }.toSet

    // One bottom-up pass seals the reused ids and resets the others, inside the shared bodies
    // too: a nested repartition is rewritten before the enclosing one becomes a CTEReuseRelation.
    val result = sealOrReset(plan, reusedIds)

    LogicalPlanIntegrity.validateCTEReuseRelations(result) match {
      case Some(msg) =>
        // Seal nothing rather than an invalid plan: reset every plan-reuse id so the repartitions
        // plan as ordinary shuffles; only guaranteed reuse is skipped for this query.
        logWarning(s"CTEReuseRelation validation failed, leaving plan-reuse repartitions " +
          s"unsealed:\n$msg")
        sealOrReset(plan, Set.empty)
      case None => result
    }
  }

  private def sealOrReset(plan: LogicalPlan, reusedIds: Set[Long]): LogicalPlan = {
    plan.transformUpWithSubqueriesAndPruning(_.containsPattern(PLAN_REUSE_REPARTITION)) {
      case r: PlanReusableRepartition if r.isForPlanReuse =>
        if (reusedIds.contains(r.repartitionId)) {
          CTEReuseRelation(
            cteId = r.repartitionId,
            partitioning = r.partitioning,
            sharedSubplan = r)
        } else {
          r.withRepartitionId(0L)
        }
    }
  }
}
