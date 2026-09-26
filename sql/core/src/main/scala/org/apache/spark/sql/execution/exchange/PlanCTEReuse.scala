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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{QUERY_ID, QUERY_PLAN}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

/**
 * Unwraps [[CTEReuseExchange]] nodes early in the preparation pipeline (AQE off).
 *
 * Each [[CTEReuseExchange]] is replaced by its `subplan` (a [[ShuffleExchangeExec]] with a
 * `LOCAL_SHUFFLE_FOR_CTE(cteId)` origin over the CTE body). The `cteId` is carried by the origin
 * itself (set in `SparkStrategies` when the shuffle is built), so [[VerifyCTEReuse]] can later
 * confirm the copies were reused without relying on a tree-node tag -- the origin survives node
 * reconstructions (e.g. photonization rebuilding the shuffle) that would drop a tag.
 *
 * After unwrapping, all preparation rules (EnsureRequirements, PhotonStageCompiler, columnar
 * transitions, codegen, etc.) process each CTE subplan copy normally, in its own consumer context.
 * The `LOCAL_SHUFFLE_FOR_CTE` shuffle serves as a protected boundary: `EnsureRequirements` /
 * `EnsureRequirementsDP` treat it as immutable (see `ShuffleExchangeLike.isCreatedForSubplanReuse`)
 * and add consumer-dependent operators above it, so the subtree below stays canonically equal
 * across copies. The final `ReuseExchangeAndSubquery` then reuses the copies by canonical equality.
 *
 * Runs in both main and subquery preparation batches so CTE references inside subqueries are also
 * unwrapped.
 */
case object UnwrapCTEReuseExchange extends Rule[SparkPlan] with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    // Plain transformUp (not transformUpWithSubqueries) is sufficient: subquery plans are prepared
    // separately by PlanSubqueries, which calls QueryExecution.prepareExecutedPlan ->
    // preparations(subquery = true), so this rule is applied recursively to each subquery's plan
    // on its own. We only need to handle CTEReuseExchange nodes in the current plan tree here.
    plan.transformUp {
      case cte: CTEReuseExchange =>
        // Recursively unwrap nested CTEReuseExchange nodes inside the subplan
        // (e.g., cte_inner inside cte_outer's body).
        val unwrapped = apply(cte.subplan)
        unwrapped match {
          case s: ShuffleExchangeLike if s.shuffleOrigin.isInstanceOf[LOCAL_SHUFFLE_FOR_CTE] =>
            // The cteId is already carried by the LOCAL_SHUFFLE_FOR_CTE origin (SparkStrategies set
            // it from the cteId in CTEReuseRelation); nothing extra to attach here.
            logDebug(log"CTE reuse (AQE off): unwrapped CTEReuseExchange for " +
              log"cteId=${MDC(QUERY_ID, cte.cteId)}")
          case other =>
            // The CTEReuseExchange subplan should always be a LOCAL_SHUFFLE_FOR_CTE shuffle
            // (SparkStrategies builds it that way). If it isn't, guaranteed reuse cannot be
            // tracked -- log the error (it is still unwrapped so the query can run, just without
            // guaranteed reuse for this cteId).
            logError(log"CTE reuse (AQE off): expected a LOCAL_SHUFFLE_FOR_CTE shuffle under " +
              log"CTEReuseExchange(cteId=${MDC(QUERY_ID, cte.cteId)}), but got " +
              log"${MDC(QUERY_PLAN, other.nodeName)}. Plan:\n${MDC(QUERY_PLAN, other.treeString)}")
        }
        unwrapped
    }
  }
}

/**
 * Verifies that guaranteed CTE shuffle reuse held (AQE off). Runs at the very end of the
 * preparation pipeline (inside `reuseRules`), after [[ReuseExchangeAndSubquery]], and only on the
 * final main-query pass -- not the per-subquery `PlanSubqueries` invocations, where reuse is not
 * yet complete.
 *
 * We rely on the stock [[ReuseExchangeAndSubquery]] (canonical matching) to reuse the CTE shuffles:
 * because all copies of a `cteId` are planned from the same canonicalized `sharedSubplan` and the
 * `LOCAL_SHUFFLE_FOR_CTE` shuffle is protected from `EnsureRequirements`, the copies stay
 * canonically equal and are deduplicated into [[ReusedExchangeExec]]. After that pass, at most one
 * live (non-reused) shuffle per `cteId` should remain.
 *
 * If two or more live shuffles share a `cteId` tag, reuse did NOT apply for that CTE (e.g. a
 * consumer forced a divergent form above the boundary). This is a best-effort optimization, not a
 * correctness invariant, so:
 *  - when [[SQLConf.FAIL_ON_CTE_REUSE_WITHOUT_AQE]] is on (test default),
 *    throw, to catch reuse regressions in tests;
 *  - otherwise, log the plan / error / cteIds.
 *
 * This rule does not modify the plan.
 */
case class VerifyCTEReuse(
    failOnReuseFailure: Boolean) extends Rule[SparkPlan] with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    // Count live (non-reused) shuffles per cteId, keyed by the cteId carried in the
    // LOCAL_SHUFFLE_FOR_CTE origin. ReusedExchangeExec is a leaf whose canonical form is its child,
    // and it does not itself expose a LOCAL_SHUFFLE_FOR_CTE origin, so only un-deduplicated
    // shuffles are counted. Reading the id from the origin (rather than a tree-node tag) means node
    // reconstructions that preserve `shuffleOrigin` but drop tags -- e.g. photonization rebuilding
    // the shuffle -- do not blind this check.
    val countByCteId = scala.collection.mutable.HashMap.empty[Long, Int]
    plan.foreachWithSubqueries {
      case s: ShuffleExchangeLike =>
        s.shuffleOrigin match {
          case LOCAL_SHUFFLE_FOR_CTE(cteId) =>
            countByCteId(cteId) = countByCteId.getOrElse(cteId, 0) + 1
          case _ =>
        }
      case _ =>
    }

    val notReused = countByCteId.filter { case (_, count) => count >= 2 }
    if (notReused.nonEmpty) {
      val summary = notReused.toSeq.sortBy(_._1)
        .map { case (id, count) => s"cteId=$id ($count shuffles)" }.mkString(", ")
      if (failOnReuseFailure) {
        throw SparkException.internalError(
          s"Guaranteed CTE shuffle reuse (AQE off) failed: [$summary] have >= 2 un-reused " +
            s"shuffles after ReuseExchangeAndSubquery.\nPlan:\n${plan.treeString}")
      } else {
        notReused.foreach { case (cteId, _) =>
          logWarning(log"Guaranteed CTE shuffle reuse (AQE off) not applied for " +
            log"cteId=${MDC(QUERY_ID, cteId)}.")
        }
        logWarning(log"CTE reuse (AQE off) not applied: ${MDC(QUERY_PLAN, summary)}. " +
          log"Plan:\n${MDC(QUERY_PLAN, plan.treeString)}")
      }
    }
    plan
  }
}
