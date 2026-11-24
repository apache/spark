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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.DeduplicateRelations
import org.apache.spark.sql.catalyst.expressions.{Alias, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, FORCE_SKIP_INLINE_CTE_DEF,
  PLAN_EXPRESSION}
import org.apache.spark.sql.internal.SQLConf

/**
 * Replaces CTE references that have not been previously inlined with repartition operations
 * which will then be planned as shuffles and reused across different reference points.
 *
 * A definition marked `forceSkipInline` that a batch query references at least twice is lowered,
 * when [[SQLConf.REPLACE_CTE_REF_WITH_CTE_REUSE]] is on, to a plan-reuse
 * [[RepartitionByExpression]] (a nonzero `repartitionId` shared by every copy of the definition),
 * which [[ReplaceRepartitionWithCTEReuse]] then seals into [[CTEReuseRelation]]s so the body is
 * materialized once. References are counted by the copies of the plan that holds them: a reference
 * inside the body of a definition that is not sealed counts once per copy of that body, a reference
 * inside a sealed definition, which is materialized once, counts once, and a reference inside a
 * definition that nothing reads counts zero. Every other definition, including a forced definition
 * read only once and every definition of a streaming query, keeps the plain lowering: a
 * repartition without a plan-reuse id, or no extra repartition when the body already repartitions
 * or hosts a merged scalar subquery. The one exception is a forced definition pinned through
 * `forcePartitioning` while the flag is on: it is repartitioned on its pin even when it is not
 * sealed.
 *
 * Note that this rule should be called at the very end of the optimization phase to best guarantee
 * that CTE repartition shuffles are reused.
 */
object ReplaceCTERefWithRepartition extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case _: Subquery => plan
    case _ =>
      val ctx = new LoweringContext(sealedDefinitions(plan))
      replaceWithRepartition(plan, mutable.HashMap.empty[Long, LogicalPlan], ctx)
  }

  /**
   * The ids of the definitions to seal for guaranteed reuse: with the reuse flag on and in a batch
   * query, each `forceSkipInline` definition whose body the lowered plan holds two or more copies
   * of. Streaming queries seal nothing, so the per-micro-batch rewrites, such as the reseeding of
   * random expressions, keep reaching every copy of a body.
   */
  private def sealedDefinitions(plan: LogicalPlan): Set[Long] = {
    // A plan with no forced definition, inside subqueries included, has nothing to seal.
    if (plan.isStreaming || !conf.getConf(SQLConf.REPLACE_CTE_REF_WITH_CTE_REUSE) ||
        !plan.containsPattern(FORCE_SKIP_INLINE_CTE_DEF)) {
      return Set.empty
    }
    // For each reference, the definition whose body holds it (None for the main query), counted
    // across the main query, its subqueries and every definition's body.
    val defs = mutable.HashMap.empty[Long, CTERelationDef]
    val refsByDef = mutable.HashMap.empty[Long, mutable.HashMap[Option[Long], Int]]
    def collect(p: LogicalPlan, enclosing: Option[Long]): Unit = {
      if (p.containsPattern(CTE)) {
        p match {
          case d: CTERelationDef =>
            defs(d.id) = d
            collect(d.child, Some(d.id))
          case ref: CTERelationRef =>
            val counts = refsByDef.getOrElseUpdate(ref.cteId, mutable.HashMap.empty)
            counts(enclosing) = counts.getOrElse(enclosing, 0) + 1
          case _ =>
            p.children.foreach(collect(_, enclosing))
            p.expressions.foreach(_.foreach {
              case s: SubqueryExpression => collect(s.plan, enclosing)
              case _ =>
            })
        }
      }
    }
    collect(plan, None)

    // The number of copies of each definition's body in the lowered plan. The body of a definition
    // that is not sealed is copied once per copy of each plan that references it; a sealed body is
    // materialized once, so the references inside it count once.
    val copies = mutable.HashMap.empty[Long, Int]
    val toSeal = mutable.HashSet.empty[Long]
    def copiesOf(enclosing: Option[Long]): Int = enclosing match {
      case None => 1
      case Some(id) =>
        val n = numCopies(id)
        if (toSeal.contains(id)) 1 else n
    }
    def numCopies(id: Long): Int = copies.get(id) match {
      case Some(n) => n
      case None =>
        // A definition only references earlier definitions; guard against a cycle anyway.
        copies(id) = 1
        val n = refsByDef.get(id).map(_.iterator.map { case (enclosing, count) =>
          count * copiesOf(enclosing)
        }.sum).getOrElse(0)
        copies(id) = n
        if (n >= 2 && defs.get(id).exists(_.forceSkipInline)) {
          toSeal += id
        }
        n
    }
    defs.keys.foreach(numCopies)
    toSeal.toSet
  }

  /**
   * Per-query state of the lowering: the definitions to seal, and the plan-reuse id assigned to
   * each deterministic shared body, so that forced definitions with canonically equal bodies are
   * materialized once, as canonical exchange reuse does for plain CTE repartitions.
   */
  private class LoweringContext(sealedIds: Set[Long]) {
    private val idByBody = mutable.HashMap.empty[LogicalPlan, Long]

    def shouldSeal(cteId: Long): Boolean = sealedIds.contains(cteId)

    def assignId(root: RepartitionByExpression): RepartitionByExpression = {
      if (root.isForPlanReuse) {
        // The root already is the plan-reuse repartition of another shared definition that this
        // body merely selects (e.g. `SELECT * FROM other_cte`), so it shares that identity.
        root
      } else if (root.deterministic) {
        root.withRepartitionId(
          idByBody.getOrElseUpdate(root.canonicalized, RepartitionIdGenerator.newRepartitionId))
      } else {
        root.withRepartitionId(RepartitionIdGenerator.newRepartitionId)
      }
    }
  }

  private def canSkipExtraRepartition(p: LogicalPlan): Boolean = p match {
    case _: RepartitionOperation => true
    case _: RebalancePartitions => true
    case _ => false
  }

  /**
   * The plain lowering of a CTE definition: no plan-reuse id, and no extra shuffle when the body
   * already repartitions or hosts a merged scalar subquery.
   */
  private def plainLowering(cteDef: CTERelationDef, inlined: LogicalPlan): LogicalPlan = {
    if (canSkipExtraRepartition(inlined) || cteDef.underSubquery) {
      inlined
    } else {
      RepartitionByExpression(Seq.empty, inlined, None)
    }
  }

  /**
   * The root of a shared forced definition as one repartition that can carry the plan-reuse id.
   * A root that already plans to a single shuffle is converted to a [[RepartitionByExpression]]
   * with the same partitioning instead of being wrapped in a second shuffle: a shuffling
   * [[Repartition]] keeps its partition count, and a [[RebalancePartitions]] keeps its keys, its
   * partition count and its advisory partition size (both take their partitioning from
   * [[HasPartitionExpressions]]). A converted rebalance gives up its adaptive coalescing and skew
   * splitting, because a CTE reuse shuffle keeps one frozen layout for all its consumers. A
   * coalesce root is wrapped in a repartition to its partition count, and any other root, such as
   * a merged-subquery body, in a round-robin repartition.
   */
  private def sealableRoot(inlined: LogicalPlan): RepartitionByExpression = inlined match {
    case r: RepartitionByExpression => r
    case Repartition(numPartitions, true, child) =>
      RepartitionByExpression(Seq.empty, child, Some(numPartitions))
    case Repartition(numPartitions, false, _) =>
      RepartitionByExpression(Seq.empty, inlined, Some(numPartitions))
    case r: RebalancePartitions =>
      RepartitionByExpression(
        r.partitionExpressions, r.child, r.optNumPartitions, r.optAdvisoryPartitionSize)
    case _ =>
      RepartitionByExpression(Seq.empty, inlined, None)
  }

  private def lower(
      cteDef: CTERelationDef,
      inlined: LogicalPlan,
      ctx: LoweringContext): LogicalPlan = {
    // The reuse-oriented lowering (pinned partitioning, plan-reuse id) only runs for a
    // `forceSkipInline` definition with guaranteed reuse enabled; otherwise this rule keeps the
    // plain lowering, so turning the flag off fully rolls the feature back. A pin on a definition
    // that is not `forceSkipInline` is ignored because such a definition may be inlined by
    // `InlineCTE` before this rule runs.
    if (!cteDef.forceSkipInline || !conf.getConf(SQLConf.REPLACE_CTE_REF_WITH_CTE_REUSE)) {
      return plainLowering(cteDef, inlined)
    }
    // Only a definition whose body a batch query holds two or more copies of is sealed for reuse
    // (see `sealedDefinitions`). A single copy already runs the body once.
    val seal = ctx.shouldSeal(cteDef.id)
    cteDef.forcePartitioning match {
      case Some(h: HashPartitioning) =>
        // A pinned partitioning materializes the CTE as a RepartitionByExpression on the pinned
        // expressions and partition count, whether or not it is sealed. It takes precedence over
        // `canSkipExtraRepartition`: the pin states the exact partitioning we must produce.
        val pinned = RepartitionByExpression(h.expressions, inlined, Some(h.numPartitions))
        if (seal) ctx.assignId(pinned) else pinned
      case Some(other) =>
        // forcePartitioning is an internal dev API; only HashPartitioning is supported
        // today (see CTERelationDef.forcePartitioning).
        throw new UnsupportedOperationException(
          s"CTERelationDef.forcePartitioning supports only HashPartitioning, but got " +
            s"${other.getClass.getSimpleName}")
      case None if seal =>
        ctx.assignId(sealableRoot(inlined))
      case None =>
        plainLowering(cteDef, inlined)
    }
  }

  private def replaceWithRepartition(
      plan: LogicalPlan,
      cteMap: mutable.HashMap[Long, LogicalPlan],
      ctx: LoweringContext): LogicalPlan = plan match {
    case WithCTE(child, cteDefs) =>
      cteDefs.foreach { cteDef =>
        val inlined = replaceWithRepartition(cteDef.child, cteMap, ctx)
        cteMap.put(cteDef.id, lower(cteDef, inlined, ctx))
      }
      replaceWithRepartition(child, cteMap, ctx)

    case ref: CTERelationRef =>
      val cteDefPlan = cteMap.getOrElse(ref.cteId,
        throw SparkException.internalError(
          s"No CTERelationDef found for CTERelationRef(cteId=${ref.cteId})."))

      if (ref.outputSet == cteDefPlan.outputSet) {
        cteDefPlan
      } else {
        val ctePlan = DeduplicateRelations.deduplicateRight(cteDefPlan, cteDefPlan)
        val projectList = ref.output.zip(ctePlan.output).map { case (tgtAttr, srcAttr) =>
          Alias(srcAttr, tgtAttr.name)(exprId = tgtAttr.exprId)
        }
        Project(projectList, ctePlan)
      }

    case _ if plan.containsPattern(CTE) =>
      plan
        .withNewChildren(plan.children.map(c => replaceWithRepartition(c, cteMap, ctx)))
        .transformExpressionsWithPruning(_.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
          case e: SubqueryExpression =>
            e.withNewPlan(replaceWithRepartition(e.plan, cteMap, ctx))
        }

    case _ => plan
  }
}
