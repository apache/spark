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
 * which [[ReplaceRepartitionWithCTEReuse]] then seals into [[CTEReuseRelation]]s. With AQE on, the
 * body is then materialized once; with AQE off, the sealed copies are deduplicated by exchange
 * reuse on a best-effort basis (see [[ReplaceRepartitionWithCTEReuse]]). References are counted
 * by the copies of the plan that holds them: a reference inside the body of a definition that is
 * not sealed counts once per copy of that body, a reference inside a sealed definition, whose body
 * all of its references share, counts once, and a reference inside a definition that nothing
 * reads counts zero. A definition that the plan reads but that is not sealed on its own, such as a
 * forced definition read only once or an ordinary retained or MATERIALIZED CTE, is sealed together
 * with a sealed definition when its deterministic body, lowered as a sealed root, is canonically
 * equal to that definition's root: it then shares that body's plan-reuse id, so the body is still
 * computed once, as canonical exchange reuse did for their plain repartitions (see
 * `LoweringContext.unsealedTwins`). Every other definition, including every definition of a
 * streaming query, keeps the plain lowering: a repartition without a plan-reuse id, or no extra
 * repartition when the body already repartitions or hosts a merged scalar subquery. The one
 * exception is a forced definition pinned through `forcePartitioning` while the flag is on: it is
 * repartitioned on its pin even when it is not sealed.
 *
 * Note that this rule should be called at the very end of the optimization phase to best guarantee
 * that CTE repartition shuffles are reused.
 */
object ReplaceCTERefWithRepartition extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case _: Subquery => plan
    case _ =>
      val (lowered, ctx) = lowerPlan(plan, twins = Set.empty)
      val twins = ctx.unsealedTwins
      if (twins.isEmpty) {
        lowered
      } else {
        // Lower again with the twins sealed too, so that each shares the plan-reuse id of the
        // sealed body it equals. Sealing a twin that the plan reads more than once lowers the
        // number of copies of the bodies it references, which can change their lowering and so
        // the twin's own root. Keep the first lowering when a twin then no longer shares the id
        // of a definition sealed on its own.
        val (relowered, twinCtx) = lowerPlan(plan, twins)
        if (twinCtx.twinsShareSealedIds(twins)) relowered else lowered
      }
  }

  private def lowerPlan(plan: LogicalPlan, twins: Set[Long]): (LogicalPlan, LoweringContext) = {
    val (sealedIds, copies) = sealedDefinitions(plan, twins)
    val ctx = new LoweringContext(sealedIds, copies)
    (replaceWithRepartition(plan, mutable.HashMap.empty[Long, LogicalPlan], ctx), ctx)
  }

  /**
   * The ids of the definitions to seal for guaranteed reuse: with the reuse flag on and in a batch
   * query, each `forceSkipInline` definition whose body the lowered plan holds two or more copies
   * of, and each definition in `twins` (see `LoweringContext.unsealedTwins`) whose body the lowered
   * plan holds at least one copy of. Streaming queries seal nothing, so the per-micro-batch
   * rewrites, such as the reseeding of random expressions, keep reaching every copy of a body.
   * Also returns the number of copies of every definition's body in the lowered plan (empty when
   * the plan is streaming, the reuse flag is off, or the plan has no `forceSkipInline`
   * definition).
   */
  private def sealedDefinitions(
      plan: LogicalPlan,
      twins: Set[Long]): (Set[Long], Map[Long, Int]) = {
    // A plan with no forced definition, inside subqueries included, has nothing to seal.
    if (plan.isStreaming || !conf.getConf(SQLConf.REPLACE_CTE_REF_WITH_CTE_REUSE) ||
        !plan.containsPattern(FORCE_SKIP_INLINE_CTE_DEF)) {
      return (Set.empty, Map.empty)
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
        if ((n >= 2 && defs.get(id).exists(_.forceSkipInline)) || (n >= 1 && twins.contains(id))) {
          toSeal += id
        }
        n
    }
    defs.keys.foreach(numCopies)
    (toSeal.toSet, copies.toMap)
  }

  /**
   * Per-query state of the lowering: the definitions to seal, the number of copies of each
   * definition's body, the definition that lowered each root, and the plan-reuse id assigned to
   * each deterministic shared body, so that sealed definitions with canonically equal bodies are
   * materialized once, as canonical exchange reuse does for plain CTE repartitions. It also keeps
   * the root that each definition the plan reads but does not seal would be sealed with, so that
   * such a definition whose deterministic body equals a sealed body can share that body's single
   * materialization instead of computing it once more (see `unsealedTwins`).
   */
  private class LoweringContext(sealedIds: Set[Long], copies: Map[Long, Int]) {
    private val idByBody = mutable.HashMap.empty[LogicalPlan, Long]
    // The plan-reuse id each sealed definition's root was given.
    private val idByDef = mutable.HashMap.empty[Long, Long]
    // The root each definition that the plan reads but does not seal would be sealed with.
    private val unsealedRoots = mutable.LinkedHashMap.empty[Long, RepartitionByExpression]
    // The definition whose lowering produced each lowered plan, by identity. A definition whose
    // body only selects another definition lowers to that definition's plan object itself, which
    // keeps its first owner.
    private val ownerByLowering = new java.util.IdentityHashMap[LogicalPlan, java.lang.Long]()

    def shouldSeal(cteId: Long): Boolean = sealedIds.contains(cteId)

    def recordLowering(cteId: Long, lowered: LogicalPlan): Unit = {
      ownerByLowering.putIfAbsent(lowered, cteId)
    }

    /**
     * Whether `inlined`, the inlined body of the definition `cteId`, is the lowered plan of
     * another definition that is not sealed and whose body the lowered plan also holds outside
     * this definition. Stamping such a root with a plan-reuse id would give this definition's
     * copy a different canonical form than the other copies of that body, so they would no
     * longer share one shuffle.
     */
    def isRetainedElsewhere(cteId: Long, inlined: LogicalPlan): Boolean = {
      Option(ownerByLowering.get(inlined)).map(_.longValue).exists { owner =>
        owner != cteId && !shouldSeal(owner) && copies.getOrElse(owner, 0) >= 2
      }
    }

    /** Gives `root`, the sealed root of the definition `cteId`, its plan-reuse id. */
    def seal(cteId: Long, root: RepartitionByExpression): RepartitionByExpression = {
      val sealedRoot = assignId(root)
      idByDef(cteId) = sealedRoot.repartitionId
      sealedRoot
    }

    /**
     * Records `root`, the root that the definition `cteId`, which is not sealed, would be sealed
     * with. Only a plan that seals a definition and reads `cteId` records it.
     */
    def recordUnsealed(cteId: Long, root: => RepartitionByExpression): Unit = {
      if (sealedIds.nonEmpty && copies.getOrElse(cteId, 0) >= 1) {
        unsealedRoots(cteId) = root
      }
    }

    /**
     * The definitions that the plan reads but does not seal whose deterministic root, as it would
     * be sealed, is canonically equal to the root of a sealed definition. On the base lowering
     * their plain repartitions and the sealed definition's were canonically equal, so exchange
     * reuse computed the body once; the protected shuffle of a sealed body no longer matches a
     * plain one, so they must be sealed with it to keep that single computation. A root that
     * already carries a plan-reuse id, of a sealed definition the body only selects, already
     * shares it. A twin is found whichever definition comes first, because this is read after the
     * whole plan is lowered.
     */
    def unsealedTwins: Set[Long] = unsealedRoots.iterator.collect {
      case (cteId, root) if !root.isForPlanReuse && root.deterministic &&
          idByBody.contains(root.canonicalized) => cteId
    }.toSet

    /** Whether every one of `twins` shares the plan-reuse id of a definition sealed on its own. */
    def twinsShareSealedIds(twins: Set[Long]): Boolean = {
      val ownIds = idByDef.iterator.collect { case (cteId, id) if !twins.contains(cteId) => id }
        .toSet
      twins.forall(cteId => idByDef.get(cteId).exists(ownIds.contains))
    }

    private def assignId(root: RepartitionByExpression): RepartitionByExpression = {
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
   *
   * The returned root is planned as a protected `LOCAL_SHUFFLE_FOR_CTE` shuffle, which
   * `EnsureRequirements` never removes. So a user hash repartition root that the body already
   * satisfies, such as `REPARTITION(k)` over `GROUP BY k`, stays as a shuffle of the CTE output,
   * where the base lowering let `EnsureRequirements` remove it: a forced CTE whose copies exchange
   * reuse already shared there shuffles its output one more time. The alternative, letting the
   * inner plan of a deterministic body materialize at the body's own last shuffle, would re-run
   * the operators above that shuffle once per consumer and need a second materialization shape
   * for the CTE reuse stage, while a non-deterministic body would still need the root shuffle. It
   * is not taken: the root shuffle is the one materialization point every reference reads.
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
    // A pin is only honored for a `forceSkipInline` definition with guaranteed reuse enabled; with
    // the flag off, every definition keeps the plain lowering, so turning the flag off fully rolls
    // the feature back. A pin on a definition that is not `forceSkipInline` is ignored because
    // such a definition may be inlined by `InlineCTE` before this rule runs.
    val pinned =
      if (cteDef.forceSkipInline && conf.getConf(SQLConf.REPLACE_CTE_REF_WITH_CTE_REUSE)) {
        cteDef.forcePartitioning match {
          case Some(h: HashPartitioning) =>
            // A pinned partitioning materializes the CTE as a RepartitionByExpression on the
            // pinned expressions and partition count, whether or not it is sealed. It takes
            // precedence over `canSkipExtraRepartition`: the pin states the exact partitioning we
            // must produce.
            Some(RepartitionByExpression(h.expressions, inlined, Some(h.numPartitions)))
          case Some(other) =>
            // forcePartitioning is an internal dev API; only HashPartitioning is supported
            // today (see CTERelationDef.forcePartitioning).
            throw new UnsupportedOperationException(
              s"CTERelationDef.forcePartitioning supports only HashPartitioning, but got " +
                s"${other.getClass.getSimpleName}")
          case None => None
        }
      } else {
        None
      }
    // The root this definition carries the plan-reuse id on when it is sealed.
    def sealedRoot: RepartitionByExpression = pinned.getOrElse {
      if (ctx.isRetainedElsewhere(cteDef.id, inlined)) {
        // The body only selects another, unsealed definition that the plan also reads outside
        // this one, so `inlined` is that definition's lowered root, shared with its other copies.
        // Keep that root unchanged, so every copy of the other body still plans to one shuffle,
        // and seal a new repartition above it with the same partitioning. This costs one more
        // shuffle of this definition's output than stamping the root in place.
        val converted = sealableRoot(inlined)
        RepartitionByExpression(
          converted.partitionExpressions,
          inlined,
          converted.optNumPartitions,
          converted.optAdvisoryPartitionSize)
      } else {
        sealableRoot(inlined)
      }
    }
    // Only a forced definition whose body a batch query holds two or more copies of, or a
    // definition whose deterministic body equals such a sealed body, is sealed for reuse (see
    // `sealedDefinitions`). A single copy of any other body already runs it once.
    if (ctx.shouldSeal(cteDef.id)) {
      ctx.seal(cteDef.id, sealedRoot)
    } else {
      ctx.recordUnsealed(cteDef.id, sealedRoot)
      pinned.getOrElse(plainLowering(cteDef, inlined))
    }
  }

  private def replaceWithRepartition(
      plan: LogicalPlan,
      cteMap: mutable.HashMap[Long, LogicalPlan],
      ctx: LoweringContext): LogicalPlan = plan match {
    case WithCTE(child, cteDefs) =>
      cteDefs.foreach { cteDef =>
        val inlined = replaceWithRepartition(cteDef.child, cteMap, ctx)
        val lowered = lower(cteDef, inlined, ctx)
        ctx.recordLowering(cteDef.id, lowered)
        cteMap.put(cteDef.id, lowered)
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
