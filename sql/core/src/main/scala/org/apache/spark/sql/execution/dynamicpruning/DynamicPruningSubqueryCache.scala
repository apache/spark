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

package org.apache.spark.sql.execution.dynamicpruning

import java.util.IdentityHashMap

import scala.collection.mutable
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration.Duration

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, BroadcastValueProjection, DynamicPruningSubquery, Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{HintInfo, Join, JoinHint, LogicalPlan, UnaryNode}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.{BaseSubqueryExec, ProjectedBroadcastValueSubqueryExec, SparkPlan, SubqueryBroadcastExec, SubqueryExec}

/**
 * Query-local memoization of DPP construction, with separate caches for preparation stages.
 * Lookups run before the planning or preparation they can reuse, including nested DPP.
 * Cached subquery producers remain traversable by later reuse rules; broadcast eligibility
 * stays with the calling rules.
 * Plan rendering and SQL event serialization can still expand shared plans along each reference
 * path, even after construction and final reuse finish.
 */
private[execution] class DynamicPruningSubqueryCache(session: SparkSession) {
  import DynamicPruningSubqueryCache._

  private val hintSignatures = new HintSignatureCache
  private val plannedQueries = new ConstructionCache[LogicalPlan, PlannedQuery]
  private val adaptiveQueries = new ConstructionCache[BuildQueryKey, CompiledQuery]
  private val adaptiveProjectionQueries = new ConstructionCache[BuildQueryKey, CompiledQuery]
  private val broadcasts = new ConstructionCache[BuildQueryKey, BaseSubqueryExec]
  private val adaptiveBroadcasts = new ConstructionCache[BroadcastKey, BaseSubqueryExec]
  private val independentQueries =
    new ConstructionCache[(LogicalPlan, Boolean), BaseSubqueryExec]

  def planQuery(query: LogicalPlan)(build: => SparkPlan): PlannedQuery = {
    if (!canReuse(query)) {
      new PlannedQuery(query, build)
    } else {
      plannedQueries.getOrBuild(hintSignatures.signature(query)) { new PlannedQuery(query, build) }
    }
  }

  def compileAdaptiveQuery(
      query: LogicalPlan,
      keys: Seq[Expression],
      indices: Seq[Int],
      onlyInBroadcast: Boolean)(build: => SparkPlan): CompiledQuery = {
    if (!canReuse(query) || !canReuseExpressions(keys)) {
      CompiledQuery(query, build)
    } else {
      val key = buildQueryKey(query, keys, indices, onlyInBroadcast, None)
      adaptiveQueries.getOrBuild(key) { CompiledQuery(query, build) }
    }
  }

  def compileAdaptiveProjection(
      query: LogicalPlan,
      keys: Seq[Expression],
      value: Expression)(build: => SparkPlan): CompiledQuery = {
    if (!canReuse(query) || !canReuseExpressions(keys :+ value)) {
      CompiledQuery(query, build)
    } else {
      val key = buildQueryKey(query, keys, Nil, onlyInBroadcast = true, value = Some(value))
      adaptiveProjectionQueries.getOrBuild(key) { CompiledQuery(query, build) }
    }
  }

  def prepareBroadcast(
      query: LogicalPlan,
      keys: Seq[Expression],
      indices: Seq[Int],
      value: Option[Expression])(build: => BaseSubqueryExec): BaseSubqueryExec = {
    if (!canReuse(query) || !canReuseExpressions(keys ++ value)) {
      build
    } else {
      val key = buildQueryKey(query, keys, indices, onlyInBroadcast = false, value = value)
      reuseSubquery(broadcasts, key)(build)
    }
  }

  def prepareAdaptiveBroadcast(
      query: LogicalPlan,
      exchange: SparkPlan,
      keys: Seq[Expression],
      indices: Seq[Int],
      value: Option[Expression])(build: => BaseSubqueryExec): BaseSubqueryExec = {
    if (!canReuse(query) || !exchange.deterministic ||
        !canReuseExpressions(keys ++ value)) {
      build
    } else {
      val key = BroadcastKey(hintSignatures.signature(query), exchange.canonicalized,
        keys.map(QueryPlan.normalizeExpressions(_, exchange.output)), indices,
        value.map(QueryPlan.normalizeExpressions(_, exchange.output)))
      reuseSubquery(adaptiveBroadcasts, key)(build)
    }
  }

  def prepareIndependentQuery(
      query: LogicalPlan,
      adaptive: Boolean)(build: => BaseSubqueryExec): BaseSubqueryExec = {
    if (!canReuse(query)) {
      build
    } else {
      reuseSubquery(independentQueries, (hintSignatures.signature(query), adaptive))(build)
    }
  }

  private def canReuse(query: LogicalPlan): Boolean = {
    session.sessionState.conf.subqueryReuseEnabled && query.deterministic
  }

  private def canReuseExpressions(expressions: Seq[Expression]): Boolean = {
    expressions.forall(e => e.deterministic && !e.exists(_.isInstanceOf[SubqueryExpression]))
  }

  private def reuseSubquery[K](
      cache: ConstructionCache[K, BaseSubqueryExec],
      key: K)(build: => BaseSubqueryExec): BaseSubqueryExec = {
    var constructed = false
    val producer = cache.getOrBuild(key) {
      constructed = true
      build
    }
    if (constructed) {
      producer
    } else {
      // Keep each reference traversable and distinct for the later reuse rules. Only copy the
      // subquery root: its prepared child is shared, and the construction callback is not rerun.
      val reference = producer match {
        case s: SubqueryBroadcastExec => s.copy()
        case s: ProjectedBroadcastValueSubqueryExec => s.copy()
        case s: SubqueryExec => s.copy()
        case other => other
      }
      reference.copyTagsFrom(producer)
      reference
    }
  }

  private def buildQueryKey(
      query: LogicalPlan,
      keys: Seq[Expression],
      indices: Seq[Int],
      onlyInBroadcast: Boolean,
      value: Option[Expression]): BuildQueryKey = {
    BuildQueryKey(hintSignatures.signature(query),
      keys.map(QueryPlan.normalizeExpressions(_, query.output)), indices, onlyInBroadcast,
      value.map(QueryPlan.normalizeExpressions(_, query.output)))
  }
}

private[execution] object DynamicPruningSubqueryCache {
  private case class BuildQueryKey(
      query: LogicalPlan,
      keys: Seq[Expression],
      indices: Seq[Int],
      onlyInBroadcast: Boolean,
      value: Option[Expression])

  private case class BroadcastKey(
      query: LogicalPlan,
      exchange: SparkPlan,
      keys: Seq[Expression],
      indices: Seq[Int],
      value: Option[Expression])

  /**
   * Logical canonicalization drops join hints and DPP broadcast projection metadata, both of
   * which affect physical planning. Retain them on their owning plans before canonicalization,
   * including plans inside subquery expressions.
   * A separate list of positions would lose that association when canonicalization reorders
   * commutative predicates.
   *
   * Memoize the key-only plan and expression copies by identity. Shared nested DPP plans remain
   * shared, and later lookups reuse the copies and their lazy canonicalized forms and hash codes.
   * PlanningHint nodes belong only to cache keys; the planner always receives the original query.
   * Update the key if canonicalization drops other metadata that affects planning.
   */
  private[dynamicpruning] class HintSignatureCache {
    private val plans = new IdentityHashMap[LogicalPlan, LogicalPlan]
    private val expressions = new IdentityHashMap[Expression, Expression]

    def signature(plan: LogicalPlan): LogicalPlan = synchronized {
      keyPlan(plan).canonicalized
    }

    private def keyPlan(plan: LogicalPlan): LogicalPlan = {
      val cached = plans.get(plan)
      if (cached != null) return cached
      val rewritten = plan.withNewChildren(plan.children.map(keyPlan)).mapExpressions(keyExpression)
      val result = rewritten match {
        case join: Join if !join.hint.isEmpty => PlanningHint(join, Some(join.hint), None, None)
        case other => other
      }
      plans.put(plan, result)
      result
    }

    private def keyExpression(expression: Expression): Expression = {
      val cached = expressions.get(expression)
      if (cached != null) return cached
      val rewritten = expression.withNewChildren(expression.children.map(keyExpression))
      val result = rewritten match {
        case s: SubqueryExpression =>
          val plan = keyPlan(s.plan)
          val projection = s match {
            case dpp: DynamicPruningSubquery =>
              dpp.usableBroadcastValueProjection.map(projectionSignature)
            case _ => None
          }
          val hinted = if (s.hint.nonEmpty || projection.nonEmpty) {
            PlanningHint(plan, None, s.hint, projection)
          } else {
            plan
          }
          if (hinted eq s.plan) s else s.withNewPlan(hinted)
        case other => other
      }
      expressions.put(expression, result)
      result
    }

    private def projectionSignature(projection: BroadcastValueProjection): ProjectionSignature = {
      val output = projection.sourcePlan.output
      ProjectionSignature(
        keyPlan(projection.sourcePlan).canonicalized,
        projection.sourceHashKeys.map { key =>
          QueryPlan.normalizeExpressions(keyExpression(key), output)
        },
        QueryPlan.normalizeExpressions(keyExpression(projection.valueExpression), output))
    }
  }

  private case class ProjectionSignature(
      source: LogicalPlan,
      keys: Seq[Expression],
      value: Expression)

  private case class PlanningHint(
      child: LogicalPlan,
      join: Option[JoinHint],
      subquery: Option[HintInfo],
      projection: Option[ProjectionSignature]) extends UnaryNode {
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
      copy(child = newChild)
  }

  // An adaptive plan has completed initial preparation, but its stage optimizer may not have
  // run yet. Sharing this entry leaves nested DPP to be resolved during stage optimization.
  case class CompiledQuery(query: LogicalPlan, plan: SparkPlan) {
    def bindKeys(request: LogicalPlan, keys: Seq[Expression]): Seq[Expression] = {
      val mapping = AttributeMap(request.output.zip(query.output))
      keys.map(_.transform { case a: Attribute => mapping.getOrElse(a, a) })
    }
  }

  /**
   * Retain a canonical comparison snapshot, but give the executable input to at most one builder.
   * A different broadcast request uses a separate input rather than cloning an already prepared
   * scan, RDD, or subquery expression. Repeated identical requests reuse the complete subquery.
   */
  class PlannedQuery(val query: LogicalPlan, plan: SparkPlan) {
    val comparisonPlan: SparkPlan = plan.canonicalized
    val output: Seq[Attribute] = plan.output
    private var input: Option[SparkPlan] = Some(plan)

    def takeInput(): Option[SparkPlan] = synchronized {
      val result = input
      input = None
      result
    }

    def bindKeys(request: LogicalPlan, keys: Seq[Expression]): Seq[Expression] = {
      CompiledQuery(query, comparisonPlan).bindKeys(request, keys)
    }
  }

  /**
   * Build outside the cache monitor. Callers must avoid cross-thread construction cycles,
   * which are not detected by the same-thread recursion check.
   */
  private[dynamicpruning] class ConstructionCache[K, V] {
    private case class Entry(owner: Thread, result: Promise[V])
    private val entries = mutable.HashMap.empty[K, Entry]

    def getOrBuild(key: K)(build: => V): V = {
      val (entry, shouldBuild) = synchronized {
        entries.get(key) match {
          case Some(existing) => (existing, false)
          case None =>
            val created = Entry(Thread.currentThread(), Promise[V]())
            entries.put(key, created)
            (created, true)
        }
      }
      if (shouldBuild) {
        try {
          val value = build
          entry.result.success(value)
          value
        } catch {
          case error: Throwable =>
            synchronized { entries.remove(key) }
            entry.result.failure(error)
            throw error
        }
      } else {
        require(entry.owner != Thread.currentThread() || entry.result.isCompleted,
          "Cyclic dynamic partition pruning subquery construction")
        // Preserve construction exceptions, including AQE's unsupported-subquery fallback signal.
        // scalastyle:off awaitresult
        Await.result(entry.result.future, Duration.Inf)
        // scalastyle:on awaitresult
      }
    }
  }
}
