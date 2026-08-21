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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, CollationKey, Expression, ExprId, KnownNotNull, NamedExpression, PythonAggregate, PythonUDAF}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, First}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils
import org.apache.spark.sql.internal.SQLConf

/**
 * This rule rewrites [[Aggregate]] operators whose grouping keys contain non-binary (collated)
 * strings so that they can be executed with hash-based aggregation instead of falling back to the
 * much more expensive sort-based aggregation.
 *
 * Hash-based aggregation ([[org.apache.spark.sql.execution.aggregate.HashAggregateExec]] and
 * [[org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec]]) keys its in-memory map on
 * the binary representation of the grouping keys, which is only correct when every grouping key is
 * binary-stable (see [[UnsafeRowUtils.isBinaryStable]]). Non-binary collations such as UTF8_LCASE
 * are not binary-stable (e.g. 'a' and 'A' compare as equal but have different bytes), so the
 * planner disables hash aggregation and uses
 * [[org.apache.spark.sql.execution.aggregate.SortAggregateExec]], which sorts the whole input.
 *
 * This rule mirrors [[RewriteCollationJoin]] (which does the equivalent for hash joins): it
 * injects [[CollationKey]] into the grouping keys so that grouping is performed on the
 * collation-normalized bytes, which are binary-stable. Since the original grouping value is no
 * longer a grouping key, any reference to it in the aggregate output is preserved by wrapping it
 * in a [[First]] aggregate, which returns an arbitrary representative of the (collation-equal)
 * group. All representatives of a group are collation-equal, so the choice is semantically
 * irrelevant, just like the row a sort-based aggregate would surface for the group.
 *
 * With binary-stable grouping keys the planner can use regular hash aggregation
 * ([[org.apache.spark.sql.execution.aggregate.HashAggregateExec]]) when the aggregation buffer is
 * mutable; when a projected key is carried via [[First]] the buffer is not mutable, so the planner
 * uses object-hash aggregation as the sort-avoiding fallback (see `AggUtils`).
 *
 * The rule runs after [[org.apache.spark.sql.catalyst.optimizer.PullOutGroupingExpressions]] (so
 * the injected [[CollationKey]] is not pulled out and survives as a grouping expression), but
 * before the operators that lower DISTINCT / dropDuplicates / INTERSECT ALL / EXCEPT ALL and
 * multiple distinct aggregates into new [[Aggregate]]s. Those lowerings build aggregates with
 * surrounding structure (e.g. a `Generate` over a grouping [[Aggregate]] for set operations) whose
 * attribute references this rewrite cannot safely remap, so they are intentionally left alone and
 * keep using sort-based aggregation.
 *
 * When [[org.apache.spark.sql.catalyst.optimizer.RewriteDistinctAggregates]] later replaces an
 * already-normalized grouping key with a fresh attribute (for multiple distinct aggregates), it
 * carries the collation-key provenance forward as attribute metadata (see
 * [[CollationKey.withCollationKeyMetadata]]) so the physical planner can still recognize the
 * normalized key. That provenance is only a performance hint for choosing object-hash over sort;
 * correctness never depends on it (see `AggUtils`).
 */
object RewriteCollationAggregate extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.COLLATION_HASH_AGGREGATION_ENABLED) ||
        !plan.containsPattern(AGGREGATE)) {
      return plan
    }
    // A bare grouping key projected in the output is replaced with a `First` aggregate carrying a
    // fresh expression id (its old id is still referenced by the injected `First`), so use
    // `transformUpWithNewOutput` to propagate the new output attributes to parent operators.
    plan.transformUpWithNewOutput {
      // Skip streaming aggregates: normalizing the grouping keys would change the state store key
      // schema and break recovery from existing checkpoints. Skip aggregates that contain Python
      // aggregate functions: injecting a JVM `First` to carry a projected key would produce a
      // mix of Python and JVM aggregate functions, which the physical planner cannot place.
      case agg: Aggregate
          if !agg.isStreaming && !hasPythonAggregate(agg) &&
            agg.groupingExpressions.exists(canNormalize) =>
        val newAgg = rewrite(agg)
        newAgg -> agg.output.zip(newAgg.output)
    }
  }

  private def hasPythonAggregate(agg: Aggregate): Boolean =
    agg.aggregateExpressions.exists(_.exists {
      case _: PythonUDAF | _: PythonAggregate => true
      case _ => false
    })

  /**
   * A grouping expression is rewritable only if it is not already binary-stable and injecting
   * `CollationKey` makes it binary-stable. Types that `CollationKey` cannot normalize (e.g. maps
   * containing collated strings, which it leaves unchanged) are left alone so that the aggregation
   * keeps using sort-based aggregation rather than producing an invalid hash-aggregatable plan.
   */
  private def canNormalize(e: Expression): Boolean =
    !UnsafeRowUtils.isBinaryStable(e.dataType) &&
      UnsafeRowUtils.isBinaryStable(CollationKey.injectCollationKey(e).dataType)

  private def rewrite(agg: Aggregate): Aggregate = {
    // Compute the collation-key-injected form of each grouping expression once (injecting can
    // recurse through structs/arrays, so avoid doing it repeatedly). A grouping expression is
    // normalized only when injecting `CollationKey` actually makes it binary-stable.
    val injected = agg.groupingExpressions.map { e =>
      val inj = CollationKey.injectCollationKey(e)
      val normalize = !UnsafeRowUtils.isBinaryStable(e.dataType) &&
        UnsafeRowUtils.isBinaryStable(inj.dataType)
      (e, inj, normalize)
    }

    // The grouping expressions that are normalized, and therefore no longer directly available as
    // grouping keys in the output.
    val rewrittenGrouping = injected.collect { case (e, _, true) => e }

    // Group on the collation key of normalizable grouping expressions; leave the rest as is.
    val newGroupingExprs = injected.map { case (e, inj, normalize) => if (normalize) inj else e }

    // Wrap a rewritten grouping value in First while preserving its nullability, so the output
    // schema (including the nullability of expressions derived from the grouping key) is unchanged.
    // First is always nullable, but for a non-nullable grouping key every row in a group is
    // non-null, so the representative is non-null too.
    def firstOf(e: Expression): Expression = {
      val first = First(e, ignoreNulls = false).toAggregateExpression()
      if (e.nullable) first else KnownNotNull(first)
    }

    // Replace references to the rewritten grouping keys in the aggregate (output) expressions with
    // First(originalKey). We must not descend into existing aggregate functions: their arguments
    // are evaluated per input row and the original (collated) columns are still available from the
    // child, so those references must be left untouched. Aliases are preserved (only their child is
    // rewritten) so output names and expression ids stay stable.
    def wrapGroupingRefs(e: Expression): Expression = e match {
      case _: AggregateExpression => e
      case a: Alias => a.withNewChildren(Seq(wrapGroupingRefs(a.child)))
      case _ if rewrittenGrouping.exists(_.semanticEquals(e)) => firstOf(e)
      case _ => e.mapChildren(wrapGroupingRefs)
    }

    // A bare grouping key projected in the output becomes a First aggregate with a fresh expression
    // id. Repeated projections of the same key must share one replacement, otherwise a single old
    // output id would map to multiple new ids and fail `transformUpWithNewOutput`. Name, qualifier
    // and metadata are preserved so the output column is unchanged apart from its expression id.
    val replaced = mutable.HashMap.empty[ExprId, NamedExpression]
    val newAggregateExprs = agg.aggregateExpressions.map {
      case attr: Attribute if rewrittenGrouping.exists(_.semanticEquals(attr)) =>
        replaced.getOrElseUpdate(attr.exprId,
          Alias(firstOf(attr), attr.name)(
            qualifier = attr.qualifier, explicitMetadata = Some(attr.metadata)))
      case other =>
        wrapGroupingRefs(other).asInstanceOf[NamedExpression]
    }

    agg.copy(groupingExpressions = newGroupingExprs, aggregateExpressions = newAggregateExprs)
  }
}
