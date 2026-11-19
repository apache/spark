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

import org.apache.spark.sql.catalyst.expressions.{And, Expression, ExprId, Literal, NamedExpression, Predicate, PredicateHelper, SubqueryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, HintInfo, LogicalPlan, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{PER_PARTITION_RUNTIME_FILTER_SUBQUERY, TreePattern}
import org.apache.spark.sql.catalyst.trees.UnaryLike

/**
 * The placeholder of a per-partition runtime bloom filter of `key`, the join key of the application
 * side. [[InjectRuntimeFilter]] adds it as a conjunct of a `Filter` when the creation side is too
 * big for a bloom filter of all its keys. `plan` is that creation side, with its join key as its
 * only column. Predicate pushdown moves the placeholder along `key`, and `MergeSubplans` merges the
 * subqueries in `plan` as any others. [[CleanupPerPartitionRuntimeFilters]] then gives it way to
 * `true`.
 *
 * Like `DynamicPruningSubquery`, it cannot be evaluated.
 *
 * `sizing` sizes the bloom filters of its aggregate.
 */
case class PerPartitionRuntimeFilterSubquery(
    key: Expression,
    plan: LogicalPlan,
    sizing: PerPartitionSizing,
    exprId: ExprId = NamedExpression.newExprId)
  extends SubqueryExpression(plan, Seq(key), exprId, Nil, None)
  with Predicate
  with Unevaluable
  with UnaryLike[Expression] {

  override def child: Expression = key

  override def nullable: Boolean = false

  override def hint: Option[HintInfo] = None

  override def withNewHint(hint: Option[HintInfo]): PerPartitionRuntimeFilterSubquery = this

  override def withNewPlan(plan: LogicalPlan): PerPartitionRuntimeFilterSubquery =
    copy(plan = plan)

  override def withNewOuterAttrs(
      outerAttrs: Seq[Expression]): PerPartitionRuntimeFilterSubquery = {
    assert(outerAttrs.size == 1)
    copy(key = outerAttrs.head)
  }

  final override def nodePatternsInternal(): Seq[TreePattern] =
    Seq(PER_PARTITION_RUNTIME_FILTER_SUBQUERY)

  override def toString: String = s"per_partition_runtime_filter#${exprId.id} $conditionString"

  override lazy val canonicalized: PerPartitionRuntimeFilterSubquery =
    copy(key = key.canonicalized, plan = plan.canonicalized, exprId = ExprId(0))

  override protected def withNewChildInternal(
      newChild: Expression): PerPartitionRuntimeFilterSubquery = copy(key = newChild)
}

object PerPartitionRuntimeFilterSubquery extends PredicateHelper {

  /** `child` under the `conjuncts` that are not placeholders. */
  private[optimizer] def withoutPlaceholders(
      conjuncts: Seq[Expression],
      child: LogicalPlan): LogicalPlan =
    conjuncts.filterNot(_.isInstanceOf[PerPartitionRuntimeFilterSubquery]).reduceOption(And)
      .map(Filter(_, child)).getOrElse(child)

  /** A `Filter` with placeholders among its conjuncts, as its conjuncts, placeholders and child. */
  private[optimizer] object WithPlaceholders {
    def unapply(plan: LogicalPlan): Option[
        (Seq[Expression], Seq[PerPartitionRuntimeFilterSubquery], LogicalPlan)] = plan match {
      case Filter(condition, child)
          if condition.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY) =>
        val conjuncts = splitConjunctivePredicates(condition)
        val placeholders = conjuncts.collect { case p: PerPartitionRuntimeFilterSubquery => p }
        if (placeholders.isEmpty) None else Some((conjuncts, placeholders, child))
      case _ => None
    }
  }
}

/**
 * The sizing of a per-partition runtime bloom filter, for its whole creation side:
 *  - `numItems`, the expected number of items;
 *  - `numBits`, the number of bits, when the creation side has no row count. With one, each bloom
 *    filter gets the number of bits its expected items need;
 *  - `minPartitionKeys`, the number of partition keys it needs at least, so that an even share of
 *    it is within the size threshold of a bloom filter's input.
 */
case class PerPartitionSizing(numItems: Long, numBits: Option[Long], minPartitionKeys: Long)

/**
 * Gives each [[PerPartitionRuntimeFilterSubquery]] way to `true`, in subqueries too, after
 * predicate pushdown.
 *
 * The optimizer runs on each subquery on its own first, as a `Subquery`, and there this rule does
 * nothing. The run on the whole query handles the placeholders in its subqueries too, after
 * `MergeSubplans`.
 *
 * The rule cannot be excluded, since a placeholder cannot be evaluated.
 */
object CleanupPerPartitionRuntimeFilters extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case _: Subquery => plan
    case _ if !plan.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY) => plan
    case _ =>
      plan.transformDownWithSubqueriesAndPruning(
          _.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY)) {
        case node => node.transformExpressionsWithPruning(
            _.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY)) {
          case _: PerPartitionRuntimeFilterSubquery => Literal.TrueLiteral
        }
      }
  }
}
