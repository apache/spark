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

import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeSet, BloomFilterMightContain, Expression, ExprId, Literal, NamedExpression, Predicate, PredicateHelper, SubqueryExpression, TransformExpression, Unevaluable, XxHash64}
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.optimizer.InjectRuntimeFilter.perPartitionCreationTransform
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, HintInfo, LogicalPlan, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{PER_PARTITION_RUNTIME_FILTER_SUBQUERY, TreePattern}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.internal.SQLConf

/**
 * The placeholder of a per-partition runtime bloom filter of `key`, the join key of the application
 * side. [[InjectRuntimeFilter]] adds it as a conjunct of a `Filter` when the creation side is too
 * big for a bloom filter of all its keys. `plan` is that creation side, with its join key as its
 * only column. Predicate pushdown moves the placeholder along `key`, and `MergeSubplans` merges the
 * subqueries in `plan` as any others. [[CleanupPerPartitionRuntimeFilters]] then keeps it only
 * where it landed right above a scan that can apply it. The V2 scan strategy gives the scan the
 * bloom filters of an aggregate of `plan`, see `aggregateOf`.
 *
 * Like `DynamicPruningSubquery`, it cannot be evaluated. Where no scan takes it, it gives way to
 * `true`.
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

  /**
   * The columns of `scan` that the V2 scan strategy offers a per-partition bloom filter over. Those
   * are the columns the scan declared, when storage filters are on and every operation pushed into
   * the scan works on each row by itself. Unlike the strategy's other storage filters, a
   * per-partition bloom filter is offered also when the scan reads only the filter's columns, since
   * no filter above the scan drops the rows it rejects. It is not offered to the read of a
   * group-based DELETE. `ReplaceDataExec` counts the deleted rows from the rows that read returns.
   * So the count would leave out the rows the readers reject.
   */
  def storageFilterColumnsOf(scan: DataSourceV2ScanRelation): AttributeSet =
    if (SQLConf.get.v2StorageFilterPushdownEnabled && scan.onlyRowWisePushdowns &&
        !scan.isGroupBasedDeleteRead) {
      scan.storageFilterAttrs
    } else {
      AttributeSet.empty
    }

  // The partition transform of `placeholder`'s creation side, when `scan` can apply the
  // placeholder's bloom filters, see `aggregateOf`.
  private def creationTransformOf(
      placeholder: PerPartitionRuntimeFilterSubquery,
      scan: DataSourceV2ScanRelation): Option[TransformExpression] =
    perPartitionCreationTransform(placeholder.key, scan, placeholder.plan.output.head,
      placeholder.plan)

  /** Whether `scan` can apply the bloom filters of `placeholder`, see `aggregateOf`. */
  def appliesTo(placeholder: PerPartitionRuntimeFilterSubquery, scan: DataSourceV2ScanRelation)
      : Boolean = creationTransformOf(placeholder, scan).isDefined

  /**
   * The aggregate of `placeholder`'s creation side, when `scan` can apply the placeholder's bloom
   * filters. The placeholder's key is over the columns of the scan, as the V2 scan strategy sees
   * it. The scan can apply them when it takes a storage filter on the key, and the key still has
   * the creation side's partition transform (`InjectRuntimeFilter.perPartitionCreationTransform`).
   * The aggregate groups the creation side by its partition transform, with the partition key and
   * a bloom filter of its join key per group. The bloom filter is its last column.
   */
  def aggregateOf(
      placeholder: PerPartitionRuntimeFilterSubquery,
      scan: DataSourceV2ScanRelation): Option[LogicalPlan] = {
    val creationKey = placeholder.plan.output.head
    creationTransformOf(placeholder, scan).map { creation =>
      val partitionKey = Alias(creation, "partitionKey")()
      val bloomFilter = Alias(new BloomFilterAggregate(new XxHash64(Seq(creationKey)))
        .toAggregateExpression(), "bloomFilter")()
      PullOutGroupingExpressions(Aggregate(
        Seq(creation), Seq(partitionKey, bloomFilter), placeholder.plan))
    }
  }

  /** The check of each row's `key` against the bloom filters of `aggregate`, from `aggregateOf`. */
  def checkOf(key: Expression, aggregate: LogicalPlan): Expression =
    BloomFilterMightContain(aggregate.output.last, new XxHash64(Seq(key)))

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
 * Keeps a [[PerPartitionRuntimeFilterSubquery]] only where predicate pushdown left it in a `Filter`
 * right above a V2 scan, with only filters and projections in between, where the scan can apply
 * its bloom filters (see `PerPartitionRuntimeFilterSubquery.aggregateOf`). The V2 scan strategy
 * then finds it among the filters of the scan. A placeholder gives way to `true` when:
 *  - it has more than one copy, as when pushdown puts one into every branch of a union. The
 *    aggregate of each copy would read the creation side again;
 *  - it is no longer a conjunct of a `Filter`, as when `MergeSubplans` propagates its filter into a
 *    projection;
 *  - the scan below cannot take it, or there is no scan right below;
 *  - its key no longer has the creation side's partition transform there.
 *
 * The optimizer runs on each subquery on its own first, as a `Subquery`, and there this rule does
 * nothing. The run on the whole query handles the placeholders in its subqueries too, after
 * `MergeSubplans`.
 *
 * The rule cannot be excluded, since a placeholder cannot be evaluated.
 */
object CleanupPerPartitionRuntimeFilters extends Rule[LogicalPlan] {
  import PerPartitionRuntimeFilterSubquery.appliesTo

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case _: Subquery => plan
    case _ if !plan.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY) => plan
    case _ =>
      // The number of copies of each placeholder, in `plan` and its subqueries.
      val copies = plan.collectWithSubqueries { case node => node.expressions }.flatten
        .filter(_.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY))
        .flatMap(_.collect { case p: PerPartitionRuntimeFilterSubquery => p.exprId })
        .groupBy(identity).view.mapValues(_.size).toMap
      // The placeholders a scan takes. The V2 scan strategy finds each among the filters right
      // above its scan, with its key over the columns of the scan.
      val kept = plan.collectWithSubqueries {
        case filter: Filter
            if filter.condition.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY) =>
          filter match {
            case PhysicalOperation(_, filters, scan: DataSourceV2ScanRelation) =>
              filters.collect {
                case p: PerPartitionRuntimeFilterSubquery
                    if copies(p.exprId) == 1 && appliesTo(p, scan) => p.exprId
              }
            case _ => Nil
          }
      }.flatten.toSet
      plan.transformDownWithSubqueriesAndPruning(
          _.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY)) {
        case node => node.transformExpressionsWithPruning(
            _.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY)) {
          case p: PerPartitionRuntimeFilterSubquery if !kept.contains(p.exprId) =>
            Literal.TrueLiteral
        }
      }
  }

}
