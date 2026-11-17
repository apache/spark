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

import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, BloomFilterMightContain, EqualTo, Expression, ExprId, IsNull, Literal, NamedExpression, Or, Predicate, PredicateHelper, SubqueryExpression, Unevaluable, XxHash64}
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.optimizer.InjectRuntimeFilter.pairedPartitionTransforms
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, HintInfo, Join, JoinHint, LogicalPlan, Project, SHUFFLE_HASH, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{PER_PARTITION_RUNTIME_FILTER_SUBQUERY, TreePattern}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{BooleanType, Metadata, MetadataBuilder}

/**
 * The placeholder of a per-partition runtime bloom filter of `key`, the join key of the application
 * side. [[InjectRuntimeFilter]] adds it as a conjunct of a `Filter` when the creation side is too
 * big for a bloom filter of all its keys. `plan` is that creation side, with its join key as its
 * only column. Predicate pushdown moves the placeholder along `key`, and `MergeSubplans` merges the
 * subqueries in `plan` as any others. [[InsertPerPartitionRuntimeFilterJoins]] then builds a semi
 * join from it, with an aggregate of `plan`.
 *
 * Like `DynamicPruningSubquery`, it cannot be evaluated. Where it gets no semi join, it gives way
 * to `true`.
 *
 * `sizing` sizes the bloom filters of its semi join.
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
   * The metadata key of the bloom filter column of a per-partition bloom filter's aggregate.
   * [[InsertPerPartitionRuntimeFilterJoins]] sets it. That is how [[InjectRuntimeFilter]] and
   * `ChoosePerPartitionRuntimeFilters` tell the semi join from one a query wrote. Its value holds
   * the [[PerPartitionSizing]] of the placeholder.
   */
  private val PER_PARTITION_RUNTIME_FILTER = "__per_partition_runtime_filter"

  /**
   * The metadata of the bloom filter column of the aggregate of `placeholder`'s per-partition bloom
   * filter, with its sizing.
   */
  private[optimizer] def bloomFilterMetadata(
      placeholder: PerPartitionRuntimeFilterSubquery): Metadata = {
    val sizing = new MetadataBuilder()
      .putLong("numItems", placeholder.sizing.numItems)
      .putLong("minPartitionKeys", placeholder.sizing.minPartitionKeys)
    placeholder.sizing.numBits.foreach(sizing.putLong("numBits", _))
    new MetadataBuilder().putMetadata(PER_PARTITION_RUNTIME_FILTER, sizing.build()).build()
  }

  /**
   * For the condition of a semi join of a per-partition bloom filter, the bloom filter column and
   * the key it checks. The column carries [[PER_PARTITION_RUNTIME_FILTER]].
   */
  def perPartitionRuntimeFilter(condition: Expression): Option[(Attribute, Expression)] =
    splitConjunctivePredicates(condition).collectFirst {
      case BloomFilterMightContain(bloomFilter: Attribute, XxHash64(Seq(key), _))
          if bloomFilter.metadata.contains(PER_PARTITION_RUNTIME_FILTER) => (bloomFilter, key)
    }

  /** The sizing of the per-partition bloom filter with the column `bloomFilter`. */
  def sizingOf(bloomFilter: Attribute): PerPartitionSizing = {
    val sizing = bloomFilter.metadata.getMetadata(PER_PARTITION_RUNTIME_FILTER)
    PerPartitionSizing(
      sizing.getLong("numItems"),
      Option.when(sizing.contains("numBits"))(sizing.getLong("numBits")),
      sizing.getLong("minPartitionKeys"))
  }

  /** The key that `plan` checks, when it is the join of a placeholder's semi join. */
  def semiJoinKey(plan: LogicalPlan): Option[Expression] = plan match {
    case Join(_, _, ExistenceJoin(_), Some(condition), _) =>
      perPartitionRuntimeFilter(condition).map(_._2)
    case _ => None
  }

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
 * Replaces each [[PerPartitionRuntimeFilterSubquery]] with its semi join, right where predicate
 * pushdown put it. So the operators the placeholder moved past get only the rows the semi join
 * keeps. The semi join checks each row on its key alone, so it is correct wherever the placeholder
 * is.
 *
 * The optimizer runs on each subquery on its own first, as a `Subquery`, and there this rule does
 * nothing. The run on the whole query builds the semi joins in its subqueries too, after
 * `MergeSubplans`.
 *
 * A placeholder gives way to `true` when:
 *  - it has more than one copy, as when pushdown puts one into every branch of a union. The semi
 *    join of each copy would read the creation side again;
 *  - it is no longer a conjunct of a `Filter`, as when `MergeSubplans` propagates its filter into a
 *    projection;
 *  - its key no longer has the creation side's partition transform there.
 *
 * The rule cannot be excluded, since a placeholder cannot be evaluated.
 */
object InsertPerPartitionRuntimeFilterJoins extends Rule[LogicalPlan] {
  import PerPartitionRuntimeFilterSubquery.{bloomFilterMetadata, withoutPlaceholders,
    WithPlaceholders}

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case _: Subquery => plan
    case _ if !plan.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY) => plan
    case _ =>
      // The number of copies of each placeholder, in `plan` and its subqueries.
      val copies = plan.collectWithSubqueries { case node => node.expressions }.flatten
        .flatMap(_.collect { case p: PerPartitionRuntimeFilterSubquery => p.exprId })
        .groupBy(identity).view.mapValues(_.size).toMap
      val withSemiJoins = plan.transformUpWithSubqueriesAndPruning(
          _.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY)) {
        case WithPlaceholders(conjuncts, placeholders, child) =>
          placeholders.filter(p => copies(p.exprId) == 1)
            .foldLeft(withoutPlaceholders(conjuncts, child)) { (node, p) =>
              semiJoin(node, p).getOrElse(node)
            }
      }
      withoutLeftovers(withSemiJoins)
  }

  // `plan` with each placeholder left giving way to `true`, in its subqueries too.
  private def withoutLeftovers(plan: LogicalPlan): LogicalPlan =
    plan.transformDownWithSubqueriesAndPruning(
        _.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY)) {
      case node => node.transformExpressionsWithPruning(
          _.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY)) {
        case _: PerPartitionRuntimeFilterSubquery => Literal.TrueLiteral
      }
    }

  /**
   * `plan` in a semi join with an aggregate of the creation side of `placeholder`. The aggregate
   * groups that side by its partition transform, with a bloom filter of its join key per group. The
   * join checks each row's key against the bloom filter of the row's partition key, which the
   * application side's transform of the key gives. The transform may return null for a key that is
   * not null, and the join above can match such a row. So the semi join is an existence join, with
   * a filter that keeps the rows that pass the check or whose partition key is null.
   *
   * None when the two keys no longer have the same partition transform there.
   */
  private def semiJoin(
      plan: LogicalPlan,
      placeholder: PerPartitionRuntimeFilterSubquery): Option[LogicalPlan] = {
    val creationKey = placeholder.plan.output.head
    val key = placeholder.key
    pairedPartitionTransforms(key, plan, creationKey, placeholder.plan).map {
      case (application, creation) =>
        val creationPartitionKey = Alias(creation, "partitionKey")()
        val bloomFilter = Alias(new BloomFilterAggregate(new XxHash64(Seq(creationKey)))
          .toAggregateExpression(), "bloomFilter")(
          explicitMetadata = Some(bloomFilterMetadata(placeholder)))
        val aggregate = PullOutGroupingExpressions(Aggregate(
          Seq(creation), Seq(creationPartitionKey, bloomFilter), placeholder.plan))
        val partitionKey = Alias(application, "partitionKey")()
        val condition = And(
          EqualTo(partitionKey.toAttribute, creationPartitionKey.toAttribute),
          BloomFilterMightContain(bloomFilter.toAttribute, new XxHash64(Seq(key))))
        val exists = AttributeReference("exists", BooleanType, nullable = false)()
        val join = Join(
          Project(plan.output :+ partitionKey, plan),
          aggregate,
          ExistenceJoin(exists),
          Some(condition),
          JoinHint(None, Some(HintInfo(strategy = Some(SHUFFLE_HASH)))))
        Project(plan.output, Filter(Or(exists, IsNull(partitionKey.toAttribute)), join))
    }
  }
}
