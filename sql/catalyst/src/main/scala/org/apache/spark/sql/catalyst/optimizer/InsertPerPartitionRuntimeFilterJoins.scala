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

import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, BloomFilterMightContain, EqualTo, Expression, ExprId, GetStructField, IsNull, Literal, Or, PredicateHelper, ScalarSubquery, SubqueryExpression, TransformExpression, UnaryExpression, Unevaluable, XxHash64}
import org.apache.spark.sql.catalyst.optimizer.InjectRuntimeFilter.{globalBloomFilterOf, perPartitionRuntimeFilter}
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.catalyst.plans.logical.{Filter, HintInfo, Join, JoinHint, LogicalPlan, Project, SHUFFLE_HASH}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{PER_PARTITION_RUNTIME_FILTER_PLACEHOLDER, TreePattern}
import org.apache.spark.sql.types.{BooleanType, DataType}

/**
 * A per-partition runtime bloom filter that is not built yet. It checks `child`, the join key of
 * the application side. [[InjectRuntimeFilter]] adds it as a conjunct of a `Filter`. So predicate
 * pushdown moves it along the key, as it moves the bloom filter of the whole creation side. On the
 * way, pushdown rewrites the key through the aliases. [[InsertPerPartitionRuntimeFilterJoins]] then
 * replaces it with a semi join of `aggregate`, which holds the bloom filter `bloomFilter` of each
 * partition key `creationPartitionKey` of the creation side. `transform` is the partition transform
 * the semi join applies to the key.
 *
 * It cannot be evaluated. `aggregate` is not one of its children, so no rule changes the aggregate.
 */
case class PerPartitionRuntimeFilterPlaceholder(
    child: Expression,
    transform: TransformExpression,
    aggregate: LogicalPlan,
    creationPartitionKey: Attribute,
    bloomFilter: Attribute) extends UnaryExpression with Unevaluable {

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = false

  override def prettyName: String = "per_partition_runtime_filter"

  // Without the aggregate, which would print the creation side's plan in the condition.
  override def toString: String = s"$prettyName($child)"

  final override val nodePatterns: Seq[TreePattern] = Seq(PER_PARTITION_RUNTIME_FILTER_PLACEHOLDER)

  // Two placeholders are the same check when they check the same key against the same aggregate,
  // in place of the same bloom filter of the whole creation side, or of none. So `MergeSubplans`
  // can merge the copies of one subplan that have no such bloom filter. The two columns count by
  // their position in the aggregate's output, and `transform` without its children, which the
  // semi join replaces with the key.
  override lazy val canonicalized: Expression = {
    def column(a: Attribute): Attribute = AttributeReference("none", a.dataType, a.nullable,
      a.metadata)(ExprId(aggregate.output.indexWhere(_.exprId == a.exprId)))
    PerPartitionRuntimeFilterPlaceholder(child.canonicalized, transform.copy(children = Nil),
      aggregate.canonicalized, column(creationPartitionKey), column(bloomFilter))
  }

  override protected def withNewChildInternal(
      newChild: Expression): PerPartitionRuntimeFilterPlaceholder = copy(child = newChild)
}

object PerPartitionRuntimeFilterPlaceholder extends PredicateHelper {

  /** The placeholders among the conjuncts of the filters of `plan`, one per copy. */
  def placeholdersIn(plan: LogicalPlan): Seq[PerPartitionRuntimeFilterPlaceholder] =
    if (!plan.containsPattern(PER_PARTITION_RUNTIME_FILTER_PLACEHOLDER)) {
      Nil
    } else {
      plan.flatMap {
        case Filter(condition, _) => splitConjunctivePredicates(condition).collect {
          case p: PerPartitionRuntimeFilterPlaceholder => p
        }
        case _ => Nil
      }
    }

  /** The key that `plan` checks, when it is the join of a placeholder's semi join. */
  def semiJoinKey(plan: LogicalPlan): Option[Expression] = plan match {
    case Join(_, _, ExistenceJoin(_), Some(condition), _) =>
      perPartitionRuntimeFilter(condition).map(_._2)
    case _ => None
  }

  /**
   * `plan` with the placeholders taken out of its filters. For each placeholder, `replace` gets the
   * plan below it and the placeholder, and returns the plan that goes there.
   */
  def replaced(plan: LogicalPlan)(
      replace: (LogicalPlan, PerPartitionRuntimeFilterPlaceholder) => LogicalPlan): LogicalPlan =
    plan.transformUpWithPruning(_.containsPattern(PER_PARTITION_RUNTIME_FILTER_PLACEHOLDER)) {
      case Filter(condition, child)
          if condition.containsPattern(PER_PARTITION_RUNTIME_FILTER_PLACEHOLDER) =>
        val (placeholders, others) = splitConjunctivePredicates(condition).partitionMap {
          case p: PerPartitionRuntimeFilterPlaceholder => Left(p)
          case other => Right(other)
        }
        placeholders.foldLeft(others.reduceOption(And).map(Filter(_, child)).getOrElse(child))(
          replace)
    }
}

/**
 * Replaces each [[PerPartitionRuntimeFilterPlaceholder]] with its semi join, right where predicate
 * pushdown put it. So the operators the placeholder moved past get only the rows the semi
 * join keeps. The semi join checks each row on its key alone, so it is correct wherever the
 * placeholder is.
 *
 * Each semi join reads its creation side again, so a placeholder with more than one copy is
 * dropped, with its per-partition bloom filter. Pushdown puts a copy into every branch of a union.
 * A placeholder can replace the bloom filter of the whole creation side. It is also dropped when
 * the subquery of that bloom filter runs anyway, and so reads the creation side too. That subquery
 * runs anyway when:
 *  - another check reads it, or a subquery with the same plan;
 *  - `MergeSubplans` merged it with others.
 * A semi join that an earlier optimizer pass built, as for an IN subquery that became a join, goes
 * in those cases too.
 *
 * A placeholder that is no longer a conjunct of a `Filter` becomes `true`. That happens when
 * `MergeSubplans` propagates its filter into a projection. Without it, the query is as it was
 * before the placeholder.
 *
 * Nothing can evaluate a placeholder, so this rule cannot be excluded.
 */
object InsertPerPartitionRuntimeFilterJoins extends Rule[LogicalPlan] with PredicateHelper {
  import PerPartitionRuntimeFilterPlaceholder.{placeholdersIn, replaced}

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // A placeholder with more than one copy is dropped, so only the aggregates of the others can
    // check a global bloom filter.
    val singles = placeholdersIn(plan).groupBy(_.bloomFilter.exprId).values.collect {
      case Seq(p) => p
    }.toSeq
    lazy val checks = (plan +: singles.map(_.aggregate)).flatMap(globalChecks)
    def replacesGlobal(bloomFilter: Attribute): Boolean = {
      val global = globalBloomFilterOf(bloomFilter)
      global < 0 || (checks.find(_._1 == global) match {
        case Some((_, subquery, false)) => checks.count(_._2 == subquery) == 1
        case _ => false
      })
    }
    val kept = singles.filter(p => replacesGlobal(p.bloomFilter)).map(_.bloomFilter.exprId).toSet
    // The semi joins an earlier optimizer pass built are judged again, since this pass can check
    // their global bloom filters elsewhere too. The join of one that goes gives way to its
    // application side, and the filter on its flag goes too. A flag with no filter right above
    // the join is true for every row.
    object StaleJoin {
      def unapply(plan: LogicalPlan): Option[(LogicalPlan, Attribute)] = plan match {
        case Join(left, _, ExistenceJoin(exists), Some(condition), _)
            if perPartitionRuntimeFilter(condition).exists(f => !replacesGlobal(f._1)) =>
          Some((left, exists))
        case _ => None
      }
    }
    val withoutStale = if (!conf.runtimeBloomFilterPerPartitionEnabled) {
      plan
    } else {
      plan.transformDown {
        case Filter(condition, StaleJoin(left, exists)) =>
          splitConjunctivePredicates(condition).filterNot(_.references.contains(exists))
            .reduceOption(And).map(Filter(_, left)).getOrElse(left)
        case StaleJoin(left, exists) =>
          Project(left.output :+ Alias(Literal.TrueLiteral, exists.name)(exists.exprId), left)
      }
    }
    withoutLeftovers(replaced(withoutStale)((node, p) =>
      if (kept(p.bloomFilter.exprId)) semiJoin(node, p) else node))
  }

  // The checks of bloom filters of a whole creation side in `plan` and its subqueries. Each has the
  // exprId of its subquery, the subquery's canonical plan, and whether `MergeSubplans` merged it
  // with others. Subqueries with the same canonical plan run once.
  private def globalChecks(plan: LogicalPlan): Seq[(Long, LogicalPlan, Boolean)] =
    plan.collectWithSubqueries { case node => node.expressions }.flatten.flatMap(_.collect {
      case BloomFilterMightContain(subquery: ScalarSubquery, _) =>
        (subquery.exprId.id, subquery.plan.canonicalized, false)
      case BloomFilterMightContain(GetStructField(subquery: ScalarSubquery, _, _), _) =>
        (subquery.exprId.id, subquery.plan.canonicalized, true)
    })

  // `plan` with `true` for each placeholder left, in its subqueries too.
  private def withoutLeftovers(plan: LogicalPlan): LogicalPlan =
    plan.transformAllExpressionsWithPruning(
        _.containsPattern(PER_PARTITION_RUNTIME_FILTER_PLACEHOLDER)) {
      case subquery: SubqueryExpression => subquery.withNewPlan(withoutLeftovers(subquery.plan))
      case _: PerPartitionRuntimeFilterPlaceholder => Literal.TrueLiteral
    }

  /**
   * `plan` in a semi join with the aggregate of `placeholder`. The join checks each row's key, the
   * child of `placeholder`, against the bloom filter of the row's partition key, which the
   * placeholder's transform of the key gives. The transform may return null for a key that is not
   * null, and the join above can match such a row. So the semi join is an existence join, with a
   * filter that keeps the rows that pass the check or whose partition key is null.
   */
  private def semiJoin(
      plan: LogicalPlan,
      placeholder: PerPartitionRuntimeFilterPlaceholder): LogicalPlan = {
    val key = placeholder.child
    val partitionKey = Alias(placeholder.transform.copy(children = Seq(key)), "partitionKey")()
    val condition = And(
      EqualTo(partitionKey.toAttribute, placeholder.creationPartitionKey),
      BloomFilterMightContain(placeholder.bloomFilter, new XxHash64(Seq(key))))
    val exists = AttributeReference("exists", BooleanType, nullable = false)()
    val join = Join(
      Project(plan.output :+ partitionKey, plan),
      placeholder.aggregate,
      ExistenceJoin(exists),
      Some(condition),
      JoinHint(None, Some(HintInfo(strategy = Some(SHUFFLE_HASH)))))
    Project(plan.output, Filter(Or(exists, IsNull(partitionKey.toAttribute)), join))
  }
}
