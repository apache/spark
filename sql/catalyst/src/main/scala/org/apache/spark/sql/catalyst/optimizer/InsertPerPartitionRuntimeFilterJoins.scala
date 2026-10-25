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

import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, BloomFilterMightContain, EqualTo, Expression, ExprId, IsNull, Literal, Or, PredicateHelper, SubqueryExpression, TransformExpression, UnaryExpression, Unevaluable, XxHash64}
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

  /** The key that `plan` checks, when it has the shape of the join of a placeholder's semi join. */
  def semiJoinKey(plan: LogicalPlan): Option[Expression] = plan match {
    case Join(_, _, ExistenceJoin(_), Some(condition), _) =>
      splitConjunctivePredicates(condition).collectFirst {
        case BloomFilterMightContain(_: Attribute, XxHash64(Seq(key), _)) => key
      }
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
 *
 * A placeholder that is no longer a conjunct of a `Filter` becomes `true`. That happens when
 * `MergeSubplans` propagates its filter into a projection. Without it, the query is as it was
 * before the placeholder.
 *
 * Nothing can evaluate a placeholder, so this rule cannot be excluded.
 */
object InsertPerPartitionRuntimeFilterJoins extends Rule[LogicalPlan] {
  import PerPartitionRuntimeFilterPlaceholder.{placeholdersIn, replaced}

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val kept = placeholdersIn(plan).groupBy(_.bloomFilter.exprId).collect {
      case (id, Seq(_)) => id
    }.toSet
    withoutLeftovers(
      replaced(plan)((node, p) => if (kept(p.bloomFilter.exprId)) semiJoin(node, p) else node))
  }

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
