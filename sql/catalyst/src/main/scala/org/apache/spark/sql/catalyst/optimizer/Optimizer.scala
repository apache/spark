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

import scala.annotation.tailrec
import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.api.java.function.FilterFunction
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{CatalystConf, SimpleCatalystConf}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.planning.{ExtractFiltersAndInnerJoins, Unions}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types._

/**
 * Abstract class all optimizers should inherit of, contains the standard batches (extending
 * Optimizers can override this.
 */
abstract class Optimizer(sessionCatalog: SessionCatalog, conf: CatalystConf)
  extends RuleExecutor[LogicalPlan] {

  protected val fixedPoint = FixedPoint(conf.optimizerMaxIterations)

  def batches: Seq[Batch] = {
    // Technically some of the rules in Finish Analysis are not optimizer rules and belong more
    // in the analyzer, because they are needed for correctness (e.g. ComputeCurrentTime).
    // However, because we also use the analyzer to canonicalized queries (for view definition),
    // we do not eliminate subqueries or compute current time in the analyzer.
    Batch("Finish Analysis", Once,
      EliminateSubqueryAliases,
      ReplaceExpressions,
      ComputeCurrentTime,
      GetCurrentDatabase(sessionCatalog),
      RewriteDistinctAggregates) ::
    //////////////////////////////////////////////////////////////////////////////////////////
    // Optimizer rules start here
    //////////////////////////////////////////////////////////////////////////////////////////
    // - Do the first call of CombineUnions before starting the major Optimizer rules,
    //   since it can reduce the number of iteration and the other rules could add/move
    //   extra operators between two adjacent Union operators.
    // - Call CombineUnions again in Batch("Operator Optimizations"),
    //   since the other rules might make two separate Unions operators adjacent.
    Batch("Union", Once,
      CombineUnions) ::
    Batch("Subquery", Once,
      OptimizeSubqueries) ::
    Batch("Replace Operators", fixedPoint,
      ReplaceIntersectWithSemiJoin,
      ReplaceExceptWithAntiJoin,
      ReplaceDistinctWithAggregate) ::
    Batch("Aggregate", fixedPoint,
      RemoveLiteralFromGroupExpressions,
      RemoveRepetitionFromGroupExpressions) ::
    Batch("Operator Optimizations", fixedPoint,
      // Operator push down
      PushProjectionThroughUnion,
      ReorderJoin,
      EliminateOuterJoin,
      PushPredicateThroughJoin,
      PushDownPredicate,
      LimitPushDown,
      ColumnPruning,
      InferFiltersFromConstraints,
      // Operator combine
      CollapseRepartition,
      CollapseProject,
      CombineFilters,
      CombineLimits,
      CombineUnions,
      // Constant folding and strength reduction
      NullPropagation,
      FoldablePropagation,
      OptimizeIn(conf),
      ConstantFolding,
      ReorderAssociativeOperator,
      LikeSimplification,
      BooleanSimplification,
      SimplifyConditionals,
      RemoveDispensableExpressions,
      SimplifyBinaryComparison,
      PruneFilters,
      EliminateSorts,
      SimplifyCasts,
      SimplifyCaseConversionExpressions,
      RewriteCorrelatedScalarSubquery,
      EliminateSerialization,
      RemoveAliasOnlyProject) ::
    Batch("Check Cartesian Products", Once,
      CheckCartesianProducts(conf)) ::
    Batch("Decimal Optimizations", fixedPoint,
      DecimalAggregates) ::
    Batch("Typed Filter Optimization", fixedPoint,
      CombineTypedFilters) ::
    Batch("LocalRelation", fixedPoint,
      ConvertToLocalRelation,
      PropagateEmptyRelation) ::
    Batch("OptimizeCodegen", Once,
      OptimizeCodegen(conf)) ::
    Batch("RewriteSubquery", Once,
      RewritePredicateSubquery,
      CollapseProject) :: Nil
  }

  /**
   * Optimize all the subqueries inside expression.
   */
  object OptimizeSubqueries extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case s: SubqueryExpression =>
        s.withNewPlan(Optimizer.this.execute(s.plan))
    }
  }
}

/**
 * An optimizer used in test code.
 *
 * To ensure extendability, we leave the standard rules in the abstract optimizer rules, while
 * specific rules go to the subclasses
 */
object SimpleTestOptimizer extends SimpleTestOptimizer

class SimpleTestOptimizer extends Optimizer(
  new SessionCatalog(
    new InMemoryCatalog,
    EmptyFunctionRegistry,
    new SimpleCatalystConf(caseSensitiveAnalysis = true)),
  new SimpleCatalystConf(caseSensitiveAnalysis = true))

/**
 * Removes the Project only conducting Alias of its child node.
 * It is created mainly for removing extra Project added in EliminateSerialization rule,
 * but can also benefit other operators.
 */
object RemoveAliasOnlyProject extends Rule[LogicalPlan] {
  /**
   * Returns true if the project list is semantically same as child output, after strip alias on
   * attribute.
   */
  private def isAliasOnly(
      projectList: Seq[NamedExpression],
      childOutput: Seq[Attribute]): Boolean = {
    if (projectList.length != childOutput.length) {
      false
    } else {
      stripAliasOnAttribute(projectList).zip(childOutput).forall {
        case (a: Attribute, o) if a semanticEquals o => true
        case _ => false
      }
    }
  }

  private def stripAliasOnAttribute(projectList: Seq[NamedExpression]) = {
    projectList.map {
      // Alias with metadata can not be stripped, or the metadata will be lost.
      // If the alias name is different from attribute name, we can't strip it either, or we may
      // accidentally change the output schema name of the root plan.
      case a @ Alias(attr: Attribute, name) if a.metadata == Metadata.empty && name == attr.name =>
        attr
      case other => other
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    val aliasOnlyProject = plan.collectFirst {
      case p @ Project(pList, child) if isAliasOnly(pList, child.output) => p
    }

    aliasOnlyProject.map { case proj =>
      val attributesToReplace = proj.output.zip(proj.child.output).filterNot {
        case (a1, a2) => a1 semanticEquals a2
      }
      val attrMap = AttributeMap(attributesToReplace)
      plan transform {
        case plan: Project if plan eq proj => plan.child
        case plan => plan transformExpressions {
          case a: Attribute if attrMap.contains(a) => attrMap(a)
        }
      }
    }.getOrElse(plan)
  }
}

/**
 * Pushes down [[LocalLimit]] beneath UNION ALL and beneath the streamed inputs of outer joins.
 */
object LimitPushDown extends Rule[LogicalPlan] {

  private def stripGlobalLimitIfPresent(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case GlobalLimit(_, child) => child
      case _ => plan
    }
  }

  private def maybePushLimit(limitExp: Expression, plan: LogicalPlan): LogicalPlan = {
    (limitExp, plan.maxRows) match {
      case (IntegerLiteral(maxRow), Some(childMaxRows)) if maxRow < childMaxRows =>
        LocalLimit(limitExp, stripGlobalLimitIfPresent(plan))
      case (_, None) =>
        LocalLimit(limitExp, stripGlobalLimitIfPresent(plan))
      case _ => plan
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Adding extra Limits below UNION ALL for children which are not Limit or do not have Limit
    // descendants whose maxRow is larger. This heuristic is valid assuming there does not exist any
    // Limit push-down rule that is unable to infer the value of maxRows.
    // Note: right now Union means UNION ALL, which does not de-duplicate rows, so it is safe to
    // pushdown Limit through it. Once we add UNION DISTINCT, however, we will not be able to
    // pushdown Limit.
    case LocalLimit(exp, Union(children)) =>
      LocalLimit(exp, Union(children.map(maybePushLimit(exp, _))))
    // Add extra limits below OUTER JOIN. For LEFT OUTER and FULL OUTER JOIN we push limits to the
    // left and right sides, respectively. For FULL OUTER JOIN, we can only push limits to one side
    // because we need to ensure that rows from the limited side still have an opportunity to match
    // against all candidates from the non-limited side. We also need to ensure that this limit
    // pushdown rule will not eventually introduce limits on both sides if it is applied multiple
    // times. Therefore:
    //   - If one side is already limited, stack another limit on top if the new limit is smaller.
    //     The redundant limit will be collapsed by the CombineLimits rule.
    //   - If neither side is limited, limit the side that is estimated to be bigger.
    case LocalLimit(exp, join @ Join(left, right, joinType, _)) =>
      val newJoin = joinType match {
        case RightOuter => join.copy(right = maybePushLimit(exp, right))
        case LeftOuter => join.copy(left = maybePushLimit(exp, left))
        case FullOuter =>
          (left.maxRows, right.maxRows) match {
            case (None, None) =>
              if (left.statistics.sizeInBytes >= right.statistics.sizeInBytes) {
                join.copy(left = maybePushLimit(exp, left))
              } else {
                join.copy(right = maybePushLimit(exp, right))
              }
            case (Some(_), Some(_)) => join
            case (Some(_), None) => join.copy(left = maybePushLimit(exp, left))
            case (None, Some(_)) => join.copy(right = maybePushLimit(exp, right))

          }
        case _ => join
      }
      LocalLimit(exp, newJoin)
  }
}

/**
 * Pushes Project operator to both sides of a Union operator.
 * Operations that are safe to pushdown are listed as follows.
 * Union:
 * Right now, Union means UNION ALL, which does not de-duplicate rows. So, it is
 * safe to pushdown Filters and Projections through it. Filter pushdown is handled by another
 * rule PushDownPredicate. Once we add UNION DISTINCT, we will not be able to pushdown Projections.
 */
object PushProjectionThroughUnion extends Rule[LogicalPlan] with PredicateHelper {

  /**
   * Maps Attributes from the left side to the corresponding Attribute on the right side.
   */
  private def buildRewrites(left: LogicalPlan, right: LogicalPlan): AttributeMap[Attribute] = {
    assert(left.output.size == right.output.size)
    AttributeMap(left.output.zip(right.output))
  }

  /**
   * Rewrites an expression so that it can be pushed to the right side of a
   * Union or Except operator. This method relies on the fact that the output attributes
   * of a union/intersect/except are always equal to the left child's output.
   */
  private def pushToRight[A <: Expression](e: A, rewrites: AttributeMap[Attribute]) = {
    val result = e transform {
      case a: Attribute => rewrites(a)
    }

    // We must promise the compiler that we did not discard the names in the case of project
    // expressions.  This is safe since the only transformation is from Attribute => Attribute.
    result.asInstanceOf[A]
  }

  /**
   * Splits the condition expression into small conditions by `And`, and partition them by
   * deterministic, and finally recombine them by `And`. It returns an expression containing
   * all deterministic expressions (the first field of the returned Tuple2) and an expression
   * containing all non-deterministic expressions (the second field of the returned Tuple2).
   */
  private def partitionByDeterministic(condition: Expression): (Expression, Expression) = {
    val andConditions = splitConjunctivePredicates(condition)
    andConditions.partition(_.deterministic) match {
      case (deterministic, nondeterministic) =>
        deterministic.reduceOption(And).getOrElse(Literal(true)) ->
        nondeterministic.reduceOption(And).getOrElse(Literal(true))
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {

    // Push down deterministic projection through UNION ALL
    case p @ Project(projectList, Union(children)) =>
      assert(children.nonEmpty)
      if (projectList.forall(_.deterministic)) {
        val newFirstChild = Project(projectList, children.head)
        val newOtherChildren = children.tail.map { child =>
          val rewrites = buildRewrites(children.head, child)
          Project(projectList.map(pushToRight(_, rewrites)), child)
        }
        Union(newFirstChild +: newOtherChildren)
      } else {
        p
      }
  }
}

/**
 * Attempts to eliminate the reading of unneeded columns from the query plan.
 *
 * Since adding Project before Filter conflicts with PushPredicatesThroughProject, this rule will
 * remove the Project p2 in the following pattern:
 *
 *   p1 @ Project(_, Filter(_, p2 @ Project(_, child))) if p2.outputSet.subsetOf(p2.inputSet)
 *
 * p2 is usually inserted by this rule and useless, p1 could prune the columns anyway.
 */
object ColumnPruning extends Rule[LogicalPlan] {
  private def sameOutput(output1: Seq[Attribute], output2: Seq[Attribute]): Boolean =
    output1.size == output2.size &&
      output1.zip(output2).forall(pair => pair._1.semanticEquals(pair._2))

  def apply(plan: LogicalPlan): LogicalPlan = removeProjectBeforeFilter(plan transform {
    // Prunes the unused columns from project list of Project/Aggregate/Expand
    case p @ Project(_, p2: Project) if (p2.outputSet -- p.references).nonEmpty =>
      p.copy(child = p2.copy(projectList = p2.projectList.filter(p.references.contains)))
    case p @ Project(_, a: Aggregate) if (a.outputSet -- p.references).nonEmpty =>
      p.copy(
        child = a.copy(aggregateExpressions = a.aggregateExpressions.filter(p.references.contains)))
    case a @ Project(_, e @ Expand(_, _, grandChild)) if (e.outputSet -- a.references).nonEmpty =>
      val newOutput = e.output.filter(a.references.contains(_))
      val newProjects = e.projections.map { proj =>
        proj.zip(e.output).filter { case (_, a) =>
          newOutput.contains(a)
        }.unzip._1
      }
      a.copy(child = Expand(newProjects, newOutput, grandChild))

    // Prunes the unused columns from child of `DeserializeToObject`
    case d @ DeserializeToObject(_, _, child) if (child.outputSet -- d.references).nonEmpty =>
      d.copy(child = prunedChild(child, d.references))

    // Prunes the unused columns from child of Aggregate/Expand/Generate
    case a @ Aggregate(_, _, child) if (child.outputSet -- a.references).nonEmpty =>
      a.copy(child = prunedChild(child, a.references))
    case e @ Expand(_, _, child) if (child.outputSet -- e.references).nonEmpty =>
      e.copy(child = prunedChild(child, e.references))
    case g: Generate if !g.join && (g.child.outputSet -- g.references).nonEmpty =>
      g.copy(child = prunedChild(g.child, g.references))

    // Turn off `join` for Generate if no column from it's child is used
    case p @ Project(_, g: Generate)
        if g.join && !g.outer && p.references.subsetOf(g.generatedSet) =>
      p.copy(child = g.copy(join = false))

    // Eliminate unneeded attributes from right side of a Left Existence Join.
    case j @ Join(_, right, LeftExistence(_), _) =>
      j.copy(right = prunedChild(right, j.references))

    // all the columns will be used to compare, so we can't prune them
    case p @ Project(_, _: SetOperation) => p
    case p @ Project(_, _: Distinct) => p
    // Eliminate unneeded attributes from children of Union.
    case p @ Project(_, u: Union) =>
      if ((u.outputSet -- p.references).nonEmpty) {
        val firstChild = u.children.head
        val newOutput = prunedChild(firstChild, p.references).output
        // pruning the columns of all children based on the pruned first child.
        val newChildren = u.children.map { p =>
          val selected = p.output.zipWithIndex.filter { case (a, i) =>
            newOutput.contains(firstChild.output(i))
          }.map(_._1)
          Project(selected, p)
        }
        p.copy(child = u.withNewChildren(newChildren))
      } else {
        p
      }

    // Prune unnecessary window expressions
    case p @ Project(_, w: Window) if (w.windowOutputSet -- p.references).nonEmpty =>
      p.copy(child = w.copy(
        windowExpressions = w.windowExpressions.filter(p.references.contains)))

    // Eliminate no-op Window
    case w: Window if w.windowExpressions.isEmpty => w.child

    // Eliminate no-op Projects
    case p @ Project(_, child) if sameOutput(child.output, p.output) => child

    // Can't prune the columns on LeafNode
    case p @ Project(_, _: LeafNode) => p

    // for all other logical plans that inherits the output from it's children
    case p @ Project(_, child) =>
      val required = child.references ++ p.references
      if ((child.inputSet -- required).nonEmpty) {
        val newChildren = child.children.map(c => prunedChild(c, required))
        p.copy(child = child.withNewChildren(newChildren))
      } else {
        p
      }
  })

  /** Applies a projection only when the child is producing unnecessary attributes */
  private def prunedChild(c: LogicalPlan, allReferences: AttributeSet) =
    if ((c.outputSet -- allReferences.filter(c.outputSet.contains)).nonEmpty) {
      Project(c.output.filter(allReferences.contains), c)
    } else {
      c
    }

  /**
   * The Project before Filter is not necessary but conflict with PushPredicatesThroughProject,
   * so remove it.
   */
  private def removeProjectBeforeFilter(plan: LogicalPlan): LogicalPlan = plan transform {
    case p1 @ Project(_, f @ Filter(_, p2 @ Project(_, child)))
      if p2.outputSet.subsetOf(child.outputSet) =>
      p1.copy(child = f.copy(child = child))
  }
}

/**
 * Combines two adjacent [[Project]] operators into one and perform alias substitution,
 * merging the expressions into one single expression.
 */
object CollapseProject extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case p1 @ Project(_, p2: Project) =>
      if (haveCommonNonDeterministicOutput(p1.projectList, p2.projectList)) {
        p1
      } else {
        p2.copy(projectList = buildCleanedProjectList(p1.projectList, p2.projectList))
      }
    case p @ Project(_, agg: Aggregate) =>
      if (haveCommonNonDeterministicOutput(p.projectList, agg.aggregateExpressions)) {
        p
      } else {
        agg.copy(aggregateExpressions = buildCleanedProjectList(
          p.projectList, agg.aggregateExpressions))
      }
  }

  private def collectAliases(projectList: Seq[NamedExpression]): AttributeMap[Alias] = {
    AttributeMap(projectList.collect {
      case a: Alias => a.toAttribute -> a
    })
  }

  private def haveCommonNonDeterministicOutput(
      upper: Seq[NamedExpression], lower: Seq[NamedExpression]): Boolean = {
    // Create a map of Aliases to their values from the lower projection.
    // e.g., 'SELECT ... FROM (SELECT a + b AS c, d ...)' produces Map(c -> Alias(a + b, c)).
    val aliases = collectAliases(lower)

    // Collapse upper and lower Projects if and only if their overlapped expressions are all
    // deterministic.
    upper.exists(_.collect {
      case a: Attribute if aliases.contains(a) => aliases(a).child
    }.exists(!_.deterministic))
  }

  private def buildCleanedProjectList(
      upper: Seq[NamedExpression],
      lower: Seq[NamedExpression]): Seq[NamedExpression] = {
    // Create a map of Aliases to their values from the lower projection.
    // e.g., 'SELECT ... FROM (SELECT a + b AS c, d ...)' produces Map(c -> Alias(a + b, c)).
    val aliases = collectAliases(lower)

    // Substitute any attributes that are produced by the lower projection, so that we safely
    // eliminate it.
    // e.g., 'SELECT c + 1 FROM (SELECT a + b AS C ...' produces 'SELECT a + b + 1 ...'
    // Use transformUp to prevent infinite recursion.
    val rewrittenUpper = upper.map(_.transformUp {
      case a: Attribute => aliases.getOrElse(a, a)
    })
    // collapse upper and lower Projects may introduce unnecessary Aliases, trim them here.
    rewrittenUpper.map { p =>
      CleanupAliases.trimNonTopLevelAliases(p).asInstanceOf[NamedExpression]
    }
  }
}

/**
 * Combines adjacent [[Repartition]] and [[RepartitionByExpression]] operator combinations
 * by keeping only the one.
 * 1. For adjacent [[Repartition]]s, collapse into the last [[Repartition]].
 * 2. For adjacent [[RepartitionByExpression]]s, collapse into the last [[RepartitionByExpression]].
 * 3. For a combination of [[Repartition]] and [[RepartitionByExpression]], collapse as a single
 *    [[RepartitionByExpression]] with the expression and last number of partition.
 */
object CollapseRepartition extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    // Case 1
    case Repartition(numPartitions, shuffle, Repartition(_, _, child)) =>
      Repartition(numPartitions, shuffle, child)
    // Case 2
    case RepartitionByExpression(exprs, RepartitionByExpression(_, child, _), numPartitions) =>
      RepartitionByExpression(exprs, child, numPartitions)
    // Case 3
    case Repartition(numPartitions, _, r: RepartitionByExpression) =>
      r.copy(numPartitions = Some(numPartitions))
    // Case 3
    case RepartitionByExpression(exprs, Repartition(_, _, child), numPartitions) =>
      RepartitionByExpression(exprs, child, numPartitions)
  }
}

/**
 * Generate a list of additional filters from an operator's existing constraint but remove those
 * that are either already part of the operator's condition or are part of the operator's child
 * constraints. These filters are currently inserted to the existing conditions in the Filter
 * operators and on either side of Join operators.
 *
 * Note: While this optimization is applicable to all types of join, it primarily benefits Inner and
 * LeftSemi joins.
 */
object InferFiltersFromConstraints extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, child) =>
      val newFilters = filter.constraints --
        (child.constraints ++ splitConjunctivePredicates(condition))
      if (newFilters.nonEmpty) {
        Filter(And(newFilters.reduce(And), condition), child)
      } else {
        filter
      }

    case join @ Join(left, right, joinType, conditionOpt) =>
      // Only consider constraints that can be pushed down completely to either the left or the
      // right child
      val constraints = join.constraints.filter { c =>
        c.references.subsetOf(left.outputSet) || c.references.subsetOf(right.outputSet)
      }
      // Remove those constraints that are already enforced by either the left or the right child
      val additionalConstraints = constraints -- (left.constraints ++ right.constraints)
      val newConditionOpt = conditionOpt match {
        case Some(condition) =>
          val newFilters = additionalConstraints -- splitConjunctivePredicates(condition)
          if (newFilters.nonEmpty) Option(And(newFilters.reduce(And), condition)) else None
        case None =>
          additionalConstraints.reduceOption(And)
      }
      if (newConditionOpt.isDefined) Join(left, right, joinType, newConditionOpt) else join
  }
}

/**
 * Combines all adjacent [[Union]] operators into a single [[Union]].
 */
object CombineUnions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Unions(children) => Union(children)
  }
}

/**
 * Combines two adjacent [[Filter]] operators into one, merging the non-redundant conditions into
 * one conjunctive predicate.
 */
object CombineFilters extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Filter(fc, nf @ Filter(nc, grandChild)) =>
      (ExpressionSet(splitConjunctivePredicates(fc)) --
        ExpressionSet(splitConjunctivePredicates(nc))).reduceOption(And) match {
        case Some(ac) =>
          Filter(And(nc, ac), grandChild)
        case None =>
          nf
      }
  }
}

/**
 * Removes no-op SortOrder from Sort
 */
object EliminateSorts extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case s @ Sort(orders, _, child) if orders.isEmpty || orders.exists(_.child.foldable) =>
      val newOrders = orders.filterNot(_.child.foldable)
      if (newOrders.isEmpty) child else s.copy(order = newOrders)
  }
}

/**
 * Removes filters that can be evaluated trivially.  This can be done through the following ways:
 * 1) by eliding the filter for cases where it will always evaluate to `true`.
 * 2) by substituting a dummy empty relation when the filter will always evaluate to `false`.
 * 3) by eliminating the always-true conditions given the constraints on the child's output.
 */
object PruneFilters extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // If the filter condition always evaluate to true, remove the filter.
    case Filter(Literal(true, BooleanType), child) => child
    // If the filter condition always evaluate to null or false,
    // replace the input with an empty relation.
    case Filter(Literal(null, _), child) => LocalRelation(child.output, data = Seq.empty)
    case Filter(Literal(false, BooleanType), child) => LocalRelation(child.output, data = Seq.empty)
    // If any deterministic condition is guaranteed to be true given the constraints on the child's
    // output, remove the condition
    case f @ Filter(fc, p: LogicalPlan) =>
      val (prunedPredicates, remainingPredicates) =
        splitConjunctivePredicates(fc).partition { cond =>
          cond.deterministic && p.constraints.contains(cond)
        }
      if (prunedPredicates.isEmpty) {
        f
      } else if (remainingPredicates.isEmpty) {
        p
      } else {
        val newCond = remainingPredicates.reduce(And)
        Filter(newCond, p)
      }
  }
}

/**
 * Pushes [[Filter]] operators through many operators iff:
 * 1) the operator is deterministic
 * 2) the predicate is deterministic and the operator will not change any of rows.
 *
 * This heuristic is valid assuming the expression evaluation cost is minimal.
 */
object PushDownPredicate extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // SPARK-13473: We can't push the predicate down when the underlying projection output non-
    // deterministic field(s).  Non-deterministic expressions are essentially stateful. This
    // implies that, for a given input row, the output are determined by the expression's initial
    // state and all the input rows processed before. In another word, the order of input rows
    // matters for non-deterministic expressions, while pushing down predicates changes the order.
    case filter @ Filter(condition, project @ Project(fields, grandChild))
      if fields.forall(_.deterministic) =>

      // Create a map of Aliases to their values from the child projection.
      // e.g., 'SELECT a + b AS c, d ...' produces Map(c -> a + b).
      val aliasMap = AttributeMap(fields.collect {
        case a: Alias => (a.toAttribute, a.child)
      })

      project.copy(child = Filter(replaceAlias(condition, aliasMap), grandChild))

    // Push [[Filter]] operators through [[Window]] operators. Parts of the predicate that can be
    // pushed beneath must satisfy the following conditions:
    // 1. All the expressions are part of window partitioning key. The expressions can be compound.
    // 2. Deterministic.
    // 3. Placed before any non-deterministic predicates.
    case filter @ Filter(condition, w: Window)
        if w.partitionSpec.forall(_.isInstanceOf[AttributeReference]) =>
      val partitionAttrs = AttributeSet(w.partitionSpec.flatMap(_.references))

      val (candidates, containingNonDeterministic) =
        splitConjunctivePredicates(condition).span(_.deterministic)

      val (pushDown, rest) = candidates.partition { cond =>
        cond.references.subsetOf(partitionAttrs)
      }

      val stayUp = rest ++ containingNonDeterministic

      if (pushDown.nonEmpty) {
        val pushDownPredicate = pushDown.reduce(And)
        val newWindow = w.copy(child = Filter(pushDownPredicate, w.child))
        if (stayUp.isEmpty) newWindow else Filter(stayUp.reduce(And), newWindow)
      } else {
        filter
      }

    case filter @ Filter(condition, aggregate: Aggregate) =>
      // Find all the aliased expressions in the aggregate list that don't include any actual
      // AggregateExpression, and create a map from the alias to the expression
      val aliasMap = AttributeMap(aggregate.aggregateExpressions.collect {
        case a: Alias if a.child.find(_.isInstanceOf[AggregateExpression]).isEmpty =>
          (a.toAttribute, a.child)
      })

      // For each filter, expand the alias and check if the filter can be evaluated using
      // attributes produced by the aggregate operator's child operator.
      val (candidates, containingNonDeterministic) =
        splitConjunctivePredicates(condition).span(_.deterministic)

      val (pushDown, rest) = candidates.partition { cond =>
        val replaced = replaceAlias(cond, aliasMap)
        replaced.references.subsetOf(aggregate.child.outputSet)
      }

      val stayUp = rest ++ containingNonDeterministic

      if (pushDown.nonEmpty) {
        val pushDownPredicate = pushDown.reduce(And)
        val replaced = replaceAlias(pushDownPredicate, aliasMap)
        val newAggregate = aggregate.copy(child = Filter(replaced, aggregate.child))
        // If there is no more filter to stay up, just eliminate the filter.
        // Otherwise, create "Filter(stayUp) <- Aggregate <- Filter(pushDownPredicate)".
        if (stayUp.isEmpty) newAggregate else Filter(stayUp.reduce(And), newAggregate)
      } else {
        filter
      }

    case filter @ Filter(condition, union: Union) =>
      // Union could change the rows, so non-deterministic predicate can't be pushed down
      val (pushDown, stayUp) = splitConjunctivePredicates(condition).span(_.deterministic)

      if (pushDown.nonEmpty) {
        val pushDownCond = pushDown.reduceLeft(And)
        val output = union.output
        val newGrandChildren = union.children.map { grandchild =>
          val newCond = pushDownCond transform {
            case e if output.exists(_.semanticEquals(e)) =>
              grandchild.output(output.indexWhere(_.semanticEquals(e)))
          }
          assert(newCond.references.subsetOf(grandchild.outputSet))
          Filter(newCond, grandchild)
        }
        val newUnion = union.withNewChildren(newGrandChildren)
        if (stayUp.nonEmpty) {
          Filter(stayUp.reduceLeft(And), newUnion)
        } else {
          newUnion
        }
      } else {
        filter
      }

    case filter @ Filter(condition, u: UnaryNode)
        if canPushThrough(u) && u.expressions.forall(_.deterministic) =>
      pushDownPredicate(filter, u.child) { predicate =>
        u.withNewChildren(Seq(Filter(predicate, u.child)))
      }
  }

  private def canPushThrough(p: UnaryNode): Boolean = p match {
    // Note that some operators (e.g. project, aggregate, union) are being handled separately
    // (earlier in this rule).
    case _: AppendColumns => true
    case _: BroadcastHint => true
    case _: Distinct => true
    case _: Generate => true
    case _: Pivot => true
    case _: RedistributeData => true
    case _: Repartition => true
    case _: ScriptTransformation => true
    case _: Sort => true
    case _ => false
  }

  private def pushDownPredicate(
      filter: Filter,
      grandchild: LogicalPlan)(insertFilter: Expression => LogicalPlan): LogicalPlan = {
    // Only push down the predicates that is deterministic and all the referenced attributes
    // come from grandchild.
    // TODO: non-deterministic predicates could be pushed through some operators that do not change
    // the rows.
    val (candidates, containingNonDeterministic) =
      splitConjunctivePredicates(filter.condition).span(_.deterministic)

    val (pushDown, rest) = candidates.partition { cond =>
      cond.references.subsetOf(grandchild.outputSet)
    }

    val stayUp = rest ++ containingNonDeterministic

    if (pushDown.nonEmpty) {
      val newChild = insertFilter(pushDown.reduceLeft(And))
      if (stayUp.nonEmpty) {
        Filter(stayUp.reduceLeft(And), newChild)
      } else {
        newChild
      }
    } else {
      filter
    }
  }
}

/**
 * Pushes down [[Filter]] operators where the `condition` can be
 * evaluated using only the attributes of the left or right side of a join.  Other
 * [[Filter]] conditions are moved into the `condition` of the [[Join]].
 *
 * And also pushes down the join filter, where the `condition` can be evaluated using only the
 * attributes of the left or right side of sub query when applicable.
 *
 * Check https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior for more details
 */
object PushPredicateThroughJoin extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * Splits join condition expressions or filter predicates (on a given join's output) into three
   * categories based on the attributes required to evaluate them. Note that we explicitly exclude
   * on-deterministic (i.e., stateful) condition expressions in canEvaluateInLeft or
   * canEvaluateInRight to prevent pushing these predicates on either side of the join.
   *
   * @return (canEvaluateInLeft, canEvaluateInRight, haveToEvaluateInBoth)
   */
  private def split(condition: Seq[Expression], left: LogicalPlan, right: LogicalPlan) = {
    // Note: In order to ensure correctness, it's important to not change the relative ordering of
    // any deterministic expression that follows a non-deterministic expression. To achieve this,
    // we only consider pushing down those expressions that precede the first non-deterministic
    // expression in the condition.
    val (pushDownCandidates, containingNonDeterministic) = condition.span(_.deterministic)
    val (leftEvaluateCondition, rest) =
      pushDownCandidates.partition(_.references.subsetOf(left.outputSet))
    val (rightEvaluateCondition, commonCondition) =
        rest.partition(expr => expr.references.subsetOf(right.outputSet))

    (leftEvaluateCondition, rightEvaluateCondition, commonCondition ++ containingNonDeterministic)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // push the where condition down into join filter
    case f @ Filter(filterCondition, Join(left, right, joinType, joinCondition)) =>
      val (leftFilterConditions, rightFilterConditions, commonFilterCondition) =
        split(splitConjunctivePredicates(filterCondition), left, right)
      joinType match {
        case _: InnerLike =>
          // push down the single side `where` condition into respective sides
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val (newJoinConditions, others) =
            commonFilterCondition.partition(e => !SubqueryExpression.hasCorrelatedSubquery(e))
          val newJoinCond = (newJoinConditions ++ joinCondition).reduceLeftOption(And)

          val join = Join(newLeft, newRight, joinType, newJoinCond)
          if (others.nonEmpty) {
            Filter(others.reduceLeft(And), join)
          } else {
            join
          }
        case RightOuter =>
          // push down the right side only `where` condition
          val newLeft = left
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, RightOuter, newJoinCond)

          (leftFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        case LeftOuter | LeftExistence(_) =>
          // push down the left side only `where` condition
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, joinType, newJoinCond)

          (rightFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        case FullOuter => f // DO Nothing for Full Outer Join
        case NaturalJoin(_) => sys.error("Untransformed NaturalJoin node")
        case UsingJoin(_, _) => sys.error("Untransformed Using join node")
      }

    // push down the join filter into sub query scanning if applicable
    case j @ Join(left, right, joinType, joinCondition) =>
      val (leftJoinConditions, rightJoinConditions, commonJoinCondition) =
        split(joinCondition.map(splitConjunctivePredicates).getOrElse(Nil), left, right)

      joinType match {
        case _: InnerLike | LeftExistence(_) =>
          // push down the single side only join filter for both sides sub queries
          val newLeft = leftJoinConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = rightJoinConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = commonJoinCondition.reduceLeftOption(And)

          Join(newLeft, newRight, joinType, newJoinCond)
        case RightOuter =>
          // push down the left side only join filter for left side sub query
          val newLeft = leftJoinConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right
          val newJoinCond = (rightJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, RightOuter, newJoinCond)
        case LeftOuter =>
          // push down the right side only join filter for right sub query
          val newLeft = left
          val newRight = rightJoinConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = (leftJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, LeftOuter, newJoinCond)
        case FullOuter => j
        case NaturalJoin(_) => sys.error("Untransformed NaturalJoin node")
        case UsingJoin(_, _) => sys.error("Untransformed Using join node")
      }
  }
}

/**
 * Combines two adjacent [[Limit]] operators into one, merging the
 * expressions into one single expression.
 */
object CombineLimits extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case GlobalLimit(le, GlobalLimit(ne, grandChild)) =>
      GlobalLimit(Least(Seq(ne, le)), grandChild)
    case LocalLimit(le, LocalLimit(ne, grandChild)) =>
      LocalLimit(Least(Seq(ne, le)), grandChild)
    case Limit(le, Limit(ne, grandChild)) =>
      Limit(Least(Seq(ne, le)), grandChild)
  }
}

/**
 * Check if there any cartesian products between joins of any type in the optimized plan tree.
 * Throw an error if a cartesian product is found without an explicit cross join specified.
 * This rule is effectively disabled if the CROSS_JOINS_ENABLED flag is true.
 *
 * This rule must be run AFTER the ReorderJoin rule since the join conditions for each join must be
 * collected before checking if it is a cartesian product. If you have
 * SELECT * from R, S where R.r = S.s,
 * the join between R and S is not a cartesian product and therefore should be allowed.
 * The predicate R.r = S.s is not recognized as a join condition until the ReorderJoin rule.
 */
case class CheckCartesianProducts(conf: CatalystConf)
    extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * Check if a join is a cartesian product. Returns true if
   * there are no join conditions involving references from both left and right.
   */
  def isCartesianProduct(join: Join)
      : Boolean = {
    val conditions = join.condition.map(splitConjunctivePredicates).getOrElse(Nil)
    !conditions.map(_.references).exists(refs => refs.exists(join.left.outputSet.contains)
        && refs.exists(join.right.outputSet.contains))
  }

  def apply(plan: LogicalPlan): LogicalPlan =
    if (conf.crossJoinEnabled) {
      plan
    } else plan transform {
      case j @ Join(left, right, Inner | LeftOuter | RightOuter | FullOuter, condition)
        if isCartesianProduct(j) =>
          throw new AnalysisException(
            s"""Detected cartesian product for ${j.joinType.sql} join between logical plans
               |${left.treeString(false).trim}
               |and
               |${right.treeString(false).trim}
               |Join condition is missing or trivial.
               |Use the CROSS JOIN syntax to allow cartesian products between these relations."""
            .stripMargin)
    }
}

/**
 * Speeds up aggregates on fixed-precision decimals by executing them on unscaled Long values.
 *
 * This uses the same rules for increasing the precision and scale of the output as
 * [[org.apache.spark.sql.catalyst.analysis.DecimalPrecision]].
 */
object DecimalAggregates extends Rule[LogicalPlan] {
  import Decimal.MAX_LONG_DIGITS

  /** Maximum number of decimal digits representable precisely in a Double */
  private val MAX_DOUBLE_DIGITS = 15

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsDown {
      case we @ WindowExpression(ae @ AggregateExpression(af, _, _, _), _) => af match {
        case Sum(e @ DecimalType.Expression(prec, scale)) if prec + 10 <= MAX_LONG_DIGITS =>
          MakeDecimal(we.copy(windowFunction = ae.copy(aggregateFunction = Sum(UnscaledValue(e)))),
            prec + 10, scale)

        case Average(e @ DecimalType.Expression(prec, scale)) if prec + 4 <= MAX_DOUBLE_DIGITS =>
          val newAggExpr =
            we.copy(windowFunction = ae.copy(aggregateFunction = Average(UnscaledValue(e))))
          Cast(
            Divide(newAggExpr, Literal.create(math.pow(10.0, scale), DoubleType)),
            DecimalType(prec + 4, scale + 4))

        case _ => we
      }
      case ae @ AggregateExpression(af, _, _, _) => af match {
        case Sum(e @ DecimalType.Expression(prec, scale)) if prec + 10 <= MAX_LONG_DIGITS =>
          MakeDecimal(ae.copy(aggregateFunction = Sum(UnscaledValue(e))), prec + 10, scale)

        case Average(e @ DecimalType.Expression(prec, scale)) if prec + 4 <= MAX_DOUBLE_DIGITS =>
          val newAggExpr = ae.copy(aggregateFunction = Average(UnscaledValue(e)))
          Cast(
            Divide(newAggExpr, Literal.create(math.pow(10.0, scale), DoubleType)),
            DecimalType(prec + 4, scale + 4))

        case _ => ae
      }
    }
  }
}

/**
 * Converts local operations (i.e. ones that don't require data exchange) on LocalRelation to
 * another LocalRelation.
 *
 * This is relatively simple as it currently handles only a single case: Project.
 */
object ConvertToLocalRelation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Project(projectList, LocalRelation(output, data))
        if !projectList.exists(hasUnevaluableExpr) =>
      val projection = new InterpretedProjection(projectList, output)
      LocalRelation(projectList.map(_.toAttribute), data.map(projection))
  }

  private def hasUnevaluableExpr(expr: Expression): Boolean = {
    expr.find(e => e.isInstanceOf[Unevaluable] && !e.isInstanceOf[AttributeReference]).isDefined
  }
}

/**
 * Replaces logical [[Distinct]] operator with an [[Aggregate]] operator.
 * {{{
 *   SELECT DISTINCT f1, f2 FROM t  ==>  SELECT f1, f2 FROM t GROUP BY f1, f2
 * }}}
 */
object ReplaceDistinctWithAggregate extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Distinct(child) => Aggregate(child.output, child.output, child)
  }
}

/**
 * Replaces logical [[Intersect]] operator with a left-semi [[Join]] operator.
 * {{{
 *   SELECT a1, a2 FROM Tab1 INTERSECT SELECT b1, b2 FROM Tab2
 *   ==>  SELECT DISTINCT a1, a2 FROM Tab1 LEFT SEMI JOIN Tab2 ON a1<=>b1 AND a2<=>b2
 * }}}
 *
 * Note:
 * 1. This rule is only applicable to INTERSECT DISTINCT. Do not use it for INTERSECT ALL.
 * 2. This rule has to be done after de-duplicating the attributes; otherwise, the generated
 *    join conditions will be incorrect.
 */
object ReplaceIntersectWithSemiJoin extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Intersect(left, right) =>
      assert(left.output.size == right.output.size)
      val joinCond = left.output.zip(right.output).map { case (l, r) => EqualNullSafe(l, r) }
      Distinct(Join(left, right, LeftSemi, joinCond.reduceLeftOption(And)))
  }
}

/**
 * Replaces logical [[Except]] operator with a left-anti [[Join]] operator.
 * {{{
 *   SELECT a1, a2 FROM Tab1 EXCEPT SELECT b1, b2 FROM Tab2
 *   ==>  SELECT DISTINCT a1, a2 FROM Tab1 LEFT ANTI JOIN Tab2 ON a1<=>b1 AND a2<=>b2
 * }}}
 *
 * Note:
 * 1. This rule is only applicable to EXCEPT DISTINCT. Do not use it for EXCEPT ALL.
 * 2. This rule has to be done after de-duplicating the attributes; otherwise, the generated
 *    join conditions will be incorrect.
 */
object ReplaceExceptWithAntiJoin extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Except(left, right) =>
      assert(left.output.size == right.output.size)
      val joinCond = left.output.zip(right.output).map { case (l, r) => EqualNullSafe(l, r) }
      Distinct(Join(left, right, LeftAnti, joinCond.reduceLeftOption(And)))
  }
}

/**
 * Removes literals from group expressions in [[Aggregate]], as they have no effect to the result
 * but only makes the grouping key bigger.
 */
object RemoveLiteralFromGroupExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case a @ Aggregate(grouping, _, _) =>
      val newGrouping = grouping.filter(!_.foldable)
      a.copy(groupingExpressions = newGrouping)
  }
}

/**
 * Removes repetition from group expressions in [[Aggregate]], as they have no effect to the result
 * but only makes the grouping key bigger.
 */
object RemoveRepetitionFromGroupExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case a @ Aggregate(grouping, _, _) =>
      val newGrouping = ExpressionSet(grouping).toSeq
      a.copy(groupingExpressions = newGrouping)
  }
}
