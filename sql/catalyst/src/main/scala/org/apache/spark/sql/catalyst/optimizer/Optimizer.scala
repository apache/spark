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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * Abstract class all optimizers should inherit of, contains the standard batches (extending
 * Optimizers can override this.
 */
abstract class Optimizer(sessionCatalog: SessionCatalog)
  extends RuleExecutor[LogicalPlan] {

  // Check for structural integrity of the plan in test mode. Currently we only check if a plan is
  // still resolved after the execution of each rule.
  override protected def isPlanIntegral(plan: LogicalPlan): Boolean = {
    !Utils.isTesting || plan.resolved
  }

  protected def fixedPoint = FixedPoint(SQLConf.get.optimizerMaxIterations)

  /**
   * Defines the default rule batches in the Optimizer.
   *
   * Implementations of this class should override this method, and [[nonExcludableRules]] if
   * necessary, instead of [[batches]]. The rule batches that eventually run in the Optimizer,
   * i.e., returned by [[batches]], will be (defaultBatches - (excludedRules - nonExcludableRules)).
   */
  def defaultBatches: Seq[Batch] = {
    val operatorOptimizationRuleSet =
      Seq(
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
        CollapseWindow,
        CombineFilters,
        CombineLimits,
        CombineUnions,
        // Constant folding and strength reduction
        NullPropagation,
        ConstantPropagation,
        FoldablePropagation,
        OptimizeIn,
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
        RemoveRedundantAliases,
        RemoveRedundantProject,
        SimplifyExtractValueOps,
        CombineConcats) ++
        extendedOperatorOptimizationRules

    val operatorOptimizationBatch: Seq[Batch] = {
      val rulesWithoutInferFiltersFromConstraints =
        operatorOptimizationRuleSet.filterNot(_ == InferFiltersFromConstraints)
      Batch("Operator Optimization before Inferring Filters", fixedPoint,
        rulesWithoutInferFiltersFromConstraints: _*) ::
      Batch("Infer Filters", Once,
        InferFiltersFromConstraints) ::
      Batch("Operator Optimization after Inferring Filters", fixedPoint,
        rulesWithoutInferFiltersFromConstraints: _*) :: Nil
    }

    (Batch("Eliminate Distinct", Once, EliminateDistinct) ::
    // Technically some of the rules in Finish Analysis are not optimizer rules and belong more
    // in the analyzer, because they are needed for correctness (e.g. ComputeCurrentTime).
    // However, because we also use the analyzer to canonicalized queries (for view definition),
    // we do not eliminate subqueries or compute current time in the analyzer.
    Batch("Finish Analysis", Once,
      EliminateSubqueryAliases,
      EliminateView,
      ReplaceExpressions,
      ComputeCurrentTime,
      GetCurrentDatabase(sessionCatalog),
      RewriteDistinctAggregates,
      ReplaceDeduplicateWithAggregate) ::
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
    // run this once earlier. this might simplify the plan and reduce cost of optimizer.
    // for example, a query such as Filter(LocalRelation) would go through all the heavy
    // optimizer rules that are triggered when there is a filter
    // (e.g. InferFiltersFromConstraints). if we run this batch earlier, the query becomes just
    // LocalRelation and does not trigger many rules
    Batch("LocalRelation early", fixedPoint,
      ConvertToLocalRelation,
      PropagateEmptyRelation) ::
    Batch("Pullup Correlated Expressions", Once,
      PullupCorrelatedPredicates) ::
    Batch("Subquery", Once,
      OptimizeSubqueries) ::
    Batch("Replace Operators", fixedPoint,
      RewriteExceptAll,
      RewriteIntersectAll,
      ReplaceIntersectWithSemiJoin,
      ReplaceExceptWithFilter,
      ReplaceExceptWithAntiJoin,
      ReplaceDistinctWithAggregate) ::
    Batch("Aggregate", fixedPoint,
      RemoveLiteralFromGroupExpressions,
      RemoveRepetitionFromGroupExpressions) :: Nil ++
    operatorOptimizationBatch) :+
    Batch("Join Reorder", Once,
      CostBasedJoinReorder) :+
    Batch("Remove Redundant Sorts", Once,
      RemoveRedundantSorts) :+
    Batch("Decimal Optimizations", fixedPoint,
      DecimalAggregates) :+
    Batch("Object Expressions Optimization", fixedPoint,
      EliminateMapObjects,
      CombineTypedFilters) :+
    Batch("LocalRelation", fixedPoint,
      ConvertToLocalRelation,
      PropagateEmptyRelation) :+
    Batch("Extract PythonUDF From JoinCondition", Once,
      PullOutPythonUDFInJoinCondition) :+
    // The following batch should be executed after batch "Join Reorder" "LocalRelation" and
    // "Extract PythonUDF From JoinCondition".
    Batch("Check Cartesian Products", Once,
      CheckCartesianProducts) :+
    Batch("RewriteSubquery", Once,
      RewritePredicateSubquery,
      ColumnPruning,
      CollapseProject,
      RemoveRedundantProject) :+
    Batch("UpdateAttributeReferences", Once,
      UpdateNullabilityInAttributeReferences)
  }

  /**
   * Defines rules that cannot be excluded from the Optimizer even if they are specified in
   * SQL config "excludedRules".
   *
   * Implementations of this class can override this method if necessary. The rule batches
   * that eventually run in the Optimizer, i.e., returned by [[batches]], will be
   * (defaultBatches - (excludedRules - nonExcludableRules)).
   */
  def nonExcludableRules: Seq[String] =
    EliminateDistinct.ruleName ::
      EliminateSubqueryAliases.ruleName ::
      EliminateView.ruleName ::
      ReplaceExpressions.ruleName ::
      ComputeCurrentTime.ruleName ::
      GetCurrentDatabase(sessionCatalog).ruleName ::
      RewriteDistinctAggregates.ruleName ::
      ReplaceDeduplicateWithAggregate.ruleName ::
      ReplaceIntersectWithSemiJoin.ruleName ::
      ReplaceExceptWithFilter.ruleName ::
      ReplaceExceptWithAntiJoin.ruleName ::
      RewriteExceptAll.ruleName ::
      RewriteIntersectAll.ruleName ::
      ReplaceDistinctWithAggregate.ruleName ::
      PullupCorrelatedPredicates.ruleName ::
      RewriteCorrelatedScalarSubquery.ruleName ::
      RewritePredicateSubquery.ruleName ::
      PullOutPythonUDFInJoinCondition.ruleName :: Nil

  /**
   * Optimize all the subqueries inside expression.
   */
  object OptimizeSubqueries extends Rule[LogicalPlan] {
    private def removeTopLevelSort(plan: LogicalPlan): LogicalPlan = {
      plan match {
        case Sort(_, _, child) => child
        case Project(fields, child) => Project(fields, removeTopLevelSort(child))
        case other => other
      }
    }
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case s: SubqueryExpression =>
        val Subquery(newPlan) = Optimizer.this.execute(Subquery(s.plan))
        // At this point we have an optimized subquery plan that we are going to attach
        // to this subquery expression. Here we can safely remove any top level sort
        // in the plan as tuples produced by a subquery are un-ordered.
        s.withNewPlan(removeTopLevelSort(newPlan))
    }
  }

  /**
   * Override to provide additional rules for the operator optimization batch.
   */
  def extendedOperatorOptimizationRules: Seq[Rule[LogicalPlan]] = Nil

  /**
   * Returns (defaultBatches - (excludedRules - nonExcludableRules)), the rule batches that
   * eventually run in the Optimizer.
   *
   * Implementations of this class should override [[defaultBatches]], and [[nonExcludableRules]]
   * if necessary, instead of this method.
   */
  final override def batches: Seq[Batch] = {
    val excludedRulesConf =
      SQLConf.get.optimizerExcludedRules.toSeq.flatMap(Utils.stringToSeq)
    val excludedRules = excludedRulesConf.filter { ruleName =>
      val nonExcludable = nonExcludableRules.contains(ruleName)
      if (nonExcludable) {
        logWarning(s"Optimization rule '${ruleName}' was not excluded from the optimizer " +
          s"because this rule is a non-excludable rule.")
      }
      !nonExcludable
    }
    if (excludedRules.isEmpty) {
      defaultBatches
    } else {
      defaultBatches.flatMap { batch =>
        val filteredRules = batch.rules.filter { rule =>
          val exclude = excludedRules.contains(rule.ruleName)
          if (exclude) {
            logInfo(s"Optimization rule '${rule.ruleName}' is excluded from the optimizer.")
          }
          !exclude
        }
        if (batch.rules == filteredRules) {
          Some(batch)
        } else if (filteredRules.nonEmpty) {
          Some(Batch(batch.name, batch.strategy, filteredRules: _*))
        } else {
          logInfo(s"Optimization batch '${batch.name}' is excluded from the optimizer " +
            s"as all enclosed rules have been excluded.")
          None
        }
      }
    }
  }
}

/**
 * Remove useless DISTINCT for MAX and MIN.
 * This rule should be applied before RewriteDistinctAggregates.
 */
object EliminateDistinct extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions  {
    case ae: AggregateExpression if ae.isDistinct =>
      ae.aggregateFunction match {
        case _: Max | _: Min => ae.copy(isDistinct = false)
        case _ => ae
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
    new SQLConf().copy(SQLConf.CASE_SENSITIVE -> true)))

/**
 * Remove redundant aliases from a query plan. A redundant alias is an alias that does not change
 * the name or metadata of a column, and does not deduplicate it.
 */
object RemoveRedundantAliases extends Rule[LogicalPlan] {

  /**
   * Create an attribute mapping from the old to the new attributes. This function will only
   * return the attribute pairs that have changed.
   */
  private def createAttributeMapping(current: LogicalPlan, next: LogicalPlan)
      : Seq[(Attribute, Attribute)] = {
    current.output.zip(next.output).filterNot {
      case (a1, a2) => a1.semanticEquals(a2)
    }
  }

  /**
   * Remove the top-level alias from an expression when it is redundant.
   */
  private def removeRedundantAlias(e: Expression, blacklist: AttributeSet): Expression = e match {
    // Alias with metadata can not be stripped, or the metadata will be lost.
    // If the alias name is different from attribute name, we can't strip it either, or we
    // may accidentally change the output schema name of the root plan.
    case a @ Alias(attr: Attribute, name)
      if a.metadata == Metadata.empty &&
        name == attr.name &&
        !blacklist.contains(attr) &&
        !blacklist.contains(a) =>
      attr
    case a => a
  }

  /**
   * Remove redundant alias expression from a LogicalPlan and its subtree. A blacklist is used to
   * prevent the removal of seemingly redundant aliases used to deduplicate the input for a (self)
   * join or to prevent the removal of top-level subquery attributes.
   */
  private def removeRedundantAliases(plan: LogicalPlan, blacklist: AttributeSet): LogicalPlan = {
    plan match {
      // We want to keep the same output attributes for subqueries. This means we cannot remove
      // the aliases that produce these attributes
      case Subquery(child) =>
        Subquery(removeRedundantAliases(child, blacklist ++ child.outputSet))

      // A join has to be treated differently, because the left and the right side of the join are
      // not allowed to use the same attributes. We use a blacklist to prevent us from creating a
      // situation in which this happens; the rule will only remove an alias if its child
      // attribute is not on the black list.
      case Join(left, right, joinType, condition) =>
        val newLeft = removeRedundantAliases(left, blacklist ++ right.outputSet)
        val newRight = removeRedundantAliases(right, blacklist ++ newLeft.outputSet)
        val mapping = AttributeMap(
          createAttributeMapping(left, newLeft) ++
          createAttributeMapping(right, newRight))
        val newCondition = condition.map(_.transform {
          case a: Attribute => mapping.getOrElse(a, a)
        })
        Join(newLeft, newRight, joinType, newCondition)

      case _ =>
        // Remove redundant aliases in the subtree(s).
        val currentNextAttrPairs = mutable.Buffer.empty[(Attribute, Attribute)]
        val newNode = plan.mapChildren { child =>
          val newChild = removeRedundantAliases(child, blacklist)
          currentNextAttrPairs ++= createAttributeMapping(child, newChild)
          newChild
        }

        // Create the attribute mapping. Note that the currentNextAttrPairs can contain duplicate
        // keys in case of Union (this is caused by the PushProjectionThroughUnion rule); in this
        // case we use the the first mapping (which should be provided by the first child).
        val mapping = AttributeMap(currentNextAttrPairs)

        // Create a an expression cleaning function for nodes that can actually produce redundant
        // aliases, use identity otherwise.
        val clean: Expression => Expression = plan match {
          case _: Project => removeRedundantAlias(_, blacklist)
          case _: Aggregate => removeRedundantAlias(_, blacklist)
          case _: Window => removeRedundantAlias(_, blacklist)
          case _ => identity[Expression]
        }

        // Transform the expressions.
        newNode.mapExpressions { expr =>
          clean(expr.transform {
            case a: Attribute => mapping.getOrElse(a, a)
          })
        }
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = removeRedundantAliases(plan, AttributeSet.empty)
}

/**
 * Remove projections from the query plan that do not make any modifications.
 */
object RemoveRedundantProject extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case p @ Project(_, child) if p.output == child.output => child
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

  private def maybePushLocalLimit(limitExp: Expression, plan: LogicalPlan): LogicalPlan = {
    (limitExp, plan.maxRowsPerPartition) match {
      case (IntegerLiteral(newLimit), Some(childMaxRows)) if newLimit < childMaxRows =>
        // If the child has a cap on max rows per partition and the cap is larger than
        // the new limit, put a new LocalLimit there.
        LocalLimit(limitExp, stripGlobalLimitIfPresent(plan))

      case (_, None) =>
        // If the child has no cap, put the new LocalLimit.
        LocalLimit(limitExp, stripGlobalLimitIfPresent(plan))

      case _ =>
        // Otherwise, don't put a new LocalLimit.
        plan
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
      LocalLimit(exp, Union(children.map(maybePushLocalLimit(exp, _))))
    // Add extra limits below OUTER JOIN. For LEFT OUTER and RIGHT OUTER JOIN we push limits to
    // the left and right sides, respectively. It's not safe to push limits below FULL OUTER
    // JOIN in the general case without a more invasive rewrite.
    // We also need to ensure that this limit pushdown rule will not eventually introduce limits
    // on both sides if it is applied multiple times. Therefore:
    //   - If one side is already limited, stack another limit on top if the new limit is smaller.
    //     The redundant limit will be collapsed by the CombineLimits rule.
    case LocalLimit(exp, join @ Join(left, right, joinType, _)) =>
      val newJoin = joinType match {
        case RightOuter => join.copy(right = maybePushLocalLimit(exp, right))
        case LeftOuter => join.copy(left = maybePushLocalLimit(exp, left))
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
    } match {
      // Make sure exprId is unique in each child of Union.
      case Alias(child, alias) => Alias(child, alias)()
      case other => other
    }

    // We must promise the compiler that we did not discard the names in the case of project
    // expressions.  This is safe since the only transformation is from Attribute => Attribute.
    result.asInstanceOf[A]
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

    // Prunes the unused columns from child of Aggregate/Expand/Generate/ScriptTransformation
    case a @ Aggregate(_, _, child) if (child.outputSet -- a.references).nonEmpty =>
      a.copy(child = prunedChild(child, a.references))
    case f @ FlatMapGroupsInPandas(_, _, _, child) if (child.outputSet -- f.references).nonEmpty =>
      f.copy(child = prunedChild(child, f.references))
    case e @ Expand(_, _, child) if (child.outputSet -- e.references).nonEmpty =>
      e.copy(child = prunedChild(child, e.references))
    case s @ ScriptTransformation(_, _, _, child, _)
        if (child.outputSet -- s.references).nonEmpty =>
      s.copy(child = prunedChild(child, s.references))

    // prune unrequired references
    case p @ Project(_, g: Generate) if p.references != g.outputSet =>
      val requiredAttrs = p.references -- g.producedAttributes ++ g.generator.references
      val newChild = prunedChild(g.child, requiredAttrs)
      val unrequired = g.generator.references -- p.references
      val unrequiredIndices = newChild.output.zipWithIndex.filter(t => unrequired.contains(t._1))
        .map(_._2)
      p.copy(child = g.copy(child = newChild, unrequiredChildIndex = unrequiredIndices))

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
   * so remove it. Since the Projects have been added top-down, we need to remove in bottom-up
   * order, otherwise lower Projects can be missed.
   */
  private def removeProjectBeforeFilter(plan: LogicalPlan): LogicalPlan = plan transformUp {
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
 * Combines adjacent [[RepartitionOperation]] operators
 */
object CollapseRepartition extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    // Case 1: When a Repartition has a child of Repartition or RepartitionByExpression,
    // 1) When the top node does not enable the shuffle (i.e., coalesce API), but the child
    //   enables the shuffle. Returns the child node if the last numPartitions is bigger;
    //   otherwise, keep unchanged.
    // 2) In the other cases, returns the top node with the child's child
    case r @ Repartition(_, _, child: RepartitionOperation) => (r.shuffle, child.shuffle) match {
      case (false, true) => if (r.numPartitions >= child.numPartitions) child else r
      case _ => r.copy(child = child.child)
    }
    // Case 2: When a RepartitionByExpression has a child of Repartition or RepartitionByExpression
    // we can remove the child.
    case r @ RepartitionByExpression(_, child: RepartitionOperation, _) =>
      r.copy(child = child.child)
  }
}

/**
 * Collapse Adjacent Window Expression.
 * - If the partition specs and order specs are the same and the window expression are
 *   independent and are of the same window function type, collapse into the parent.
 */
object CollapseWindow extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case w1 @ Window(we1, ps1, os1, w2 @ Window(we2, ps2, os2, grandChild))
        if ps1 == ps2 && os1 == os2 && w1.references.intersect(w2.windowOutputSet).isEmpty &&
          // This assumes Window contains the same type of window expressions. This is ensured
          // by ExtractWindowFunctions.
          WindowFunctionType.functionType(we1.head) == WindowFunctionType.functionType(we2.head) =>
      w1.copy(windowExpressions = we2 ++ we1, child = grandChild)
  }
}

/**
 * Generate a list of additional filters from an operator's existing constraint but remove those
 * that are either already part of the operator's condition or are part of the operator's child
 * constraints. These filters are currently inserted to the existing conditions in the Filter
 * operators and on either side of Join operators.
 *
 * Note: While this optimization is applicable to a lot of types of join, it primarily benefits
 * Inner and LeftSemi joins.
 */
object InferFiltersFromConstraints extends Rule[LogicalPlan]
  with PredicateHelper with ConstraintHelper {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (SQLConf.get.constraintPropagationEnabled) {
      inferFilters(plan)
    } else {
      plan
    }
  }

  private def inferFilters(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, child) =>
      val newFilters = filter.constraints --
        (child.constraints ++ splitConjunctivePredicates(condition))
      if (newFilters.nonEmpty) {
        Filter(And(newFilters.reduce(And), condition), child)
      } else {
        filter
      }

    case join @ Join(left, right, joinType, conditionOpt) =>
      joinType match {
        // For inner join, we can infer additional filters for both sides. LeftSemi is kind of an
        // inner join, it just drops the right side in the final output.
        case _: InnerLike | LeftSemi =>
          val allConstraints = getAllConstraints(left, right, conditionOpt)
          val newLeft = inferNewFilter(left, allConstraints)
          val newRight = inferNewFilter(right, allConstraints)
          join.copy(left = newLeft, right = newRight)

        // For right outer join, we can only infer additional filters for left side.
        case RightOuter =>
          val allConstraints = getAllConstraints(left, right, conditionOpt)
          val newLeft = inferNewFilter(left, allConstraints)
          join.copy(left = newLeft)

        // For left join, we can only infer additional filters for right side.
        case LeftOuter | LeftAnti =>
          val allConstraints = getAllConstraints(left, right, conditionOpt)
          val newRight = inferNewFilter(right, allConstraints)
          join.copy(right = newRight)

        case _ => join
      }
  }

  private def getAllConstraints(
      left: LogicalPlan,
      right: LogicalPlan,
      conditionOpt: Option[Expression]): Set[Expression] = {
    val baseConstraints = left.constraints.union(right.constraints)
      .union(conditionOpt.map(splitConjunctivePredicates).getOrElse(Nil).toSet)
    baseConstraints.union(inferAdditionalConstraints(baseConstraints))
  }

  private def inferNewFilter(plan: LogicalPlan, constraints: Set[Expression]): LogicalPlan = {
    val newPredicates = constraints
      .union(constructIsNotNullConstraints(constraints, plan.output))
      .filter { c =>
        c.references.nonEmpty && c.references.subsetOf(plan.outputSet) && c.deterministic
      } -- plan.constraints
    if (newPredicates.isEmpty) {
      plan
    } else {
      Filter(newPredicates.reduce(And), plan)
    }
  }
}

/**
 * Combines all adjacent [[Union]] operators into a single [[Union]].
 */
object CombineUnions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case u: Union => flattenUnion(u, false)
    case Distinct(u: Union) => Distinct(flattenUnion(u, true))
  }

  private def flattenUnion(union: Union, flattenDistinct: Boolean): Union = {
    val stack = mutable.Stack[LogicalPlan](union)
    val flattened = mutable.ArrayBuffer.empty[LogicalPlan]
    while (stack.nonEmpty) {
      stack.pop() match {
        case Distinct(Union(children)) if flattenDistinct =>
          stack.pushAll(children.reverse)
        case Union(children) =>
          stack.pushAll(children.reverse)
        case child =>
          flattened += child
      }
    }
    Union(flattened)
  }
}

/**
 * Combines two adjacent [[Filter]] operators into one, merging the non-redundant conditions into
 * one conjunctive predicate.
 */
object CombineFilters extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // The query execution/optimization does not guarantee the expressions are evaluated in order.
    // We only can combine them if and only if both are deterministic.
    case Filter(fc, nf @ Filter(nc, grandChild)) if fc.deterministic && nc.deterministic =>
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
 * Removes redundant Sort operation. This can happen:
 * 1) if the Sort operator is a local sort and the child is already sorted
 * 2) if there is another Sort operator separated by 0...n Project/Filter operators
 */
object RemoveRedundantSorts extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform applyLocally

  private val applyLocally: PartialFunction[LogicalPlan, LogicalPlan] = {
    case Sort(orders, false, child) if SortOrder.orderingSatisfies(child.outputOrdering, orders) =>
      applyLocally.lift(child).getOrElse(child)
    case s @ Sort(_, _, child) => s.copy(child = recursiveRemoveSort(child))
  }

  def recursiveRemoveSort(plan: LogicalPlan): LogicalPlan = plan match {
    case Sort(_, _, child) => recursiveRemoveSort(child)
    case other if canEliminateSort(other) =>
      other.withNewChildren(other.children.map(recursiveRemoveSort))
    case _ => plan
  }

  def canEliminateSort(plan: LogicalPlan): Boolean = plan match {
    case p: Project => p.projectList.forall(_.deterministic)
    case f: Filter => f.condition.deterministic
    case _: ResolvedHint => true
    case _ => false
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
    case Filter(Literal(null, _), child) =>
      LocalRelation(child.output, data = Seq.empty, isStreaming = plan.isStreaming)
    case Filter(Literal(false, BooleanType), child) =>
      LocalRelation(child.output, data = Seq.empty, isStreaming = plan.isStreaming)
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
    // This also applies to Aggregate.
    case Filter(condition, project @ Project(fields, grandChild))
      if fields.forall(_.deterministic) && canPushThroughCondition(grandChild, condition) =>

      // Create a map of Aliases to their values from the child projection.
      // e.g., 'SELECT a + b AS c, d ...' produces Map(c -> a + b).
      val aliasMap = AttributeMap(fields.collect {
        case a: Alias => (a.toAttribute, a.child)
      })

      project.copy(child = Filter(replaceAlias(condition, aliasMap), grandChild))

    case filter @ Filter(condition, aggregate: Aggregate)
      if aggregate.aggregateExpressions.forall(_.deterministic)
        && aggregate.groupingExpressions.nonEmpty =>
      // Find all the aliased expressions in the aggregate list that don't include any actual
      // AggregateExpression, and create a map from the alias to the expression
      val aliasMap = AttributeMap(aggregate.aggregateExpressions.collect {
        case a: Alias if a.child.find(_.isInstanceOf[AggregateExpression]).isEmpty =>
          (a.toAttribute, a.child)
      })

      // For each filter, expand the alias and check if the filter can be evaluated using
      // attributes produced by the aggregate operator's child operator.
      val (candidates, nonDeterministic) =
        splitConjunctivePredicates(condition).partition(_.deterministic)

      val (pushDown, rest) = candidates.partition { cond =>
        val replaced = replaceAlias(cond, aliasMap)
        cond.references.nonEmpty && replaced.references.subsetOf(aggregate.child.outputSet)
      }

      val stayUp = rest ++ nonDeterministic

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

    // Push [[Filter]] operators through [[Window]] operators. Parts of the predicate that can be
    // pushed beneath must satisfy the following conditions:
    // 1. All the expressions are part of window partitioning key. The expressions can be compound.
    // 2. Deterministic.
    // 3. Placed before any non-deterministic predicates.
    case filter @ Filter(condition, w: Window)
      if w.partitionSpec.forall(_.isInstanceOf[AttributeReference]) =>
      val partitionAttrs = AttributeSet(w.partitionSpec.flatMap(_.references))

      val (candidates, nonDeterministic) =
        splitConjunctivePredicates(condition).partition(_.deterministic)

      val (pushDown, rest) = candidates.partition { cond =>
        cond.references.subsetOf(partitionAttrs)
      }

      val stayUp = rest ++ nonDeterministic

      if (pushDown.nonEmpty) {
        val pushDownPredicate = pushDown.reduce(And)
        val newWindow = w.copy(child = Filter(pushDownPredicate, w.child))
        if (stayUp.isEmpty) newWindow else Filter(stayUp.reduce(And), newWindow)
      } else {
        filter
      }

    case filter @ Filter(condition, union: Union) =>
      // Union could change the rows, so non-deterministic predicate can't be pushed down
      val (pushDown, stayUp) = splitConjunctivePredicates(condition).partition(_.deterministic)

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

    case filter @ Filter(condition, watermark: EventTimeWatermark) =>
      val (pushDown, stayUp) = splitConjunctivePredicates(condition).partition { p =>
        p.deterministic && !p.references.contains(watermark.eventTime)
      }

      if (pushDown.nonEmpty) {
        val pushDownPredicate = pushDown.reduceLeft(And)
        val newWatermark = watermark.copy(child = Filter(pushDownPredicate, watermark.child))
        // If there is no more filter to stay up, just eliminate the filter.
        // Otherwise, create "Filter(stayUp) <- watermark <- Filter(pushDownPredicate)".
        if (stayUp.isEmpty) newWatermark else Filter(stayUp.reduceLeft(And), newWatermark)
      } else {
        filter
      }

    case filter @ Filter(_, u: UnaryNode)
        if canPushThrough(u) && u.expressions.forall(_.deterministic) =>
      pushDownPredicate(filter, u.child) { predicate =>
        u.withNewChildren(Seq(Filter(predicate, u.child)))
      }
  }

  private def canPushThrough(p: UnaryNode): Boolean = p match {
    // Note that some operators (e.g. project, aggregate, union) are being handled separately
    // (earlier in this rule).
    case _: AppendColumns => true
    case _: ResolvedHint => true
    case _: Distinct => true
    case _: Generate => true
    case _: Pivot => true
    case _: RepartitionByExpression => true
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
    val (candidates, nonDeterministic) =
      splitConjunctivePredicates(filter.condition).partition(_.deterministic)

    val (pushDown, rest) = candidates.partition { cond =>
      cond.references.subsetOf(grandchild.outputSet)
    }

    val stayUp = rest ++ nonDeterministic

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

  /**
   * Check if we can safely push a filter through a projection, by making sure that predicate
   * subqueries in the condition do not contain the same attributes as the plan they are moved
   * into. This can happen when the plan and predicate subquery have the same source.
   */
  private def canPushThroughCondition(plan: LogicalPlan, condition: Expression): Boolean = {
    val attributes = plan.outputSet
    val matched = condition.find {
      case s: SubqueryExpression => s.plan.outputSet.intersect(attributes).nonEmpty
      case _ => false
    }
    matched.isEmpty
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
   * non-deterministic (i.e., stateful) condition expressions in canEvaluateInLeft or
   * canEvaluateInRight to prevent pushing these predicates on either side of the join.
   *
   * @return (canEvaluateInLeft, canEvaluateInRight, haveToEvaluateInBoth)
   */
  private def split(condition: Seq[Expression], left: LogicalPlan, right: LogicalPlan) = {
    val (pushDownCandidates, nonDeterministic) = condition.partition(_.deterministic)
    val (leftEvaluateCondition, rest) =
      pushDownCandidates.partition(_.references.subsetOf(left.outputSet))
    val (rightEvaluateCondition, commonCondition) =
        rest.partition(expr => expr.references.subsetOf(right.outputSet))

    (leftEvaluateCondition, rightEvaluateCondition, commonCondition ++ nonDeterministic)
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
            commonFilterCondition.partition(canEvaluateWithinJoin)
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
        case _: InnerLike | LeftSemi =>
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
        case LeftOuter | LeftAnti | ExistenceJoin(_) =>
          // push down the right side only join filter for right sub query
          val newLeft = left
          val newRight = rightJoinConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = (leftJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, joinType, newJoinCond)
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
 *
 * This rule must be run AFTER the batch "LocalRelation", since a join with empty relation should
 * not be a cartesian product.
 */
object CheckCartesianProducts extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * Check if a join is a cartesian product. Returns true if
   * there are no join conditions involving references from both left and right.
   */
  def isCartesianProduct(join: Join): Boolean = {
    val conditions = join.condition.map(splitConjunctivePredicates).getOrElse(Nil)

    conditions match {
      case Seq(Literal.FalseLiteral) | Seq(Literal(null, BooleanType)) => false
      case _ => !conditions.map(_.references).exists(refs =>
        refs.exists(join.left.outputSet.contains) && refs.exists(join.right.outputSet.contains))
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan =
    if (SQLConf.get.crossJoinEnabled) {
      plan
    } else plan transform {
      case j @ Join(left, right, Inner | LeftOuter | RightOuter | FullOuter, _)
        if isCartesianProduct(j) =>
          throw new AnalysisException(
            s"""Detected implicit cartesian product for ${j.joinType.sql} join between logical plans
               |${left.treeString(false).trim}
               |and
               |${right.treeString(false).trim}
               |Join condition is missing or trivial.
               |Either: use the CROSS JOIN syntax to allow cartesian products between these
               |relations, or: enable implicit cartesian products by setting the configuration
               |variable spark.sql.crossJoin.enabled=true"""
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
            DecimalType(prec + 4, scale + 4), Option(SQLConf.get.sessionLocalTimeZone))

        case _ => we
      }
      case ae @ AggregateExpression(af, _, _, _) => af match {
        case Sum(e @ DecimalType.Expression(prec, scale)) if prec + 10 <= MAX_LONG_DIGITS =>
          MakeDecimal(ae.copy(aggregateFunction = Sum(UnscaledValue(e))), prec + 10, scale)

        case Average(e @ DecimalType.Expression(prec, scale)) if prec + 4 <= MAX_DOUBLE_DIGITS =>
          val newAggExpr = ae.copy(aggregateFunction = Average(UnscaledValue(e)))
          Cast(
            Divide(newAggExpr, Literal.create(math.pow(10.0, scale), DoubleType)),
            DecimalType(prec + 4, scale + 4), Option(SQLConf.get.sessionLocalTimeZone))

        case _ => ae
      }
    }
  }
}

/**
 * Converts local operations (i.e. ones that don't require data exchange) on LocalRelation to
 * another LocalRelation.
 *
 * This is relatively simple as it currently handles only 2 single case: Project and Limit.
 */
object ConvertToLocalRelation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Project(projectList, LocalRelation(output, data, isStreaming))
        if !projectList.exists(hasUnevaluableExpr) =>
      val projection = new InterpretedMutableProjection(projectList, output)
      projection.initialize(0)
      LocalRelation(projectList.map(_.toAttribute), data.map(projection(_).copy()), isStreaming)

    case Limit(IntegerLiteral(limit), LocalRelation(output, data, isStreaming)) =>
      LocalRelation(output, data.take(limit), isStreaming)

    case Filter(condition, LocalRelation(output, data, isStreaming))
        if !hasUnevaluableExpr(condition) =>
      val predicate = InterpretedPredicate.create(condition, output)
      predicate.initialize(0)
      LocalRelation(output, data.filter(row => predicate.eval(row)), isStreaming)
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
 * Replaces logical [[Deduplicate]] operator with an [[Aggregate]] operator.
 */
object ReplaceDeduplicateWithAggregate extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Deduplicate(keys, child) if !child.isStreaming =>
      val keyExprIds = keys.map(_.exprId)
      val aggCols = child.output.map { attr =>
        if (keyExprIds.contains(attr.exprId)) {
          attr
        } else {
          Alias(new First(attr).toAggregateExpression(), attr.name)(attr.exprId)
        }
      }
      // SPARK-22951: Physical aggregate operators distinguishes global aggregation and grouping
      // aggregations by checking the number of grouping keys. The key difference here is that a
      // global aggregation always returns at least one row even if there are no input rows. Here
      // we append a literal when the grouping key list is empty so that the result aggregate
      // operator is properly treated as a grouping aggregation.
      val nonemptyKeys = if (keys.isEmpty) Literal(1) :: Nil else keys
      Aggregate(nonemptyKeys, aggCols, child)
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
    case Intersect(left, right, false) =>
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
    case Except(left, right, false) =>
      assert(left.output.size == right.output.size)
      val joinCond = left.output.zip(right.output).map { case (l, r) => EqualNullSafe(l, r) }
      Distinct(Join(left, right, LeftAnti, joinCond.reduceLeftOption(And)))
  }
}

/**
 * Replaces logical [[Except]] operator using a combination of Union, Aggregate
 * and Generate operator.
 *
 * Input Query :
 * {{{
 *    SELECT c1 FROM ut1 EXCEPT ALL SELECT c1 FROM ut2
 * }}}
 *
 * Rewritten Query:
 * {{{
 *   SELECT c1
 *   FROM (
 *     SELECT replicate_rows(sum_val, c1)
 *       FROM (
 *         SELECT c1, sum_val
 *           FROM (
 *             SELECT c1, sum(vcol) AS sum_val
 *               FROM (
 *                 SELECT 1L as vcol, c1 FROM ut1
 *                 UNION ALL
 *                 SELECT -1L as vcol, c1 FROM ut2
 *              ) AS union_all
 *            GROUP BY union_all.c1
 *          )
 *        WHERE sum_val > 0
 *       )
 *   )
 * }}}
 */

object RewriteExceptAll extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Except(left, right, true) =>
      assert(left.output.size == right.output.size)

      val newColumnLeft = Alias(Literal(1L), "vcol")()
      val newColumnRight = Alias(Literal(-1L), "vcol")()
      val modifiedLeftPlan = Project(Seq(newColumnLeft) ++ left.output, left)
      val modifiedRightPlan = Project(Seq(newColumnRight) ++ right.output, right)
      val unionPlan = Union(modifiedLeftPlan, modifiedRightPlan)
      val aggSumCol =
        Alias(AggregateExpression(Sum(unionPlan.output.head.toAttribute), Complete, false), "sum")()
      val aggOutputColumns = left.output ++ Seq(aggSumCol)
      val aggregatePlan = Aggregate(left.output, aggOutputColumns, unionPlan)
      val filteredAggPlan = Filter(GreaterThan(aggSumCol.toAttribute, Literal(0L)), aggregatePlan)
      val genRowPlan = Generate(
        ReplicateRows(Seq(aggSumCol.toAttribute) ++ left.output),
        unrequiredChildIndex = Nil,
        outer = false,
        qualifier = None,
        left.output,
        filteredAggPlan
      )
      Project(left.output, genRowPlan)
  }
}

/**
 * Replaces logical [[Intersect]] operator using a combination of Union, Aggregate
 * and Generate operator.
 *
 * Input Query :
 * {{{
 *    SELECT c1 FROM ut1 INTERSECT ALL SELECT c1 FROM ut2
 * }}}
 *
 * Rewritten Query:
 * {{{
 *   SELECT c1
 *   FROM (
 *        SELECT replicate_row(min_count, c1)
 *        FROM (
 *             SELECT c1, If (vcol1_cnt > vcol2_cnt, vcol2_cnt, vcol1_cnt) AS min_count
 *             FROM (
 *                  SELECT   c1, count(vcol1) as vcol1_cnt, count(vcol2) as vcol2_cnt
 *                  FROM (
 *                       SELECT true as vcol1, null as , c1 FROM ut1
 *                       UNION ALL
 *                       SELECT null as vcol1, true as vcol2, c1 FROM ut2
 *                       ) AS union_all
 *                  GROUP BY c1
 *                  HAVING vcol1_cnt >= 1 AND vcol2_cnt >= 1
 *                  )
 *             )
 *         )
 * }}}
 */
object RewriteIntersectAll extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Intersect(left, right, true) =>
      assert(left.output.size == right.output.size)

      val trueVcol1 = Alias(Literal(true), "vcol1")()
      val nullVcol1 = Alias(Literal(null, BooleanType), "vcol1")()

      val trueVcol2 = Alias(Literal(true), "vcol2")()
      val nullVcol2 = Alias(Literal(null, BooleanType), "vcol2")()

      // Add a projection on the top of left and right plans to project out
      // the additional virtual columns.
      val leftPlanWithAddedVirtualCols = Project(Seq(trueVcol1, nullVcol2) ++ left.output, left)
      val rightPlanWithAddedVirtualCols = Project(Seq(nullVcol1, trueVcol2) ++ right.output, right)

      val unionPlan = Union(leftPlanWithAddedVirtualCols, rightPlanWithAddedVirtualCols)

      // Expressions to compute count and minimum of both the counts.
      val vCol1AggrExpr =
        Alias(AggregateExpression(Count(unionPlan.output(0)), Complete, false), "vcol1_count")()
      val vCol2AggrExpr =
        Alias(AggregateExpression(Count(unionPlan.output(1)), Complete, false), "vcol2_count")()
      val ifExpression = Alias(If(
        GreaterThan(vCol1AggrExpr.toAttribute, vCol2AggrExpr.toAttribute),
        vCol2AggrExpr.toAttribute,
        vCol1AggrExpr.toAttribute
      ), "min_count")()

      val aggregatePlan = Aggregate(left.output,
        Seq(vCol1AggrExpr, vCol2AggrExpr) ++ left.output, unionPlan)
      val filterPlan = Filter(And(GreaterThanOrEqual(vCol1AggrExpr.toAttribute, Literal(1L)),
        GreaterThanOrEqual(vCol2AggrExpr.toAttribute, Literal(1L))), aggregatePlan)
      val projectMinPlan = Project(left.output ++ Seq(ifExpression), filterPlan)

      // Apply the replicator to replicate rows based on min_count
      val genRowPlan = Generate(
        ReplicateRows(Seq(ifExpression.toAttribute) ++ left.output),
        unrequiredChildIndex = Nil,
        outer = false,
        qualifier = None,
        left.output,
        projectMinPlan
      )
      Project(left.output, genRowPlan)
  }
}

/**
 * Removes literals from group expressions in [[Aggregate]], as they have no effect to the result
 * but only makes the grouping key bigger.
 */
object RemoveLiteralFromGroupExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case a @ Aggregate(grouping, _, _) if grouping.nonEmpty =>
      val newGrouping = grouping.filter(!_.foldable)
      if (newGrouping.nonEmpty) {
        a.copy(groupingExpressions = newGrouping)
      } else {
        // All grouping expressions are literals. We should not drop them all, because this can
        // change the return semantics when the input of the Aggregate is empty (SPARK-17114). We
        // instead replace this by single, easy to hash/sort, literal expression.
        a.copy(groupingExpressions = Seq(Literal(0, IntegerType)))
      }
  }
}

/**
 * Removes repetition from group expressions in [[Aggregate]], as they have no effect to the result
 * but only makes the grouping key bigger.
 */
object RemoveRepetitionFromGroupExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case a @ Aggregate(grouping, _, _) if grouping.size > 1 =>
      val newGrouping = ExpressionSet(grouping).toSeq
      if (newGrouping.size == grouping.size) {
        a
      } else {
        a.copy(groupingExpressions = newGrouping)
      }
  }
}

/**
 * Updates nullability in [[AttributeReference]]s if nullability is different between
 * non-leaf plan's expressions and the children output.
 */
object UpdateNullabilityInAttributeReferences extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case p if !p.isInstanceOf[LeafNode] =>
      val nullabilityMap = AttributeMap(p.children.flatMap(_.output).map { x => x -> x.nullable })
      p transformExpressions {
        case ar: AttributeReference if nullabilityMap.contains(ar) =>
          ar.withNullability(nullabilityMap(ar))
      }
  }
}
