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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, PlanHelper, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{COMMON_EXPR_REF, WITH_EXPRESSION}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * Rewrites the `With` expressions by adding a `Project` to pre-evaluate the common expressions, or
 * just inline them if they are cheap.
 *
 * Since this rule can introduce new `Project` operators, it is advised to run [[CollapseProject]]
 * after this rule.
 *
 * Note: For now we only use `With` in a few `RuntimeReplaceable` expressions. If we expand its
 *       usage, we should support aggregate/window functions as well.
 */
object RewriteWithExpression extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformUpWithSubqueriesAndPruning(_.containsPattern(WITH_EXPRESSION)) {
      // For aggregates, separate the computation of the aggregations themselves from the final
      // result by moving the final result computation into a projection above it. This prevents
      // this rule from producing an invalid Aggregate operator.
      case p @ PhysicalAggregation(
          groupingExpressions, aggregateExpressions, resultExpressions, child)
          if p.expressions.exists(_.containsPattern(WITH_EXPRESSION)) =>
        // PhysicalAggregation returns aggregateExpressions as attribute references, which we change
        // to aliases so that they can be referred to by resultExpressions.
        val aggExprs = aggregateExpressions.map(
          ae => Alias(ae, "_aggregateexpression")(ae.resultId))
        val aggExprIds = aggExprs.map(_.exprId).toSet
        val resExprs = resultExpressions.map(_.transform {
          case a: AttributeReference if aggExprIds.contains(a.exprId) =>
            a.withName("_aggregateexpression")
        }.asInstanceOf[NamedExpression])
        // Rewrite the projection and the aggregate separately and then piece them together.
        val agg = Aggregate(groupingExpressions, groupingExpressions ++ aggExprs, child)
        val rewrittenAgg = applyInternal(agg)
        val proj = Project(resExprs, rewrittenAgg)
        val rewrittenProj = applyInternal(proj)
        // A `With` in a conditional branch is left in the plan, so the guard above stays true for
        // it on every iteration of this fixed-point batch, and restructuring unconditionally would
        // add one `Project` per iteration. Hand back the original operator when neither rewrite
        // changed anything: `mapExpressions` and `withNewChildren` preserve reference equality when
        // they rewrite nothing, which is what makes this detectable.
        if ((rewrittenAgg eq agg) && (rewrittenProj eq proj)) p else rewrittenProj
      case p if p.expressions.exists(_.containsPattern(WITH_EXPRESSION)) =>
        applyInternal(p)
    }
  }

  private def applyInternal(p: LogicalPlan): LogicalPlan = {
    val inputPlans = p.children
    val commonExprIdSet = p.expressions
      .flatMap(_.collect { case r: CommonExpressionRef => r.id })
      .groupBy(identity)
      .transform((_, v) => v.size)
      .filter(_._2 > 1)
      .keySet
    val commonExprsPerChild = Array.fill(inputPlans.length)(mutable.ListBuffer.empty[(Alias, Long)])
    var newPlan: LogicalPlan = p.mapExpressions { expr =>
      rewriteWithExprAndInputPlans(expr, inputPlans, commonExprsPerChild, commonExprIdSet)
    }
    val newChildren = inputPlans.zip(commonExprsPerChild).map { case (inputPlan, commonExprs) =>
      if (commonExprs.isEmpty) {
        inputPlan
      } else {
        Project(inputPlan.output ++ commonExprs.map(_._1), inputPlan)
      }
    }
    newPlan = newPlan.withNewChildren(newChildren)
    // Since we add extra Projects with extra columns to pre-evaluate the common expressions,
    // the current operator may have extra columns if it inherits the output columns from its
    // child, and we need to project away the extra columns to keep the plan schema unchanged.
    assert(p.output.length <= newPlan.output.length)
    if (p.output.length < newPlan.output.length) {
      assert(p.outputSet.subsetOf(newPlan.outputSet))
      Project(p.output, newPlan)
    } else {
      newPlan
    }
  }

  /**
   * Whether substituting this definition into its references is as good as evaluating it once.
   * True when it is referenced once, since substituting then evaluates it once either way, or when
   * it is cheap enough to evaluate repeatedly and deterministic, so the repeated evaluations agree.
   *
   * `CollapseProject.isCheap` answers what one evaluation costs, not whether a second one is
   * allowed: it admits a `PythonUDF`, which may be nondeterministic. Determinism is checked here
   * rather than read into the cost test, so that a reader of either call site can see which of the
   * two questions is being asked.
   */
  private def canSubstitute(
      child: Expression,
      id: CommonExpressionId,
      commonExprIdSet: Set[CommonExpressionId]): Boolean = {
    !commonExprIdSet.contains(id) || (CollapseProject.isCheap(child) && child.deterministic)
  }

  /**
   * `w` with every definition that gains nothing from being memoized inlined into its references:
   * one cheap enough to evaluate twice, and one that is referenced once anyway. This is the test
   * the main rewrite already applies before it hoists a definition into a project.
   *
   * Inlining matters beyond the per-row bookkeeping it saves. A `With` is not foldable, so it hides
   * whatever it wraps from `ConstantFolding`, `PushFoldableIntoBranches`, `SimplifyConditionals`
   * and `ReplaceNullWithFalseInPredicate`, all of which run in later batches. Dropping the `With`
   * once nothing is left to memoize keeps `CASE WHEN c THEN nullif(1, 1) END` folding as it did
   * before this rule learned to leave one behind.
   */
  private def inlineDefsThatGainNothing(
      w: With,
      commonExprIdSet: Set[CommonExpressionId]): Expression = {
    val (toInline, toKeep) = w.defs.partition { d =>
      canSubstitute(d.child, d.id, commonExprIdSet)
    }
    if (toInline.isEmpty) {
      w
    } else {
      val refToExpr = toInline.map(d => d.id -> d.child).toMap
      val newChild = w.child.transformWithPruning(_.containsPattern(COMMON_EXPR_REF)) {
        // A ref of a definition kept here, or of an enclosing `With`, is left for its owner.
        case ref: CommonExpressionRef if refToExpr.contains(ref.id) => refToExpr(ref.id)
      }
      // `copy` rather than `withNewChildren`, which requires the child count to be unchanged. The
      // references of the kept definitions are carried over unbound; the new `With` binds them, and
      // `w` is discarded here, so no two `With`s hold the same reference.
      if (toKeep.isEmpty) newChild else w.copy(child = newChild, defs = toKeep)
    }
  }

  private def rewriteWithExprAndInputPlans(
      e: Expression,
      inputPlans: Seq[LogicalPlan],
      commonExprsPerChild: Array[mutable.ListBuffer[(Alias, Long)]],
      commonExprIdSet: Set[CommonExpressionId],
      isNestedWith: Boolean = false): Expression = {
    if (!e.containsPattern(WITH_EXPRESSION)) return e
    e match {
      // Do not handle nested With in one pass. Leave it to the next rule executor batch.
      case w: With if !isNestedWith =>
        // Rewrite nested With expressions first
        val child = rewriteWithExprAndInputPlans(
          w.child, inputPlans, commonExprsPerChild, commonExprIdSet, isNestedWith = true)
        val defs = w.defs.map(rewriteWithExprAndInputPlans(
          _, inputPlans, commonExprsPerChild, commonExprIdSet, isNestedWith = true))
        val refToExpr = mutable.HashMap.empty[CommonExpressionId, Expression]

        defs.zipWithIndex.foreach { case (CommonExpressionDef(child, id), index) =>
          if (id.canonicalized) {
            throw SparkException.internalError(
              "Cannot rewrite canonicalized Common expression definitions")
          }

          if (canSubstitute(child, id, commonExprIdSet)) {
            refToExpr(id) = child
          } else {
            val childPlanIndex = inputPlans.indexWhere(
              c => child.references.subsetOf(c.outputSet)
            )
            if (childPlanIndex == -1) {
              // When we cannot rewrite the common expressions, force to inline them so that the
              // query can still run. This can happen if the join condition contains `With` and
              // the common expression references columns from both join sides.
              // TODO: things can go wrong if the common expression is nondeterministic. We
              //       don't fix it for now to match the old buggy behavior when certain
              //       `RuntimeReplaceable` did not use the `With` expression.
              // TODO: we should calculate the ref count and also inline the common expression
              //       if it's ref count is 1.
              refToExpr(id) = child
            } else {
              val commonExprs = commonExprsPerChild(childPlanIndex)
              val existingCommonExpr = commonExprs.find(_._2 == id.id)
              if (existingCommonExpr.isDefined) {
                if (Utils.isTesting) {
                  assert(existingCommonExpr.get._1.child.semanticEquals(child))
                }
                refToExpr(id) = existingCommonExpr.get._1.toAttribute
              } else {
                val aliasName = if (SQLConf.get.getConf(SQLConf.USE_COMMON_EXPR_ID_FOR_ALIAS)) {
                  s"_common_expr_${id.id}"
                } else {
                  s"_common_expr_$index"
                }
                val alias = Alias(child, aliasName)()
                val fakeProj = Project(Seq(alias), inputPlans(childPlanIndex))
                if (PlanHelper.specialExpressionsInUnsupportedOperator(fakeProj).nonEmpty) {
                  // We have to inline the common expression if it cannot be put in a Project.
                  refToExpr(id) = child
                } else {
                  commonExprs.append((alias, id.id))
                  refToExpr(id) = alias.toAttribute
                }
              }
            }
          }
        }

        child.transformWithPruning(_.containsPattern(COMMON_EXPR_REF)) {
          // `child` may contain nested With and we only replace `CommonExpressionRef` that
          // references common expressions in the current `With`.
          case ref: CommonExpressionRef if refToExpr.contains(ref.id) =>
            if (ref.id.canonicalized) {
              throw SparkException.internalError(
                "Cannot rewrite canonicalized Common expression references")
            }
            refToExpr(ref.id)
        }

      case c: ConditionalExpression =>
        val newAlwaysEvaluatedInputs = c.alwaysEvaluatedInputs.map(
          rewriteWithExprAndInputPlans(
            _, inputPlans, commonExprsPerChild, commonExprIdSet, isNestedWith))
        val newExpr = c.withNewAlwaysEvaluatedInputs(newAlwaysEvaluatedInputs)
        // A `With` in a conditional branch cannot go into a project, which is always evaluated
        // while the branch may not be. It stays where it is and memoizes its definition per row
        // instead, but only the definitions that gain something from it. Use transformUp to handle
        // nested With.
        newExpr.transformUpWithPruning(_.containsPattern(WITH_EXPRESSION)) {
          case w: With => inlineDefsThatGainNothing(w, commonExprIdSet)
        }

      case other => other.mapChildren(
        rewriteWithExprAndInputPlans(
          _, inputPlans, commonExprsPerChild, commonExprIdSet, isNestedWith)
      )
    }
  }
}
