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
import scala.util.Try

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
        applyInternal(proj)
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
   * Whether a branch condition can be restated as a guard on the pre-evaluated column. The guard is
   * evaluated a second time, in the project, so the condition must give the same answer there:
   * it has to be deterministic, and it cannot carry a `With` or a `CommonExpressionRef`, neither of
   * which survives outside the expression it belongs to.
   */
  private def canGuardOn(cond: Expression): Boolean = {
    cond.deterministic &&
      !cond.containsAnyPattern(WITH_EXPRESSION, COMMON_EXPR_REF)
  }

  /** The predicate that holds exactly for the rows where `cond` evaluates to true. */
  private def isTrue(cond: Expression): Expression = {
    if (cond.nullable) EqualNullSafe(cond, Literal.TrueLiteral) else cond
  }

  /** The predicate that holds exactly for the rows where `cond` evaluates to false or null. */
  private def isNotTrue(cond: Expression): Expression = {
    if (cond.nullable) Not(EqualNullSafe(cond, Literal.TrueLiteral)) else Not(cond)
  }

  /**
   * The predicate that holds exactly for the rows where every one of `preds` holds. An empty
   * `preds` means the branch is always evaluated, so the guard is trivially true.
   */
  private def conjunction(preds: Seq[Expression]): Expression = {
    preds.reduceOption(And(_, _)).getOrElse(Literal.TrueLiteral)
  }

  /**
   * Whether pre-evaluating this common expression in a conditional branch is safe, where inlining
   * is otherwise preferred because the branch may not be evaluated at all.
   *
   * The `guard` in [[preEvaluateInChildProject]] confines the evaluation to the rows that reach the
   * branch, but not to the rows that reach this particular reference: a reference behind a
   * short-circuiting operator, say the second one in `a > 0 AND rand() BETWEEN 0.4 AND 0.6`, is not
   * read on every row of the branch. So pre-evaluating is safe only when the expression cannot
   * raise, or it would turn a wrong result into a spurious error. That rules out more than it may
   * seem: `randstr(-1, 0)` raises on its constant length and `reflect(...)` raises on constant
   * arguments, so neither a nondeterministic root nor foldable children are enough. The generators
   * below produce a value from a seed or from the task context, evaluate no argument beyond a
   * foldable seed, and so have nothing to raise on. Any other nondeterministic expression, say
   * `rand() / col`, keeps the existing inlining and its existing wrong result.
   *
   * Only nondeterministic expressions are worth pre-evaluating here at all: inlining a
   * deterministic one repeats the work but returns the same value, so the branch keeps deciding
   * whether it runs.
   */
  private def canPreEvaluateInBranch(child: Expression): Boolean = {
    val cannotRaise = child match {
      // `rand`/`randn`: the seed is their only child and the analyzer rejects a non-foldable one
      // with `SEED_EXPRESSION_IS_UNFOLDABLE`, so there is no row data left to raise on.
      case _: NondeterministicUnaryRDG => true
      // `uuid`, `monotonically_increasing_id`, `spark_partition_id`, `input_file_name` and the
      // input-file-block pair: no children to evaluate at all.
      case _: LeafExpression => true
      case _ => false
    }
    !child.deterministic && cannotRaise
  }

  /**
   * Adds `child`, guarded by `guard`, as a pre-evaluated column to the input plan that supplies the
   * references of both, and returns the attribute referring to it. The guard keeps `child` from
   * being evaluated on the rows that do not reach its branch, which for a stateful generator such
   * as `rand()` is what keeps the pre-evaluation from advancing the generator on those rows. The
   * column holds a default value there; no reference reads it, since a reference is only reached
   * when the guard holds.
   *
   * Returns `None` when the guarded expression cannot be placed in a project, so that the caller
   * inlines it instead, matching the main `With` rewrite. Also returns `None` when this definition
   * already has a pre-evaluated column under a different guard, since that column is not computed
   * on the rows this branch is reached on.
   */
  private def preEvaluateInChildProject(
      child: Expression,
      guard: Expression,
      id: CommonExpressionId,
      index: Int,
      inputPlans: Seq[LogicalPlan],
      commonExprsPerChild: Array[mutable.ListBuffer[(Alias, Long)]]): Option[Expression] = {
    // `Literal.default` has no value for a few exotic types; inline those.
    val guarded = guard match {
      case Literal.TrueLiteral => Some(child)
      case _ => Try(If(guard, child, Literal.default(child.dataType))).toOption
    }
    guarded.flatMap { guarded =>
      val childPlanIndex = inputPlans.indexWhere(c => guarded.references.subsetOf(c.outputSet))
      if (childPlanIndex == -1) {
        None
      } else {
        val commonExprs = commonExprsPerChild(childPlanIndex)
        commonExprs.find(_._2 == id.id) match {
          case Some((existing, _)) =>
            Some(existing.toAttribute).filter(_ => existing.child.semanticEquals(guarded))
          case None =>
            val aliasName = if (SQLConf.get.getConf(SQLConf.USE_COMMON_EXPR_ID_FOR_ALIAS)) {
              s"_common_expr_${id.id}"
            } else {
              s"_common_expr_$index"
            }
            val alias = Alias(guarded, aliasName)()
            val fakeProj = Project(Seq(alias), inputPlans(childPlanIndex))
            if (PlanHelper.specialExpressionsInUnsupportedOperator(fakeProj).nonEmpty) {
              None
            } else {
              commonExprs.append((alias, id.id))
              Some(alias.toAttribute)
            }
        }
      }
    }
  }

  /**
   * Rewrites the `With` expressions of one conditional branch, which `guard` holds for. Inlines the
   * common expressions, since the branch may not be evaluated at all and a project always is,
   * except for the nondeterministic definitions that inlining would break; those are pre-evaluated
   * under the guard. See [[canPreEvaluateInBranch]] and [[preEvaluateInChildProject]].
   */
  private def rewriteWithInBranch(
      branch: Expression,
      guard: Option[Expression],
      inputPlans: Seq[LogicalPlan],
      commonExprsPerChild: Array[mutable.ListBuffer[(Alias, Long)]],
      commonExprIdSet: Set[CommonExpressionId]): Expression = {
    // Use transformUp to handle nested With.
    branch.transformUpWithPruning(_.containsPattern(WITH_EXPRESSION)) {
      case With(child, defs) =>
        // Each definition is decided on its own, so a definition that has to stay inline does not
        // drag its siblings along, nor is it dragged along by them.
        val refToExpr = defs.zipWithIndex.map { case (d, index) =>
          val canPreEvaluate =
            commonExprIdSet.contains(d.id) && canPreEvaluateInBranch(d.child)
          val preEvaluated = if (canPreEvaluate) {
            guard.flatMap(
              preEvaluateInChildProject(
                d.child, _, d.id, index, inputPlans, commonExprsPerChild))
          } else {
            None
          }
          d.id -> preEvaluated.getOrElse(d.child)
        }.toMap
        child.transformWithPruning(_.containsPattern(COMMON_EXPR_REF)) {
          case ref: CommonExpressionRef => refToExpr(ref.id)
        }
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

          if (CollapseProject.isCheap(child) || !commonExprIdSet.contains(id)) {
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
        def rewriteBranch(branch: Expression, guard: Option[Expression]): Expression = {
          rewriteWithInBranch(branch, guard, inputPlans, commonExprsPerChild, commonExprIdSet)
        }
        newExpr match {
          case i @ If(pred, trueValue, falseValue) =>
            val (trueGuard, falseGuard) = if (canGuardOn(pred)) {
              (Some(isTrue(pred)), Some(isNotTrue(pred)))
            } else {
              (None, None)
            }
            i.copy(
              trueValue = rewriteBranch(trueValue, trueGuard),
              falseValue = rewriteBranch(falseValue, falseGuard))

          case cw @ CaseWhen(branches, elseValue) =>
            // A condition is only evaluated when no preceding condition held, and a value only when
            // its own condition held on top of that. Accumulate the preceding conditions left to
            // right, dropping the whole chain at the first one that cannot be restated as a guard.
            // The chain is a left-to-right `And` of non-nullable predicates, so it short-circuits
            // exactly where `CaseWhen` stops looking, and a condition that can raise raises on the
            // same rows as before.
            val precedingNotTrue = branches.map(_._1).scanLeft(Option(Seq.empty[Expression])) {
              case (preceding, cond) =>
                preceding.filter(_ => canGuardOn(cond)).map(_ :+ isNotTrue(cond))
            }
            val newBranches = branches.zip(precedingNotTrue).map {
              case ((cond, value), preceding) =>
                val valueGuard = preceding
                  .filter(_ => canGuardOn(cond))
                  .map(p => conjunction(p :+ isTrue(cond)))
                (rewriteBranch(cond, preceding.map(conjunction)), rewriteBranch(value, valueGuard))
            }
            cw.copy(
              branches = newBranches,
              elseValue = elseValue.map(rewriteBranch(_, precedingNotTrue.last.map(conjunction))))

          case co @ Coalesce(children) =>
            // A child is only evaluated when every preceding child returned null.
            val precedingNull = children.scanLeft(Option(Seq.empty[Expression])) {
              case (preceding, child) =>
                preceding.filter(_ => canGuardOn(child)).map(_ :+ IsNull(child))
            }
            co.copy(children = children.zip(precedingNull).map { case (child, preceding) =>
              rewriteBranch(child, preceding.map(conjunction))
            })

          // Any other conditional expression: no guard, so every definition is inlined.
          case other => rewriteBranch(other, None)
        }

      case other => other.mapChildren(
        rewriteWithExprAndInputPlans(
          _, inputPlans, commonExprsPerChild, commonExprIdSet, isNestedWith)
      )
    }
  }
}
