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
import org.apache.spark.sql.catalyst.trees.TreePattern.{COMMON_EXPR_REF, CURRENT_LIKE, WITH_EXPRESSION}
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

  /**
   * Rewrites the `With` expressions in a single expression tree by inlining their common
   * expressions. Uses `transformUp` to handle nested `With`.
   *
   * Inlining duplicates a definition at every reference, so a definition referenced more than once
   * must be safe to duplicate (see `isSafeToDuplicate`); anything else throws, as there is no plan
   * here to pre-evaluate it into.
   *
   * Does not descend into subquery plans (e.g. `ScalarSubquery`). A caller whose expression
   * may contain a subquery must rewrite those plans separately.
   */
  private[sql] def applyForExpression(expression: Expression): Expression =
    inlineWith(expression, checkDuplication = true)

  // The plan-level rewrite shares this to inline `With` in conditional branches, which may not be
  // evaluated and so can't be pulled into a Project; it passes false to inline unconditionally.
  private def inlineWith(expression: Expression, checkDuplication: Boolean): Expression = {
    // Which definitions are safe to duplicate can only be decided outer-first, so it is collected
    // in a separate pass before inlining bottom-up.
    val safeIds = if (checkDuplication) {
      safeToDuplicateIds(expression)
    } else {
      Set.empty[CommonExpressionId]
    }
    expression.transformUpWithPruning(_.containsPattern(WITH_EXPRESSION)) {
      case With(child, defs) =>
        if (checkDuplication) {
          // Nested `With` is already rewritten, so a ref left in `child` belongs to `defs` or to
          // an enclosing `With`. Only the ids defined here are checked.
          val refCounts = child.collect { case ref: CommonExpressionRef => ref.id }
            .groupBy(identity)
            .transform((_, refs) => refs.size)
          defs.foreach { commonExprDef =>
            // Canonicalization re-numbers ids per `With`, so sibling `With`s reuse ids and break
            // the global uniqueness `safeIds` relies on. Reject them, as the plan-level rewrite
            // does.
            if (commonExprDef.id.canonicalized) {
              throw SparkException.internalError(
                "Cannot inline canonicalized common expression definitions")
            }
            if (refCounts.getOrElse(commonExprDef.id, 0) > 1 &&
              !safeIds.contains(commonExprDef.id)) {
              throw SparkException.internalError(
                "Cannot inline a common expression definition that is referenced more than " +
                  s"once and is not safe to duplicate: ${commonExprDef.child.sql}")
            }
          }
        }
        val refToExpr = defs.map(commonExprDef => commonExprDef.id -> commonExprDef.child).toMap
        // The guard skips refs to an enclosing `With`, which are inlined when `transformUp` reaches
        // it. Without it those refs hit `Map.apply` on a missing key and throw
        // NoSuchElementException.
        child.transformWithPruning(_.containsPattern(COMMON_EXPR_REF)) {
          case ref: CommonExpressionRef if refToExpr.contains(ref.id) => refToExpr(ref.id)
        }
    }
  }

  /**
   * Ids of the definitions that are safe to duplicate. Walks outer-first, so a definition that
   * references an enclosing one is decided after it, and visits a definition's own nested `With`
   * before the definition itself. Ids are globally unique, so one flat set is enough.
   */
  private def safeToDuplicateIds(expression: Expression): Set[CommonExpressionId] = {
    val safeIds = mutable.Set.empty[CommonExpressionId]
    def visit(e: Expression): Unit = if (e.containsPattern(WITH_EXPRESSION)) {
      e match {
        case With(child, defs) =>
          defs.foreach { commonExprDef =>
            visit(commonExprDef.child)
            if (isSafeToDuplicate(commonExprDef.child, safeIds)) {
              safeIds += commonExprDef.id
            }
          }
          visit(child)
        case other => other.children.foreach(visit)
      }
    }
    visit(expression)
    safeIds.toSet
  }

  /**
   * Whether a copy of `e` at every reference always evaluates to the same value. `foldable` is not
   * enough: `current_timestamp()` re-reads the clock, a TIME -> TIMESTAMP cast derives the current
   * date, `aes_encrypt` is a foldable `StaticInvoke` with a random IV, and a Hive UDF is foldable
   * on a user-declared determinism flag.
   */
  private def isSafeToDuplicate(
      e: Expression,
      safeIds: collection.Set[CommonExpressionId]): Boolean = {
    // A whitelist: an unrecognized expression is rejected rather than assumed safe.
    def isLiteralTree(e: Expression): Boolean = e match {
      case _: Literal => true
      // A ref is safe exactly when the definition it points to is.
      case ref: CommonExpressionRef => safeIds.contains(ref.id)
      // Any other leaf varies per row (attribute) or reads a clock.
      case _: LeafExpression => false
      // ComputeCurrentTime stabilizes a TIME -> TIMESTAMP cast into a DateTimeUtils.makeTimestamp*
      // builder, which depends only on its arguments (none read a clock). Accept it here, ahead
      // of the blanket NonSQLExpression rejection below, when every argument is a literal tree.
      case _ if ComputeCurrentTime.isMakeTimestampBuilder(e) =>
        e.children.forall(isLiteralTree)
      // Arbitrary Java methods and UDFs can be foldable without being pure.
      case _: NonSQLExpression | _: UserDefinedExpression => false
      // A TIME -> TIMESTAMP cast has no date of its own; it derives one from the current date at
      // eval time, so duplicated copies could resolve different dates. Reject it. Scope to exactly
      // this source/target pair -- other timestamp-target casts (e.g. CAST('1970-01-01' AS
      // TIMESTAMP)) are ordinary deterministic casts and fall through to the generic check below.
      case c: Cast
          if Cast.isTimeToTimestampNTZ(c.child.dataType, c.dataType) ||
            Cast.isTimeToTimestampLTZ(c.child.dataType, c.dataType) => false
      case _ => e.deterministic && e.children.forall(isLiteralTree)
    }
    // `current_time()` is a non-leaf whose only leaf is a literal, so the tree walk accepts it;
    // the CURRENT_LIKE pattern is what rejects it.
    !e.containsPattern(CURRENT_LIKE) && isLiteralTree(e)
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
        // For With in the conditional branches, they may not be evaluated at all and we can't
        // pull the common expressions into a project which will always be evaluated. Inline it
        // unconditionally. Use transformUp to handle nested With.
        inlineWith(newExpr, checkDuplication = false)

      case other => other.mapChildren(
        rewriteWithExprAndInputPlans(
          _, inputPlans, commonExprsPerChild, commonExprIdSet, isNestedWith)
      )
    }
  }
}
