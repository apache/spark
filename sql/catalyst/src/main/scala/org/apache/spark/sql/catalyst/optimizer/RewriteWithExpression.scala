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
import org.apache.spark.sql.catalyst.trees.TreePattern.{COMMON_EXPR_REF, PLAN_EXPRESSION,
  WITH_EXPRESSION}
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
    // Each entry is the pre-evaluated column, the id of the definition it holds, and the guard it
    // is evaluated under, if any. See `preEvaluateInChildProject`.
    val commonExprsPerChild = Array.fill(inputPlans.length)(
      mutable.ListBuffer.empty[(Alias, Long, Option[Expression])])
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
   * The form of a branch condition that can be repeated as a guard in the child project, if there
   * is one. Every `With` is inlined: a `CommonExpressionRef` means nothing outside the expression
   * it belongs to, and a definition the main rewrite hoisted goes into a column this same pass is
   * still adding, which a `Project` cannot reference from its own project list. Inlining evaluates
   * the definition a second time, which for a deterministic condition costs work but returns the
   * same answer.
   *
   * A reference usually appears more than once, so inlining doubles the condition for every level
   * of nested `With`, and the result of that is what goes into the plan. The guard is given up
   * beyond [[guardSizeLimit]] nodes rather than carrying an expression whose size is exponential in
   * the nesting depth.
   *
   * The result has to be evaluable on every row of the project, not just on the rows that reach
   * the branch, so [[ExprUtils.canEvaluateUnconditionally]] decides whether it may be repeated --
   * the same question the optimizer asks to hoist a join conjunct above its probe, and to lift the
   * base out of a conditional aggregate. A condition it turns down is left alone and its
   * definition pre-evaluated without a guard: a `Project` is not a conditional context, and
   * subexpression elimination hoists a part of the guard repeated across guards out of the
   * enclosing `If` and evaluates it up front, so a guard that can raise raises on rows the branch
   * never reached. A nondeterministic condition is turned down for a different reason: a second
   * evaluation of it would not agree with the one the branch itself makes.
   */
  private def guardForm(cond: Expression): Option[Expression] = {
    val inlined = cond.transformUpWithPruning(_.containsPattern(WITH_EXPRESSION)) {
      case With(child, defs) =>
        val refToExpr = defs.map(d => d.id -> d.child).toMap
        child.transformWithPruning(_.containsPattern(COMMON_EXPR_REF)) {
          case ref: CommonExpressionRef if refToExpr.contains(ref.id) => refToExpr(ref.id)
        }
    }
    // A ref left behind belongs to an enclosing `With` and is unevaluable here, which the check
    // turns down along with everything else it does not know. The size limit is applied first, so
    // that folding measures a bounded tree.
    if (withinSizeLimit(inlined) && ExprUtils.canEvaluateUnconditionally(foldedForm(inlined))) {
      Some(inlined)
    } else {
      None
    }
  }

  /**
   * `e` with every subtree that is constant for the whole query replaced by its value. A comparison
   * written against a literal still carries the cast an implicit coercion put around it -- `a > 0`
   * on an int column arrives here as `a > cast(0 as int)` -- and a cast is not a node
   * [[ExprUtils.canEvaluateUnconditionally]] admits, so asking it about the condition as written
   * would give up the guard on most conditions anyone writes.
   *
   * Evaluating the subtree here is what makes this sound, rather than the later [[ConstantFolding]]
   * batch that will do the same to the plan: a subtree that reads no row data and returned a value
   * here returns that same value on every row, so repeating it in the project cannot raise there.
   * Both halves are needed. `foldable` alone does not give the first, since a node may report
   * itself foldable while ignoring its children -- `typeof(6 / a)` is foldable because it answers
   * from the child's type without dividing anything, and a comparison against it is foldable in
   * turn -- so folding by `foldable` would hide the division from the check while leaving it in the
   * guard that runs. Requiring no references keeps such a subtree in the tree for the check to turn
   * down, and the two conditions the check makes of the whole condition are asked of the subtree as
   * well, so that folding cannot answer them by deleting their subject.
   *
   * A subtree that raises instead of folding -- `cast('x' as int)` under ANSI -- is left as it is,
   * so the check turns the guard down. `Try` catches what `ConstantFolding` catches, and leaves the
   * same node behind. Only the answer is taken from this: the guard that goes into the plan is the
   * condition as written, which the later batch folds along with everything else.
   */
  private def foldedForm(e: Expression): Expression = e.transformUp {
    case f if f.foldable && f.references.isEmpty && f.deterministic &&
        !f.containsPattern(PLAN_EXPRESSION) && !f.isInstanceOf[Literal] =>
      // A stateful expression is nondeterministic, so the copy is only for the symmetry with
      // `ConstantFolding`, which evaluates a foldable subtree the same way.
      Try(Literal.create(f.freshCopyIfContainsStatefulExpression().eval(EmptyRow), f.dataType))
        .getOrElse(f)
  }

  /**
   * The most nodes a condition may contribute to a guard. A condition someone writes is far
   * smaller, but the guard of a later branch accumulates all the preceding conditions, so this ties
   * off around a dozen branches of ordinary conditions -- and it bounds what inlining a deeply
   * nested `With` can produce out of one condition. The guard itself is a few nodes larger, since
   * each condition is wrapped in the predicate that asks whether it held. Exceeding the bound costs
   * the guard, not the pre-evaluation, so it only decides how precisely the generator tracks its
   * branch.
   */
  private val guardSizeLimit = 100

  /**
   * Whether `e` has at most [[guardSizeLimit]] nodes, and stops descending as soon as it does not.
   * Inlining a `With` puts the same subtree under both of its references, so a nested one produces
   * a tree whose size is exponential while the object graph stays small: counting all of it would
   * cost as much as the plan this bound exists to avoid.
   */
  private def withinSizeLimit(e: Expression): Boolean = remainingBudget(e, guardSizeLimit) >= 0

  /**
   * `budget` less the number of nodes in `e`, or any negative number once it is used up. Bounded by
   * `budget` rather than by the size of `e`, so the caller can add to an expression it has already
   * measured without measuring the whole of it again.
   */
  private def remainingBudget(e: Expression, budget: Int): Int = {
    if (budget < 0) return budget
    var left = budget - 1
    val children = e.children.iterator
    while (left >= 0 && children.hasNext) {
      left = remainingBudget(children.next(), left)
    }
    left
  }

  /**
   * The predicate that holds exactly for the rows where `cond` evaluates to true.
   * `EqualNullSafe` is used even for a condition that declares itself non-nullable, so that a null
   * slipping through cannot make both this predicate and [[isNotTrue]] false, which would leave the
   * column holding its default value on a row that reaches the branch.
   * `SimplifyBinaryComparison` drops it again when the condition really is non-nullable.
   */
  private def isTrue(cond: Expression): Expression = EqualNullSafe(cond, Literal.TrueLiteral)

  /** The predicate that holds exactly for the rows where `cond` does not evaluate to true. */
  private def isNotTrue(cond: Expression): Expression = Not(isTrue(cond))

  /**
   * A guard under construction: the predicate for the rows that reach a branch, and what is left of
   * [[guardSizeLimit]] after it. `None` once the limit is used up, since a longer `CaseWhen` only
   * adds to the predicate -- so a branch past that point has no guard, and neither has any branch
   * after it. Growing the predicate this way keeps both the predicate and its measurement bounded,
   * where accumulating the conditions and reducing them per branch is quadratic in the branch
   * count.
   */
  private type PartialGuard = Option[(Expression, Int)]

  private val emptyGuard: PartialGuard = Some((Literal.TrueLiteral, guardSizeLimit))

  /** `guard` with `pred` conjoined, or `None` if that does not fit in the remaining budget. */
  private def andAlso(guard: PartialGuard, pred: Expression): PartialGuard = {
    guard.flatMap { case (acc, budget) =>
      // The `And` node itself costs one, except against the literal that stands for no condition.
      val (combined, cost) = acc match {
        case Literal.TrueLiteral => (pred, 0)
        case _ => (And(acc, pred), 1)
      }
      val left = remainingBudget(pred, budget - cost)
      Some((combined, left)).filter(_ => left >= 0)
    }
  }

  /** The predicate of a guard, dropping the one that stands for a branch that is always reached. */
  private def guardPredicate(guard: PartialGuard): Option[Expression] = {
    guard.map(_._1).filter(_ != Literal.TrueLiteral)
  }

  /**
   * Whether pre-evaluating this common expression in a conditional branch is safe, where inlining
   * is otherwise preferred because the branch may not be evaluated at all.
   *
   * A guard holds for every row that reaches the branch, but not only for the rows that reach this
   * particular reference, and it is not tightened by anything between the branch and the reference:
   * a reference behind a short-circuiting operator, say the second one in
   * `a > 0 AND rand() BETWEEN 0.4 AND 0.6`, or one inside a nested conditional, is read on fewer
   * rows than the guard admits. So pre-evaluating is safe only when the expression cannot raise, or
   * it would turn a wrong result into a spurious error. That rules out more than it may seem:
   * `randstr(-1, 0)` raises on its constant length and `reflect(...)` raises on constant arguments,
   * so neither a nondeterministic root nor foldable children are enough. The generators below
   * produce a value from a seed or from the task context, evaluate no argument beyond a foldable
   * seed, and so have nothing to raise on. Any other nondeterministic expression, say
   * `rand() / col`, keeps the existing inlining and its existing wrong result.
   *
   * Only nondeterministic expressions are worth pre-evaluating here at all: inlining a
   * deterministic one repeats the work but returns the same value, so the branch keeps deciding
   * whether it runs. The expressions are listed one by one rather than matched as a trait, so that
   * every one of them is known to have a `Literal.default` for its type, which
   * [[preEvaluateInChildProject]] uses as the value on the rows the guard excludes.
   */
  private def canPreEvaluateInBranch(child: Expression): Boolean = {
    val cannotRaise = child match {
      // `rand`/`randn`: the seed is their only child and the analyzer rejects a non-foldable one
      // with `SEED_EXPRESSION_IS_UNFOLDABLE`, so there is no row data left to raise on.
      case _: NondeterministicUnaryRDG => true
      // These have no children to evaluate at all.
      case _: Uuid | _: MonotonicallyIncreasingID | _: SparkPartitionID | _: InputFileName |
          _: InputFileBlockStart | _: InputFileBlockLength => true
      case _ => false
    }
    !child.deterministic && cannotRaise
  }

  /**
   * Adds `child` as a pre-evaluated column to the input plan that supplies its references, and
   * returns the attribute referring to it. When `guard` is given and the same input plan can
   * evaluate it, the column is `If(guard, child, default)`: the guard keeps `child` from being
   * evaluated on the rows that do not reach its branch, which for a stateful generator such as
   * `rand()` is what keeps the pre-evaluation from advancing the generator on those rows. The
   * column holds a default value there; no reference reads it, since a reference is only reached
   * when the guard holds.
   *
   * A guard is not always available: the branch's conditions may not be repeatable in a project, or
   * no single input plan may evaluate both them and `child`. The column is then pre-evaluated
   * without one, so the generator is advanced on rows the branch does not reach -- and in a join,
   * is drawn once per row of one side rather than once per joined row. That is the behavior before
   * guards were introduced. Falling back to inlining instead would hand each reference its own
   * value, which is the bug this pre-evaluation exists to fix.
   *
   * Returns `None` when the column cannot be placed in a project at all, so that the caller inlines
   * it, matching the main `With` rewrite.
   */
  private def preEvaluateInChildProject(
      child: Expression,
      guard: Option[Expression],
      id: CommonExpressionId,
      index: Int,
      inputPlans: Seq[LogicalPlan],
      commonExprsPerChild: Array[mutable.ListBuffer[(Alias, Long, Option[Expression])]])
    : Option[Expression] = {
    // Prefer an input plan that can evaluate the guard too, so that the guard survives; `child` is
    // a generator with no references of its own, which every input plan trivially satisfies.
    val childPlanIndex = guard
      .map { g =>
        val refs = child.references ++ g.references
        inputPlans.indexWhere(c => refs.subsetOf(c.outputSet))
      }
      .filter(_ >= 0)
      .getOrElse(inputPlans.indexWhere(c => child.references.subsetOf(c.outputSet)))
    if (childPlanIndex == -1) {
      None
    } else {
      val inputPlan = inputPlans(childPlanIndex)
      val commonExprs = commonExprsPerChild(childPlanIndex)
      val guardToUse = guard.filter(_.references.subsetOf(inputPlan.outputSet))
      // Reuse a column only when it computes the same thing: the same definition under a different
      // guard is not evaluated on the rows this branch is reached on. The main rewrite matches on
      // the guard as well, so that an unconditional reference never reads a guarded column.
      def existing(g: Option[Expression]): Option[Expression] = commonExprs
        .find { case (_, aliasId, aliasGuard) =>
          aliasId == id.id && aliasGuard.map(_.canonicalized) == g.map(_.canonicalized)
        }
        .map(_._1.toAttribute)
      def add(g: Option[Expression]): Option[Expression] = {
        val guarded = g.map(If(_, child, Literal.default(child.dataType))).getOrElse(child)
        // A definition can end up with more than one column, one per distinct guard, so the guarded
        // ones are numbered to keep `useCommonExprIdForAlias` free of duplicate alias names.
        val guardSuffix = if (g.isDefined) s"_${commonExprs.count(_._2 == id.id)}" else ""
        val aliasName = if (SQLConf.get.getConf(SQLConf.USE_COMMON_EXPR_ID_FOR_ALIAS)) {
          s"_common_expr_${id.id}$guardSuffix"
        } else {
          s"_common_expr_$index$guardSuffix"
        }
        val alias = Alias(guarded, aliasName)()
        val fakeProj = Project(Seq(alias), inputPlan)
        if (PlanHelper.specialExpressionsInUnsupportedOperator(fakeProj).nonEmpty) {
          None
        } else {
          commonExprs.append((alias, id.id, g))
          Some(alias.toAttribute)
        }
      }
      existing(guardToUse)
        .orElse(add(guardToUse))
        // Nothing a guard is built out of is rejected by the project today, so this only matters if
        // either list widens later: pre-evaluating without the guard still beats inlining, so drop
        // the guard rather than the whole column.
        .orElse(if (guardToUse.isDefined) existing(None).orElse(add(None)) else None)
    }
  }

  /**
   * Rewrites the `With` expressions of one conditional branch, which `guard` holds for -- `None`
   * when the branch's own conditions cannot be repeated as a guard. Inlines the common expressions,
   * since the branch may not be evaluated at all and a project always is, except for the
   * nondeterministic definitions that inlining would break; those are pre-evaluated, under the
   * guard when there is one. See [[canPreEvaluateInBranch]] and [[preEvaluateInChildProject]].
   */
  private def rewriteWithInBranch(
      branch: Expression,
      guard: Option[Expression],
      inputPlans: Seq[LogicalPlan],
      commonExprsPerChild: Array[mutable.ListBuffer[(Alias, Long, Option[Expression])]],
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
            preEvaluateInChildProject(
              d.child, guard, d.id, index, inputPlans, commonExprsPerChild)
          } else {
            None
          }
          d.id -> preEvaluated.getOrElse(d.child)
        }.toMap
        child.transformWithPruning(_.containsPattern(COMMON_EXPR_REF)) {
          // `child` may contain a nested `With`, whose refs point at definitions of an enclosing
          // `With` that this pass has not reached yet.
          case ref: CommonExpressionRef if refToExpr.contains(ref.id) => refToExpr(ref.id)
        }
    }
  }

  private def rewriteWithExprAndInputPlans(
      e: Expression,
      inputPlans: Seq[LogicalPlan],
      commonExprsPerChild: Array[mutable.ListBuffer[(Alias, Long, Option[Expression])]],
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
              // Only an unguarded column can be read here: this reference is evaluated on every row
              // the operator sees, while a guarded column holds its default value outside its
              // branch. A guarded one is left alone and a second, unguarded column is added.
              val existingCommonExpr = commonExprs.find { case (_, aliasId, aliasGuard) =>
                aliasId == id.id && aliasGuard.isEmpty
              }
              if (existingCommonExpr.isDefined) {
                if (Utils.isTesting) {
                  // `semanticEquals` is false for a nondeterministic definition, which the branch
                  // rewrite can also have put in an unguarded column.
                  assert(
                    existingCommonExpr.get._1.child.canonicalized == child.canonicalized)
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
                  commonExprs.append((alias, id.id, None))
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
        // The guards come from the conditions as they were before this pass rewrote them. The first
        // condition is in `alwaysEvaluatedInputs`, so the rewrite above may have replaced a common
        // expression in it with a column this pass is still adding, and a `Project` cannot read a
        // column from its own project list.
        (c, newExpr) match {
          case (If(pred, _, _), i: If) =>
            val guard = guardForm(pred)
            i.copy(
              trueValue = rewriteBranch(i.trueValue, guard.map(isTrue)),
              falseValue = rewriteBranch(i.falseValue, guard.map(isNotTrue)))

          case (CaseWhen(origBranches, _), cw: CaseWhen) =>
            // A condition is only evaluated when no preceding condition held, and a value only when
            // its own condition held on top of that. Accumulate the preceding conditions left to
            // right, dropping the whole chain at the first one that cannot be repeated as a guard.
            // The chain is a left-to-right `And` of non-nullable predicates, so it short-circuits
            // exactly where `CaseWhen` stops looking.
            val condGuards = origBranches.map(b => guardForm(b._1))
            val precedingNotTrue = condGuards.scanLeft(emptyGuard) {
              case (preceding, condGuard) =>
                condGuard.flatMap(g => andAlso(preceding, isNotTrue(g)))
            }
            val newBranches = cw.branches.zip(condGuards).zip(precedingNotTrue).map {
              case (((cond, value), condGuard), preceding) =>
                val valueGuard =
                  condGuard.flatMap(g => guardPredicate(andAlso(preceding, isTrue(g))))
                (rewriteBranch(cond, guardPredicate(preceding)),
                  rewriteBranch(value, valueGuard))
            }
            cw.copy(
              branches = newBranches,
              elseValue = cw.elseValue.map(
                rewriteBranch(_, guardPredicate(precedingNotTrue.last))))

          case (Coalesce(origChildren), co: Coalesce) =>
            // A child is only evaluated when every preceding child returned null.
            val childGuards = origChildren.map(guardForm)
            val precedingNull = childGuards.scanLeft(emptyGuard) {
              case (preceding, childGuard) =>
                childGuard.flatMap(g => andAlso(preceding, IsNull(g)))
            }
            co.copy(children = co.children.zip(precedingNull).map { case (child, preceding) =>
              rewriteBranch(child, guardPredicate(preceding))
            })

          case (NaNvl(left, _), n: NaNvl) =>
            // The right child is only evaluated when the left one is NaN. `IsNaN` is false for a
            // null input, so it already excludes the rows where the left child is null.
            n.copy(right = rewriteBranch(n.right, guardForm(left).map(IsNaN)))

          // Any other conditional expression: no guard, so a definition is pre-evaluated for every
          // row of the operator, or inlined when it cannot be pre-evaluated at all.
          case (_, other) => rewriteBranch(other, None)
        }

      case other => other.mapChildren(
        rewriteWithExprAndInputPlans(
          _, inputPlans, commonExprsPerChild, commonExprIdSet, isNestedWith)
      )
    }
  }
}
