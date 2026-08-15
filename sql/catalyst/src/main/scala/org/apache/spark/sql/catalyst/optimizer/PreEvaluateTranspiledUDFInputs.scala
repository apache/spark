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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, ExprId, GetArrayItem, GetStructField, OuterReference, PlanExpression, TranspiledUDFParameter}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Command, Expand, Generate, LateralJoin, LogicalPlan, PlanHelper, Project}
import org.apache.spark.sql.catalyst.trees.TreePattern.TRANSPILED_UDF_PARAMETER
import org.apache.spark.util.Utils

/**
 * Gives every input of a transpiled Python UDF a projection so that they can be reused and
 * pre-evaluated. This is best effort for deterministic inputs and guaranteed for non-deterministic
 * inputs.
 *
 * Called by `ConvertToCatalyst` on each plan node once that node's options have been substituted.
 * The plan surgery mirrors [[RewriteWithExpression]]`.applyInternal`: pick the child that can
 * compute the column, add a [[Project]] there, and project the extra columns away again above.
 *
 * An input stays inline (evaluated at each use) when pre-evaluating it cannot help, cannot be done,
 * or would not be safe: see [[isCheapInput]] for the first and `canPreEvaluate` plus the child
 * selection in `register` for the rest -- an [[Aggregate]] outside an aggregate function is the one
 * worth knowing about, since it covers every `GROUP BY` query whose grouping expression the user
 * wrote without the UDF. An argument the option never uses is not evaluated at all -- substitution
 * dropped it before this rewrite sees it -- which is a deliberate difference from the interpreted
 * UDF.
 */
object PreEvaluateTranspiledUDFInputs {

  private type InputKey = Either[Expression, ExprId]

  /** Prefix of the aliases this rewrite adds */
  val INPUT_ALIAS_PREFIX = "_udf_input"

  private case class PreEvaluated(alias: Alias, childIndex: Int)

  def apply(plan: LogicalPlan): LogicalPlan = {
    // no-op on plans without transpiled UDF parameters (UDFs with zero args).
    if (!plan.expressions.exists(_.containsPattern(TRANSPILED_UDF_PARAMETER))) {
      return plan
    }
    val preEvaluated = collectInputs(plan)
    val columns = preEvaluated.map { case (key, p) => key -> p.alias.toAttribute }

    // Unwrap every marker: one that was pre-evaluated becomes the column, one that was not becomes
    // its argument again, and in the latter case the walk continues into that argument so a marker
    // nested inside it still gets its own column. A marker nested inside a *pre-evaluated* argument
    // is not reached here: it travels into the new Project with the argument, and is pre-evaluated
    // one level further down (see `newChildren`).
    //
    // Hand-rolled rather than a transform for a reason: `transformDown` does not re-apply the rule
    // to a replacement, so a marker wrapping a marker would survive (reachable -- an argument that
    // is itself a transpiled call), and `transformUp` would rewrite the inner marker first and lose
    // the outer one's argument.
    def rewrite(e: Expression): Expression = e match {
      case p: TranspiledUDFParameter =>
        columns.get(keyOf(p)).getOrElse(rewrite(p.child))
      case other if other.containsPattern(TRANSPILED_UDF_PARAMETER) => other.mapChildren(rewrite)
      case other => other
    }

    val newChildren = plan.children.zipWithIndex.map { case (child, index) =>
      val aliases = preEvaluated.values.filter(_.childIndex == index).map(_.alias).toSeq
      // A pre-evaluated argument keeps the markers nested inside it -- an inner call's own inputs,
      // which are used several times *within* this column and so need a column of their own.
      // Recurse so they get one, in a Project below this one. Each level strips one level of
      // nesting, so this terminates.
      if (aliases.isEmpty) child else apply(Project(child.output ++ aliases, child))
    }
    val newPlan = plan.mapExpressions(rewrite)
    if (Utils.isTesting) {
      assert(!newPlan.expressions.exists(hasReachableMarker),
        s"A transpiled UDF parameter marker survived the rewrite: $newPlan")
    }
    val rewritten = newPlan.withNewChildren(newChildren)
    // The pre-evaluating Projects carry extra columns, which an operator inheriting its child's
    // output (Filter, Join, Sort, ...) would otherwise widen the query's schema with. Project them
    // away again, as RewriteWithExpression does for common expressions; CollapseProject and
    // ColumnPruning tidy the leftovers up.
    assert(plan.output.length <= rewritten.output.length,
      s"Pre-evaluating an input narrowed the operator's output: $rewritten")
    if (plan.output.length < rewritten.output.length) {
      assert(plan.outputSet.subsetOf(rewritten.outputSet),
        s"Pre-evaluating an input dropped an output attribute: $rewritten")
      Project(plan.output, rewritten)
    } else {
      rewritten
    }
  }

  /**
   * The arguments of `plan`'s options to pre-evaluate, each as the [[Alias]] to add to one of
   * `plan`'s children. Walks the markers outermost-first: an argument that is pre-evaluated takes
   * the markers nested inside it into its column, where `apply` pre-evaluates them one level down.
   */
  private def collectInputs(plan: LogicalPlan): mutable.LinkedHashMap[InputKey, PreEvaluated] = {
    // Insertion-ordered so that the columns, and their names, do not depend on hash iteration
    // order -- a plan has to be the same plan from run to run.
    val preEvaluated = mutable.LinkedHashMap.empty[InputKey, PreEvaluated]
    val declined = mutable.HashSet.empty[InputKey]
    // Counts every column this operator has named, rather than the ones it kept: a column dropped
    // again by the type check below has already taken its name, and reusing it would put two live
    // columns with the same name in one Project.
    var named = 0

    // Whether `p`'s argument is being pre-evaluated, registering it if this is the first copy.
    // `underAggregate` says an aggregate function encloses this use; see `canPreEvaluate`.
    def register(p: TranspiledUDFParameter, underAggregate: Boolean): Boolean = {
      val arg = stripMarkers(p.child)
      val key = keyOf(arg, p.id)
      if (declined.contains(key)) return false
      preEvaluated.get(key) match {
        case Some(existing) =>
          // One column stands in for every copy that keys to it, so it can only do that if they
          // agree on what they are. Copies of one deterministic argument key on the argument, so
          // they always do; copies of a nondeterministic one key on the marker id, and analysis
          // rewrites each copy on its own (a reseeded `rand()`, a cast pushed into one use
          // site), so if two disagree, give up on the parameter and leave every copy inline.
          // Markers nested inside a copy already registered keep their inline evaluation in that
          // case -- a missed pre-evaluation for a rare shape, not a wrong answer.
          //
          // Reuse also has to be legal *at this copy*, not just where the column was registered:
          // one argument can appear both inside an aggregate function and bare in the same
          // Aggregate (`SELECT a + 1, count(f(a + 1)), g(a + 1) ... GROUP BY a + 1`), and only the
          // enclosed one may have a column. Inheriting the first copy's context there would leave
          // the bare use reading a column that is not a grouping expression, which is an invalid
          // Aggregate.
          val reusable = existing.alias.dataType == arg.dataType &&
            existing.alias.nullable == arg.nullable &&
            // A nondeterministic key compares copies by id, not by shape, so also require that they
            // read the same columns: the column is built from the first copy, and a second copy
            // referencing anything else would silently lose it.
            existing.alias.references == arg.references &&
            canPreEvaluate(arg, underAggregate)
          if (!reusable) {
            preEvaluated.remove(key)
            declined.add(key)
          }
          reusable
        case None =>
          // The column has to be computable from a single child of this operator: an argument
          // reading columns from both sides of a join has nowhere to live, and so does one reading
          // a lambda variable (a NamedLambdaVariable references itself, and no child outputs it).
          // An argument with no references at all (`rand(1)`) can be computed anywhere, and lands
          // on the first child.
          val childIndex =
            if (!canPreEvaluate(arg, underAggregate)) -1
            else plan.children.indexWhere(c => arg.references.subsetOf(c.outputSet))
          if (childIndex < 0) {
            declined.add(key)
            return false
          }
          // Named from a counter local to this operator, not from the id, so that the same query
          // produces the same plan string from run to run. Two levels of nesting can therefore
          // repeat a name, which is legal (they carry different exprIds) and is what a self-join's
          // duplicate column names look like too.
          //
          // Keep the markers nested inside the argument, for the caller to pre-evaluate one level
          // down; they are transparent, so the column is the same either way.
          val alias = Alias(p.child, s"${INPUT_ALIAS_PREFIX}_$named")()
          named += 1
          // An aggregate, window or generator expression cannot live in a Project either. This is
          // reachable: `udf(sum(x))` in an Aggregate binds the parameter to the aggregate itself.
          val projectable = PlanHelper.specialExpressionsInUnsupportedOperator(
            Project(Seq(alias), plan.children(childIndex))).isEmpty
          if (projectable) {
            preEvaluated(key) = PreEvaluated(alias, childIndex)
          } else {
            declined.add(key)
          }
          projectable
      }
    }

    // Whether this operator may pre-evaluate `arg` at all, before asking which child could compute
    // it. Three refusals, none of them about the column itself:
    //
    //  - In an Aggregate, only a use inside an aggregate function. Anything else in an Aggregate's
    //    expressions has to *be* a grouping expression (analysis rejects it otherwise), and
    //    rewriting it to read a column breaks that correspondence whenever the grouping expression
    //    is not rewritten the same way -- which is exactly what happens when the user wrote it
    //    without the UDF, as in `SELECT a + 1, f(a + 1) FROM t GROUP BY a + 1`: only the argument
    //    carries a marker, so only that side would change, and the Aggregate becomes invalid
    //    ("the non-aggregating expression _udf_input_0 is based on columns which are not
    //    participating in the GROUP BY clause"). This is the hazard RewriteWithExpression avoids by
    //    splitting the Aggregate through PhysicalAggregation; declining is the cheaper answer, and
    //    a grouping key is computed once per row anyway.
    //  - A nondeterministic argument only where the operator produces one output row per input row
    //    (see preservesRowCount). Below one side of a join, or below a Generate or an Expand, a
    //    draw would be made once per input row and then reused for every row it produces -- not a
    //    fresh draw per output row but a correlated one, a bigger change than the drift this
    //    rewrite otherwise accepts.
    //  - Nothing carrying an OuterReference: that belongs to the enclosing query, and a Project
    //    inside a correlated subquery is not the place to evaluate it.
    def canPreEvaluate(arg: Expression, underAggregate: Boolean): Boolean =
      !isCheapInput(arg) &&
        (underAggregate || !plan.isInstanceOf[Aggregate]) &&
        (arg.deterministic || preservesRowCount(plan)) &&
        !arg.exists(_.isInstanceOf[OuterReference])

    def collect(e: Expression, underAggregate: Boolean): Unit = e match {
      case p: TranspiledUDFParameter =>
        if (!register(p, underAggregate)) collect(p.child, underAggregate)
      case other if other.containsPattern(TRANSPILED_UDF_PARAMETER) =>
        val enclosed = underAggregate || other.isInstanceOf[AggregateExpression]
        other.children.foreach(collect(_, enclosed))
      case _ =>
    }

    plan.expressions.foreach(collect(_, underAggregate = false))
    preEvaluated
  }

  private def keyOf(p: TranspiledUDFParameter): InputKey = keyOf(stripMarkers(p.child), p.id)

  private def keyOf(arg: Expression, id: ExprId): InputKey =
    if (arg.deterministic) Left(arg.canonicalized) else Right(id)

  /**
   * Whether `option` has an argument this rewrite would want a column for -- that is, a marked
   * argument that is not [[isCheapInput]].
   *
   * `ConvertToCatalyst` asks before transpiling into a predicate, where a column is worse than
   * useless: predicate pushdown inlines it back into the predicate it pushes below this Project
   * (nothing a transpiled argument is likely to be counts as [[Expression.expensive]]). That puts a
   * repeated argument back at every use site and back inside the body's branches, so an argument
   * the interpreted UDF evaluates once per row might be evaluated twice there, or not at all on
   * rows that skip a branch. An option whose arguments are all cheap needs no column, has nothing
   * to inline back, and is as safe in a predicate as anywhere.
   *
   * Deliberately conservative: it does not replay the child selection and operator guards in
   * `register`, so it can say "yes" for an argument that would have been declined anyway (a lambda
   * variable, an aggregate). Transpiling would have been safe there too; keeping Python is the
   * cheaper mistake.
   */
  def needsPreEvaluatedColumn(option: Expression): Boolean = {
    def check(e: Expression): Boolean = e match {
      case p: TranspiledUDFParameter => !isCheapInput(stripMarkers(p.child)) || check(p.child)
      case other if other.containsPattern(TRANSPILED_UDF_PARAMETER) => other.children.exists(check)
      case _ => false
    }
    check(option)
  }

  /**
   * Cheap enough that reading it at every use site costs nothing, so pre-evaluating it would only
   * grow the plan: a column read, an outer reference, anything foldable (which constant folding
   * collapses at each use site anyway, better than a column), or a struct field / fixed array
   * position read from one of those.
   *
   * Deliberately not `CollapseProject.isCheap`, which answers a different question -- whether
   * collapsing two Projects duplicates work -- and so also counts an `Alias`, a `BoundReference`,
   * and (unless `spark.sql.optimizer.avoidCollapseUDFWithExpensiveExpr` is on) a [[PythonUDF]].
   * A Python call is worth a column here even though `ExtractPythonUDFs` keys its eval column on
   * the canonicalized UDF, folding structurally equal deterministic copies into one round trip: the
   * copies each re-evaluate the UDF's own *arguments*, and a nondeterministic UDF is not folded at
   * all, so it really would be one call per use.
   */
  private def isCheapInput(e: Expression): Boolean = e match {
    case _: Attribute | _: OuterReference => true
    case _ if e.foldable => true
    // A struct field is an offset read and an array item at a fixed position is an index. Not
    // every ExtractValue is cheap: GetMapValue walks the key array comparing keys, so leaving a
    // map probe inline would pay that scan at every use site.
    case g: GetStructField => isCheapInput(g.child)
    case g: GetArrayItem if g.ordinal.foldable => isCheapInput(g.child)
    case _ => false
  }

  /**
   * Whether `plan` emits one row per input row, so a column computed below it is drawn once per row
   * the operator emits. A join pairs rows, and [[Generate]] and [[Expand]] fan one row out into
   * several, all of which would reuse a single draw. Deliberately a small allow-by-exclusion list:
   * a new fan-out operator should be added here rather than silently inheriting a draw.
   */
  private def preservesRowCount(plan: LogicalPlan): Boolean = plan match {
    case _: Generate | _: Expand | _: LateralJoin => false
    case other => other.children.length == 1
  }

  private def stripMarkers(e: Expression): Expression =
    e.transformUpWithPruning(_.containsPattern(TRANSPILED_UDF_PARAMETER)) {
      case p: TranspiledUDFParameter => p.child
    }

  /**
   * Whether `e` holds a marker this rewrite is responsible for. A [[PlanExpression]]'s plan is not
   * an expression child, so `collect` and `rewrite` never walk into a subquery -- the rule reaches
   * those markers when the transform descends into the subquery plan itself. `containsPattern` does
   * not make that distinction (a subquery expression unions its plan's pattern bits into its own),
   * so the post-condition needs this rather than the pattern.
   */
  private def hasReachableMarker(e: Expression): Boolean = e match {
    case _ if !e.containsPattern(TRANSPILED_UDF_PARAMETER) => false
    case _: TranspiledUDFParameter => true
    case _: PlanExpression[_] => false
    case _ => e.children.exists(hasReachableMarker)
  }
}
