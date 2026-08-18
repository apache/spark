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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, ExprId, Generator, GetArrayItem, GetStructField, LambdaFunction, OuterReference, TranspiledUDFParameter, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Command, Expand, Generate, LateralJoin, LogicalPlan, MergeRows, Project}
import org.apache.spark.sql.catalyst.trees.TreePattern.TRANSPILED_UDF_PARAMETER

/**
 * Adds a [[Project]] below the operator with one column per argument a transpiled Python UDF's
 * option uses, so an argument the option uses several times is computed once per row instead of
 * once per use. That is what the Python eval operator we are replacing does: one column per
 * argument.
 *
 * Called by `ConvertToCatalyst` on each plan node once that node's options have been substituted.
 * Same plan rewrite as [[RewriteWithExpression]]`.applyInternal`: pick the child that can compute
 * the column, add a [[Project]] there, then project the extra columns back off above.
 *
 * This is best effort, and the gaps are worth knowing about:
 *   - cheap arguments ([[isCheapInput]]) and the shapes [[childIndexFor]] turns down are left where
 *     they are, so an argument in one of those spots is still computed once per use;
 *   - a later rule can put a column back inline. `CollapseProject` does that for a deterministic
 *     column read only once, and predicate pushdown does it for a whole deterministic predicate
 *     (`PushPredicateThroughNonJoin` substitutes the alias and pushes the Filter below this
 *     Project), so a repeated deterministic argument in a `where` is back to once per use;
 *   - an argument used both directly and nested inside another argument that got a column gets a
 *     column at each level, because each level starts over (see `newChildren`), so it is computed
 *     once per level;
 *   - an argument the option never uses is never computed at all, because substitution dropped it
 *     before we get here. That one we accept on purpose.
 *
 * A nondeterministic column also costs something at the other end: `PushPredicateThroughNonJoin`
 * will not push anything through a Project whose list is nondeterministic, so in
 * `where(a = 1 and transpiled_udf_over_rand(...))` the `a = 1` conjunct stops at the Filter too and
 * the scan loses its pruning. That barrier is exactly what keeps the draw from happening twice, so
 * it is the trade rather than a bug, but it is worth knowing about.
 */
object PreEvaluateTranspiledUDFInputs {

  /** Prefix of the aliases this rewrite adds. Read by tests. */
  private[sql] val INPUT_ALIAS_PREFIX = "_udf_input"

  private case class PreEvaluated(alias: Alias, childIndex: Int)

  def apply(plan: LogicalPlan): LogicalPlan = {
    // No markers means no option used a non-foldable argument -- a zero-argument UDF, or one whose
    // arguments were all foldable and so left unmarked.
    if (!plan.expressions.exists(_.containsPattern(TRANSPILED_UDF_PARAMETER))) {
      return plan
    }
    // A Command keeps its query in a field rather than a child and has no output of its own, so the
    // schema guard at the bottom of this method cannot undo a widened child. Slipping a Project in
    // also hides the relation `DataSourceV2Strategy` matches on, which turns
    // `DELETE FROM t WHERE udf(a + 1) > 0` into an internal error. So add no column here -- but the
    // markers still have to come off, since this rewrite is the only thing that takes them off.
    if (plan.isInstanceOf[Command]) {
      return plan.mapExpressions(stripMarkers)
    }

    // One column per argument, keyed by `keyOf` so the copies that owe the body a single value
    // share one. Insertion-ordered, and the names come from the map's size, so the same query
    // prints the same plan every run.
    val columns = mutable.LinkedHashMap.empty[Either[Expression, ExprId], PreEvaluated]

    // One walk: it registers a column the first time it meets an argument worth one, and rewrites
    // every marker to read that column. A marker we do not give a column to is taken off and we
    // keep walking into its argument, so a marker nested inside it can still get one.
    //
    // `canPreEvaluate` is the position, threaded down rather than recomputed, and starts false in
    // an [[Aggregate]]: anything there that no aggregate function wraps has to *be* a grouping
    // expression, so rewriting the argument but not the grouping expression leaves an invalid
    // Aggregate. That is what happens whenever the user wrote the grouping expression without the
    // UDF, as in `SELECT a + 1, f(a + 1) ... GROUP BY a + 1`. Entering an aggregate function turns
    // it back on; entering a lambda turns it off, because a column below the operator is computed
    // once per row where the lambda body runs once per element.
    //
    // Written out by hand instead of using a transform, because both directions break here.
    // `transformDown` does not re-apply itself to what it just put in, so a marker wrapping a
    // marker would survive -- and that does happen, when an argument is itself a transpiled call.
    // `transformUp` rewrites the inner marker first and loses the outer one's argument.
    def rewrite(e: Expression, canPreEvaluate: Boolean): Expression = e match {
      case p: TranspiledUDFParameter if canPreEvaluate =>
        val key = keyOf(p)
        // A key we already have covers this copy too, nested markers and all. A new one gets its
        // alias built from `p.child` with those markers left on, for the recursion below to deal
        // with one level down.
        val column = columns.get(key).orElse {
          childIndexFor(plan, p.child).map { index =>
            val alias = Alias(p.child, s"${INPUT_ALIAS_PREFIX}_${columns.size}")()
            val entry = PreEvaluated(alias, index)
            columns(key) = entry
            entry
          }
        }
        column.map(_.alias.toAttribute).getOrElse(rewrite(p.child, canPreEvaluate))
      case p: TranspiledUDFParameter => rewrite(p.child, canPreEvaluate)
      case other if other.containsPattern(TRANSPILED_UDF_PARAMETER) =>
        val here = other match {
          case _: AggregateExpression => true
          case _: LambdaFunction => false
          case _ => canPreEvaluate
        }
        other.mapChildren(rewrite(_, here))
      case other => other
    }

    val rewritten = plan.mapExpressions(rewrite(_, !plan.isInstanceOf[Aggregate]))
    val newChildren = plan.children.zipWithIndex.map { case (child, index) =>
      val aliases = columns.values.filter(_.childIndex == index).map(_.alias).toSeq
      // An argument we gave a column to still has its own markers nested inside it -- an inner
      // call's arguments, used several times *within* this column. Recurse so those get a Project
      // below this one. Each level peels off one level of nesting, so this stops.
      if (aliases.isEmpty) child else apply(Project(child.output ++ aliases, child))
    }
    val result = rewritten.withNewChildren(newChildren)
    // An operator that just passes its child's output through (Filter, Join, Sort, ...) would
    // otherwise leak the extra columns into the query's schema, so project them back off, same as
    // RewriteWithExpression. CollapseProject and ColumnPruning clean up what is left.
    if (plan.output.length < result.output.length) Project(plan.output, result) else result
  }

  /**
   * Which copies of a marked argument share one column. Deterministic copies are all the same
   * value, so they key on the argument itself, which means two parameters bound to equal arguments
   * share a column and so do two separate calls in the same operator -- same as the Python eval
   * operator. A nondeterministic copy keys on its parameter's id instead: `f(rand(), rand())` owes
   * the body two draws, and two copies of one parameter owe it one even where `ResolveRandomSeed`
   * gave them different seeds.
   *
   * The id is minted per `UserDefinedPythonFunction.builder` call, so two plan occurrences of one
   * reused `Column` share it and therefore share a draw where the Python path would make two. See
   * [[TranspiledUDFParameter]].
   */
  private def keyOf(p: TranspiledUDFParameter): Either[Expression, ExprId] =
    if (p.child.deterministic) Left(p.child.canonicalized) else Right(p.id)

  /** Takes every marker off `e` without giving anything a column. */
  private def stripMarkers(e: Expression): Expression =
    e.transformUpWithPruning(_.containsPattern(TRANSPILED_UDF_PARAMETER)) {
      case p: TranspiledUDFParameter => p.child
    }

  /**
   * Which child of `plan` gets the column for `arg`, or None to leave `arg` where it is. Depends
   * only on the argument, so copies sharing a key -- which have equal arguments, give or take a
   * different `rand()` seed -- always get the same answer.
   */
  private def childIndexFor(plan: LogicalPlan, arg: Expression): Option[Int] = {
    if (isCheapInput(arg)) {
      return None
    }
    // An aggregate, window or generator expression cannot live in a Project. Analysis pulls all
    // three into their own operator, so this is here to stop us building an invalid Project, not
    // because we expect to hit it.
    val unprojectable = arg.exists {
      case _: AggregateExpression | _: WindowExpression | _: Generator => true
      case _ => false
    }
    if (unprojectable) {
      return None
    }
    // Below an operator that fans rows out, one draw would be made per input row and then reused
    // for every row it produces -- correlated across output rows instead of one draw per row.
    if (!arg.deterministic && !preservesRowCount(plan)) {
      return None
    }
    // An outer reference belongs to the query outside; a Project inside a correlated subquery is
    // not where we get to compute it.
    if (arg.exists(_.isInstanceOf[OuterReference])) {
      return None
    }
    // One child has to be able to compute the whole column: an argument reading both sides of a
    // join has nowhere to go, and neither does one reading a lambda variable (a
    // NamedLambdaVariable references itself, and no child outputs it). An argument that reads
    // nothing at all (`rand(1)`) goes on the first child.
    val index = plan.children.indexWhere(c => arg.references.subsetOf(c.outputSet))
    if (index < 0) None else Some(index)
  }

  /**
   * Cheap enough to read at every use site that a column would only make the plan bigger: an
   * attribute, anything foldable (folding kills it at each use site, which beats a column), or a
   * struct field / fixed array position read off one of those. Not every `ExtractValue` is cheap --
   * `GetMapValue` walks the key array comparing keys.
   *
   * Not `CollapseProject.isCheap`, on purpose: that one answers whether collapsing two Projects
   * duplicates work, so it also counts a [[PythonUDF]] as cheap. A Python call is worth a column
   * here even though `ExtractPythonUDFs` folds structurally equal deterministic copies into one
   * round trip, because each copy still recomputes the UDF's own *arguments* -- and a
   * nondeterministic UDF does not get folded at all.
   */
  private def isCheapInput(e: Expression): Boolean = e match {
    case _: Attribute => true
    case _ if e.foldable => true
    // A marker is see-through, so ask about what it wraps. Reachable when an argument is itself a
    // transpiled call; without this a cheap argument under a marker would get a pointless column.
    case p: TranspiledUDFParameter => isCheapInput(p.child)
    case g: GetStructField => isCheapInput(g.child)
    case g: GetArrayItem if g.ordinal.foldable => isCheapInput(g.child)
    case _ => false
  }

  /**
   * Whether `plan` puts out one row per input row. A deny list on purpose: a new fan-out operator
   * has to get added here, rather than quietly inheriting a draw it should not get.
   */
  private def preservesRowCount(plan: LogicalPlan): Boolean = plan match {
    case _: Generate | _: Expand | _: LateralJoin | _: MergeRows => false
    case other => other.children.length == 1
  }
}
