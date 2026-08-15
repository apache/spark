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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, ExprId, Generator, GetArrayItem, GetStructField, OuterReference, TranspiledUDFParameter, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, Generate, LateralJoin, LogicalPlan, MergeRows, Project}
import org.apache.spark.sql.catalyst.trees.TreePattern.TRANSPILED_UDF_PARAMETER

/**
 * Hoists the arguments a transpiled Python UDF's option uses into a [[Project]] below the operator,
 * so that an argument used several times is evaluated once per row -- as the Python eval operator
 * this replaces does, computing one column per argument.
 *
 * Best effort: [[isCheapInput]] arguments and the shapes [[childIndexFor]] declines stay inline,
 * and a later rule may inline a column back (`CollapseProject` does for a deterministic column read
 * once). An argument the option never uses is not evaluated at all, since substitution dropped it
 * before this rewrite sees it -- a deliberate difference from the interpreted UDF.
 *
 * Called by `ConvertToCatalyst` on each plan node once that node's options have been substituted.
 * The plan surgery mirrors [[RewriteWithExpression]]`.applyInternal`: pick the child that can
 * compute the column, add a [[Project]] there, and project the extra columns away again above.
 */
object PreEvaluateTranspiledUDFInputs {

  /**
   * Which copies of a marked argument share one column. Deterministic copies are interchangeable,
   * so they key on the argument itself -- which shares a column between two parameters bound to
   * equal arguments, and between separate calls, as the Python eval operator does. Nondeterministic
   * ones key on the parameter's id instead: `f(rand(), rand())` owes the body two draws, and two
   * copies of one parameter owe it one even where `ResolveRandomSeed` reseeded them apart.
   */
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

    // Unwrap every marker: a pre-evaluated one becomes its column, one left inline becomes its
    // argument again and the walk continues into it so a marker nested inside still gets a column.
    //
    // Hand-rolled rather than a transform: `transformDown` does not re-apply the rule to a
    // replacement, so a marker wrapping a marker would survive (reachable -- an argument that is
    // itself a transpiled call), and `transformUp` rewrites the inner marker first and loses the
    // outer one's argument.
    def rewrite(e: Expression, underAggregate: Boolean): Expression = e match {
      // Asking `mustStayInline` again rather than keying it in: one argument can appear both inside
      // an aggregate function and bare in the same Aggregate, and only the enclosed use may read a
      // column even though the two key the same.
      case p: TranspiledUDFParameter if !mustStayInline(plan, underAggregate) =>
        preEvaluated.get(keyOf(p)).map(_.alias.toAttribute)
          .getOrElse(rewrite(p.child, underAggregate))
      case p: TranspiledUDFParameter => rewrite(p.child, underAggregate)
      case other if other.containsPattern(TRANSPILED_UDF_PARAMETER) =>
        other.mapChildren(rewrite(_, underAggregate || other.isInstanceOf[AggregateExpression]))
      case other => other
    }

    val newChildren = plan.children.zipWithIndex.map { case (child, index) =>
      val aliases = preEvaluated.values.filter(_.childIndex == index).map(_.alias).toSeq
      // A pre-evaluated argument keeps the markers nested inside it -- an inner call's own inputs,
      // used several times *within* this column. Recurse so they get a Project below this one; each
      // level strips one level of nesting, so this terminates.
      if (aliases.isEmpty) child else apply(Project(child.output ++ aliases, child))
    }
    val rewritten = plan.mapExpressions(rewrite(_, underAggregate = false))
      .withNewChildren(newChildren)
    // An operator inheriting its child's output (Filter, Join, Sort, ...) would otherwise widen the
    // query's schema with the extra columns. Project them away, as RewriteWithExpression does;
    // CollapseProject and ColumnPruning tidy the leftovers up.
    if (plan.output.length < rewritten.output.length) Project(plan.output, rewritten) else rewritten
  }

  /**
   * The arguments of `plan`'s options to pre-evaluate, each as the [[Alias]] to add to one of
   * `plan`'s children. Walks the markers outermost-first: an argument that is pre-evaluated takes
   * the markers nested inside it into its column, where `apply` pre-evaluates them one level down.
   */
  private def collectInputs(plan: LogicalPlan): mutable.LinkedHashMap[InputKey, PreEvaluated] = {
    // Insertion-ordered, and named from the map's size rather than from an id, so that a query
    // produces the same plan string from run to run.
    val preEvaluated = mutable.LinkedHashMap.empty[InputKey, PreEvaluated]

    def collect(e: Expression, underAggregate: Boolean): Unit = e match {
      case p: TranspiledUDFParameter if !mustStayInline(plan, underAggregate) =>
        val key = keyOf(p)
        // A key already registered stands in for this copy too, nested markers and all.
        if (!preEvaluated.contains(key)) {
          childIndexFor(plan, p.child) match {
            case Some(index) =>
              val name = s"${INPUT_ALIAS_PREFIX}_${preEvaluated.size}"
              preEvaluated(key) = PreEvaluated(Alias(p.child, name)(), index)
            case None => collect(p.child, underAggregate)
          }
        }
      case p: TranspiledUDFParameter => collect(p.child, underAggregate)
      case other if other.containsPattern(TRANSPILED_UDF_PARAMETER) =>
        val enclosed = underAggregate || other.isInstanceOf[AggregateExpression]
        other.children.foreach(collect(_, enclosed))
      case _ =>
    }

    plan.expressions.foreach(collect(_, underAggregate = false))
    preEvaluated
  }

  private def keyOf(p: TranspiledUDFParameter): InputKey =
    if (p.child.deterministic) Left(p.child.canonicalized) else Right(p.id)

  /**
   * Whether a use of an argument at this position has to stay inline whatever the argument is.
   * Everything in an [[Aggregate]]'s expressions that no aggregate function encloses has to *be* a
   * grouping expression, and rewriting only the argument's side of that correspondence makes the
   * Aggregate invalid whenever the user wrote the grouping expression without the UDF (`SELECT a +
   * 1, f(a + 1) ... GROUP BY a + 1`). This is the hazard RewriteWithExpression avoids by splitting
   * the Aggregate through PhysicalAggregation; declining is cheaper, and a grouping key is computed
   * once per row anyway.
   */
  private def mustStayInline(plan: LogicalPlan, underAggregate: Boolean): Boolean =
    plan.isInstanceOf[Aggregate] && !underAggregate

  /**
   * Which child of `plan` gets the column for `arg`, or None to leave `arg` inline. A function of
   * the argument alone, so copies that share a key -- which have equal arguments, up to a reseeded
   * `rand()` -- always get the same answer.
   */
  private def childIndexFor(plan: LogicalPlan, arg: Expression): Option[Int] = {
    val worthHoisting = !isCheapInput(arg) &&
      // An aggregate, window or generator expression cannot live in a Project. Analysis lifts all
      // three into their own operator, and `mustStayInline` covers the Aggregate case, so this
      // guards against building an invalid Project rather than against a shape seen in practice.
      !arg.exists {
        case _: AggregateExpression | _: WindowExpression | _: Generator => true
        case _ => false
      } &&
      // Below a fan-out operator a draw would be made once per input row and reused for every row
      // it produces -- correlated across output rows rather than one draw per row.
      (arg.deterministic || preservesRowCount(plan)) &&
      // An outer reference belongs to the enclosing query; a Project inside a correlated subquery
      // is not the place to evaluate it.
      !arg.exists(_.isInstanceOf[OuterReference])
    if (!worthHoisting) {
      None
    } else {
      // The column has to be computable from a single child: an argument reading both sides of a
      // join has nowhere to live, and neither does one reading a lambda variable (a
      // NamedLambdaVariable references itself, and no child outputs it). An argument with no
      // references at all (`rand(1)`) lands on the first child.
      val index = plan.children.indexWhere(c => arg.references.subsetOf(c.outputSet))
      if (index < 0) None else Some(index)
    }
  }

  /**
   * Cheap enough that reading it at every use site costs nothing, so a column would only grow the
   * plan: an attribute, anything foldable (folding collapses it at each use site, better than a
   * column), or a struct field / fixed array position read from one of those. Not every
   * `ExtractValue` is cheap -- `GetMapValue` walks the key array comparing keys.
   *
   * Deliberately not `CollapseProject.isCheap`, which answers whether collapsing two Projects
   * duplicates work and so also counts a [[PythonUDF]]. A Python call is worth a column here even
   * though `ExtractPythonUDFs` folds structurally equal deterministic copies into one round trip:
   * the copies each re-evaluate the UDF's own *arguments*, and a nondeterministic UDF is not folded
   * at all.
   */
  private def isCheapInput(e: Expression): Boolean = e match {
    case _: Attribute => true
    case _ if e.foldable => true
    case g: GetStructField => isCheapInput(g.child)
    case g: GetArrayItem if g.ordinal.foldable => isCheapInput(g.child)
    case _ => false
  }

  /**
   * Whether `plan` emits one row per input row. Deliberately an allow-by-exclusion list: a new
   * fan-out operator should be added here rather than silently inheriting a draw.
   */
  private def preservesRowCount(plan: LogicalPlan): Boolean = plan match {
    case _: Generate | _: Expand | _: LateralJoin | _: MergeRows => false
    case other => other.children.length == 1
  }
}
