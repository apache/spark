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

import scala.collection.immutable.HashSet
import org.apache.spark.sql.catalyst.analysis.{CleanupAliases, EliminateSubQueries}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types._

abstract class Optimizer extends RuleExecutor[LogicalPlan]

object DefaultOptimizer extends Optimizer {
  val batches =
    // SubQueries are only needed for analysis and can be removed before execution.
    Batch("Remove SubQueries", FixedPoint(100),
      EliminateSubQueries) ::
    Batch("Aggregate", FixedPoint(100),
      ReplaceDistinctWithAggregate,
      RemoveLiteralFromGroupExpressions) ::
    Batch("Operator Optimizations", FixedPoint(100),
      // Operator push down
      SetOperationPushDown,
      SamplePushDown,
      PushPredicateThroughJoin,
      PushPredicateThroughProject,
      PushPredicateThroughGenerate,
      ColumnPruning,
      // Operator combine
      ProjectCollapsing,
      CombineFilters,
      CombineLimits,
      // Constant folding
      NullPropagation,
      OptimizeIn,
      ConstantFolding,
      LikeSimplification,
      BooleanSimplification,
      RemovePositive,
      SimplifyFilters,
      SimplifyCasts,
      SimplifyCaseConversionExpressions) ::
    Batch("Decimal Optimizations", FixedPoint(100),
      DecimalAggregates) ::
    Batch("LocalRelation", FixedPoint(100),
      ConvertToLocalRelation) :: Nil
}

/**
 * Pushes operations down into a Sample.
 */
object SamplePushDown extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Push down filter into sample
    case Filter(condition, s @ Sample(lb, up, replace, seed, child)) =>
      Sample(lb, up, replace, seed,
        Filter(condition, child))
    // Push down projection into sample
    case Project(projectList, s @ Sample(lb, up, replace, seed, child)) =>
      Sample(lb, up, replace, seed,
        Project(projectList, child))
  }
}

/**
 * Pushes certain operations to both sides of a Union, Intersect or Except operator.
 * Operations that are safe to pushdown are listed as follows.
 * Union:
 * Right now, Union means UNION ALL, which does not de-duplicate rows. So, it is
 * safe to pushdown Filters and Projections through it. Once we add UNION DISTINCT,
 * we will not be able to pushdown Projections.
 *
 * Intersect:
 * It is not safe to pushdown Projections through it because we need to get the
 * intersect of rows by comparing the entire rows. It is fine to pushdown Filters
 * with deterministic condition.
 *
 * Except:
 * It is not safe to pushdown Projections through it because we need to get the
 * intersect of rows by comparing the entire rows. It is fine to pushdown Filters
 * with deterministic condition.
 */
object SetOperationPushDown extends Rule[LogicalPlan] with PredicateHelper {

  /**
   * Maps Attributes from the left side to the corresponding Attribute on the right side.
   */
  private def buildRewrites(bn: BinaryNode): AttributeMap[Attribute] = {
    assert(bn.isInstanceOf[Union] || bn.isInstanceOf[Intersect] || bn.isInstanceOf[Except])
    assert(bn.left.output.size == bn.right.output.size)

    AttributeMap(bn.left.output.zip(bn.right.output))
  }

  /**
   * Rewrites an expression so that it can be pushed to the right side of a
   * Union, Intersect or Except operator. This method relies on the fact that the output attributes
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
    // Push down filter into union
    case Filter(condition, u @ Union(left, right)) =>
      val (deterministic, nondeterministic) = partitionByDeterministic(condition)
      val rewrites = buildRewrites(u)
      Filter(nondeterministic,
        Union(
          Filter(deterministic, left),
          Filter(pushToRight(deterministic, rewrites), right)
        )
      )

    // Push down deterministic projection through UNION ALL
    case p @ Project(projectList, u @ Union(left, right)) =>
      if (projectList.forall(_.deterministic)) {
        val rewrites = buildRewrites(u)
        Union(
          Project(projectList, left),
          Project(projectList.map(pushToRight(_, rewrites)), right))
      } else {
        p
      }

    // Push down filter through INTERSECT
    case Filter(condition, i @ Intersect(left, right)) =>
      val (deterministic, nondeterministic) = partitionByDeterministic(condition)
      val rewrites = buildRewrites(i)
      Filter(nondeterministic,
        Intersect(
          Filter(deterministic, left),
          Filter(pushToRight(deterministic, rewrites), right)
        )
      )

    // Push down filter through EXCEPT
    case Filter(condition, e @ Except(left, right)) =>
      val (deterministic, nondeterministic) = partitionByDeterministic(condition)
      val rewrites = buildRewrites(e)
      Filter(nondeterministic,
        Except(
          Filter(deterministic, left),
          Filter(pushToRight(deterministic, rewrites), right)
        )
      )
  }
}

/**
 * Attempts to eliminate the reading of unneeded columns from the query plan using the following
 * transformations:
 *
 *  - Inserting Projections beneath the following operators:
 *   - Aggregate
 *   - Generate
 *   - Project <- Join
 *   - LeftSemiJoin
 */
object ColumnPruning extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case a @ Aggregate(_, _, e @ Expand(_, groupByExprs, _, child))
      if (child.outputSet -- AttributeSet(groupByExprs) -- a.references).nonEmpty =>
      a.copy(child = e.copy(child = prunedChild(child, AttributeSet(groupByExprs) ++ a.references)))

    // Eliminate attributes that are not needed to calculate the specified aggregates.
    case a @ Aggregate(_, _, child) if (child.outputSet -- a.references).nonEmpty =>
      a.copy(child = Project(a.references.toSeq, child))

    // Eliminate attributes that are not needed to calculate the Generate.
    case g: Generate if !g.join && (g.child.outputSet -- g.references).nonEmpty =>
      g.copy(child = Project(g.references.toSeq, g.child))

    case p @ Project(_, g: Generate) if g.join && p.references.subsetOf(g.generatedSet) =>
      p.copy(child = g.copy(join = false))

    case p @ Project(projectList, g: Generate) if g.join =>
      val neededChildOutput = p.references -- g.generatorOutput ++ g.references
      if (neededChildOutput == g.child.outputSet) {
        p
      } else {
        Project(projectList, g.copy(child = Project(neededChildOutput.toSeq, g.child)))
      }

    case p @ Project(projectList, a @ Aggregate(groupingExpressions, aggregateExpressions, child))
        if (a.outputSet -- p.references).nonEmpty =>
      Project(
        projectList,
        Aggregate(
          groupingExpressions,
          aggregateExpressions.filter(e => p.references.contains(e)),
          child))

    // Eliminate unneeded attributes from either side of a Join.
    case Project(projectList, Join(left, right, joinType, condition)) =>
      // Collect the list of all references required either above or to evaluate the condition.
      val allReferences: AttributeSet =
        AttributeSet(
          projectList.flatMap(_.references.iterator)) ++
          condition.map(_.references).getOrElse(AttributeSet(Seq.empty))

      /** Applies a projection only when the child is producing unnecessary attributes */
      def pruneJoinChild(c: LogicalPlan): LogicalPlan = prunedChild(c, allReferences)

      Project(projectList, Join(pruneJoinChild(left), pruneJoinChild(right), joinType, condition))

    // Eliminate unneeded attributes from right side of a LeftSemiJoin.
    case Join(left, right, LeftSemi, condition) =>
      // Collect the list of all references required to evaluate the condition.
      val allReferences: AttributeSet =
        condition.map(_.references).getOrElse(AttributeSet(Seq.empty))

      Join(left, prunedChild(right, allReferences), LeftSemi, condition)

    // Push down project through limit, so that we may have chance to push it further.
    case Project(projectList, Limit(exp, child)) =>
      Limit(exp, Project(projectList, child))

    // Push down project if possible when the child is sort
    case p @ Project(projectList, s @ Sort(_, _, grandChild))
      if s.references.subsetOf(p.outputSet) =>
      s.copy(child = Project(projectList, grandChild))

    // Eliminate no-op Projects
    case Project(projectList, child) if child.output == projectList => child
  }

  /** Applies a projection only when the child is producing unnecessary attributes */
  private def prunedChild(c: LogicalPlan, allReferences: AttributeSet) =
    if ((c.outputSet -- allReferences.filter(c.outputSet.contains)).nonEmpty) {
      Project(allReferences.filter(c.outputSet.contains).toSeq, c)
    } else {
      c
    }
}

/**
 * Combines two adjacent [[Project]] operators into one and perform alias substitution,
 * merging the expressions into one single expression.
 */
object ProjectCollapsing extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case p @ Project(projectList1, Project(projectList2, child)) =>
      // Create a map of Aliases to their values from the child projection.
      // e.g., 'SELECT ... FROM (SELECT a + b AS c, d ...)' produces Map(c -> Alias(a + b, c)).
      val aliasMap = AttributeMap(projectList2.collect {
        case a: Alias => (a.toAttribute, a)
      })

      // We only collapse these two Projects if their overlapped expressions are all
      // deterministic.
      val hasNondeterministic = projectList1.exists(_.collect {
        case a: Attribute if aliasMap.contains(a) => aliasMap(a).child
      }.exists(!_.deterministic))

      if (hasNondeterministic) {
        p
      } else {
        // Substitute any attributes that are produced by the child projection, so that we safely
        // eliminate it.
        // e.g., 'SELECT c + 1 FROM (SELECT a + b AS C ...' produces 'SELECT a + b + 1 ...'
        // TODO: Fix TransformBase to avoid the cast below.
        val substitutedProjection = projectList1.map(_.transform {
          case a: Attribute => aliasMap.getOrElse(a, a)
        }).asInstanceOf[Seq[NamedExpression]]
        // collapse 2 projects may introduce unnecessary Aliases, trim them here.
        val cleanedProjection = substitutedProjection.map(p =>
          CleanupAliases.trimNonTopLevelAliases(p).asInstanceOf[NamedExpression]
        )
        Project(cleanedProjection, child)
      }
  }
}

/**
 * Simplifies LIKE expressions that do not need full regular expressions to evaluate the condition.
 * For example, when the expression is just checking to see if a string starts with a given
 * pattern.
 */
object LikeSimplification extends Rule[LogicalPlan] {
  // if guards below protect from escapes on trailing %.
  // Cases like "something\%" are not optimized, but this does not affect correctness.
  private val startsWith = "([^_%]+)%".r
  private val endsWith = "%([^_%]+)".r
  private val contains = "%([^_%]+)%".r
  private val equalTo = "([^_%]*)".r

  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Like(l, Literal(utf, StringType)) =>
      utf.toString match {
        case startsWith(pattern) if !pattern.endsWith("\\") =>
          StartsWith(l, Literal(pattern))
        case endsWith(pattern) =>
          EndsWith(l, Literal(pattern))
        case contains(pattern) if !pattern.endsWith("\\") =>
          Contains(l, Literal(pattern))
        case equalTo(pattern) =>
          EqualTo(l, Literal(pattern))
        case _ =>
          Like(l, Literal.create(utf, StringType))
      }
  }
}

/**
 * Replaces [[Expression Expressions]] that can be statically evaluated with
 * equivalent [[Literal]] values. This rule is more specific with
 * Null value propagation from bottom to top of the expression tree.
 */
object NullPropagation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case e @ Count(Literal(null, _)) => Cast(Literal(0L), e.dataType)
      case e @ IsNull(c) if !c.nullable => Literal.create(false, BooleanType)
      case e @ IsNotNull(c) if !c.nullable => Literal.create(true, BooleanType)
      case e @ GetArrayItem(Literal(null, _), _) => Literal.create(null, e.dataType)
      case e @ GetArrayItem(_, Literal(null, _)) => Literal.create(null, e.dataType)
      case e @ GetMapValue(Literal(null, _), _) => Literal.create(null, e.dataType)
      case e @ GetMapValue(_, Literal(null, _)) => Literal.create(null, e.dataType)
      case e @ GetStructField(Literal(null, _), _, _) => Literal.create(null, e.dataType)
      case e @ GetArrayStructFields(Literal(null, _), _, _, _, _) =>
        Literal.create(null, e.dataType)
      case e @ EqualNullSafe(Literal(null, _), r) => IsNull(r)
      case e @ EqualNullSafe(l, Literal(null, _)) => IsNull(l)
      case e @ Count(expr) if !expr.nullable => Count(Literal(1))

      // For Coalesce, remove null literals.
      case e @ Coalesce(children) =>
        val newChildren = children.filter {
          case Literal(null, _) => false
          case _ => true
        }
        if (newChildren.length == 0) {
          Literal.create(null, e.dataType)
        } else if (newChildren.length == 1) {
          newChildren.head
        } else {
          Coalesce(newChildren)
        }

      case e @ Substring(Literal(null, _), _, _) => Literal.create(null, e.dataType)
      case e @ Substring(_, Literal(null, _), _) => Literal.create(null, e.dataType)
      case e @ Substring(_, _, Literal(null, _)) => Literal.create(null, e.dataType)

      // MaxOf and MinOf can't do null propagation
      case e: MaxOf => e
      case e: MinOf => e

      // Put exceptional cases above if any
      case e @ BinaryArithmetic(Literal(null, _), _) => Literal.create(null, e.dataType)
      case e @ BinaryArithmetic(_, Literal(null, _)) => Literal.create(null, e.dataType)

      case e @ BinaryComparison(Literal(null, _), _) => Literal.create(null, e.dataType)
      case e @ BinaryComparison(_, Literal(null, _)) => Literal.create(null, e.dataType)

      case e: StringRegexExpression => e.children match {
        case Literal(null, _) :: right :: Nil => Literal.create(null, e.dataType)
        case left :: Literal(null, _) :: Nil => Literal.create(null, e.dataType)
        case _ => e
      }

      case e: StringPredicate => e.children match {
        case Literal(null, _) :: right :: Nil => Literal.create(null, e.dataType)
        case left :: Literal(null, _) :: Nil => Literal.create(null, e.dataType)
        case _ => e
      }
    }
  }
}

/**
 * Replaces [[Expression Expressions]] that can be statically evaluated with
 * equivalent [[Literal]] values.
 */
object ConstantFolding extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsDown {
      // Skip redundant folding of literals. This rule is technically not necessary. Placing this
      // here avoids running the next rule for Literal values, which would create a new Literal
      // object and running eval unnecessarily.
      case l: Literal => l

      // Fold expressions that are foldable.
      case e if e.foldable => Literal.create(e.eval(EmptyRow), e.dataType)
    }
  }
}

/**
 * Replaces [[In (value, seq[Literal])]] with optimized version[[InSet (value, HashSet[Literal])]]
 * which is much faster
 */
object OptimizeIn extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsDown {
      case In(v, list) if !list.exists(!_.isInstanceOf[Literal]) && list.size > 10 =>
        val hSet = list.map(e => e.eval(EmptyRow))
        InSet(v, HashSet() ++ hSet)
    }
  }
}

/**
 * Simplifies boolean expressions:
 * 1. Simplifies expressions whose answer can be determined without evaluating both sides.
 * 2. Eliminates / extracts common factors.
 * 3. Merge same expressions
 * 4. Removes `Not` operator.
 */
object BooleanSimplification extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case and @ And(left, right) => (left, right) match {
        // true && r  =>  r
        case (Literal(true, BooleanType), r) => r
        // l && true  =>  l
        case (l, Literal(true, BooleanType)) => l
        // false && r  =>  false
        case (Literal(false, BooleanType), _) => Literal(false)
        // l && false  =>  false
        case (_, Literal(false, BooleanType)) => Literal(false)
        // a && a  =>  a
        case (l, r) if l fastEquals r => l
        // (a || b) && (a || c)  =>  a || (b && c)
        case _ =>
          // 1. Split left and right to get the disjunctive predicates,
          //   i.e. lhs = (a, b), rhs = (a, c)
          // 2. Find the common predict between lhsSet and rhsSet, i.e. common = (a)
          // 3. Remove common predict from lhsSet and rhsSet, i.e. ldiff = (b), rdiff = (c)
          // 4. Apply the formula, get the optimized predicate: common || (ldiff && rdiff)
          val lhs = splitDisjunctivePredicates(left)
          val rhs = splitDisjunctivePredicates(right)
          val common = lhs.filter(e => rhs.exists(e.semanticEquals(_)))
          if (common.isEmpty) {
            // No common factors, return the original predicate
            and
          } else {
            val ldiff = lhs.filterNot(e => common.exists(e.semanticEquals(_)))
            val rdiff = rhs.filterNot(e => common.exists(e.semanticEquals(_)))
            if (ldiff.isEmpty || rdiff.isEmpty) {
              // (a || b || c || ...) && (a || b) => (a || b)
              common.reduce(Or)
            } else {
              // (a || b || c || ...) && (a || b || d || ...) =>
              // ((c || ...) && (d || ...)) || a || b
              (common :+ And(ldiff.reduce(Or), rdiff.reduce(Or))).reduce(Or)
            }
          }
      }  // end of And(left, right)

      case or @ Or(left, right) => (left, right) match {
        // true || r  =>  true
        case (Literal(true, BooleanType), _) => Literal(true)
        // r || true  =>  true
        case (_, Literal(true, BooleanType)) => Literal(true)
        // false || r  =>  r
        case (Literal(false, BooleanType), r) => r
        // l || false  =>  l
        case (l, Literal(false, BooleanType)) => l
        // a || a => a
        case (l, r) if l fastEquals r => l
        // (a && b) || (a && c)  =>  a && (b || c)
        case _ =>
           // 1. Split left and right to get the conjunctive predicates,
           //   i.e.  lhs = (a, b), rhs = (a, c)
           // 2. Find the common predict between lhsSet and rhsSet, i.e. common = (a)
           // 3. Remove common predict from lhsSet and rhsSet, i.e. ldiff = (b), rdiff = (c)
           // 4. Apply the formula, get the optimized predicate: common && (ldiff || rdiff)
          val lhs = splitConjunctivePredicates(left)
          val rhs = splitConjunctivePredicates(right)
          val common = lhs.filter(e => rhs.exists(e.semanticEquals(_)))
          if (common.isEmpty) {
            // No common factors, return the original predicate
            or
          } else {
            val ldiff = lhs.filterNot(e => common.exists(e.semanticEquals(_)))
            val rdiff = rhs.filterNot(e => common.exists(e.semanticEquals(_)))
            if (ldiff.isEmpty || rdiff.isEmpty) {
              // (a && b) || (a && b && c && ...) => a && b
              common.reduce(And)
            } else {
              // (a && b && c && ...) || (a && b && d && ...) =>
              // ((c && ...) || (d && ...)) && a && b
              (common :+ Or(ldiff.reduce(And), rdiff.reduce(And))).reduce(And)
            }
          }
      }  // end of Or(left, right)

      case not @ Not(exp) => exp match {
        // not(true)  =>  false
        case Literal(true, BooleanType) => Literal(false)
        // not(false)  =>  true
        case Literal(false, BooleanType) => Literal(true)
        // not(l > r)  =>  l <= r
        case GreaterThan(l, r) => LessThanOrEqual(l, r)
        // not(l >= r)  =>  l < r
        case GreaterThanOrEqual(l, r) => LessThan(l, r)
        // not(l < r)  =>  l >= r
        case LessThan(l, r) => GreaterThanOrEqual(l, r)
        // not(l <= r)  =>  l > r
        case LessThanOrEqual(l, r) => GreaterThan(l, r)
        // not(not(e))  =>  e
        case Not(e) => e
        case _ => not
      }  // end of Not(exp)

      // if (true) a else b  =>  a
      // if (false) a else b  =>  b
      case e @ If(Literal(v, _), trueValue, falseValue) => if (v == true) trueValue else falseValue
    }
  }
}

/**
 * Combines two adjacent [[Filter]] operators into one, merging the
 * conditions into one conjunctive predicate.
 */
object CombineFilters extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case ff @ Filter(fc, nf @ Filter(nc, grandChild)) => Filter(And(nc, fc), grandChild)
  }
}

/**
 * Removes filters that can be evaluated trivially.  This is done either by eliding the filter for
 * cases where it will always evaluate to `true`, or substituting a dummy empty relation when the
 * filter will always evaluate to `false`.
 */
object SimplifyFilters extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // If the filter condition always evaluate to true, remove the filter.
    case Filter(Literal(true, BooleanType), child) => child
    // If the filter condition always evaluate to null or false,
    // replace the input with an empty relation.
    case Filter(Literal(null, _), child) => LocalRelation(child.output, data = Seq.empty)
    case Filter(Literal(false, BooleanType), child) => LocalRelation(child.output, data = Seq.empty)
  }
}

/**
 * Pushes [[Filter]] operators through [[Project]] operators, in-lining any [[Alias Aliases]]
 * that were defined in the projection.
 *
 * This heuristic is valid assuming the expression evaluation cost is minimal.
 */
object PushPredicateThroughProject extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, project @ Project(fields, grandChild)) =>
      // Create a map of Aliases to their values from the child projection.
      // e.g., 'SELECT a + b AS c, d ...' produces Map(c -> a + b).
      val aliasMap = AttributeMap(fields.collect {
        case a: Alias => (a.toAttribute, a.child)
      })

      // Split the condition into small conditions by `And`, so that we can push down part of this
      // condition without nondeterministic expressions.
      val andConditions = splitConjunctivePredicates(condition)

      val (deterministic, nondeterministic) = andConditions.partition(_.collect {
        case a: Attribute if aliasMap.contains(a) => aliasMap(a)
      }.forall(_.deterministic))

      // If there is no nondeterministic conditions, push down the whole condition.
      if (nondeterministic.isEmpty) {
        project.copy(child = Filter(replaceAlias(condition, aliasMap), grandChild))
      } else {
        // If they are all nondeterministic conditions, leave it un-changed.
        if (deterministic.isEmpty) {
          filter
        } else {
          // Push down the small conditions without nondeterministic expressions.
          val pushedCondition = deterministic.map(replaceAlias(_, aliasMap)).reduce(And)
          Filter(nondeterministic.reduce(And),
            project.copy(child = Filter(pushedCondition, grandChild)))
        }
      }
  }

  // Substitute any attributes that are produced by the child projection, so that we safely
  // eliminate it.
  private def replaceAlias(condition: Expression, sourceAliases: AttributeMap[Expression]) = {
    condition.transform {
      case a: Attribute => sourceAliases.getOrElse(a, a)
    }
  }
}

/**
 * Push [[Filter]] operators through [[Generate]] operators. Parts of the predicate that reference
 * attributes generated in [[Generate]] will remain above, and the rest should be pushed beneath.
 */
object PushPredicateThroughGenerate extends Rule[LogicalPlan] with PredicateHelper {

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, g: Generate) =>
      // Predicates that reference attributes produced by the `Generate` operator cannot
      // be pushed below the operator.
      val (pushDown, stayUp) = splitConjunctivePredicates(condition).partition {
        conjunct => conjunct.references subsetOf g.child.outputSet
      }
      if (pushDown.nonEmpty) {
        val pushDownPredicate = pushDown.reduce(And)
        val withPushdown = Generate(g.generator, join = g.join, outer = g.outer,
          g.qualifier, g.generatorOutput, Filter(pushDownPredicate, g.child))
        stayUp.reduceOption(And).map(Filter(_, withPushdown)).getOrElse(withPushdown)
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
 * And also Pushes down the join filter, where the `condition` can be evaluated using only the
 * attributes of the left or right side of sub query when applicable.
 *
 * Check https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior for more details
 */
object PushPredicateThroughJoin extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * Splits join condition expressions into three categories based on the attributes required
   * to evaluate them.
   * @return (canEvaluateInLeft, canEvaluateInRight, haveToEvaluateInBoth)
   */
  private def split(condition: Seq[Expression], left: LogicalPlan, right: LogicalPlan) = {
    val (leftEvaluateCondition, rest) =
        condition.partition(_.references subsetOf left.outputSet)
    val (rightEvaluateCondition, commonCondition) =
        rest.partition(_.references subsetOf right.outputSet)

    (leftEvaluateCondition, rightEvaluateCondition, commonCondition)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // push the where condition down into join filter
    case f @ Filter(filterCondition, Join(left, right, joinType, joinCondition)) =>
      val (leftFilterConditions, rightFilterConditions, commonFilterCondition) =
        split(splitConjunctivePredicates(filterCondition), left, right)

      joinType match {
        case Inner =>
          // push down the single side `where` condition into respective sides
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = (commonFilterCondition ++ joinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, Inner, newJoinCond)
        case RightOuter =>
          // push down the right side only `where` condition
          val newLeft = left
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, RightOuter, newJoinCond)

          (leftFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        case _ @ (LeftOuter | LeftSemi) =>
          // push down the left side only `where` condition
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, joinType, newJoinCond)

          (rightFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        case FullOuter => f // DO Nothing for Full Outer Join
      }

    // push down the join filter into sub query scanning if applicable
    case f @ Join(left, right, joinType, joinCondition) =>
      val (leftJoinConditions, rightJoinConditions, commonJoinCondition) =
        split(joinCondition.map(splitConjunctivePredicates).getOrElse(Nil), left, right)

      joinType match {
        case _ @ (Inner | LeftSemi) =>
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
        case FullOuter => f
      }
  }
}

/**
 * Removes [[Cast Casts]] that are unnecessary because the input is already the correct type.
 */
object SimplifyCasts extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Cast(e, dataType) if e.dataType == dataType => e
  }
}

/**
 * Removes [[UnaryPositive]] identify function
 */
object RemovePositive extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case UnaryPositive(child) => child
  }
}

/**
 * Combines two adjacent [[Limit]] operators into one, merging the
 * expressions into one single expression.
 */
object CombineLimits extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case ll @ Limit(le, nl @ Limit(ne, grandChild)) =>
      Limit(If(LessThan(ne, le), ne, le), grandChild)
  }
}

/**
 * Removes the inner case conversion expressions that are unnecessary because
 * the inner conversion is overwritten by the outer one.
 */
object SimplifyCaseConversionExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case Upper(Upper(child)) => Upper(child)
      case Upper(Lower(child)) => Upper(child)
      case Lower(Upper(child)) => Lower(child)
      case Lower(Lower(child)) => Lower(child)
    }
  }
}

/**
 * Speeds up aggregates on fixed-precision decimals by executing them on unscaled Long values.
 *
 * This uses the same rules for increasing the precision and scale of the output as
 * [[org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion.DecimalPrecision]].
 */
object DecimalAggregates extends Rule[LogicalPlan] {
  import Decimal.MAX_LONG_DIGITS

  /** Maximum number of decimal digits representable precisely in a Double */
  private val MAX_DOUBLE_DIGITS = 15

  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Sum(e @ DecimalType.Expression(prec, scale)) if prec + 10 <= MAX_LONG_DIGITS =>
      MakeDecimal(Sum(UnscaledValue(e)), prec + 10, scale)

    case Average(e @ DecimalType.Expression(prec, scale)) if prec + 4 <= MAX_DOUBLE_DIGITS =>
      Cast(
        Divide(Average(UnscaledValue(e)), Literal.create(math.pow(10.0, scale), DoubleType)),
        DecimalType(prec + 4, scale + 4))
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
    case Project(projectList, LocalRelation(output, data)) =>
      val projection = new InterpretedProjection(projectList, output)
      LocalRelation(projectList.map(_.toAttribute), data.map(projection))
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
