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

import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types._

/*
 * Optimization rules defined in this file should not affect the structure of the logical plan.
 */


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
 * Reorder associative integral-type operators and fold all constants into one.
 */
object ReorderAssociativeOperator extends Rule[LogicalPlan] {
  private def flattenAdd(
    expression: Expression,
    groupSet: ExpressionSet): Seq[Expression] = expression match {
    case expr @ Add(l, r) if !groupSet.contains(expr) =>
      flattenAdd(l, groupSet) ++ flattenAdd(r, groupSet)
    case other => other :: Nil
  }

  private def flattenMultiply(
    expression: Expression,
    groupSet: ExpressionSet): Seq[Expression] = expression match {
    case expr @ Multiply(l, r) if !groupSet.contains(expr) =>
      flattenMultiply(l, groupSet) ++ flattenMultiply(r, groupSet)
    case other => other :: Nil
  }

  private def collectGroupingExpressions(plan: LogicalPlan): ExpressionSet = plan match {
    case Aggregate(groupingExpressions, aggregateExpressions, child) =>
      ExpressionSet.apply(groupingExpressions)
    case _ => ExpressionSet(Seq())
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan =>
      // We have to respect aggregate expressions which exists in grouping expressions when plan
      // is an Aggregate operator, otherwise the optimized expression could not be derived from
      // grouping expressions.
      val groupingExpressionSet = collectGroupingExpressions(q)
      q transformExpressionsDown {
      case a: Add if a.deterministic && a.dataType.isInstanceOf[IntegralType] =>
        val (foldables, others) = flattenAdd(a, groupingExpressionSet).partition(_.foldable)
        if (foldables.size > 1) {
          val foldableExpr = foldables.reduce((x, y) => Add(x, y))
          val c = Literal.create(foldableExpr.eval(EmptyRow), a.dataType)
          if (others.isEmpty) c else Add(others.reduce((x, y) => Add(x, y)), c)
        } else {
          a
        }
      case m: Multiply if m.deterministic && m.dataType.isInstanceOf[IntegralType] =>
        val (foldables, others) = flattenMultiply(m, groupingExpressionSet).partition(_.foldable)
        if (foldables.size > 1) {
          val foldableExpr = foldables.reduce((x, y) => Multiply(x, y))
          val c = Literal.create(foldableExpr.eval(EmptyRow), m.dataType)
          if (others.isEmpty) c else Multiply(others.reduce((x, y) => Multiply(x, y)), c)
        } else {
          m
        }
    }
  }
}


/**
 * Optimize IN predicates:
 * 1. Removes literal repetitions.
 * 2. Replaces [[In (value, seq[Literal])]] with optimized version
 *    [[InSet (value, HashSet[Literal])]] which is much faster.
 */
case class OptimizeIn(conf: CatalystConf) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsDown {
      case expr @ In(v, list) if expr.inSetConvertible =>
        val newList = ExpressionSet(list).toSeq
        if (newList.size > conf.optimizerInSetConversionThreshold) {
          val hSet = newList.map(e => e.eval(EmptyRow))
          InSet(v, HashSet() ++ hSet)
        } else if (newList.size < list.size) {
          expr.copy(list = newList)
        } else { // newList.length == list.length
          expr
        }
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
      case TrueLiteral And e => e
      case e And TrueLiteral => e
      case FalseLiteral Or e => e
      case e Or FalseLiteral => e

      case FalseLiteral And _ => FalseLiteral
      case _ And FalseLiteral => FalseLiteral
      case TrueLiteral Or _ => TrueLiteral
      case _ Or TrueLiteral => TrueLiteral

      case a And b if a.semanticEquals(b) => a
      case a Or b if a.semanticEquals(b) => a

      case a And (b Or c) if Not(a).semanticEquals(b) => And(a, c)
      case a And (b Or c) if Not(a).semanticEquals(c) => And(a, b)
      case (a Or b) And c if a.semanticEquals(Not(c)) => And(b, c)
      case (a Or b) And c if b.semanticEquals(Not(c)) => And(a, c)

      case a Or (b And c) if Not(a).semanticEquals(b) => Or(a, c)
      case a Or (b And c) if Not(a).semanticEquals(c) => Or(a, b)
      case (a And b) Or c if a.semanticEquals(Not(c)) => Or(b, c)
      case (a And b) Or c if b.semanticEquals(Not(c)) => Or(a, c)

      // Common factor elimination for conjunction
      case and @ (left And right) =>
        // 1. Split left and right to get the disjunctive predicates,
        //   i.e. lhs = (a, b), rhs = (a, c)
        // 2. Find the common predict between lhsSet and rhsSet, i.e. common = (a)
        // 3. Remove common predict from lhsSet and rhsSet, i.e. ldiff = (b), rdiff = (c)
        // 4. Apply the formula, get the optimized predicate: common || (ldiff && rdiff)
        val lhs = splitDisjunctivePredicates(left)
        val rhs = splitDisjunctivePredicates(right)
        val common = lhs.filter(e => rhs.exists(e.semanticEquals))
        if (common.isEmpty) {
          // No common factors, return the original predicate
          and
        } else {
          val ldiff = lhs.filterNot(e => common.exists(e.semanticEquals))
          val rdiff = rhs.filterNot(e => common.exists(e.semanticEquals))
          if (ldiff.isEmpty || rdiff.isEmpty) {
            // (a || b || c || ...) && (a || b) => (a || b)
            common.reduce(Or)
          } else {
            // (a || b || c || ...) && (a || b || d || ...) =>
            // ((c || ...) && (d || ...)) || a || b
            (common :+ And(ldiff.reduce(Or), rdiff.reduce(Or))).reduce(Or)
          }
        }

      // Common factor elimination for disjunction
      case or @ (left Or right) =>
        // 1. Split left and right to get the conjunctive predicates,
        //   i.e.  lhs = (a, b), rhs = (a, c)
        // 2. Find the common predict between lhsSet and rhsSet, i.e. common = (a)
        // 3. Remove common predict from lhsSet and rhsSet, i.e. ldiff = (b), rdiff = (c)
        // 4. Apply the formula, get the optimized predicate: common && (ldiff || rdiff)
        val lhs = splitConjunctivePredicates(left)
        val rhs = splitConjunctivePredicates(right)
        val common = lhs.filter(e => rhs.exists(e.semanticEquals))
        if (common.isEmpty) {
          // No common factors, return the original predicate
          or
        } else {
          val ldiff = lhs.filterNot(e => common.exists(e.semanticEquals))
          val rdiff = rhs.filterNot(e => common.exists(e.semanticEquals))
          if (ldiff.isEmpty || rdiff.isEmpty) {
            // (a && b) || (a && b && c && ...) => a && b
            common.reduce(And)
          } else {
            // (a && b && c && ...) || (a && b && d && ...) =>
            // ((c && ...) || (d && ...)) && a && b
            (common :+ Or(ldiff.reduce(And), rdiff.reduce(And))).reduce(And)
          }
        }

      case Not(TrueLiteral) => FalseLiteral
      case Not(FalseLiteral) => TrueLiteral

      case Not(a GreaterThan b) => LessThanOrEqual(a, b)
      case Not(a GreaterThanOrEqual b) => LessThan(a, b)

      case Not(a LessThan b) => GreaterThanOrEqual(a, b)
      case Not(a LessThanOrEqual b) => GreaterThan(a, b)

      case Not(a Or b) => And(Not(a), Not(b))
      case Not(a And b) => Or(Not(a), Not(b))

      case Not(Not(e)) => e
    }
  }
}


/**
 * Simplifies binary comparisons with semantically-equal expressions:
 * 1) Replace '<=>' with 'true' literal.
 * 2) Replace '=', '<=', and '>=' with 'true' literal if both operands are non-nullable.
 * 3) Replace '<' and '>' with 'false' literal if both operands are non-nullable.
 */
object SimplifyBinaryComparison extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      // True with equality
      case a EqualNullSafe b if a.semanticEquals(b) => TrueLiteral
      case a EqualTo b if !a.nullable && !b.nullable && a.semanticEquals(b) => TrueLiteral
      case a GreaterThanOrEqual b if !a.nullable && !b.nullable && a.semanticEquals(b) =>
        TrueLiteral
      case a LessThanOrEqual b if !a.nullable && !b.nullable && a.semanticEquals(b) => TrueLiteral

      // False with inequality
      case a GreaterThan b if !a.nullable && !b.nullable && a.semanticEquals(b) => FalseLiteral
      case a LessThan b if !a.nullable && !b.nullable && a.semanticEquals(b) => FalseLiteral
    }
  }
}


/**
 * Simplifies conditional expressions (if / case).
 */
object SimplifyConditionals extends Rule[LogicalPlan] with PredicateHelper {
  private def falseOrNullLiteral(e: Expression): Boolean = e match {
    case FalseLiteral => true
    case Literal(null, _) => true
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case If(TrueLiteral, trueValue, _) => trueValue
      case If(FalseLiteral, _, falseValue) => falseValue
      case If(Literal(null, _), _, falseValue) => falseValue

      case e @ CaseWhen(branches, elseValue) if branches.exists(x => falseOrNullLiteral(x._1)) =>
        // If there are branches that are always false, remove them.
        // If there are no more branches left, just use the else value.
        // Note that these two are handled together here in a single case statement because
        // otherwise we cannot determine the data type for the elseValue if it is None (i.e. null).
        val newBranches = branches.filter(x => !falseOrNullLiteral(x._1))
        if (newBranches.isEmpty) {
          elseValue.getOrElse(Literal.create(null, e.dataType))
        } else {
          e.copy(branches = newBranches)
        }

      case e @ CaseWhen(branches, _) if branches.headOption.map(_._1) == Some(TrueLiteral) =>
        // If the first branch is a true literal, remove the entire CaseWhen and use the value
        // from that. Note that CaseWhen.branches should never be empty, and as a result the
        // headOption (rather than head) added above is just an extra (and unnecessary) safeguard.
        branches.head._2
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
  private val startsAndEndsWith = "([^_%]+)%([^_%]+)".r
  private val contains = "%([^_%]+)%".r
  private val equalTo = "([^_%]*)".r

  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Like(input, Literal(pattern, StringType)) =>
      pattern.toString match {
        case startsWith(prefix) if !prefix.endsWith("\\") =>
          StartsWith(input, Literal(prefix))
        case endsWith(postfix) =>
          EndsWith(input, Literal(postfix))
        // 'a%a' pattern is basically same with 'a%' && '%a'.
        // However, the additional `Length` condition is required to prevent 'a' match 'a%a'.
        case startsAndEndsWith(prefix, postfix) if !prefix.endsWith("\\") =>
          And(GreaterThanOrEqual(Length(input), Literal(prefix.size + postfix.size)),
            And(StartsWith(input, Literal(prefix)), EndsWith(input, Literal(postfix))))
        case contains(infix) if !infix.endsWith("\\") =>
          Contains(input, Literal(infix))
        case equalTo(str) =>
          EqualTo(input, Literal(str))
        case _ =>
          Like(input, Literal.create(pattern, StringType))
      }
  }
}


/**
 * Replaces [[Expression Expressions]] that can be statically evaluated with
 * equivalent [[Literal]] values. This rule is more specific with
 * Null value propagation from bottom to top of the expression tree.
 */
object NullPropagation extends Rule[LogicalPlan] {
  private def nonNullLiteral(e: Expression): Boolean = e match {
    case Literal(null, _) => false
    case _ => true
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case e @ WindowExpression(Cast(Literal(0L, _), _), _) =>
        Cast(Literal(0L), e.dataType)
      case e @ AggregateExpression(Count(exprs), _, _, _) if !exprs.exists(nonNullLiteral) =>
        Cast(Literal(0L), e.dataType)
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
      case ae @ AggregateExpression(Count(exprs), _, false, _) if !exprs.exists(_.nullable) =>
        // This rule should be only triggered when isDistinct field is false.
        ae.copy(aggregateFunction = Count(Literal(1)))

      // For Coalesce, remove null literals.
      case e @ Coalesce(children) =>
        val newChildren = children.filter(nonNullLiteral)
        if (newChildren.isEmpty) {
          Literal.create(null, e.dataType)
        } else if (newChildren.length == 1) {
          newChildren.head
        } else {
          Coalesce(newChildren)
        }

      case e @ Substring(Literal(null, _), _, _) => Literal.create(null, e.dataType)
      case e @ Substring(_, Literal(null, _), _) => Literal.create(null, e.dataType)
      case e @ Substring(_, _, Literal(null, _)) => Literal.create(null, e.dataType)

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

      // If the value expression is NULL then transform the In expression to
      // Literal(null)
      case In(Literal(null, _), list) => Literal.create(null, BooleanType)

    }
  }
}


/**
 * Propagate foldable expressions:
 * Replace attributes with aliases of the original foldable expressions if possible.
 * Other optimizations will take advantage of the propagated foldable expressions.
 *
 * {{{
 *   SELECT 1.0 x, 'abc' y, Now() z ORDER BY x, y, 3
 *   ==>  SELECT 1.0 x, 'abc' y, Now() z ORDER BY 1.0, 'abc', Now()
 * }}}
 */
object FoldablePropagation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val foldableMap = AttributeMap(plan.flatMap {
      case Project(projectList, _) => projectList.collect {
        case a: Alias if a.child.foldable => (a.toAttribute, a)
      }
      case _ => Nil
    })
    val replaceFoldable: PartialFunction[Expression, Expression] = {
      case a: AttributeReference if foldableMap.contains(a) => foldableMap(a)
    }

    if (foldableMap.isEmpty) {
      plan
    } else {
      var stop = false
      CleanupAliases(plan.transformUp {
        // A leaf node should not stop the folding process (note that we are traversing up the
        // tree, starting at the leaf nodes); so we are allowing it.
        case l: LeafNode =>
          l

        // We can only propagate foldables for a subset of unary nodes.
        case u: UnaryNode if !stop && canPropagateFoldables(u) =>
          u.transformExpressions(replaceFoldable)

        // Allow inner joins. We do not allow outer join, although its output attributes are
        // derived from its children, they are actually different attributes: the output of outer
        // join is not always picked from its children, but can also be null.
        // TODO(cloud-fan): It seems more reasonable to use new attributes as the output attributes
        // of outer join.
        case j @ Join(_, _, Inner, _) if !stop =>
          j.transformExpressions(replaceFoldable)

        // We can fold the projections an expand holds. However expand changes the output columns
        // and often reuses the underlying attributes; so we cannot assume that a column is still
        // foldable after the expand has been applied.
        // TODO(hvanhovell): Expand should use new attributes as the output attributes.
        case expand: Expand if !stop =>
          val newExpand = expand.copy(projections = expand.projections.map { projection =>
            projection.map(_.transform(replaceFoldable))
          })
          stop = true
          newExpand

        case other =>
          stop = true
          other
      })
    }
  }

  /**
   * Whitelist of all [[UnaryNode]]s for which allow foldable propagation.
   */
  private def canPropagateFoldables(u: UnaryNode): Boolean = u match {
    case _: Project => true
    case _: Filter => true
    case _: SubqueryAlias => true
    case _: Aggregate => true
    case _: Window => true
    case _: Sample => true
    case _: GlobalLimit => true
    case _: LocalLimit => true
    case _: Generate => true
    case _: Distinct => true
    case _: AppendColumns => true
    case _: AppendColumnsWithObject => true
    case _: BroadcastHint => true
    case _: RepartitionByExpression => true
    case _: Repartition => true
    case _: Sort => true
    case _: TypedFilter => true
    case _ => false
  }
}


/**
 * Optimizes expressions by replacing according to CodeGen configuration.
 */
case class OptimizeCodegen(conf: CatalystConf) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case e: CaseWhen if canCodegen(e) => e.toCodegen()
  }

  private def canCodegen(e: CaseWhen): Boolean = {
    val numBranches = e.branches.size + e.elseValue.size
    numBranches <= conf.maxCaseBranchesForCodegen
  }
}


/**
 * Removes [[Cast Casts]] that are unnecessary because the input is already the correct type.
 */
object SimplifyCasts extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Cast(e, dataType) if e.dataType == dataType => e
    case c @ Cast(e, dataType) => (e.dataType, dataType) match {
      case (ArrayType(from, false), ArrayType(to, true)) if from == to => e
      case (MapType(fromKey, fromValue, false), MapType(toKey, toValue, true))
        if fromKey == toKey && fromValue == toValue => e
      case _ => c
      }
  }
}


/**
 * Removes nodes that are not necessary.
 */
object RemoveDispensableExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case UnaryPositive(child) => child
    case PromotePrecision(child) => child
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
