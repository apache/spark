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
import scala.collection.mutable.{ArrayBuffer, Stack}

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf
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
 * Substitutes [[Attribute Attributes]] which can be statically evaluated with their corresponding
 * value in conjunctive [[Expression Expressions]]
 * eg.
 * {{{
 *   SELECT * FROM table WHERE i = 5 AND j = i + 3
 *   ==>  SELECT * FROM table WHERE i = 5 AND j = 8
 * }}}
 *
 * Approach used:
 * - Populate a mapping of attribute => constant value by looking at all the equals predicates
 * - Using this mapping, replace occurrence of the attributes with the corresponding constant values
 *   in the AND node.
 */
object ConstantPropagation extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case f: Filter =>
      val (newCondition, _) = traverse(f.condition, replaceChildren = true)
      if (newCondition.isDefined) {
        f.copy(condition = newCondition.get)
      } else {
        f
      }
  }

  type EqualityPredicates = Seq[((AttributeReference, Literal), BinaryComparison)]

  /**
   * Traverse a condition as a tree and replace attributes with constant values.
   * - On matching [[And]], recursively traverse each children and get propagated mappings.
   *   If the current node is not child of another [[And]], replace all occurrences of the
   *   attributes with the corresponding constant values.
   * - If a child of [[And]] is [[EqualTo]] or [[EqualNullSafe]], propagate the mapping
   *   of attribute => constant.
   * - On matching [[Or]] or [[Not]], recursively traverse each children, propagate empty mapping.
   * - Otherwise, stop traversal and propagate empty mapping.
   * @param condition condition to be traversed
   * @param replaceChildren whether to replace attributes with constant values in children
   * @return A tuple including:
   *         1. Option[Expression]: optional changed condition after traversal
   *         2. EqualityPredicates: propagated mapping of attribute => constant
   */
  private def traverse(condition: Expression, replaceChildren: Boolean)
    : (Option[Expression], EqualityPredicates) =
    condition match {
      case e @ EqualTo(left: AttributeReference, right: Literal) => (None, Seq(((left, right), e)))
      case e @ EqualTo(left: Literal, right: AttributeReference) => (None, Seq(((right, left), e)))
      case e @ EqualNullSafe(left: AttributeReference, right: Literal) =>
        (None, Seq(((left, right), e)))
      case e @ EqualNullSafe(left: Literal, right: AttributeReference) =>
        (None, Seq(((right, left), e)))
      case a: And =>
        val (newLeft, equalityPredicatesLeft) = traverse(a.left, replaceChildren = false)
        val (newRight, equalityPredicatesRight) = traverse(a.right, replaceChildren = false)
        val equalityPredicates = equalityPredicatesLeft ++ equalityPredicatesRight
        val newSelf = if (equalityPredicates.nonEmpty && replaceChildren) {
          Some(And(replaceConstants(newLeft.getOrElse(a.left), equalityPredicates),
            replaceConstants(newRight.getOrElse(a.right), equalityPredicates)))
        } else {
          if (newLeft.isDefined || newRight.isDefined) {
            Some(And(newLeft.getOrElse(a.left), newRight.getOrElse(a.right)))
          } else {
            None
          }
        }
        (newSelf, equalityPredicates)
      case o: Or =>
        // Ignore the EqualityPredicates from children since they are only propagated through And.
        val (newLeft, _) = traverse(o.left, replaceChildren = true)
        val (newRight, _) = traverse(o.right, replaceChildren = true)
        val newSelf = if (newLeft.isDefined || newRight.isDefined) {
          Some(Or(left = newLeft.getOrElse(o.left), right = newRight.getOrElse((o.right))))
        } else {
          None
        }
        (newSelf, Seq.empty)
      case n: Not =>
        // Ignore the EqualityPredicates from children since they are only propagated through And.
        val (newChild, _) = traverse(n.child, replaceChildren = true)
        (newChild.map(Not), Seq.empty)
      case _ => (None, Seq.empty)
    }

  private def replaceConstants(condition: Expression, equalityPredicates: EqualityPredicates)
    : Expression = {
    val constantsMap = AttributeMap(equalityPredicates.map(_._1))
    val predicates = equalityPredicates.map(_._2).toSet
    def replaceConstants0(expression: Expression) = expression transform {
      case a: AttributeReference => constantsMap.getOrElse(a, a)
    }
    condition transform {
      case e @ EqualTo(_, _) if !predicates.contains(e) => replaceConstants0(e)
      case e @ EqualNullSafe(_, _) if !predicates.contains(e) => replaceConstants0(e)
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
    case _ => ExpressionSet(Seq.empty)
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
 * 1. Converts the predicate to false when the list is empty and
 *    the value is not nullable.
 * 2. Removes literal repetitions.
 * 3. Replaces [[In (value, seq[Literal])]] with optimized version
 *    [[InSet (value, HashSet[Literal])]] which is much faster.
 */
object OptimizeIn extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsDown {
      case In(v, list) if list.isEmpty =>
        // When v is not nullable, the following expression will be optimized
        // to FalseLiteral which is tested in OptimizeInSuite.scala
        If(IsNotNull(v), FalseLiteral, Literal(null, BooleanType))
      case expr @ In(v, list) if expr.inSetConvertible =>
        val newList = ExpressionSet(list).toSeq
        if (newList.length == 1
          // TODO: `EqualTo` for structural types are not working. Until SPARK-24443 is addressed,
          // TODO: we exclude them in this rule.
          && !v.isInstanceOf[CreateNamedStructLike]
          && !newList.head.isInstanceOf[CreateNamedStructLike]) {
          EqualTo(v, newList.head)
        } else if (newList.length > SQLConf.get.optimizerInSetConversionThreshold) {
          val hSet = newList.map(e => e.eval(EmptyRow))
          InSet(v, HashSet() ++ hSet)
        } else if (newList.length < list.length) {
          expr.copy(list = newList)
        } else { // newList.length == list.length && newList.length > 1
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

      case a And b if Not(a).semanticEquals(b) =>
        If(IsNull(a), Literal.create(null, a.dataType), FalseLiteral)
      case a And b if a.semanticEquals(Not(b)) =>
        If(IsNull(b), Literal.create(null, b.dataType), FalseLiteral)

      case a Or b if Not(a).semanticEquals(b) =>
        If(IsNull(a), Literal.create(null, a.dataType), TrueLiteral)
      case a Or b if a.semanticEquals(Not(b)) =>
        If(IsNull(b), Literal.create(null, b.dataType), TrueLiteral)

      case a And b if a.semanticEquals(b) => a
      case a Or b if a.semanticEquals(b) => a

      // The following optimizations are applicable only when the operands are not nullable,
      // since the three-value logic of AND and OR are different in NULL handling.
      // See the chart:
      // +---------+---------+---------+---------+
      // | operand | operand |   OR    |   AND   |
      // +---------+---------+---------+---------+
      // | TRUE    | TRUE    | TRUE    | TRUE    |
      // | TRUE    | FALSE   | TRUE    | FALSE   |
      // | FALSE   | FALSE   | FALSE   | FALSE   |
      // | UNKNOWN | TRUE    | TRUE    | UNKNOWN |
      // | UNKNOWN | FALSE   | UNKNOWN | FALSE   |
      // | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN |
      // +---------+---------+---------+---------+

      // (NULL And (NULL Or FALSE)) = NULL, but (NULL And FALSE) = FALSE. Thus, a can't be nullable.
      case a And (b Or c) if !a.nullable && Not(a).semanticEquals(b) => And(a, c)
      // (NULL And (FALSE Or NULL)) = NULL, but (NULL And FALSE) = FALSE. Thus, a can't be nullable.
      case a And (b Or c) if !a.nullable && Not(a).semanticEquals(c) => And(a, b)
      // ((NULL Or FALSE) And NULL) = NULL, but (FALSE And NULL) = FALSE. Thus, c can't be nullable.
      case (a Or b) And c if !c.nullable && a.semanticEquals(Not(c)) => And(b, c)
      // ((FALSE Or NULL) And NULL) = NULL, but (FALSE And NULL) = FALSE. Thus, c can't be nullable.
      case (a Or b) And c if !c.nullable && b.semanticEquals(Not(c)) => And(a, c)

      // (NULL Or (NULL And TRUE)) = NULL, but (NULL Or TRUE) = TRUE. Thus, a can't be nullable.
      case a Or (b And c) if !a.nullable && Not(a).semanticEquals(b) => Or(a, c)
      // (NULL Or (TRUE And NULL)) = NULL, but (NULL Or TRUE) = TRUE. Thus, a can't be nullable.
      case a Or (b And c) if !a.nullable && Not(a).semanticEquals(c) => Or(a, b)
      // ((NULL And TRUE) Or NULL) = NULL, but (TRUE Or NULL) = TRUE. Thus, c can't be nullable.
      case (a And b) Or c if !c.nullable && a.semanticEquals(Not(c)) => Or(b, c)
      // ((TRUE And NULL) Or NULL) = NULL, but (TRUE Or NULL) = TRUE. Thus, c can't be nullable.
      case (a And b) Or c if !c.nullable && b.semanticEquals(Not(c)) => Or(a, c)

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
      case If(cond, trueValue, falseValue)
        if cond.deterministic && trueValue.semanticEquals(falseValue) => trueValue

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

      case CaseWhen(branches, _) if branches.headOption.map(_._1).contains(TrueLiteral) =>
        // If the first branch is a true literal, remove the entire CaseWhen and use the value
        // from that. Note that CaseWhen.branches should never be empty, and as a result the
        // headOption (rather than head) added above is just an extra (and unnecessary) safeguard.
        branches.head._2

      case CaseWhen(branches, _) if branches.exists(_._1 == TrueLiteral) =>
        // a branch with a true condition eliminates all following branches,
        // these branches can be pruned away
        val (h, t) = branches.span(_._1 != TrueLiteral)
        CaseWhen( h :+ t.head, None)

      case e @ CaseWhen(branches, Some(elseValue))
          if branches.forall(_._2.semanticEquals(elseValue)) =>
        // For non-deterministic conditions with side effect, we can not remove it, or change
        // the ordering. As a result, we try to remove the deterministic conditions from the tail.
        var hitNonDeterministicCond = false
        var i = branches.length
        while (i > 0 && !hitNonDeterministicCond) {
          hitNonDeterministicCond = !branches(i - 1)._1.deterministic
          if (!hitNonDeterministicCond) {
            i -= 1
          }
        }
        if (i == 0) {
          elseValue
        } else {
          e.copy(branches = branches.take(i).map(branch => (branch._1, elseValue)))
        }
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
      if (pattern == null) {
        // If pattern is null, return null value directly, since "col like null" == null.
        Literal(null, BooleanType)
      } else {
        pattern.toString match {
          case startsWith(prefix) if !prefix.endsWith("\\") =>
            StartsWith(input, Literal(prefix))
          case endsWith(postfix) =>
            EndsWith(input, Literal(postfix))
          // 'a%a' pattern is basically same with 'a%' && '%a'.
          // However, the additional `Length` condition is required to prevent 'a' match 'a%a'.
          case startsAndEndsWith(prefix, postfix) if !prefix.endsWith("\\") =>
            And(GreaterThanOrEqual(Length(input), Literal(prefix.length + postfix.length)),
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
}


/**
 * Replaces [[Expression Expressions]] that can be statically evaluated with
 * equivalent [[Literal]] values. This rule is more specific with
 * Null value propagation from bottom to top of the expression tree.
 */
object NullPropagation extends Rule[LogicalPlan] {
  private def isNullLiteral(e: Expression): Boolean = e match {
    case Literal(null, _) => true
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case e @ WindowExpression(Cast(Literal(0L, _), _, _), _) =>
        Cast(Literal(0L), e.dataType, Option(SQLConf.get.sessionLocalTimeZone))
      case e @ AggregateExpression(Count(exprs), _, _, _) if exprs.forall(isNullLiteral) =>
        Cast(Literal(0L), e.dataType, Option(SQLConf.get.sessionLocalTimeZone))
      case ae @ AggregateExpression(Count(exprs), _, false, _) if !exprs.exists(_.nullable) =>
        // This rule should be only triggered when isDistinct field is false.
        ae.copy(aggregateFunction = Count(Literal(1)))

      case IsNull(c) if !c.nullable => Literal.create(false, BooleanType)
      case IsNotNull(c) if !c.nullable => Literal.create(true, BooleanType)

      case EqualNullSafe(Literal(null, _), r) => IsNull(r)
      case EqualNullSafe(l, Literal(null, _)) => IsNull(l)

      case AssertNotNull(c, _) if !c.nullable => c

      // For Coalesce, remove null literals.
      case e @ Coalesce(children) =>
        val newChildren = children.filterNot(isNullLiteral)
        if (newChildren.isEmpty) {
          Literal.create(null, e.dataType)
        } else if (newChildren.length == 1) {
          newChildren.head
        } else {
          Coalesce(newChildren)
        }

      // If the value expression is NULL then transform the In expression to null literal.
      case In(Literal(null, _), _) => Literal.create(null, BooleanType)
      case InSubquery(Seq(Literal(null, _)), _) => Literal.create(null, BooleanType)

      // Non-leaf NullIntolerant expressions will return null, if at least one of its children is
      // a null literal.
      case e: NullIntolerant if e.children.exists(isNullLiteral) =>
        Literal.create(null, e.dataType)
    }
  }
}


/**
 * Replace attributes with aliases of the original foldable expressions if possible.
 * Other optimizations will take advantage of the propagated foldable expressions. For example,
 * this rule can optimize
 * {{{
 *   SELECT 1.0 x, 'abc' y, Now() z ORDER BY x, y, 3
 * }}}
 * to
 * {{{
 *   SELECT 1.0 x, 'abc' y, Now() z ORDER BY 1.0, 'abc', Now()
 * }}}
 * and other rules can further optimize it and remove the ORDER BY operator.
 */
object FoldablePropagation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    var foldableMap = AttributeMap(plan.flatMap {
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
      CleanupAliases(plan.transformUp {
        // We can only propagate foldables for a subset of unary nodes.
        case u: UnaryNode if foldableMap.nonEmpty && canPropagateFoldables(u) =>
          u.transformExpressions(replaceFoldable)

        // Join derives the output attributes from its child while they are actually not the
        // same attributes. For example, the output of outer join is not always picked from its
        // children, but can also be null. We should exclude these miss-derived attributes when
        // propagating the foldable expressions.
        // TODO(cloud-fan): It seems more reasonable to use new attributes as the output attributes
        // of outer join.
        case j @ Join(left, right, joinType, _, _) if foldableMap.nonEmpty =>
          val newJoin = j.transformExpressions(replaceFoldable)
          val missDerivedAttrsSet: AttributeSet = AttributeSet(joinType match {
            case _: InnerLike | LeftExistence(_) => Nil
            case LeftOuter => right.output
            case RightOuter => left.output
            case FullOuter => left.output ++ right.output
          })
          foldableMap = AttributeMap(foldableMap.baseMap.values.filterNot {
            case (attr, _) => missDerivedAttrsSet.contains(attr)
          }.toSeq)
          newJoin

        // We can not replace the attributes in `Expand.output`. If there are other non-leaf
        // operators that have the `output` field, we should put them here too.
        case expand: Expand if foldableMap.nonEmpty =>
          expand.copy(projections = expand.projections.map { projection =>
            projection.map(_.transform(replaceFoldable))
          })

        // For other plans, they are not safe to apply foldable propagation, and they should not
        // propagate foldable expressions from children.
        case other if foldableMap.nonEmpty =>
          val childrenOutputSet = AttributeSet(other.children.flatMap(_.output))
          foldableMap = AttributeMap(foldableMap.baseMap.values.filterNot {
            case (attr, _) => childrenOutputSet.contains(attr)
          }.toSeq)
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
    case _: RepartitionByExpression => true
    case _: Repartition => true
    case _: Sort => true
    case _: TypedFilter => true
    case _ => false
  }
}


/**
 * Removes [[Cast Casts]] that are unnecessary because the input is already the correct type.
 */
object SimplifyCasts extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Cast(e, dataType, _) if e.dataType == dataType => e
    case c @ Cast(e, dataType, _) => (e.dataType, dataType) match {
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
 * Combine nested [[Concat]] expressions.
 */
object CombineConcats extends Rule[LogicalPlan] {

  private def flattenConcats(concat: Concat): Concat = {
    val stack = Stack[Expression](concat)
    val flattened = ArrayBuffer.empty[Expression]
    while (stack.nonEmpty) {
      stack.pop() match {
        case Concat(children) =>
          stack.pushAll(children.reverse)
        // If `spark.sql.function.concatBinaryAsString` is false, nested `Concat` exprs possibly
        // have `Concat`s with binary output. Since `TypeCoercion` casts them into strings,
        // we need to handle the case to combine all nested `Concat`s.
        case c @ Cast(Concat(children), StringType, _) =>
          val newChildren = children.map { e => c.copy(child = e) }
          stack.pushAll(newChildren.reverse)
        case child =>
          flattened += child
      }
    }
    Concat(flattened)
  }

  private def hasNestedConcats(concat: Concat): Boolean = concat.children.exists {
    case c: Concat => true
    case c @ Cast(Concat(children), StringType, _) => true
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformExpressionsDown {
    case concat: Concat if hasNestedConcats(concat) =>
      flattenConcats(concat)
  }
}
