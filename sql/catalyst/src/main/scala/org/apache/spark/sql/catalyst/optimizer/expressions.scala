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
import scala.collection.mutable.{ArrayBuffer, Map, Stack}

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
 * Substitutes [[Expression Expressions]] which can be statically evaluated with their corresponding
 * value in conjunctive [[Expression Expressions]]
 * eg.
 * {{{
 *   i = 5 AND j = i + 3            => ... i = 5 AND j = 8
 *   abs(i) = 5 AND j <= abs(i) + 3 => ... abs(i) = 5 AND j <= 8
 * }}}
 *
 * Approach used:
 * - Populate a mapping of expression => constant value by looking at all the deterministic equals
 *   predicates
 * - Using this mapping, replace occurrence of the expressions with the corresponding constant
 *   values in the AND node.
 */
object ConstantPropagation extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case f: Filter => f.mapExpressions(e => traverse(e, Some(false))._1)

    // Constant propagation can remove equalities from [[Join]] conditions as they don't add any
    // real value, but [[ExtractEquiJoinKeys]] is not prepared to handle that situation.
    // SPARK-30598 can solve this issue.
    case j: Join => j

    case o => o.mapExpressions(e => traverse(e, None)._1)
  }

  /**
   * Traverse a condition as a tree and replace expressions with constant values.
   * - On matching [[EqualTo]] or [[EqualNullSafe]], recursively traverse left and right children
   *   and propagate the expression => constant mapping.
   * - On matching [[And]], recursively traverse left subtree and collect propagated mapping to
   *   replace expressions to constants in right subtree. Then recursively traverse right subtree
   *   and collect propagated mapping to replace expressions to constants in left subtree.
   * - Otherwise, recursively traverse each children, propagate empty mapping.
   * - During expression tree traversal tracks a boolean context that controls if constant
   *   propagation of a nullable expression can be safely applied.
   *   - E.g. in the case of `WHERE a = c AND f(a)` or `IF(a = c AND f(a), ..., ...)` where `a` is a
   *     nullable expression and `c` is a constant the null result of `a = c AND f(a)` means the
   *     same as if it resulted `false` therefore constant propagation can be safely applied (`a = c
   *     AND f(a)` => `a = c AND f(c)`). This context is represented by `Some(False)`.
   *   - In the case of `SELECT a = c AND f(a)` the `null` result really means `null`. In this
   *     context constant propagation can't be applied safely. This context is represented by
   *     `None`.
   *   - There is also a 3rd context due to an enclosing `Not` in which the context flips. E.g.
   *     constant propagation can't be applied on `WHERE NOT(a = c AND f(a))` but can be again on
   *     `WHERE NOT(IF(..., NOT(a = c AND f(a)), ...)`. This context is represented by `Some(True)`.
   * @param expression expression to be traversed
   * @param nullValue optional boolean that a null boolean expression result can be considered to
   * @return A tuple including:
   *         1. Expression: possibly changed expression after traversal
   *         2. Map[Expression, Literal]: propagated mapping of expression => constant
   */
  private def traverse(
      expression: Expression,
      nullValue: Option[Boolean] = None): (Expression, Map[Expression, Literal]) =
    expression match {
      case et @ EqualTo(left, right: Literal) if safeToReplace(left, nullValue) =>
        (et.mapChildren(traverse(_)._1), Map(left.canonicalized -> right))
      case et @ EqualTo(left: Literal, right) if safeToReplace(right, nullValue) =>
        (et.mapChildren(traverse(_)._1), Map(right.canonicalized -> left))
      case ens @ EqualNullSafe(left, right: Literal) if safeToReplace(left, nullValue) =>
        (ens.mapChildren(traverse(_)._1), Map(left.canonicalized -> right))
      case ens @ EqualNullSafe(left: Literal, right) if safeToReplace(right, nullValue) =>
        (ens.mapChildren(traverse(_)._1), Map(right.canonicalized -> left))
      case a @ And(left, right) =>
        val (newLeft, equalityPredicatesLeft) = traverse(left, nullValue)
        val replacedRight = replaceConstants(right, equalityPredicatesLeft)
        val (replacedNewRight, equalityPredicatesRight) = traverse(replacedRight, nullValue)
        val replacedNewLeft = replaceConstants(newLeft, equalityPredicatesRight)
        val newAnd = a.withNewChildren(Seq(replacedNewLeft, replacedNewRight))
        (newAnd, equalityPredicatesLeft ++= equalityPredicatesRight)
      case o: Or => (o.mapChildren(traverse(_, nullValue)._1), Map.empty)
      case n: Not => (n.mapChildren(traverse(_, nullValue.map(!_))._1), Map.empty)
      case i @ If(predicate, trueValue, falseValue) =>
        val newPredicate = traverse(predicate, Some(false))._1
        val newTrueValue = traverse(trueValue, nullValue)._1
        val newFalseValue = traverse(falseValue, nullValue)._1
        val newIf = i.withNewChildren(Seq(newPredicate, newTrueValue, newFalseValue))
        (newIf, Map.empty)
      case cw @ CaseWhen(branches, elseValue) =>
        val newBranches = branches.flatMap {
          case (w, t) => Seq(traverse(w, Some(false))._1, traverse(t, nullValue)._1)
        }
        val newElseValue = elseValue.map(traverse(_, nullValue)._1)
        val newCaseWhen = cw.withNewChildren(newBranches ++ newElseValue)
        (newCaseWhen, Map.empty)
      case af @ ArrayFilter(argument, lf: LambdaFunction) =>
        val newArgument = traverse(argument, nullValue)._1
        val newLF: LambdaFunction = traverseLambdaFunction(lf, false)
        val newArrayFilter = af.withNewChildren(Seq(newArgument, newLF))
        (newArrayFilter, Map.empty)
      case ae @ ArrayExists(argument, lf: LambdaFunction) =>
        val newArgument = traverse(argument, nullValue)._1
        val newLF: LambdaFunction = traverseLambdaFunction(lf,
          SQLConf.get.getConf(SQLConf.LEGACY_ARRAY_EXISTS_FOLLOWS_THREE_VALUED_LOGIC))
        val newArrayExists = ae.withNewChildren(Seq(newArgument, newLF))
        (newArrayExists, Map.empty)
      case mf @ MapFilter(argument, lf: LambdaFunction) =>
        val newArgument = traverse(argument, nullValue)._1
        val newLF: LambdaFunction = traverseLambdaFunction(lf, false)
        val newMapFilter = mf.withNewChildren(Seq(newArgument, newLF))
        (newMapFilter, Map.empty)

      // Actually most of the expressions could propagate nullValue safely.
      // We use these few in tests.
      case a: Alias => (a.mapChildren(traverse(_, nullValue)._1), Map.empty)
      case ca: CreateArray => (ca.mapChildren(traverse(_, nullValue)._1), Map.empty)
      case gai: GetArrayItem => (gai.mapChildren(traverse(_, nullValue)._1), Map.empty)
      case cm: CreateMap => (cm.mapChildren(traverse(_, nullValue)._1), Map.empty)
      case cmv: GetMapValue => (cmv.mapChildren(traverse(_, nullValue)._1), Map.empty)

      // Stay on the safe side and don't propagate nullValue.
      case o => (o.mapChildren(traverse(_)._1), Map.empty)
    }

  private def traverseLambdaFunction(lf: LambdaFunction, threeValuedLogic: Boolean) = {
    val newFunction = traverse(lf.function, if (threeValuedLogic) None else Some(false))._1
    lf.withNewChildren(newFunction +: lf.arguments).asInstanceOf[LambdaFunction]
  }

  private def safeToReplace(expression : Expression, nullValue: Option[Boolean]) =
    !expression.foldable && expression.deterministic &&
      (!expression.nullable || nullValue.contains(false))

  private def replaceConstants(expression: Expression, constants: Map[Expression, Literal]) =
    if (constants.isEmpty) {
      expression
    } else {
      expression transform {
        case e if constants.contains(e.canonicalized) => constants(e.canonicalized)
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
          && !v.isInstanceOf[CreateNamedStruct]
          && !newList.head.isInstanceOf[CreateNamedStruct]) {
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

      case Not(IsNull(e)) => IsNotNull(e)
      case Not(IsNotNull(e)) => IsNull(e)
    }
  }
}


/**
 * Simplifies binary comparisons with semantically-equal expressions:
 * 1) Replace '<=>' with 'true' literal.
 * 2) Replace '=', '<=', and '>=' with 'true' literal if both operands are non-nullable.
 * 3) Replace '<' and '>' with 'false' literal if both operands are non-nullable.
 */
object SimplifyBinaryComparison
  extends Rule[LogicalPlan] with PredicateHelper with ConstraintHelper {

  private def canSimplifyComparison(
      left: Expression,
      right: Expression,
      notNullExpressions: => ExpressionSet): Boolean = {
    if (left.semanticEquals(right)) {
      (!left.nullable && !right.nullable) || notNullExpressions.contains(left)
    } else {
      false
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case l: LogicalPlan =>
      lazy val notNullExpressions = ExpressionSet(l match {
        case Filter(fc, _) =>
          splitConjunctivePredicates(fc).collect {
            case i: IsNotNull => i.child
          }
        case _ => Seq.empty
      })

      l transformExpressionsUp {
        // True with equality
        case a EqualNullSafe b if a.semanticEquals(b) => TrueLiteral
        case a EqualTo b if canSimplifyComparison(a, b, notNullExpressions) => TrueLiteral
        case a GreaterThanOrEqual b if canSimplifyComparison(a, b, notNullExpressions) =>
          TrueLiteral
        case a LessThanOrEqual b if canSimplifyComparison(a, b, notNullExpressions) => TrueLiteral

        // False with inequality
        case a GreaterThan b if canSimplifyComparison(a, b, notNullExpressions) => FalseLiteral
        case a LessThan b if canSimplifyComparison(a, b, notNullExpressions) => FalseLiteral
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
    case Like(input, Literal(pattern, StringType), escapeChar) =>
      if (pattern == null) {
        // If pattern is null, return null value directly, since "col like null" == null.
        Literal(null, BooleanType)
      } else {
        val escapeStr = String.valueOf(escapeChar)
        pattern.toString match {
          case startsWith(prefix) if !prefix.endsWith(escapeStr) =>
            StartsWith(input, Literal(prefix))
          case endsWith(postfix) =>
            EndsWith(input, Literal(postfix))
          // 'a%a' pattern is basically same with 'a%' && '%a'.
          // However, the additional `Length` condition is required to prevent 'a' match 'a%a'.
          case startsAndEndsWith(prefix, postfix) if !prefix.endsWith(escapeStr) =>
            And(GreaterThanOrEqual(Length(input), Literal(prefix.length + postfix.length)),
              And(StartsWith(input, Literal(prefix)), EndsWith(input, Literal(postfix))))
          case contains(infix) if !infix.endsWith(escapeStr) =>
            Contains(input, Literal(infix))
          case equalTo(str) =>
            EqualTo(input, Literal(str))
          case _ => Like(input, Literal.create(pattern, StringType), escapeChar)
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
      case e @ AggregateExpression(Count(exprs), _, _, _, _) if exprs.forall(isNullLiteral) =>
        Cast(Literal(0L), e.dataType, Option(SQLConf.get.sessionLocalTimeZone))
      case ae @ AggregateExpression(Count(exprs), _, false, _, _) if !exprs.exists(_.nullable) =>
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
