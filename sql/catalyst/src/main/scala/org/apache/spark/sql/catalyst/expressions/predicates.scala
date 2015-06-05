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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratedExpressionCode, Code, CodeGenContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types._

object InterpretedPredicate {
  def create(expression: Expression, inputSchema: Seq[Attribute]): (Row => Boolean) =
    create(BindReferences.bindReference(expression, inputSchema))

  def create(expression: Expression): (Row => Boolean) = {
    (r: Row) => expression.eval(r).asInstanceOf[Boolean]
  }
}

trait Predicate extends Expression {
  self: Product =>

  override def dataType: DataType = BooleanType
}

trait PredicateHelper {
  protected def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  protected def splitDisjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case Or(cond1, cond2) =>
        splitDisjunctivePredicates(cond1) ++ splitDisjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  /**
   * Returns true if `expr` can be evaluated using only the output of `plan`.  This method
   * can be used to determine when is is acceptable to move expression evaluation within a query
   * plan.
   *
   * For example consider a join between two relations R(a, b) and S(c, d).
   *
   * `canEvaluate(EqualTo(a,b), R)` returns `true` where as `canEvaluate(EqualTo(a,c), R)` returns
   * `false`.
   */
  protected def canEvaluate(expr: Expression, plan: LogicalPlan): Boolean =
    expr.references.subsetOf(plan.outputSet)
}


case class Not(child: Expression) extends UnaryExpression with Predicate with ExpectsInputTypes {
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable
  override def toString: String = s"NOT $child"

  override def expectedChildTypes: Seq[DataType] = Seq(BooleanType)

  override def eval(input: Row): Any = {
    child.eval(input) match {
      case null => null
      case b: Boolean => !b
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): Code = {
    defineCodeGen(ctx, ev, c => s"!($c)")
  }
}

/**
 * Evaluates to `true` if `list` contains `value`.
 */
case class In(value: Expression, list: Seq[Expression]) extends Predicate {
  override def children: Seq[Expression] = value +: list

  override def nullable: Boolean = true // TODO: Figure out correct nullability semantics of IN.
  override def toString: String = s"$value IN ${list.mkString("(", ",", ")")}"

  override def eval(input: Row): Any = {
    val evaluatedValue = value.eval(input)
    list.exists(e => e.eval(input) == evaluatedValue)
  }
}

/**
 * Optimized version of In clause, when all filter values of In clause are
 * static.
 */
case class InSet(value: Expression, hset: Set[Any])
  extends Predicate {

  override def children: Seq[Expression] = value :: Nil

  override def foldable: Boolean = value.foldable
  override def nullable: Boolean = true // TODO: Figure out correct nullability semantics of IN.
  override def toString: String = s"$value INSET ${hset.mkString("(", ",", ")")}"

  override def eval(input: Row): Any = {
    hset.contains(value.eval(input))
  }
}

case class And(left: Expression, right: Expression)
  extends BinaryExpression with Predicate with ExpectsInputTypes {

  override def expectedChildTypes: Seq[DataType] = Seq(BooleanType, BooleanType)

  override def symbol: String = "&&"

  override def eval(input: Row): Any = {
    val l = left.eval(input)
    if (l == false) {
       false
    } else {
      val r = right.eval(input)
      if (r == false) {
        false
      } else {
        if (l != null && r != null) {
          true
        } else {
          null
        }
      }
    }
  }
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): Code = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    s"""
      ${eval1.code}
      boolean ${ev.nullTerm} = false;
      boolean ${ev.primitiveTerm}  = false;

      if (!${eval1.nullTerm} && !${eval1.primitiveTerm}) {
      } else {
        ${eval2.code}
        if (!${eval2.nullTerm} && !${eval2.primitiveTerm}) {
        } else if (!${eval1.nullTerm} && !${eval2.nullTerm}) {
          ${ev.primitiveTerm} = true;
        } else {
          ${ev.nullTerm} = true;
        }
      }
     """
  }
}

case class Or(left: Expression, right: Expression)
  extends BinaryExpression with Predicate with ExpectsInputTypes {

  override def expectedChildTypes: Seq[DataType] = Seq(BooleanType, BooleanType)

  override def symbol: String = "||"

  override def eval(input: Row): Any = {
    val l = left.eval(input)
    if (l == true) {
      true
    } else {
      val r = right.eval(input)
      if (r == true) {
        true
      } else {
        if (l != null && r != null) {
          false
        } else {
          null
        }
      }
    }
  }
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): Code = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    s"""
      ${eval1.code}
      boolean ${ev.nullTerm} = false;
      boolean ${ev.primitiveTerm} = false;

      if (!${eval1.nullTerm} && ${eval1.primitiveTerm}) {
        ${ev.primitiveTerm} = true;
      } else {
        ${eval2.code}
        if (!${eval2.nullTerm} && ${eval2.primitiveTerm}) {
          ${ev.primitiveTerm} = true;
        } else if (!${eval1.nullTerm} && !${eval2.nullTerm}) {
          ${ev.primitiveTerm} = false;
        } else {
          ${ev.nullTerm} = true;
        }
      }
     """
  }
}

abstract class BinaryComparison extends BinaryExpression with Predicate {
  self: Product =>
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): Code = {
    left.dataType match {
      case dt: NumericType if ctx.isNativeType(dt) => defineCodeGen (ctx, ev, {
        (c1, c3) => s"$c1 $symbol $c3"
      })
      case TimestampType =>
        // java.sql.Timestamp does not have compare()
        super.genCode(ctx, ev)
      case other => defineCodeGen (ctx, ev, {
        (c1, c2) => s"$c1.compare($c2) $symbol 0"
      })
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (left.dataType != right.dataType) {
      TypeCheckResult.TypeCheckFailure(
        s"differing types in ${this.getClass.getSimpleName} " +
        s"(${left.dataType} and ${right.dataType}).")
    } else {
      checkTypesInternal(dataType)
    }
  }

  protected def checkTypesInternal(t: DataType): TypeCheckResult

  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    if (evalE1 == null) {
      null
    } else {
      val evalE2 = right.eval(input)
      if (evalE2 == null) {
        null
      } else {
        evalInternal(evalE1, evalE2)
      }
    }
  }

  protected def evalInternal(evalE1: Any, evalE2: Any): Any =
    sys.error(s"BinaryComparisons must override either eval or evalInternal")
}

object BinaryComparison {
  def unapply(b: BinaryComparison): Option[(Expression, Expression)] =
    Some((b.left, b.right))
}

case class EqualTo(left: Expression, right: Expression) extends BinaryComparison {
  override def symbol: String = "="

  override protected def checkTypesInternal(t: DataType) = TypeCheckResult.TypeCheckSuccess

  protected override def evalInternal(l: Any, r: Any) = {
    if (left.dataType != BinaryType) l == r
    else java.util.Arrays.equals(l.asInstanceOf[Array[Byte]], r.asInstanceOf[Array[Byte]])
  }
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): Code = {
    defineCodeGen(ctx, ev, ctx.equalFunc(left.dataType))
  }
}

case class EqualNullSafe(left: Expression, right: Expression) extends BinaryComparison {
  override def symbol: String = "<=>"

  override def nullable: Boolean = false

  override protected def checkTypesInternal(t: DataType) = TypeCheckResult.TypeCheckSuccess

  override def eval(input: Row): Any = {
    val l = left.eval(input)
    val r = right.eval(input)
    if (l == null && r == null) {
      true
    } else if (l == null || r == null) {
      false
    } else {
      l == r
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): Code = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val equalCode = ctx.equalFunc(left.dataType)(eval1.primitiveTerm, eval2.primitiveTerm)
    ev.nullTerm = "false"
    eval1.code + eval2.code + s"""
        final boolean ${ev.primitiveTerm} = (${eval1.nullTerm} && ${eval2.nullTerm}) ||
           (!${eval1.nullTerm} && $equalCode);
      """
  }
}

case class LessThan(left: Expression, right: Expression) extends BinaryComparison {
  override def symbol: String = "<"

  override protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForOrderingExpr(left.dataType, "operator " + symbol)

  private lazy val ordering = TypeUtils.getOrdering(left.dataType)

  protected override def evalInternal(evalE1: Any, evalE2: Any) = ordering.lt(evalE1, evalE2)
}

case class LessThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {
  override def symbol: String = "<="

  override protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForOrderingExpr(left.dataType, "operator " + symbol)

  private lazy val ordering = TypeUtils.getOrdering(left.dataType)

  protected override def evalInternal(evalE1: Any, evalE2: Any) = ordering.lteq(evalE1, evalE2)
}

case class GreaterThan(left: Expression, right: Expression) extends BinaryComparison {
  override def symbol: String = ">"

  override protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForOrderingExpr(left.dataType, "operator " + symbol)

  private lazy val ordering = TypeUtils.getOrdering(left.dataType)

  protected override def evalInternal(evalE1: Any, evalE2: Any) = ordering.gt(evalE1, evalE2)
}

case class GreaterThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {
  override def symbol: String = ">="

  override protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForOrderingExpr(left.dataType, "operator " + symbol)

  private lazy val ordering = TypeUtils.getOrdering(left.dataType)

  protected override def evalInternal(evalE1: Any, evalE2: Any) = ordering.gteq(evalE1, evalE2)
}

case class If(predicate: Expression, trueValue: Expression, falseValue: Expression)
  extends Expression {

  override def children: Seq[Expression] = predicate :: trueValue :: falseValue :: Nil
  override def nullable: Boolean = trueValue.nullable || falseValue.nullable

  override def checkInputDataTypes(): TypeCheckResult = {
    if (predicate.dataType != BooleanType) {
      TypeCheckResult.TypeCheckFailure(
        s"type of predicate expression in If should be boolean, not ${predicate.dataType}")
    } else if (trueValue.dataType != falseValue.dataType) {
      TypeCheckResult.TypeCheckFailure(
        s"differing types in If (${trueValue.dataType} and ${falseValue.dataType}).")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def dataType: DataType = trueValue.dataType

  override def eval(input: Row): Any = {
    if (true == predicate.eval(input)) {
      trueValue.eval(input)
    } else {
      falseValue.eval(input)
    }
  }
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): Code = {
    val condEval = predicate.gen(ctx)
    val trueEval = trueValue.gen(ctx)
    val falseEval = falseValue.gen(ctx)

    s"""
      boolean ${ev.nullTerm} = false;
      ${ctx.primitiveType(dataType)} ${ev.primitiveTerm} = ${ctx.defaultValue(dataType)};
      ${condEval.code}
      if (!${condEval.nullTerm} && ${condEval.primitiveTerm}) {
        ${trueEval.code}
        ${ev.nullTerm} = ${trueEval.nullTerm};
        ${ev.primitiveTerm} = ${trueEval.primitiveTerm};
      } else {
        ${falseEval.code}
        ${ev.nullTerm} = ${falseEval.nullTerm};
        ${ev.primitiveTerm} = ${falseEval.primitiveTerm};
      }
    """
  }

  override def toString: String = s"if ($predicate) $trueValue else $falseValue"
}

trait CaseWhenLike extends Expression {
  self: Product =>

  // Note that `branches` are considered in consecutive pairs (cond, val), and the optional last
  // element is the value for the default catch-all case (if provided).
  // Hence, `branches` consists of at least two elements, and can have an odd or even length.
  def branches: Seq[Expression]

  @transient lazy val whenList =
    branches.sliding(2, 2).collect { case Seq(whenExpr, _) => whenExpr }.toSeq
  @transient lazy val thenList =
    branches.sliding(2, 2).collect { case Seq(_, thenExpr) => thenExpr }.toSeq
  val elseValue = if (branches.length % 2 == 0) None else Option(branches.last)

  // both then and else expressions should be considered.
  def valueTypes: Seq[DataType] = (thenList ++ elseValue).map(_.dataType)
  def valueTypesEqual: Boolean = valueTypes.distinct.size == 1

  override def checkInputDataTypes(): TypeCheckResult = {
    if (valueTypesEqual) {
      checkTypesInternal()
    } else {
      TypeCheckResult.TypeCheckFailure(
        "THEN and ELSE expressions should all be same type or coercible to a common type")
    }
  }

  protected def checkTypesInternal(): TypeCheckResult

  override def dataType: DataType = thenList.head.dataType

  override def nullable: Boolean = {
    // If no value is nullable and no elseValue is provided, the whole statement defaults to null.
    thenList.exists(_.nullable) || (elseValue.map(_.nullable).getOrElse(true))
  }
}

// scalastyle:off
/**
 * Case statements of the form "CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END".
 * Refer to this link for the corresponding semantics:
 * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-ConditionalFunctions
 */
// scalastyle:on
case class CaseWhen(branches: Seq[Expression]) extends CaseWhenLike {

  // Use private[this] Array to speed up evaluation.
  @transient private[this] lazy val branchesArr = branches.toArray

  override def children: Seq[Expression] = branches

  override protected def checkTypesInternal(): TypeCheckResult = {
    if (whenList.forall(_.dataType == BooleanType)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      val index = whenList.indexWhere(_.dataType != BooleanType)
      TypeCheckResult.TypeCheckFailure(
        s"WHEN expressions in CaseWhen should all be boolean type, " +
        s"but the ${index + 1}th when expression's type is ${whenList(index)}")
    }
  }

  /** Written in imperative fashion for performance considerations. */
  override def eval(input: Row): Any = {
    val len = branchesArr.length
    var i = 0
    // If all branches fail and an elseVal is not provided, the whole statement
    // defaults to null, according to Hive's semantics.
    while (i < len - 1) {
      if (branchesArr(i).eval(input) == true) {
        return branchesArr(i + 1).eval(input)
      }
      i += 2
    }
    var res: Any = null
    if (i == len - 1) {
      res = branchesArr(i).eval(input)
    }
    return res
  }

  override def toString: String = {
    "CASE" + branches.sliding(2, 2).map {
      case Seq(cond, value) => s" WHEN $cond THEN $value"
      case Seq(elseValue) => s" ELSE $elseValue"
    }.mkString
  }
}

// scalastyle:off
/**
 * Case statements of the form "CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END".
 * Refer to this link for the corresponding semantics:
 * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-ConditionalFunctions
 */
// scalastyle:on
case class CaseKeyWhen(key: Expression, branches: Seq[Expression]) extends CaseWhenLike {

  // Use private[this] Array to speed up evaluation.
  @transient private[this] lazy val branchesArr = branches.toArray

  override def children: Seq[Expression] = key +: branches

  override protected def checkTypesInternal(): TypeCheckResult = {
    if ((key +: whenList).map(_.dataType).distinct.size > 1) {
      TypeCheckResult.TypeCheckFailure(
        "key and WHEN expressions should all be same type or coercible to a common type")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  /** Written in imperative fashion for performance considerations. */
  override def eval(input: Row): Any = {
    val evaluatedKey = key.eval(input)
    val len = branchesArr.length
    var i = 0
    // If all branches fail and an elseVal is not provided, the whole statement
    // defaults to null, according to Hive's semantics.
    while (i < len - 1) {
      if (equalNullSafe(evaluatedKey, branchesArr(i).eval(input))) {
        return branchesArr(i + 1).eval(input)
      }
      i += 2
    }
    var res: Any = null
    if (i == len - 1) {
      res = branchesArr(i).eval(input)
    }
    return res
  }

  private def equalNullSafe(l: Any, r: Any) = {
    if (l == null && r == null) {
      true
    } else if (l == null || r == null) {
      false
    } else {
      l == r
    }
  }

  override def toString: String = {
    s"CASE $key" + branches.sliding(2, 2).map {
      case Seq(cond, value) => s" WHEN $cond THEN $value"
      case Seq(elseValue) => s" ELSE $elseValue"
    }.mkString
  }
}
