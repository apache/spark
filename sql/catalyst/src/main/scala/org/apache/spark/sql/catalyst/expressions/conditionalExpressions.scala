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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._


case class If(predicate: Expression, trueValue: Expression, falseValue: Expression)
  extends Expression {

  override def children: Seq[Expression] = predicate :: trueValue :: falseValue :: Nil
  override def nullable: Boolean = trueValue.nullable || falseValue.nullable

  override def checkInputDataTypes(): TypeCheckResult = {
    if (predicate.dataType != BooleanType) {
      TypeCheckResult.TypeCheckFailure(
        s"type of predicate expression in If should be boolean, not ${predicate.dataType}")
    } else if (trueValue.dataType != falseValue.dataType) {
      TypeCheckResult.TypeCheckFailure(s"differing types in '$prettyString' " +
        s"(${trueValue.dataType.simpleString} and ${falseValue.dataType.simpleString}).")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def dataType: DataType = trueValue.dataType

  override def eval(input: InternalRow): Any = {
    if (java.lang.Boolean.TRUE.equals(predicate.eval(input))) {
      trueValue.eval(input)
    } else {
      falseValue.eval(input)
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val condEval = predicate.gen(ctx)
    val trueEval = trueValue.gen(ctx)
    val falseEval = falseValue.gen(ctx)

    s"""
      ${condEval.code}
      boolean ${ev.isNull} = false;
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      if (!${condEval.isNull} && ${condEval.value}) {
        ${trueEval.code}
        ${ev.isNull} = ${trueEval.isNull};
        ${ev.value} = ${trueEval.value};
      } else {
        ${falseEval.code}
        ${ev.isNull} = ${falseEval.isNull};
        ${ev.value} = ${falseEval.value};
      }
    """
  }

  override def toString: String = s"if ($predicate) $trueValue else $falseValue"

  override def sql: String = s"(IF(${predicate.sql}, ${trueValue.sql}, ${falseValue.sql}))"
}

/**
 * Case statements of the form "CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END".
 * When a = true, returns b; when c = true, returns d; else returns e.
 *
 * The last value is (optionally) the else value.
 */
case class CaseWhen(conditions: Seq[Expression], values: Seq[Expression]) extends Expression {

  private lazy val elseValue: Option[Expression] =
    if (values.size % 2 == 0) None else Some(values.last)

  require(values.size == conditions.size || values.size == conditions.size + 1)

  override def children: Seq[Expression] = conditions ++ values

  // both then and else expressions should be considered.
  def valueTypes: Seq[DataType] = values.map(_.dataType)

  def valueTypesEqual: Boolean = valueTypes.size <= 1 || valueTypes.sliding(2, 1).forall {
    case Seq(dt1, dt2) => dt1.sameType(dt2)
  }

  override def dataType: DataType = values.head.dataType

  override def nullable: Boolean = values.exists(_.nullable)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (valueTypesEqual) {
      if (conditions.forall(_.dataType == BooleanType)) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        val index = conditions.indexWhere(_.dataType != BooleanType)
        TypeCheckResult.TypeCheckFailure(
          s"WHEN expressions in CaseWhen should all be boolean type, " +
            s"but the ${index + 1}th when expression's type is ${conditions(index)}")
      }
    } else {
      TypeCheckResult.TypeCheckFailure(
        "THEN and ELSE expressions should all be same type or coercible to a common type")
    }
  }

  override def eval(input: InternalRow): Any = {
    var i = 0
    while (i < conditions.size) {
      if (java.lang.Boolean.TRUE.equals(conditions(i).eval(input))) {
        return values(i).eval(input)
      }
      i += 1
    }
    if (elseValue.isDefined) {
      return elseValue.get.eval(input)
    } else {
      return null
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val got = ctx.freshName("got")

    val cases = conditions.zip(values).map { case (condition, value) =>
      val cond = condition.gen(ctx)
      val res = value.gen(ctx)
      s"""
        if (!$got) {
          ${cond.code}
          if (!${cond.isNull} && ${cond.value}) {
            $got = true;
            ${res.code}
            ${ev.isNull} = ${res.isNull};
            ${ev.value} = ${res.value};
          }
        }
      """
    }.mkString("\n")

    val elseCase = {
      if (elseValue.isDefined) {
        val res = elseValue.get.gen(ctx)
        s"""
        if (!$got) {
          ${res.code}
          ${ev.isNull} = ${res.isNull};
          ${ev.value} = ${res.value};
        }
        """
      } else {
        ""
      }
    }

    s"""
      boolean $got = false;
      boolean ${ev.isNull} = true;
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      $cases
      $elseCase
    """
  }

  override def toString: String = {
    "CASE" + conditions.zip(values).map { case (condition, value) =>
      s" WHEN $condition THEN $value"
    }.mkString ++ s" ELSE ${values.last} END"
  }

  override def sql: String = prettyString
}

/** Factory methods for CaseWhen. */
object CaseWhen {
  /**
   * Creates a new CaseWhen expression based on the branches.
   * @param branches Expressions at even position are the branch conditions, and expressions at odd
   *                 position are branch values.
   */
  def apply(branches: Seq[Expression]): CaseWhen = {
    val conditions = new scala.collection.mutable.ArrayBuffer[Expression]
    val values = new scala.collection.mutable.ArrayBuffer[Expression]

    branches.zipWithIndex.foreach { case (branch, i) =>
      if (i % 2 == 0 && i != branches.size - 1) {
        // If this expression is at even position, then it is either a branch condition, or
        // the very last value that is the "else value". The "i != branches.size - 1" makes
        // sure we are not adding an EqualTo to the "else value".
        conditions += branch
      } else {
        values += branch
      }
    }

    CaseWhen(conditions, values)
  }
}

/**
 * Case statements of the form "CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END".
 * When a = b, returns c; when a = d, returns e; else returns f.
 */
object CaseKeyWhen {
  def apply(key: Expression, branches: Seq[Expression]): CaseWhen = {
    val conditions = new scala.collection.mutable.ArrayBuffer[Expression]
    val values = new scala.collection.mutable.ArrayBuffer[Expression]

    branches.zipWithIndex.foreach { case (branch, i) =>
      if (i % 2 == 0 && i != branches.size - 1) {
        // If this expression is at even position, then it is either a branch condition, or
        // the very last value that is the "else value". The "i != branches.size - 1" makes
        // sure we are not adding an EqualTo to the "else value".
        conditions += EqualTo(key, branch)
      } else {
        values += branch
      }
    }

    CaseWhen(conditions, values)
  }
}

/**
 * A function that returns the least value of all parameters, skipping null values.
 * It takes at least 2 parameters, and returns null iff all parameters are null.
 */
case class Least(children: Seq[Expression]) extends Expression {

  override def nullable: Boolean = children.forall(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length <= 1) {
      TypeCheckResult.TypeCheckFailure(s"LEAST requires at least 2 arguments")
    } else if (children.map(_.dataType).distinct.count(_ != NullType) > 1) {
      TypeCheckResult.TypeCheckFailure(
        s"The expressions should all have the same type," +
          s" got LEAST (${children.map(_.dataType)}).")
    } else {
      TypeUtils.checkForOrderingExpr(dataType, "function " + prettyName)
    }
  }

  override def dataType: DataType = children.head.dataType

  override def eval(input: InternalRow): Any = {
    children.foldLeft[Any](null)((r, c) => {
      val evalc = c.eval(input)
      if (evalc != null) {
        if (r == null || ordering.lt(evalc, r)) evalc else r
      } else {
        r
      }
    })
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val evalChildren = children.map(_.gen(ctx))
    val first = evalChildren(0)
    val rest = evalChildren.drop(1)
    def updateEval(eval: GeneratedExpressionCode): String = {
      s"""
        ${eval.code}
        if (!${eval.isNull} && (${ev.isNull} ||
          ${ctx.genGreater(dataType, ev.value, eval.value)})) {
          ${ev.isNull} = false;
          ${ev.value} = ${eval.value};
        }
      """
    }
    s"""
      ${first.code}
      boolean ${ev.isNull} = ${first.isNull};
      ${ctx.javaType(dataType)} ${ev.value} = ${first.value};
      ${rest.map(updateEval).mkString("\n")}
    """
  }
}

/**
 * A function that returns the greatest value of all parameters, skipping null values.
 * It takes at least 2 parameters, and returns null iff all parameters are null.
 */
case class Greatest(children: Seq[Expression]) extends Expression {

  override def nullable: Boolean = children.forall(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length <= 1) {
      TypeCheckResult.TypeCheckFailure(s"GREATEST requires at least 2 arguments")
    } else if (children.map(_.dataType).distinct.count(_ != NullType) > 1) {
      TypeCheckResult.TypeCheckFailure(
        s"The expressions should all have the same type," +
          s" got GREATEST (${children.map(_.dataType)}).")
    } else {
      TypeUtils.checkForOrderingExpr(dataType, "function " + prettyName)
    }
  }

  override def dataType: DataType = children.head.dataType

  override def eval(input: InternalRow): Any = {
    children.foldLeft[Any](null)((r, c) => {
      val evalc = c.eval(input)
      if (evalc != null) {
        if (r == null || ordering.gt(evalc, r)) evalc else r
      } else {
        r
      }
    })
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val evalChildren = children.map(_.gen(ctx))
    val first = evalChildren(0)
    val rest = evalChildren.drop(1)
    def updateEval(eval: GeneratedExpressionCode): String = {
      s"""
        ${eval.code}
        if (!${eval.isNull} && (${ev.isNull} ||
          ${ctx.genGreater(dataType, eval.value, ev.value)})) {
          ${ev.isNull} = false;
          ${ev.value} = ${eval.value};
        }
      """
    }
    s"""
      ${first.code}
      boolean ${ev.isNull} = ${first.isNull};
      ${ctx.javaType(dataType)} ${ev.value} = ${first.value};
      ${rest.map(updateEval).mkString("\n")}
    """
  }
}

