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
    if (true == predicate.eval(input)) {
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
}

trait CaseWhenLike extends Expression {

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

  /**
   * Whether should it fallback to interpret mode or not.
   * @return
   */
  protected def shouldFallback: Boolean = {
    branches.length > 20
  }
}

// scalastyle:off
/**
 * Case statements of the form "CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END".
 * Refer to this link for the corresponding semantics:
 * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-ConditionalFunctions
 */
// scalastyle:on
case class CaseWhen(branches: Seq[Expression]) extends CaseWhenLike with CodegenFallback {

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
  override def eval(input: InternalRow): Any = {
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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    if (shouldFallback) {
      // Fallback to interpreted mode if there are too many branches, as it may reach the
      // 64K limit (limit on bytecode size for a single function).
      return super[CodegenFallback].genCode(ctx, ev)
    }
    val len = branchesArr.length
    val got = ctx.freshName("got")

    val cases = (0 until len/2).map { i =>
      val cond = branchesArr(i * 2).gen(ctx)
      val res = branchesArr(i * 2 + 1).gen(ctx)
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

    val other = if (len % 2 == 1) {
      val res = branchesArr(len - 1).gen(ctx)
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

    s"""
      boolean $got = false;
      boolean ${ev.isNull} = true;
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      $cases
      $other
    """
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
case class CaseKeyWhen(key: Expression, branches: Seq[Expression])
  extends CaseWhenLike with CodegenFallback {

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

  private def evalElse(input: InternalRow): Any = {
    if (branchesArr.length % 2 == 0) {
      null
    } else {
      branchesArr(branchesArr.length - 1).eval(input)
    }
  }

  /** Written in imperative fashion for performance considerations. */
  override def eval(input: InternalRow): Any = {
    val evaluatedKey = key.eval(input)
    // If key is null, we can just return the else part or null if there is no else.
    // If key is not null but doesn't match any when part, we need to return
    // the else part or null if there is no else, according to Hive's semantics.
    if (evaluatedKey != null) {
      val len = branchesArr.length
      var i = 0
      while (i < len - 1) {
        if (evaluatedKey ==  branchesArr(i).eval(input)) {
          return branchesArr(i + 1).eval(input)
        }
        i += 2
      }
    }
    evalElse(input)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    if (shouldFallback) {
      // Fallback to interpreted mode if there are too many branches, as it may reach the
      // 64K limit (limit on bytecode size for a single function).
      return super[CodegenFallback].genCode(ctx, ev)
    }
    val keyEval = key.gen(ctx)
    val len = branchesArr.length
    val got = ctx.freshName("got")

    val cases = (0 until len/2).map { i =>
      val cond = branchesArr(i * 2).gen(ctx)
      val res = branchesArr(i * 2 + 1).gen(ctx)
      s"""
        if (!$got) {
          ${cond.code}
          if (!${cond.isNull} && ${ctx.genEqual(key.dataType, keyEval.value, cond.value)}) {
            $got = true;
            ${res.code}
            ${ev.isNull} = ${res.isNull};
            ${ev.value} = ${res.value};
          }
        }
      """
    }.mkString("\n")

    val other = if (len % 2 == 1) {
      val res = branchesArr(len - 1).gen(ctx)
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

    s"""
      boolean $got = false;
      boolean ${ev.isNull} = true;
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      ${keyEval.code}
      if (!${keyEval.isNull}) {
        $cases
      }
      $other
    """
  }

  override def toString: String = {
    s"CASE $key" + branches.sliding(2, 2).map {
      case Seq(cond, value) => s" WHEN $cond THEN $value"
      case Seq(elseValue) => s" ELSE $elseValue"
    }.mkString
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
    def updateEval(eval: GeneratedExpressionCode): String =
      s"""
        ${eval.code}
        if (!${eval.isNull} && (${ev.isNull} ||
          ${ctx.genGreater(dataType, ev.value, eval.value)})) {
          ${ev.isNull} = false;
          ${ev.value} = ${eval.value};
        }
      """
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
    def updateEval(eval: GeneratedExpressionCode): String =
      s"""
        ${eval.code}
        if (!${eval.isNull} && (${ev.isNull} ||
          ${ctx.genGreater(dataType, eval.value, ev.value)})) {
          ${ev.isNull} = false;
          ${ev.value} = ${eval.value};
        }
      """
    s"""
      ${first.code}
      boolean ${ev.isNull} = ${first.isNull};
      ${ctx.javaType(dataType)} ${ev.value} = ${first.value};
      ${rest.map(updateEval).mkString("\n")}
    """
  }
}

