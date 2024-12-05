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
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistryBase, TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.catalyst.trees.TreePattern.{CASE_WHEN, IF, TreePattern}
import org.apache.spark.sql.catalyst.util.TypeUtils.{ordinalNumber, toSQLExpr, toSQLId, toSQLType}
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2, expr3) - If `expr1` evaluates to true, then returns `expr2`; otherwise returns `expr3`.",
  examples = """
    Examples:
      > SELECT _FUNC_(1 < 2, 'a', 'b');
       a
  """,
  since = "1.0.0",
  group = "conditional_funcs")
// scalastyle:on line.size.limit
case class If(predicate: Expression, trueValue: Expression, falseValue: Expression)
  extends ComplexTypeMergingExpression with ConditionalExpression with TernaryLike[Expression] {

  @transient
  override lazy val inputTypesForMerging: Seq[DataType] = {
    Seq(trueValue.dataType, falseValue.dataType)
  }

  override def first: Expression = predicate
  override def second: Expression = trueValue
  override def third: Expression = falseValue
  override def nullable: Boolean = trueValue.nullable || falseValue.nullable

  /**
   * Only the condition expression will always be evaluated.
   */
  override def alwaysEvaluatedInputs: Seq[Expression] = predicate :: Nil

  override def withNewAlwaysEvaluatedInputs(alwaysEvaluatedInputs: Seq[Expression]): If = {
    copy(predicate = alwaysEvaluatedInputs.head)
  }

  override def branchGroups: Seq[Seq[Expression]] = Seq(Seq(trueValue, falseValue))

  final override val nodePatterns : Seq[TreePattern] = Seq(IF)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (predicate.dataType != BooleanType) {
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> toSQLType(BooleanType),
          "inputSql" -> toSQLExpr(predicate),
          "inputType" -> toSQLType(predicate.dataType)
        )
      )
    } else if (!TypeCoercion.haveSameType(inputTypesForMerging)) {
      DataTypeMismatch(
        errorSubClass = "DATA_DIFF_TYPES",
        messageParameters = Map(
          "functionName" -> toSQLId(prettyName),
          "dataType" -> Seq(trueValue.dataType,
            falseValue.dataType).map(toSQLType).mkString("[", ", ", "]")
        )
      )
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def eval(input: InternalRow): Any = {
    if (java.lang.Boolean.TRUE.equals(predicate.eval(input))) {
      trueValue.eval(input)
    } else {
      falseValue.eval(input)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val condEval = predicate.genCode(ctx)
    val trueEval = trueValue.genCode(ctx)
    val falseEval = falseValue.genCode(ctx)

    val code =
      code"""
         |${condEval.code}
         |boolean ${ev.isNull} = false;
         |${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
         |if (!${condEval.isNull} && ${condEval.value}) {
         |  ${trueEval.code}
         |  ${ev.isNull} = ${trueEval.isNull};
         |  ${ev.value} = ${trueEval.value};
         |} else {
         |  ${falseEval.code}
         |  ${ev.isNull} = ${falseEval.isNull};
         |  ${ev.value} = ${falseEval.value};
         |}
       """.stripMargin
    ev.copy(code = code)
  }

  override def toString: String = s"if ($predicate) $trueValue else $falseValue"

  override def sql: String = s"(IF(${predicate.sql}, ${trueValue.sql}, ${falseValue.sql}))"

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): Expression = copy(
    predicate = newFirst,
    trueValue = newSecond,
    falseValue = newThird
  )
}

/**
 * Case statements of the form "CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END".
 * When a = true, returns b; when c = true, returns d; else returns e.
 *
 * @param branches seq of (branch condition, branch value)
 * @param elseValue optional value for the else branch
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "CASE WHEN expr1 THEN expr2 [WHEN expr3 THEN expr4]* [ELSE expr5] END - When `expr1` = true, returns `expr2`; else when `expr3` = true, returns `expr4`; else returns `expr5`.",
  arguments = """
    Arguments:
      * expr1, expr3 - the branch condition expressions should all be boolean type.
      * expr2, expr4, expr5 - the branch value expressions and else value expression should all be
          same type or coercible to a common type.
  """,
  examples = """
    Examples:
      > SELECT CASE WHEN 1 > 0 THEN 1 WHEN 2 > 0 THEN 2.0 ELSE 1.2 END;
       1.0
      > SELECT CASE WHEN 1 < 0 THEN 1 WHEN 2 > 0 THEN 2.0 ELSE 1.2 END;
       2.0
      > SELECT CASE WHEN 1 < 0 THEN 1 WHEN 2 < 0 THEN 2.0 END;
       NULL
  """,
  since = "1.0.1",
  group = "conditional_funcs")
// scalastyle:on line.size.limit
case class CaseWhen(
    branches: Seq[(Expression, Expression)],
    elseValue: Option[Expression] = None)
  extends ComplexTypeMergingExpression with ConditionalExpression {

  override def children: Seq[Expression] = branches.flatMap(b => b._1 :: b._2 :: Nil) ++ elseValue

  final override val nodePatterns : Seq[TreePattern] = Seq(CASE_WHEN)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): CaseWhen = {
    if (newChildren.length % 2 == 0) {
      copy(branches = newChildren.grouped(2).map { case Seq(a, b) => (a, b) }.toSeq)
    } else {
      copy(
        branches = newChildren.dropRight(1).grouped(2).map { case Seq(a, b) => (a, b) }.toSeq,
        elseValue = newChildren.lastOption)
    }
  }

  // both then and else expressions should be considered.
  @transient
  override lazy val inputTypesForMerging: Seq[DataType] = {
    branches.map(_._2.dataType) ++ elseValue.map(_.dataType)
  }

  override def nullable: Boolean = {
    if (branches.exists(_._1 == TrueLiteral)) {
      // if any of the branch is always true
      // nullability check should only be related to branches
      // before the TrueLiteral and value of the first TrueLiteral branch
      val (h, t) = branches.span(_._1 != TrueLiteral)
      return h.exists(_._2.nullable) || t.head._2.nullable
    }
    // Result is nullable if any of the branch is nullable, or if the else value is nullable
    branches.exists(_._2.nullable) || elseValue.map(_.nullable).getOrElse(true)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (TypeCoercion.haveSameType(inputTypesForMerging)) {
      // Make sure all branch conditions are boolean types.
      if (branches.forall(_._1.dataType == BooleanType)) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        val index = branches.indexWhere(_._1.dataType != BooleanType)
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(index),
            "requiredType" -> toSQLType(BooleanType),
            "inputSql" -> toSQLExpr(branches(index)._1),
            "inputType" -> toSQLType(branches(index)._1.dataType)
          )
        )
      }
    } else {
      DataTypeMismatch(
        errorSubClass = "DATA_DIFF_TYPES",
        messageParameters = Map(
          "functionName" -> toSQLId(prettyName),
          "dataType" -> inputTypesForMerging.map(toSQLType).mkString("[", ", ", "]")
        )
      )
    }
  }

  /**
   * Like `If`, the children of `CaseWhen` only get accessed in a certain condition.
   * We should only return the first condition expression as it will always get accessed.
   */
  override def alwaysEvaluatedInputs: Seq[Expression] = children.head :: Nil

  override def withNewAlwaysEvaluatedInputs(alwaysEvaluatedInputs: Seq[Expression]): CaseWhen = {
    withNewChildrenInternal(alwaysEvaluatedInputs.toIndexedSeq ++ children.drop(1))
  }

  override def branchGroups: Seq[Seq[Expression]] = {
    // We look at subexpressions in conditions and values of `CaseWhen` separately. It is
    // because a subexpression in conditions will be run no matter which condition is matched
    // if it is shared among conditions, but it doesn't need to be shared in values. Similarly,
    // a subexpression among values doesn't need to be in conditions because no matter which
    // condition is true, it will be evaluated.
    val conditions = if (branches.length > 1) {
      branches.map(_._1)
    } else {
      // If there is only one branch, the first condition is already covered by
      // `alwaysEvaluatedInputs` and we should exclude it here.
      Nil
    }
    // For an expression to be in all branch values of a CaseWhen statement, it must also be in
    // the elseValue.
    val values = if (elseValue.nonEmpty) {
      branches.map(_._2) ++ elseValue
    } else {
      Nil
    }

    Seq(conditions, values)
  }

  override def eval(input: InternalRow): Any = {
    var i = 0
    val size = branches.size
    while (i < size) {
      if (java.lang.Boolean.TRUE.equals(branches(i)._1.eval(input))) {
        return branches(i)._2.eval(input)
      }
      i += 1
    }
    if (elseValue.isDefined) {
      elseValue.get.eval(input)
    } else {
      null
    }
  }

  override def toString: String = {
    val cases = branches.map { case (c, v) => s" WHEN $c THEN $v" }.mkString
    val elseCase = elseValue.map(" ELSE " + _).getOrElse("")
    "CASE" + cases + elseCase + " END"
  }

  override def sql: String = {
    val cases = branches.map { case (c, v) => s" WHEN ${c.sql} THEN ${v.sql}" }.mkString
    val elseCase = elseValue.map(" ELSE " + _.sql).getOrElse("")
    "CASE" + cases + elseCase + " END"
  }

  private def multiBranchesCodegen(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // This variable holds the state of the result:
    // -1 means the condition is not met yet and the result is unknown.
    val NOT_MATCHED = -1
    // 0 means the condition is met and result is not null.
    val HAS_NONNULL = 0
    // 1 means the condition is met and result is null.
    val HAS_NULL = 1
    // It is initialized to `NOT_MATCHED`, and if it's set to `HAS_NULL` or `HAS_NONNULL`,
    // We won't go on anymore on the computation.
    val resultState = ctx.freshName("caseWhenResultState")
    ev.value = JavaCode.global(
      ctx.addMutableState(CodeGenerator.javaType(dataType), ev.value),
      dataType)

    // these blocks are meant to be inside a
    // do {
    //   ...
    // } while (false);
    // loop
    val cases = branches.map { case (condExpr, valueExpr) =>
      val cond = condExpr.genCode(ctx)
      val res = valueExpr.genCode(ctx)
      s"""
         |${cond.code}
         |if (!${cond.isNull} && ${cond.value}) {
         |  ${res.code}
         |  $resultState = (byte)(${res.isNull} ? $HAS_NULL : $HAS_NONNULL);
         |  ${ev.value} = ${res.value};
         |  continue;
         |}
       """.stripMargin
    }

    val elseCode = elseValue.map { elseExpr =>
      val res = elseExpr.genCode(ctx)
      s"""
         |${res.code}
         |$resultState = (byte)(${res.isNull} ? $HAS_NULL : $HAS_NONNULL);
         |${ev.value} = ${res.value};
       """.stripMargin
    }

    val allConditions = cases ++ elseCode

    // This generates code like:
    //   caseWhenResultState = caseWhen_1(i);
    //   if (caseWhenResultState != -1) {
    //     continue;
    //   }
    //   caseWhenResultState = caseWhen_2(i);
    //   if (caseWhenResultState != -1) {
    //     continue;
    //   }
    //   ...
    // and the declared methods are:
    //   private byte caseWhen_1234() {
    //     byte caseWhenResultState = -1;
    //     do {
    //       // here the evaluation of the conditions
    //     } while (false);
    //     return caseWhenResultState;
    //   }
    val codes = ctx.splitExpressionsWithCurrentInputs(
      expressions = allConditions,
      funcName = "caseWhen",
      returnType = CodeGenerator.JAVA_BYTE,
      makeSplitFunction = func =>
        s"""
           |${CodeGenerator.JAVA_BYTE} $resultState = $NOT_MATCHED;
           |do {
           |  $func
           |} while (false);
           |return $resultState;
         """.stripMargin,
      foldFunctions = _.map { funcCall =>
        s"""
           |$resultState = $funcCall;
           |if ($resultState != $NOT_MATCHED) {
           |  continue;
           |}
         """.stripMargin
      }.mkString)

    ev.copy(code =
      code"""
         |${CodeGenerator.JAVA_BYTE} $resultState = $NOT_MATCHED;
         |do {
         |  $codes
         |} while (false);
         |// TRUE if any condition is met and the result is null, or no any condition is met.
         |final boolean ${ev.isNull} = ($resultState != $HAS_NONNULL);
       """.stripMargin)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (branches.length == 1) {
      // If we have only single branch we can use If expression and its codeGen
      If(
        branches(0)._1,
        branches(0)._2,
        elseValue.getOrElse(Literal.create(null, branches(0)._2.dataType))).doGenCode(ctx, ev)
    } else {
      multiBranchesCodegen(ctx, ev)
    }
  }
}

/** Factory methods for CaseWhen. */
object CaseWhen {

  def apply(branches: Seq[(Expression, Expression)], elseValue: Expression): CaseWhen = {
    CaseWhen(branches, Option(elseValue))
  }

  /**
   * A factory method to facilitate the creation of this expression when used in parsers.
   *
   * @param branches Expressions at even position are the branch conditions, and expressions at odd
   *                 position are branch values.
   */
  def createFromParser(branches: Seq[Expression]): CaseWhen = {
    val cases = branches.grouped(2).flatMap { g =>
      if (g.size == 2) Some((g.head, g.last)) else None
    }.toSeq  // force materialization to make the seq serializable
    val elseValue = if (branches.size % 2 != 0) Some(branches.last) else None
    CaseWhen(cases, elseValue)
  }

  val registryEntry: (String, (ExpressionInfo, FunctionBuilder)) = {
    ("when", (FunctionRegistryBase.expressionInfo[CaseWhen]("when", None), createFromParser))
  }
}

/**
 * Case statements of the form "CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END".
 * When a = b, returns c; when a = d, returns e; else returns f.
 */
object CaseKeyWhen {
  def apply(key: Expression, branches: Seq[Expression]): CaseWhen = {
    val cases = branches.grouped(2).flatMap {
      case Seq(cond, value) => Some((EqualTo(key, cond), value))
      case Seq(value) => None
    }.toArray.toImmutableArraySeq  // force materialization to make the seq serializable
    val elseValue = if (branches.size % 2 != 0) Some(branches.last) else None
    CaseWhen(cases, elseValue)
  }
}
