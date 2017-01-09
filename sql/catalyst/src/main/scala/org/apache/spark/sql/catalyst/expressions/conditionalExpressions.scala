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
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2, expr3) - If `expr1` evaluates to true, then returns `expr2`; otherwise returns `expr3`.",
  examples = """
    Examples:
      > SELECT _FUNC_(1 < 2, 'a', 'b');
       a
  """)
// scalastyle:on line.size.limit
case class If(predicate: Expression, trueValue: Expression, falseValue: Expression)
  extends ComplexTypeMergingExpression {

  @transient
  override lazy val inputTypesForMerging: Seq[DataType] = {
    Seq(trueValue.dataType, falseValue.dataType)
  }

  override def children: Seq[Expression] = predicate :: trueValue :: falseValue :: Nil
  override def nullable: Boolean = trueValue.nullable || falseValue.nullable

  override def checkInputDataTypes(): TypeCheckResult = {
    if (predicate.dataType != BooleanType) {
      TypeCheckResult.TypeCheckFailure(
        "type of predicate expression in If should be boolean, " +
          s"not ${predicate.dataType.simpleString}")
    } else if (!areInputTypesForMergingEqual) {
      TypeCheckResult.TypeCheckFailure(s"differing types in '$sql' " +
        s"(${trueValue.dataType.simpleString} and ${falseValue.dataType.simpleString}).")
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
       1
      > SELECT CASE WHEN 1 < 0 THEN 1 WHEN 2 > 0 THEN 2.0 ELSE 1.2 END;
       2
      > SELECT CASE WHEN 1 < 0 THEN 1 WHEN 2 < 0 THEN 2.0 END;
       NULL
  """)
// scalastyle:on line.size.limit
case class CaseWhen(
    branches: Seq[(Expression, Expression)],
    elseValue: Option[Expression] = None)
  extends ComplexTypeMergingExpression with Serializable {

  override def children: Seq[Expression] = branches.flatMap(b => b._1 :: b._2 :: Nil) ++ elseValue

  // both then and else expressions should be considered.
  @transient
  override lazy val inputTypesForMerging: Seq[DataType] = {
    branches.map(_._2.dataType) ++ elseValue.map(_.dataType)
  }

  override def nullable: Boolean = {
    // Result is nullable if any of the branch is nullable, or if the else value is nullable
    branches.exists(_._2.nullable) || elseValue.map(_.nullable).getOrElse(true)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (areInputTypesForMergingEqual) {
      // Make sure all branch conditions are boolean types.
      if (branches.forall(_._1.dataType == BooleanType)) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        val index = branches.indexWhere(_._1.dataType != BooleanType)
        TypeCheckResult.TypeCheckFailure(
          s"WHEN expressions in CaseWhen should all be boolean type, " +
            s"but the ${index + 1}th when expression's type is ${branches(index)._1}")
      }
    } else {
      TypeCheckResult.TypeCheckFailure(
        "THEN and ELSE expressions should all be same type or coercible to a common type")
    }
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
      return elseValue.get.eval(input)
    } else {
      return null
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

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
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
    //   if(caseWhenResultState != -1) {
    //     continue;
    //   }
    //   caseWhenResultState = caseWhen_2(i);
    //   if(caseWhenResultState != -1) {
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
    val cases = branches.grouped(2).flatMap {
      case cond :: value :: Nil => Some((cond, value))
      case value :: Nil => None
    }.toArray.toSeq  // force materialization to make the seq serializable
    val elseValue = if (branches.size % 2 != 0) Some(branches.last) else None
    CaseWhen(cases, elseValue)
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
    }.toArray.toSeq  // force materialization to make the seq serializable
    val elseValue = if (branches.size % 2 != 0) Some(branches.last) else None
    CaseWhen(cases, elseValue)
  }
}

/**
 * A function that returns the index of expr in (expr1, expr2, ...) list or 0 if not found.
 * It takes at least 2 parameters, and all parameters' types should be subtypes of AtomicType.
 * It's also acceptable to give parameters of different types.
 * If the search string is NULL, the return value is 0 because NULL fails equality comparison with any value.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr, expr1, expr2, ...) - Returns the index of expr in the expr1, expr2, ... or 0 if not found.",
  extended = """
    Examples:
      > SELECT _FUNC_(10, 9, 3, 10, 4);
       3
      > SELECT _FUNC_('a', 'b', 'c', 'd', 'a');
       4
      > SELECT _FUNC_('999', 'a', 999, 9.99, '999');
       4
  """)
case class Field(children: Seq[Expression]) extends Expression {

  /** Even if expr is not found in (expr1, expr2, ...) list, the value will be 0, not null */
  override def nullable: Boolean = false
  override def foldable: Boolean = children.forall(_.foldable)

  private lazy val ordering = TypeUtils.getInterpretedOrdering(children(0).dataType)

  private val dataTypeMatchIndex: Seq[Int] = children.tail.zip(Stream from 1).filter(
    _._1.dataType == children.head.dataType).map(_._2)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length <= 1) {
      TypeCheckResult.TypeCheckFailure(s"FIELD requires at least 2 arguments")
    } else if (!children.forall(_.dataType.isInstanceOf[AtomicType])) {
      TypeCheckResult.TypeCheckFailure(s"FIELD requires all arguments to be of AtomicType")
    } else
      TypeCheckResult.TypeCheckSuccess
  }

  override def dataType: DataType = IntegerType
  override def eval(input: InternalRow): Any = {
    val target = children.head.eval(input)
    val targetDataType = children.head.dataType
    @tailrec def findEqual(target: Any, params: Seq[Expression], index: Int): Int = {
        if (params == Nil) {
          0
        } else {
          val head = params.head
          if (dataTypeMatchIndex.contains(index) && head.eval(input) != null
            && ordering.equiv(target, head.eval(input)))
            index
          else
            findEqual(target, params.tail, index + 1)
        }
      }
    if (target == null) 0 else findEqual(target, children.tail, index = 1)
  }

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evalChildren = children.map(_.genCode(ctx))
    val target = evalChildren(0)
    val targetDataType = children(0).dataType
    val rest = evalChildren.drop(1)
    val restDataType = children.drop(1).map(_.dataType)

    def updateEval(evalWithIndex: ((ExprCode, DataType), Int)): String = {
      val ((eval, dataType), index) = evalWithIndex
      s"""
        ${eval.code}
        if (${ctx.genEqual(targetDataType, eval.value, target.value)}) {
          ${ev.value} = ${index};
        }
      """
    }

    def genIfElseStructure(code1: String, code2: String): String = {
      s"""
         ${code1}
         else {
          ${code2}
         }
       """
    }

    def dataTypeEqualsTarget(evalWithIndex: ((ExprCode, DataType), Int)): Boolean = {
      val ((eval, dataType), index) = evalWithIndex
      dataType.equals(targetDataType)
    }

    ev.copy(code =
      code"""
         |${target.code}
         |boolean ${ev.isNull} = false;
         |int ${ev.value} = 0;
         |${rest.zip(restDataType).zip(Stream from 1).filter(
        x => dataTypeMatchIndex.contains(x._2)).map(updateEval).reduceRight(genIfElseStructure)}
       """.stripMargin)
  }
}
