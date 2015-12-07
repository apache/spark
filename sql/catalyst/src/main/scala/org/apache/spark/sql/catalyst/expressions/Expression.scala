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
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.types._

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines the basic expression abstract classes in Catalyst.
////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * An expression in Catalyst.
 *
 * If an expression wants to be exposed in the function registry (so users can call it with
 * "name(arguments...)", the concrete implementation must be a case class whose constructor
 * arguments are all Expressions types. See [[Substring]] for an example.
 *
 * There are a few important traits:
 *
 * - [[Nondeterministic]]: an expression that is not deterministic.
 * - [[Unevaluable]]: an expression that is not supposed to be evaluated.
 * - [[CodegenFallback]]: an expression that does not have code gen implemented and falls back to
 *                        interpreted mode.
 *
 * - [[LeafExpression]]: an expression that has no child.
 * - [[UnaryExpression]]: an expression that has one child.
 * - [[BinaryExpression]]: an expression that has two children.
 * - [[BinaryOperator]]: a special case of [[BinaryExpression]] that requires two children to have
 *                       the same output data type.
 *
 */
abstract class Expression extends TreeNode[Expression] {

  /**
   * Returns true when an expression is a candidate for static evaluation before the query is
   * executed.
   *
   * The following conditions are used to determine suitability for constant folding:
   *  - A [[Coalesce]] is foldable if all of its children are foldable
   *  - A [[BinaryExpression]] is foldable if its both left and right child are foldable
   *  - A [[Not]], [[IsNull]], or [[IsNotNull]] is foldable if its child is foldable
   *  - A [[Literal]] is foldable
   *  - A [[Cast]] or [[UnaryMinus]] is foldable if its child is foldable
   */
  def foldable: Boolean = false

  /**
   * Returns true when the current expression always return the same result for fixed inputs from
   * children.
   *
   * Note that this means that an expression should be considered as non-deterministic if:
   * - if it relies on some mutable internal state, or
   * - if it relies on some implicit input that is not part of the children expression list.
   * - if it has non-deterministic child or children.
   *
   * An example would be `SparkPartitionID` that relies on the partition id returned by TaskContext.
   * By default leaf expressions are deterministic as Nil.forall(_.deterministic) returns true.
   */
  def deterministic: Boolean = children.forall(_.deterministic)

  def nullable: Boolean

  def references: AttributeSet = AttributeSet(children.flatMap(_.references.iterator))

  /** Returns the result of evaluating this expression on a given input Row */
  def eval(input: InternalRow = null): Any

  /**
   * Returns an [[GeneratedExpressionCode]], which contains Java source code that
   * can be used to generate the result of evaluating the expression on an input row.
   *
   * @param ctx a [[CodeGenContext]]
   * @return [[GeneratedExpressionCode]]
   */
  def gen(ctx: CodeGenContext): GeneratedExpressionCode = {
    ctx.subExprEliminationExprs.get(this).map { subExprState =>
      // This expression is repeated meaning the code to evaluated has already been added
      // as a function and called in advance. Just use it.
      val code = s"/* ${this.toCommentSafeString} */"
      GeneratedExpressionCode(code, subExprState.isNull, subExprState.value)
    }.getOrElse {
      val isNull = ctx.freshName("isNull")
      val primitive = ctx.freshName("primitive")
      val ve = GeneratedExpressionCode("", isNull, primitive)
      ve.code = genCode(ctx, ve)
      // Add `this` in the comment.
      ve.copy(s"/* ${this.toCommentSafeString} */\n" + ve.code.trim)
    }
  }

  /**
   * Returns Java source code that can be compiled to evaluate this expression.
   * The default behavior is to call the eval method of the expression. Concrete expression
   * implementations should override this to do actual code generation.
   *
   * @param ctx a [[CodeGenContext]]
   * @param ev an [[GeneratedExpressionCode]] with unique terms.
   * @return Java source code
   */
  protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String

  /**
   * Returns `true` if this expression and all its children have been resolved to a specific schema
   * and input data types checking passed, and `false` if it still contains any unresolved
   * placeholders or has data types mismatch.
   * Implementations of expressions should override this if the resolution of this type of
   * expression involves more than just the resolution of its children and type checking.
   */
  lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

  /**
   * Returns the [[DataType]] of the result of evaluating this expression.  It is
   * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
   */
  def dataType: DataType

  /**
   * Returns true if  all the children of this expression have been resolved to a specific schema
   * and false if any still contains any unresolved placeholders.
   */
  def childrenResolved: Boolean = children.forall(_.resolved)

  /**
   * Returns true when two expressions will always compute the same result, even if they differ
   * cosmetically (i.e. capitalization of names in attributes may be different).
   */
  def semanticEquals(other: Expression): Boolean = this.getClass == other.getClass && {
    def checkSemantic(elements1: Seq[Any], elements2: Seq[Any]): Boolean = {
      elements1.length == elements2.length && elements1.zip(elements2).forall {
        case (e1: Expression, e2: Expression) => e1 semanticEquals e2
        case (Some(e1: Expression), Some(e2: Expression)) => e1 semanticEquals e2
        case (t1: Traversable[_], t2: Traversable[_]) => checkSemantic(t1.toSeq, t2.toSeq)
        case (i1, i2) => i1 == i2
      }
    }
    // Non-deterministic expressions cannot be semantic equal
    if (!deterministic || !other.deterministic) return false
    val elements1 = this.productIterator.toSeq
    val elements2 = other.asInstanceOf[Product].productIterator.toSeq
    checkSemantic(elements1, elements2)
  }

  /**
   * Returns the hash for this expression. Expressions that compute the same result, even if
   * they differ cosmetically should return the same hash.
   */
  def semanticHash() : Int = {
    def computeHash(e: Seq[Any]): Int = {
      // See http://stackoverflow.com/questions/113511/hash-code-implementation
      var hash: Int = 17
      e.foreach(i => {
        val h: Int = i match {
          case e: Expression => e.semanticHash()
          case Some(e: Expression) => e.semanticHash()
          case t: Traversable[_] => computeHash(t.toSeq)
          case null => 0
          case other => other.hashCode()
        }
        hash = hash * 37 + h
      })
      hash
    }

    computeHash(this.productIterator.toSeq)
  }

  /**
   * Checks the input data types, returns `TypeCheckResult.success` if it's valid,
   * or returns a `TypeCheckResult` with an error message if invalid.
   * Note: it's not valid to call this method until `childrenResolved == true`.
   */
  def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess

  /**
   * Returns a user-facing string representation of this expression's name.
   * This should usually match the name of the function in SQL.
   */
  def prettyName: String = getClass.getSimpleName.toLowerCase

  /**
   * Returns a user-facing string representation of this expression, i.e. does not have developer
   * centric debugging information like the expression id.
   */
  def prettyString: String = {
    transform {
      case a: AttributeReference => PrettyAttribute(a.name, a.dataType)
      case u: UnresolvedAttribute => PrettyAttribute(u.name)
    }.toString
  }

  private def flatArguments = productIterator.flatMap {
    case t: Traversable[_] => t
    case single => single :: Nil
  }

  override def simpleString: String = toString

  override def toString: String = prettyName + flatArguments.mkString("(", ",", ")")

  /**
   * Returns the string representation of this expression that is safe to be put in
   * code comments of generated code.
   */
  protected def toCommentSafeString: String = this.toString
    .replace("*/", "\\*\\/")
    .replace("\\u", "\\\\u")
}


/**
 * An expression that cannot be evaluated. Some expressions don't live past analysis or optimization
 * time (e.g. Star). This trait is used by those expressions.
 */
trait Unevaluable extends Expression {

  final override def eval(input: InternalRow = null): Any =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")

  final override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")
}


/**
 * An expression that is nondeterministic.
 */
trait Nondeterministic extends Expression {
  final override def deterministic: Boolean = false
  final override def foldable: Boolean = false

  private[this] var initialized = false

  final def setInitialValues(): Unit = {
    initInternal()
    initialized = true
  }

  protected def initInternal(): Unit

  final override def eval(input: InternalRow = null): Any = {
    require(initialized, "nondeterministic expression should be initialized before evaluate")
    evalInternal(input)
  }

  protected def evalInternal(input: InternalRow): Any
}


/**
 * A leaf expression, i.e. one without any child expressions.
 */
abstract class LeafExpression extends Expression {

  def children: Seq[Expression] = Nil
}


/**
 * An expression with one input and one output. The output is by default evaluated to null
 * if the input is evaluated to null.
 */
abstract class UnaryExpression extends Expression {

  def child: Expression

  override def children: Seq[Expression] = child :: Nil

  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable

  /**
   * Default behavior of evaluation according to the default nullability of UnaryExpression.
   * If subclass of UnaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      nullSafeEval(value)
    }
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of UnaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input: Any): Any =
    sys.error(s"UnaryExpressions must override either eval or nullSafeEval")

  /**
   * Called by unary expressions to generate a code block that returns null if its parent returns
   * null, and if not not null, use `f` to generate the expression.
   *
   * As an example, the following does a boolean inversion (i.e. NOT).
   * {{{
   *   defineCodeGen(ctx, ev, c => s"!($c)")
   * }}}
   *
   * @param f function that accepts a variable name and returns Java code to compute the output.
   */
  protected def defineCodeGen(
      ctx: CodeGenContext,
      ev: GeneratedExpressionCode,
      f: String => String): String = {
    nullSafeCodeGen(ctx, ev, eval => {
      s"${ev.value} = ${f(eval)};"
    })
  }

  /**
   * Called by unary expressions to generate a code block that returns null if its parent returns
   * null, and if not not null, use `f` to generate the expression.
   *
   * @param f function that accepts the non-null evaluation result name of child and returns Java
   *          code to compute the output.
   */
  protected def nullSafeCodeGen(
      ctx: CodeGenContext,
      ev: GeneratedExpressionCode,
      f: String => String): String = {
    val eval = child.gen(ctx)
    val resultCode = f(eval.value)
    eval.code + s"""
      boolean ${ev.isNull} = ${eval.isNull};
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        $resultCode
      }
    """
  }
}


/**
 * An expression with two inputs and one output. The output is by default evaluated to null
 * if any input is evaluated to null.
 */
abstract class BinaryExpression extends Expression {

  def left: Expression
  def right: Expression

  override def children: Seq[Expression] = Seq(left, right)

  override def foldable: Boolean = left.foldable && right.foldable

  override def nullable: Boolean = left.nullable || right.nullable

  /**
   * Default behavior of evaluation according to the default nullability of BinaryExpression.
   * If subclass of BinaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val value1 = left.eval(input)
    if (value1 == null) {
      null
    } else {
      val value2 = right.eval(input)
      if (value2 == null) {
        null
      } else {
        nullSafeEval(value1, value2)
      }
    }
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of BinaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input1: Any, input2: Any): Any =
    sys.error(s"BinaryExpressions must override either eval or nullSafeEval")

  /**
   * Short hand for generating binary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts two variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
      ctx: CodeGenContext,
      ev: GeneratedExpressionCode,
      f: (String, String) => String): String = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      s"${ev.value} = ${f(eval1, eval2)};"
    })
  }

  /**
   * Short hand for generating binary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 2 non-null evaluation result names of children
   *          and returns Java code to compute the output.
   */
  protected def nullSafeCodeGen(
      ctx: CodeGenContext,
      ev: GeneratedExpressionCode,
      f: (String, String) => String): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val resultCode = f(eval1.value, eval2.value)
    s"""
      ${eval1.code}
      boolean ${ev.isNull} = ${eval1.isNull};
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${eval2.code}
        if (!${eval2.isNull}) {
          $resultCode
        } else {
          ${ev.isNull} = true;
        }
      }
    """
  }
}


/**
 * A [[BinaryExpression]] that is an operator, with two properties:
 *
 * 1. The string representation is "x symbol y", rather than "funcName(x, y)".
 * 2. Two inputs are expected to the be same type. If the two inputs have different types,
 *    the analyzer will find the tightest common type and do the proper type casting.
 */
abstract class BinaryOperator extends BinaryExpression with ExpectsInputTypes {

  /**
   * Expected input type from both left/right child expressions, similar to the
   * [[ImplicitCastInputTypes]] trait.
   */
  def inputType: AbstractDataType

  def symbol: String

  override def toString: String = s"($left $symbol $right)"

  override def inputTypes: Seq[AbstractDataType] = Seq(inputType, inputType)

  override def checkInputDataTypes(): TypeCheckResult = {
    // First check whether left and right have the same type, then check if the type is acceptable.
    if (left.dataType != right.dataType) {
      TypeCheckResult.TypeCheckFailure(s"differing types in '$prettyString' " +
        s"(${left.dataType.simpleString} and ${right.dataType.simpleString}).")
    } else if (!inputType.acceptsType(left.dataType)) {
      TypeCheckResult.TypeCheckFailure(s"'$prettyString' requires ${inputType.simpleString} type," +
        s" not ${left.dataType.simpleString}")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }
}


private[sql] object BinaryOperator {
  def unapply(e: BinaryOperator): Option[(Expression, Expression)] = Some((e.left, e.right))
}

/**
 * An expression with three inputs and one output. The output is by default evaluated to null
 * if any input is evaluated to null.
 */
abstract class TernaryExpression extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
   * Default behavior of evaluation according to the default nullability of TernaryExpression.
   * If subclass of BinaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val exprs = children
    val value1 = exprs(0).eval(input)
    if (value1 != null) {
      val value2 = exprs(1).eval(input)
      if (value2 != null) {
        val value3 = exprs(2).eval(input)
        if (value3 != null) {
          return nullSafeEval(value1, value2, value3)
        }
      }
    }
    null
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of TernaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any =
    sys.error(s"BinaryExpressions must override either eval or nullSafeEval")

  /**
   * Short hand for generating binary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts two variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
    ctx: CodeGenContext,
    ev: GeneratedExpressionCode,
    f: (String, String, String) => String): String = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2, eval3) => {
      s"${ev.value} = ${f(eval1, eval2, eval3)};"
    })
  }

  /**
   * Short hand for generating binary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 2 non-null evaluation result names of children
   *          and returns Java code to compute the output.
   */
  protected def nullSafeCodeGen(
    ctx: CodeGenContext,
    ev: GeneratedExpressionCode,
    f: (String, String, String) => String): String = {
    val evals = children.map(_.gen(ctx))
    val resultCode = f(evals(0).value, evals(1).value, evals(2).value)
    s"""
      ${evals(0).code}
      boolean ${ev.isNull} = true;
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      if (!${evals(0).isNull}) {
        ${evals(1).code}
        if (!${evals(1).isNull}) {
          ${evals(2).code}
          if (!${evals(2).isNull}) {
            ${ev.isNull} = false;  // resultCode could change nullability
            $resultCode
          }
        }
      }
    """
  }
}
