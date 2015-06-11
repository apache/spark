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

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratedExpressionCode, CodeGenContext}
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.types._


/**
 * If an expression wants to be exposed in the function registry (so users can call it with
 * "name(arguments...)", the concrete implementation must be a case class whose constructor
 * arguments are all Expressions types. In addition, if it needs to support more than one
 * constructor, define those constructors explicitly as apply methods in the companion object.
 *
 * See [[Substring]] for an example.
 */
abstract class Expression extends TreeNode[Expression] {
  self: Product =>

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
   * Returns true when the current expression always return the same result for fixed input values.
   */
  // TODO: Need to define explicit input values vs implicit input values.
  def deterministic: Boolean = true

  def nullable: Boolean

  def references: AttributeSet = AttributeSet(children.flatMap(_.references.iterator))

  /** Returns the result of evaluating this expression on a given input Row */
  def eval(input: Row = null): Any

  /**
   * Returns an [[GeneratedExpressionCode]], which contains Java source code that
   * can be used to generate the result of evaluating the expression on an input row.
   *
   * @param ctx a [[CodeGenContext]]
   * @return [[GeneratedExpressionCode]]
   */
  def gen(ctx: CodeGenContext): GeneratedExpressionCode = {
    val isNull = ctx.freshName("isNull")
    val primitive = ctx.freshName("primitive")
    val ve = GeneratedExpressionCode("", isNull, primitive)
    ve.code = genCode(ctx, ve)
    ve
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
  protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    ctx.references += this
    val objectTerm = ctx.freshName("obj")
    s"""
      /* expression: ${this} */
      Object $objectTerm = expressions[${ctx.references.size - 1}].eval(i);
      boolean ${ev.isNull} = $objectTerm == null;
      ${ctx.javaType(this.dataType)} ${ev.primitive} = ${ctx.defaultValue(this.dataType)};
      if (!${ev.isNull}) {
        ${ev.primitive} = (${ctx.boxedType(this.dataType)}) $objectTerm;
      }
    """
  }

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
   * Returns a string representation of this expression that does not have developer centric
   * debugging information like the expression id.
   */
  def prettyString: String = {
    transform {
      case a: AttributeReference => PrettyAttribute(a.name)
      case u: UnresolvedAttribute => PrettyAttribute(u.name)
    }.toString
  }

  /**
   * Returns true when two expressions will always compute the same result, even if they differ
   * cosmetically (i.e. capitalization of names in attributes may be different).
   */
  def semanticEquals(other: Expression): Boolean = this.getClass == other.getClass && {
    val elements1 = this.productIterator.toSeq
    val elements2 = other.asInstanceOf[Product].productIterator.toSeq
    elements1.length == elements2.length && elements1.zip(elements2).forall {
      case (e1: Expression, e2: Expression) => e1 semanticEquals e2
      case (i1, i2) => i1 == i2
    }
  }

  /**
   * Checks the input data types, returns `TypeCheckResult.success` if it's valid,
   * or returns a `TypeCheckResult` with an error message if invalid.
   * Note: it's not valid to call this method until `childrenResolved == true`
   * TODO: we should remove the default implementation and implement it for all
   * expressions with proper error message.
   */
  def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess
}

abstract class BinaryExpression extends Expression with trees.BinaryNode[Expression] {
  self: Product =>

  def symbol: String = sys.error(s"BinaryExpressions must override either toString or symbol")

  override def foldable: Boolean = left.foldable && right.foldable

  override def nullable: Boolean = left.nullable || right.nullable

  override def toString: String = s"($left $symbol $right)"

  /**
   * Short hand for generating binary evaluation code, which depends on two sub-evaluations of
   * the same type.  If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts two variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
      ctx: CodeGenContext,
      ev: GeneratedExpressionCode,
      f: (String, String) => String): String = {
    // TODO: Right now some timestamp tests fail if we enforce this...
    if (left.dataType != right.dataType) {
      // log.warn(s"${left.dataType} != ${right.dataType}")
    }

    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val resultCode = f(eval1.primitive, eval2.primitive)

    s"""
      ${eval1.code}
      boolean ${ev.isNull} = ${eval1.isNull};
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${eval2.code}
        if (!${eval2.isNull}) {
          ${ev.primitive} = $resultCode;
        } else {
          ${ev.isNull} = true;
        }
      }
    """
  }
}

private[sql] object BinaryExpression {
  def unapply(e: BinaryExpression): Option[(Expression, Expression)] = Some((e.left, e.right))
}

abstract class LeafExpression extends Expression with trees.LeafNode[Expression] {
  self: Product =>
}

abstract class UnaryExpression extends Expression with trees.UnaryNode[Expression] {
  self: Product =>

  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable

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
    val eval = child.gen(ctx)
    // reuse the previous isNull
    ev.isNull = eval.isNull
    eval.code + s"""
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${ev.primitive} = ${f(eval.primitive)};
      }
    """
  }
}

/**
 * Expressions that require a specific `DataType` as input should implement this trait
 * so that the proper type conversions can be performed in the analyzer.
 */
trait ExpectsInputTypes {
  self: Expression =>

  def expectedChildTypes: Seq[DataType]

  override def checkInputDataTypes(): TypeCheckResult = {
    // We will always do type casting for `ExpectsInputTypes` in `HiveTypeCoercion`,
    // so type mismatch error won't be reported here, but for underling `Cast`s.
    TypeCheckResult.TypeCheckSuccess
  }
}
