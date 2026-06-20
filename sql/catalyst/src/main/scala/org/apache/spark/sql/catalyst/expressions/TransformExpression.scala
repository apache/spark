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

import scala.annotation.tailrec
import scala.util.Try

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.FUNCTION_NAME
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, Reducer, ReducibleFunction, ScalarFunction}
import org.apache.spark.sql.connector.expressions.{Literal => V2Literal, LiteralValue}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{AtomicType, DataType, IntegerType}

/**
 * Represents a partition transform expression, for instance, `bucket`, `days`, `years`, etc.
 *
 * @param function the transform function itself. Spark will use it to decide whether two
 *                 partition transform expressions are compatible.
 */
case class TransformExpression(function: BoundFunction, children: Seq[Expression])
    extends Expression with Logging {

  override def nullable: Boolean = true

  /**
   * Extract literal children (constant parameters) from this transform. These are constant
   * arguments like width in truncate(col, width). Literals are compared when checking if two
   * transforms are the same.
   */
  private lazy val literalChildren: Seq[Literal] =
    children.collect { case l: Literal => l }

  /**
   * Whether this [[TransformExpression]] has the same semantics as `other`. For instance,
   * `bucket(32, c)` is equal to `bucket(32, d)`, but not to `bucket(16, d)` or `year(c)`.
   * Similarly, `truncate(c, 2)` is equal to `truncate(d, 2)`, but may not to `truncate(c, 4)`.
   *
   * This will be used, for instance, by Spark to determine whether storage-partitioned join can
   * be triggered, by comparing partition transforms from both sides of the join and checking
   * whether they are compatible.
   *
   * Two transforms are considered the same when they have the same function name, the same arity,
   * and each pair of corresponding children matches:
   *   - literal arguments must be equal (e.g. numBuckets for bucket, width for truncate), so that
   *     `bucket(32, c)` is not the same as `bucket(16, c)`;
   *   - nested transform arguments must recursively be the same function, so that
   *     `bucket(4, years(c))` is not the same as `bucket(4, days(c))`;
   *   - everything else must be a plain column reference on both sides. Column identity is
   *     intentionally ignored (it is reconciled separately via positional matching), but a
   *     non-reference slot such as `c + 1` or `cast(c)`, or a literal/transform-vs-reference
   *     mismatch, is treated as not the same.
   *
   * @param other
   *   the transform expression to compare to
   * @return
   *   true if this and `other` has the same semantics w.r.t to transform, false otherwise.
   */
  def isSameFunction(other: TransformExpression): Boolean =
    function.canonicalName() == other.function.canonicalName() &&
      childrenMatch(other)(_ == _)

  /**
   * Per-position match of children, requiring equal arity. Literal slots are compared by the
   * caller-supplied `literalsMatch`; nested transform slots must recursively be the same function;
   * any other slot must be a plain column reference on both sides.
   */
  private def childrenMatch(other: TransformExpression)
      (literalsMatch: (Literal, Literal) => Boolean): Boolean =
    children.length == other.children.length &&
      children.zip(other.children).forall {
        case (l1: Literal, l2: Literal) => literalsMatch(l1, l2)
        case (t1: TransformExpression, t2: TransformExpression) => t1.isSameFunction(t2)
        case (c1, c2) => TransformExpression.isColumnRef(c1) && TransformExpression.isColumnRef(c2)
      }

  /**
   * Whether this [[TransformExpression]]'s function is compatible with the `other`
   * [[TransformExpression]]'s function.
   *
   * This is true if both are instances of [[ReducibleFunction]] and there exists a [[Reducer]] r(x)
   * such that r(t1(x)) = t2(x), or r(t2(x)) = t1(x), for all input x.
   *
   * @param other the transform expression to compare to
   * @return true if compatible, false if not
   */
  def isCompatible(other: TransformExpression): Boolean = {
    if (isSameFunction(other)) {
      true
    } else {
      (function, other.function) match {
        case (f: ReducibleFunction[_, _], o: ReducibleFunction[_, _]) =>
          val thisReducer = reducer(f, this, o, other)
          val otherReducer = reducer(o, other, f, this)
          thisReducer.isDefined || otherReducer.isDefined
        case _ => false
      }
    }
  }

  /**
   * Return a [[Reducer]] for this transform expression on another
   * on the transform expression.
   * <p>
   * A [[Reducer]] exists for a transform expression function if it is
   * 'reducible' on the other expression function.
   * <p>
   * @return reducer function or None if not reducible on the other transform expression
   */
  def reducers(other: TransformExpression): Option[Reducer[_, _]] = {
    (function, other.function) match {
      case (e1: ReducibleFunction[_, _], e2: ReducibleFunction[_, _]) =>
        reducer(e1, this, e2, other)
      case _ => None
    }
  }

  /**
   * Extract all literal parameters of this transform as V2 [[V2Literal]]s, preserving each value's
   * internal representation and its `DataType`. Connectors interpret the value via the accompanying
   * `DataType` rather than relying on a pre-converted JVM type.
   *
   * Examples:
   *   bucket(4, col)        => [Literal(4, IntegerType)]
   *   truncate(col, 3)      => [Literal(3, IntegerType)]
   *   days(col)             => []  (no literals)
   */
  private def extractParameters: Array[V2Literal[_]] =
    literalChildren.map(l => LiteralValue(l.value, l.dataType): V2Literal[_]).toArray

  /**
   * Reducer precondition: same argument layout/structure as `other` (arity, aligned slots, equal
   * nested transforms, column refs elsewhere). Only literal *values* may differ. Unlike
   * [[isSameFunction]] the function name is not compared.
   */
  private def sameArgumentLayout(other: TransformExpression): Boolean =
    childrenMatch(other)((_, _) => true)

  /**
   * Whether every literal parameter is a scalar (an [[AtomicType]]). Reducer parameters are scalar
   * literals; this never forwards a complex Catalyst container (ArrayData / MapData / InternalRow)
   * across the public reducer boundary -- such a transform is simply treated as not reducible.
   */
  private def scalarLiteralParams: Boolean =
    literalChildren.forall(_.dataType.isInstanceOf[AtomicType])

  /**
   * Return a Reducer for a reducible function on another reducible function
   * Handles both parameterized (bucket, truncate) and non-parameterized (days, hours) functions.
   */
  private def reducer(
      thisFunction: ReducibleFunction[_, _],
      thisExpr: TransformExpression,
      otherFunction: ReducibleFunction[_, _],
      otherExpr: TransformExpression): Option[Reducer[_, _]] = {
    if (!thisExpr.sameArgumentLayout(otherExpr) ||
        !thisExpr.scalarLiteralParams || !otherExpr.scalarLiteralParams) {
      return None
    }

    val thisParams = thisExpr.extractParameters
    val otherParams = otherExpr.extractParameters
    val thisName = thisExpr.function.canonicalName()

    // Gate on DataType, not the boxed runtime class (DateType/YearMonthInterval box to Int).
    def isSingleInt(p: Array[V2Literal[_]]): Boolean = {
      p.length == 1 && p(0).dataType == IntegerType
    }

    // Run a reducer overload; a thrown exception or a null both become None. warnOnUoe logs a hint
    // when the function implements no usable reducer overload.
    def attempt[R](call: => R, warnOnUoe: Boolean): Option[R] = {
      val t = Try(Option(call))
      if (warnOnUoe) {
        t.failed.foreach {
          case _: UnsupportedOperationException =>
            logWarning(log"V2 function ${MDC(FUNCTION_NAME, thisName)} threw " +
              log"UnsupportedOperationException; treating as not reducible. Override " +
              log"reducer(Literal[], ReducibleFunction, Literal[]) to enable SPJ.")
          case _ =>
        }
      }
      t.toOption.flatten
    }

    if (thisParams.isEmpty && otherParams.isEmpty) {
      attempt(thisFunction.reducer(otherFunction), warnOnUoe = true)
    } else if (isSingleInt(thisParams) && isSingleInt(otherParams)) {
      // Try the deprecated int API first (legacy connectors); fall back to the generalized overload
      // when it is absent or returns null. Option.orElse fires on None, covering both.
      attempt(thisFunction.reducer(
          thisParams(0).value().asInstanceOf[Int], otherFunction,
          otherParams(0).value().asInstanceOf[Int]), warnOnUoe = false)
        .orElse(
          attempt(thisFunction.reducer(thisParams, otherFunction, otherParams), warnOnUoe = true))
    } else {
      // Parameterized functions (bucket, truncate, etc.)
      attempt(thisFunction.reducer(thisParams, otherFunction, otherParams), warnOnUoe = true)
    }
  }

  override def dataType: DataType = function.resultType()

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

  private lazy val resolvedFunction: Option[Expression] = this match {
    case TransformExpression(scalarFunc: ScalarFunction[_], arguments) =>
      Some(V2ExpressionUtils.resolveScalarFunction(scalarFunc, arguments))
    case _ => None
  }

  override def eval(input: InternalRow): Any = {
    resolvedFunction match {
      case Some(fn) => fn.eval(input)
      case None => throw QueryExecutionErrors.cannotEvaluateExpressionError(this)
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)
}

object TransformExpression {
  /**
   * Whether `e` is a bare column reference: an [[Attribute]] or a [[GetStructField]] chain
   * (struct-field access on a column). Shared by [[TransformExpression.isSameFunction]] and by
   * `KeyedPartitioning.supportsExpressions`, which both decide whether a transform's single
   * non-literal argument is a plain column.
   */
  @tailrec
  private[sql] def isColumnRef(e: Expression): Boolean = e match {
    case _: Attribute => true
    case g: GetStructField => isColumnRef(g.child)
    case _ => false
  }
}
