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

import scala.util.Try

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.FUNCTION_NAME
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, Reducer, ReducibleFunction, ReducibleParameters, ScalarFunction}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.DataType

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
   * Whether every argument of this transform is either a literal parameter or a bare column
   * reference (an [[Attribute]] or a [[GetStructField]] chain). This is the condition under which
   * the transform's column slot can be safely rewritten to a join key (see
   * `KeyedShuffleSpec.createPartitioning`): that rewrite replaces each non-literal child wholesale
   * with the clustering key, which is only correct when the child IS a plain column reference.
   *
   * It excludes nested transforms (`bucket(4, years(ts))`), transforms hidden under a wrapper
   * (`bucket(4, cast(years(ts)))`), and value-changing slots (`bucket(4, cast(a))`,
   * `bucket(4, a + 1)`). A single non-recursive pass suffices: any disqualifying node appears at
   * the top of some child, and [[isColumnRef]] rejects it without needing to look inside.
   */
  def hasOnlyReferenceArgs: Boolean = children.forall {
    case _: Literal => true
    case e => isColumnRef(e)
  }

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

  @scala.annotation.tailrec
  private def isColumnRef(e: Expression): Boolean = e match {
    case _: Attribute => true
    case g: GetStructField => isColumnRef(g.child)
    case _ => false
  }

  /**
   * Whether every non-literal child of this and `other` is structurally the same: nested transforms
   * must recursively be the same function, and any other slot must be a column reference. Literal
   * children may differ -- they are exactly the parameters a [[Reducer]] is allowed to reconcile.
   *
   * This guards the reducer path. A [[Reducer]] is derived from the outer literal parameters alone
   * (e.g. bucket numBuckets, truncate width); the nested transform children are not visible to it.
   * It is therefore only valid when those nested children are identical. Without this check,
   * `bucket(4, years(ts))` and `bucket(2, days(ts))` would be reduced via `gcd(4, 2) = 2`, silently
   * joining mismatched partitions even though `years(ts)` and `days(ts)` are different transforms.
   */
  private def nonLiteralChildrenSame(other: TransformExpression): Boolean =
    childrenMatch(other)((_, _) => true)

  /**
   * Pairwise-match this transform's children against `other`'s. Requires equal arity, recursively
   * the same function for nested transform arguments, and a plain column reference (Attribute /
   * GetStructField chain) on both sides for any other slot. The `literalsMatch` predicate decides
   * how literal parameters are compared:
   *   - `_ == _` for exact equality ([[isSameFunction]]);
   *   - `(_, _) => true` to allow them to differ ([[nonLiteralChildrenSame]], the reducer check,
   *     where differing literal parameters are exactly what a [[Reducer]] reconciles).
   *
   * Note nested transform arguments always require full sameness via [[isSameFunction]] regardless
   * of `literalsMatch`: the reducer is blind to nested transforms, so they must be identical.
   */
  private def childrenMatch(other: TransformExpression)
      (literalsMatch: (Literal, Literal) => Boolean): Boolean =
    children.length == other.children.length &&
      children.zip(other.children).forall {
        case (l1: Literal, l2: Literal) => literalsMatch(l1, l2)
        case (t1: TransformExpression, t2: TransformExpression) => t1.isSameFunction(t2)
        // any other pair: both must be plain column references; column identity is ignored, but a
        // non-reference slot (Add, Cast, ...) or a Literal/Transform-vs-ref mismatch is "not same"
        case (c1, c2) => isColumnRef(c1) && isColumnRef(c2)
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
   * Extract all literal parameters from a transform expression.
   * Returns ReducibleParameters containing the literal values in order.
   *
   * Examples:
   *   bucket(4, col)        => ReducibleParameters([4])
   *   truncate(col, 3)      => ReducibleParameters([3])
   *   days(col)             => ReducibleParameters([])  (no literals)
   */
  private def extractParameters(expr: TransformExpression): ReducibleParameters = {
    import scala.jdk.CollectionConverters._
    val values = expr.literalChildren.map {
      case Literal(value, dt) => CatalystTypeConverters.convertToScala(value, dt)
    }
    new ReducibleParameters(values.asJava)
  }

  /**
   * Return a Reducer for a reducible function on another reducible function
   * Handles both parameterized (bucket, truncate) and non-parameterized (days, hours) functions.
   */
  private def reducer(
      thisFunction: ReducibleFunction[_, _],
      thisExpr: TransformExpression,
      otherFunction: ReducibleFunction[_, _],
      otherExpr: TransformExpression): Option[Reducer[_, _]] = {
    // The reducer is derived from the literal parameters only (extractParameters drops nested
    // transform children), so it is valid only when every non-literal child is structurally
    // identical. This protects both `isCompatible` and the public `reducers` entry point from
    // reducing across unrelated nested transforms, e.g. bucket(4, years(ts)) vs bucket(2, days(ts))
    if (!thisExpr.nonLiteralChildrenSame(otherExpr)) {
      return None
    }
    val thisParams = extractParameters(thisExpr)
    val otherParams = extractParameters(otherExpr)
    val thisName = thisExpr.function.canonicalName()

    def isSingleInt(p: ReducibleParameters): Boolean = {
      p.count() == 1 && p.get(0).isInstanceOf[Int]
    }

    // Both thrown exceptions and `null` returns collapse to None; any failure
    // to compute a reducer falls back to a shuffle (no SPJ).
    def tryReduce[R](call: => R): Try[Option[R]] = {
      val attempt = Try(Option(call))
      attempt.failed.foreach {
        case e: UnsupportedOperationException =>
          logWarning(log"V2 function ${MDC(FUNCTION_NAME, thisName)} threw " +
            log"UnsupportedOperationException; treating as not reducible. Override " +
            log"reducer(ReducibleParameters, ReducibleFunction, ReducibleParameters) " +
            log"to enable SPJ.")
        case _ =>
      }

      attempt
    }

    val res: Try[Option[Reducer[_, _]]] =
      if (thisParams.isEmpty && otherParams.isEmpty) {
        tryReduce(thisFunction.reducer(otherFunction))
      } else if (isSingleInt(thisParams) && isSingleInt(otherParams)) {
        // Try deprecated int-API first for legacy connectors (e.g. Iceberg 1.10);
        // the first attempt is silent because we have a fallback. Only the fallback warns.
        Try(Option(thisFunction.reducer(
            thisParams.getInt(0), otherFunction, otherParams.getInt(0))))
          .orElse(tryReduce(thisFunction.reducer(thisParams, otherFunction, otherParams)))
      } else {
        // Parameterized functions (bucket, truncate, etc.)
        tryReduce(thisFunction.reducer(thisParams, otherFunction, otherParams))
      }
    res.toOption.flatten
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
