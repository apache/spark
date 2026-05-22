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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, Reducer, ReducibleFunction, ReducibleParameters, ScalarFunction}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{DataType, Decimal, DecimalType, StringType}
import org.apache.spark.unsafe.types.UTF8String

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
   * Two transforms are considered the same if:
   *   1. They have the same function name
   *   2. They have the same literal arguments (e.g., numBuckets for bucket, width for truncate)
   *
   * @param other
   *   the transform expression to compare to
   * @return
   *   true if this and `other` has the same semantics w.r.t to transform, false otherwise.
   */
  def isSameFunction(other: TransformExpression): Boolean = other match {
    case TransformExpression(otherFunction, _) =>
      val sameFunctionName = function.canonicalName() == otherFunction.canonicalName()

      // Compare literal arguments to ensure transforms with different parameters
      // (e.g., bucket(32, col) vs bucket(16, col), truncate(col, 2) vs truncate(col, 4))
      // are not considered the same
      val otherLiterals = other.literalChildren
      val sameLiterals = literalChildren.length == otherLiterals.length &&
        literalChildren.zip(otherLiterals).forall { case (l1, l2) =>
          l1.equals(l2)
        }

      sameFunctionName && sameLiterals
    case _ =>
      false
  }

  /**
   * Override canonicalized to ensure transforms with the same function and literals are
   * considered semantically equal, regardless of which specific column references they use.
   *
   * This is crucial for Storage Partitioned Joins - we need bucket(4, tableA.id) and bucket(4,
   * tableB.id) to be semantically equal so SPJ can be triggered.
   */
  override lazy val canonicalized: Expression = {
    // Canonicalize only the non-literal children (i.e., column references)
    val canonicalizedReferenceChildren = children.map {
      case l: Literal => l
      case other => other.canonicalized
    }
    TransformExpression(function, canonicalizedReferenceChildren)
  }

  /**
   * Override collectLeaves to only return reference children (columns), not literal parameters.
   *
   * For TransformExpression, literal children are metadata about the transform function (e.g.,
   * numBuckets=4 in bucket(4, col), width=2 in truncate(col, 2)). All consumers of
   * collectLeaves() expect only column references, not these metadata literals.
   *
   */
  override def collectLeaves(): Seq[Expression] = {
    children.flatMap {
      case _: Literal => Seq.empty // Skip literal parameters (metadata)
      case other => other.collectLeaves() // Include column references
    }
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
      case Literal(value, _: StringType) => value.asInstanceOf[UTF8String].toString
      case Literal(value, _: DecimalType) => value.asInstanceOf[Decimal].toJavaBigDecimal
      case Literal(value, _) => value.asInstanceOf[AnyRef]
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
