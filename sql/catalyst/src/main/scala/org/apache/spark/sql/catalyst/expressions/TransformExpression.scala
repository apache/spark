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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, Reducer, ReducibleFunction, ScalarFunction}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.DataType

/**
 * The identity of a partition transform function. It holds the two values
 * [[TransformExpression]] compares in `isSameFunction`, and it carries no exprIds, so an
 * expression can hold one in a plain field and expression equality still answers the same after
 * canonicalization.
 *
 * @param canonicalName the transform function's canonical name
 * @param numBucketsOpt the number of buckets if the transform is `bucket`. Unset otherwise.
 */
case class TransformFunctionId(canonicalName: String, numBucketsOpt: Option[Int])

/**
 * Represents a partition transform expression, for instance, `bucket`, `days`, `years`, etc.
 *
 * @param function the transform function itself. Spark will use it to decide whether two
 *                 partition transform expressions are compatible.
 * @param numBucketsOpt the number of buckets if the transform is `bucket`. Unset otherwise.
 * @param reducedWith the transform this one's partition keys were reduced together with, if they
 *                    were. A storage-partitioned join can reduce both sides' keys onto a common
 *                    key space, and when both sides reduce that space is a third one that neither
 *                    transform describes. The keys become `r1(f1(x))` = `r2(f2(x))`, so this
 *                    expression computes neither their values nor their data type. Set, it says
 *                    exactly that, and names the other half of the pairing that produced the space,
 *                    which is the only thing that tells two such key spaces apart. See
 *                    `KeyedShuffleSpec.reducersBothWays`, which is the only producer.
 */
case class TransformExpression(
    function: BoundFunction,
    children: Seq[Expression],
    numBucketsOpt: Option[Int] = None,
    reducedWith: Option[TransformFunctionId] = None) extends Expression {

  override def nullable: Boolean = true

  /** Drops the trailing `reducedWith` when it is unset, so the ordinary form is unchanged. */
  override protected def stringArgs: Iterator[Any] =
    if (reducedWith.isEmpty) super.stringArgs.take(productArity - 1) else super.stringArgs

  /** The identity of this expression's transform function. */
  lazy val functionId: TransformFunctionId =
    TransformFunctionId(function.canonicalName(), numBucketsOpt)

  /**
   * Whether this [[TransformExpression]] has the same semantics as `other`.
   * For instance, `bucket(32, c)` is equal to `bucket(32, d)`, but not to `bucket(16, d)` or
   * `year(c)`.
   *
   * This will be used, for instance, by Spark to determine whether storage-partitioned join can
   * be triggered, by comparing partition transforms from both sides of the join and checking
   * whether they are compatible.
   *
   * It compares the transforms only. A caller that compares partition keys has to consult
   * `reducedWith` as well, since a reduced key space is not the one its transform names.
   *
   * @param other the transform expression to compare to
   * @return true if this and `other` has the same semantics w.r.t to transform, false otherwise.
   */
  def isSameFunction(other: TransformExpression): Boolean = functionId == other.functionId

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
          val thisReducer = reducer(f, numBucketsOpt, o, other.numBucketsOpt)
          val otherReducer = reducer(o, other.numBucketsOpt, f, numBucketsOpt)
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
      case(e1: ReducibleFunction[_, _], e2: ReducibleFunction[_, _]) =>
        reducer(e1, numBucketsOpt, e2, other.numBucketsOpt)
      case _ => None
    }
  }

  /**
   * Re-targets this partition transform expression at `attr`. A partition transform expression
   * has a single leaf attribute (`KeyedPartitioning.supportsExpressions`), so this replaces that
   * attribute and keeps the rest of the expression: any field path above the leaf comes from this
   * expression, not from the re-targeted key. Re-targeting is therefore only faithful when the
   * source and the target key expressions have the same path shape.
   */
  def withReference(attr: Attribute): TransformExpression =
    transform { case _: AttributeReference => attr }.asInstanceOf[TransformExpression]

  // Return a Reducer for a reducible function on another reducible function
  private def reducer(
      thisFunction: ReducibleFunction[_, _],
      thisNumBucketsOpt: Option[Int],
      otherFunction: ReducibleFunction[_, _],
      otherNumBucketsOpt: Option[Int]): Option[Reducer[_, _]] = {
    val res = (thisNumBucketsOpt, otherNumBucketsOpt) match {
      case (Some(numBuckets), Some(otherNumBuckets)) =>
        thisFunction.reducer(numBuckets, otherFunction, otherNumBuckets)
      case _ => thisFunction.reducer(otherFunction)
    }
    Option(res)
  }

  /**
   * The unordered pair of transforms whose reduce produced this expression's keys, which is what
   * identifies the key space they landed in. Reducing is symmetric, so the two sides of one reduce
   * carry the same pair.
   */
  private def reducedKeySpace: Option[Set[TransformFunctionId]] =
    reducedWith.map(partner => Set(functionId, partner))

  /**
   * Whether this and `other` describe the same reduced key space, i.e. whether the same pair of
   * transforms was reduced together to produce both.
   *
   * Two reduces that happen to land on the same space through different pairings are not
   * recognised as one. For instance `bucket(12)` with `bucket(8)` and `bucket(12)` with
   * `bucket(20)` both reduce onto `id % 4`. The [[Reducer]] API does not name the space it reduces
   * onto, so the pairing is all there is to compare.
   */
  def hasSameReducedKeys(other: TransformExpression): Boolean =
    reducedWith.isDefined && reducedKeySpace == other.reducedKeySpace

  /** Records that this expression's keys were reduced together with `other`'s. */
  def reducedTogetherWith(other: TransformExpression): TransformExpression =
    copy(reducedWith = Some(other.functionId))

  override def dataType: DataType = function.resultType()

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

  /**
   * The scalar function call this transform stands for, with the bucket count prepended as a
   * literal argument. `None` when the bound function is not a [[ScalarFunction]], and also when a
   * join reduced this expression's keys, since then the call no longer computes them. Evaluating it
   * to place a row would send the row to a partition it does not belong to. That second arm is a
   * local gate. No caller reaches it today, because every consumer of a reduced partitioning
   * refuses it first, and the write path never sees one.
   */
  lazy val resolvedFunction: Option[Expression] = function match {
    case scalarFunc: ScalarFunction[_] if reducedWith.isEmpty =>
      val arguments = numBucketsOpt.fold(children)(n => Literal(n) +: children)
      Some(V2ExpressionUtils.resolveScalarFunction(scalarFunc, arguments))
    case _ => None
  }

  override def eval(input: InternalRow): Any = resolvedFunction match {
    case Some(fn) => fn.eval(input)
    case None => throw QueryExecutionErrors.cannotEvaluateExpressionError(this)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)
}

object TransformExpression {
  /**
   * Whether `e` is a partition expression whose keys a join reduced onto a key space that no
   * transform describes, so that `e` no longer computes them. False after a one-side reduce, where
   * the expression reported is the target transform and describes the reduced keys exactly. False
   * for anything that is not a [[TransformExpression]], an attribute in particular, since a reduce
   * always leaves a transform behind.
   */
  def hasReducedKeys(e: Expression): Boolean = e match {
    case t: TransformExpression => t.reducedWith.isDefined
    case _ => false
  }
}
