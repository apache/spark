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

import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, Reducer, ReducibleFunction}
import org.apache.spark.sql.types.DataType

/**
 * Represents a partition transform expression, for instance, `bucket`, `days`, `years`, etc.
 *
 * @param function the transform function itself. Spark will use it to decide whether two
 *                 partition transform expressions are compatible.
 * @param numBucketsOpt the number of buckets if the transform is `bucket`. Unset otherwise.
 */
case class TransformExpression(
    function: BoundFunction,
    children: Seq[Expression],
    numBucketsOpt: Option[Int] = None) extends Expression with Unevaluable {

  override def nullable: Boolean = true

  /**
   * Whether this [[TransformExpression]] has the same semantics as `other`.
   * For instance, `bucket(32, c)` is equal to `bucket(32, d)`, but not to `bucket(16, d)` or
   * `year(c)`.
   *
   * This will be used, for instance, by Spark to determine whether storage-partitioned join can
   * be triggered, by comparing partition transforms from both sides of the join and checking
   * whether they are compatible.
   *
   * @param other the transform expression to compare to
   * @return true if this and `other` has the same semantics w.r.t to transform, false otherwise.
   */
  def isSameFunction(other: TransformExpression): Boolean = other match {
    case TransformExpression(otherFunction, _, otherNumBucketsOpt) =>
      function.canonicalName() == otherFunction.canonicalName() &&
        numBucketsOpt == otherNumBucketsOpt
    case _ =>
      false
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

  override def dataType: DataType = function.resultType()

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)
}
