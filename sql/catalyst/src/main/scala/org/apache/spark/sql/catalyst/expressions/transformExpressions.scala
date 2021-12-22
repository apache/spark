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

import org.apache.spark.sql.connector.catalog.functions.BoundFunction
import org.apache.spark.sql.types.DataType

abstract class TransformExpression extends Expression with Unevaluable {
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
  def equalsTo(other: TransformExpression): Boolean
}

/**
 * A transform expression defined by a V2 data source.
 */
case class DataSourceTransformExpression(
    function: BoundFunction,
    children: Seq[Expression]) extends TransformExpression {

  override def equalsTo(other: TransformExpression): Boolean = other match {
    case DataSourceTransformExpression(otherFunction, _) =>
      function.canonicalName() == otherFunction.canonicalName()
    case _ =>
      false
  }

  override def dataType: DataType = function.resultType()

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)
}

case class DataSourceBucketTransformExpression(
    numBuckets: Int,
    function: BoundFunction,
    children: Seq[Expression]) extends TransformExpression {

  override def equalsTo(other: TransformExpression): Boolean = other match {
    case DataSourceBucketTransformExpression(otherNumBuckets, otherFunction, _) =>
      numBuckets == otherNumBuckets && function.canonicalName() == otherFunction.canonicalName()
    case _ =>
      false
  }

  override def dataType: DataType = function.resultType()

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)
}
