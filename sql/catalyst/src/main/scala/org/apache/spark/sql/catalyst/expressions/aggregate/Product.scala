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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ImplicitCastInputTypes, Literal}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{AbstractDataType, DataType, DoubleType}


/** Multiply numerical values within an aggregation group */
case class Product(child: Expression)
    extends DeclarativeAggregate with ImplicitCastInputTypes with UnaryLike[Expression] {

  override def nullable: Boolean = true

  override def dataType: DataType = DoubleType

  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType)

  private lazy val product = AttributeReference("product", dataType)()

  private lazy val one = Literal(1.0, dataType)

  override lazy val aggBufferAttributes = product :: Nil

  override lazy val initialValues: Seq[Expression] =
    Seq(Literal(null, dataType))

  override lazy val updateExpressions: Seq[Expression] = {
    // Treat the result as null until we have seen at least one child value,
    // whereupon the previous product is promoted to being unity.

    val protoResult = coalesce(product, one) * child

    if (child.nullable) {
      Seq(coalesce(protoResult, product))
    } else {
      Seq(protoResult)
    }
  }

  override lazy val mergeExpressions: Seq[Expression] =
    Seq(coalesce(coalesce(product.left, one) * product.right, product.left))

  override lazy val evaluateExpression: Expression = product

  override protected def withNewChildInternal(newChild: Expression): Product =
    copy(child = newChild)
}
