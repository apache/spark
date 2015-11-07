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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

case class Sum(child: Expression) extends DeclarativeAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  // Expected input data type.
  // TODO: Right now, we replace old aggregate functions (based on AggregateExpression1) to the
  // new version at planning time (after analysis phase). For now, NullType is added at here
  // to make it resolved when we have cases like `select sum(null)`.
  // We can use our analyzer to cast NullType to the default data type of the NumericType once
  // we remove the old aggregate functions. Then, we will not need NullType at here.
  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(LongType, DoubleType, DecimalType, NullType))

  private val resultType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision + 10, scale)
    // TODO: Remove this line once we remove the NullType from inputTypes.
    case NullType => IntegerType
    case _ => child.dataType
  }

  private val sumDataType = resultType

  private val sum = AttributeReference("sum", sumDataType)()

  private val zero = Cast(Literal(0), sumDataType)

  override val aggBufferAttributes = sum :: Nil

  override val initialValues: Seq[Expression] = Seq(
    /* sum = */ Literal.create(null, sumDataType)
  )

  override val updateExpressions: Seq[Expression] = Seq(
    /* sum = */
    Coalesce(Seq(Add(Coalesce(Seq(sum, zero)), Cast(child, sumDataType)), sum))
  )

  override val mergeExpressions: Seq[Expression] = {
    val add = Add(Coalesce(Seq(sum.left, zero)), Cast(sum.right, sumDataType))
    Seq(
      /* sum = */
      Coalesce(Seq(add, sum.left))
    )
  }

  override val evaluateExpression: Expression = Cast(sum, resultType)
}
