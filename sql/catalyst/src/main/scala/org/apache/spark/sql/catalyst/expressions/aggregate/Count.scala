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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

case class Count(child: Expression) extends DeclarativeAggregate {
  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = false

  // Return data type.
  override def dataType: DataType = LongType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  private val count = AttributeReference("count", LongType)()

  override val aggBufferAttributes = count :: Nil

  override val initialValues = Seq(
    /* count = */ Literal(0L)
  )

  override val updateExpressions = Seq(
    /* count = */ If(IsNull(child), count, count + 1L)
  )

  override val mergeExpressions = Seq(
    /* count = */ count.left + count.right
  )

  override val evaluateExpression = Cast(count, LongType)
}
