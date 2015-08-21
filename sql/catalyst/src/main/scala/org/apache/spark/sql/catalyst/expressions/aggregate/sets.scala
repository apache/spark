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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratedExpressionCode, CodeGenContext, CodegenFallback}
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.collection.OpenHashSet


/** Reduce a set using an algebraic expression. */
case class ReduceSetAlgebraic(left: Expression, right: AlgebraicAggregate)
  extends BinaryExpression with CodegenFallback {

  override def dataType: DataType = right.dataType

  private[this] val single = right.children.size == 1
  private[this] val singleValueOrdinal = right.bufferSchema.length

  // This might be taking reuse too far...
  @transient private[this] lazy val buffer = {
    val singleSize = if (single) 1 else 0
    new GenericMutableRow(singleValueOrdinal + singleSize)
  }

  @transient private[this] lazy val initial =
    InterpretedMutableProjection(right.initialValues).target(buffer)

  @transient private[this] lazy val update = {
    val schema = right.bufferAttributes ++ right.children.map { child =>
      AttributeReference("child", child.dataType, child.nullable)()
    }
    new InterpretedMutableProjection(right.updateExpressions, schema).target(buffer)
  }

  @transient private[this] lazy val evaluate =
    BindReferences.bindReference(right.evaluateExpression, right.bufferSchema.toAttributes)

  @transient private[this] lazy val joinRow = new JoinedRow

  override def eval(input: InternalRow): Any = {
    val result = left.eval(input).asInstanceOf[OpenHashSet[Any]]
    if (result != null) {
      initial(EmptyRow)
      val iterator = result.iterator
      // Prevent branch during iteration.
      if (single) {
        while (iterator.hasNext) {
          buffer.update(singleValueOrdinal, iterator.next)
          update(buffer)
        }
      } else {
        while (iterator.hasNext) {
          joinRow(buffer, iterator.next.asInstanceOf[InternalRow])
          update(joinRow)
        }
      }
      evaluate.eval(buffer)
    } else null
  }
}
/** Reduce a set using an AggregateFunction2. */
case class ReduceSetAggregate(left: Expression, right: AggregateFunction2)
  extends BinaryExpression with CodegenFallback {

  right.withNewMutableBufferOffset(0)

  override def dataType: DataType = right.dataType

  private[this] val single = right.children.size == 1
  @transient private[this] lazy val buffer = new GenericMutableRow(right.bufferSchema.size)
  @transient private[this] lazy val singleValueInput = new GenericMutableRow(1)

  override def eval(input: InternalRow): Any = {
    val result = left.eval(input).asInstanceOf[OpenHashSet[Any]]
    if (result != null) {
      right.initialize(buffer)
      val iterator = result.iterator
      if (single) {
        while (iterator.hasNext) {
          singleValueInput.update(0, iterator.next())
          right.update(buffer, singleValueInput)
        }
      } else {
        while (iterator.hasNext) {
          right.update(buffer, iterator.next().asInstanceOf[InternalRow])
        }
      }
      right.eval(buffer)
    } else null
  }
}
