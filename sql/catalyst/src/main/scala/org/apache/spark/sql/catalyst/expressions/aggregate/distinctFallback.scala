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
import org.apache.spark.sql.types.{AbstractDataType, DataType}
import org.apache.spark.util.collection.OpenHashSet

/**
 * Fallback operator for distinct operators. This will be used when a user issues multiple
 * different distinct expressions in a query.
 *
 * The operator uses the OpenHashSetUDT for de-duplicating values. It is, as a result, not possible
 * to use UnsafeRow based aggregation.
 */
case class DistinctAggregateFallback(function: AggregateFunction2) extends DeclarativeAggregate {
  override def inputTypes: Seq[AbstractDataType] = function.inputTypes
  override def nullable: Boolean = function.nullable
  override def dataType: DataType = function.dataType
  override def children: Seq[Expression] = Seq(function)

  private[this] val input = function.children match {
    case child :: Nil => child
    case children => CreateStruct(children) // TODO can we test this?
  }
  private[this] val items = AttributeReference("itemSet", new OpenHashSetUDT(input.dataType))()

  override def aggBufferAttributes: Seq[AttributeReference] = Seq(items)
  override val initialValues: Seq[Expression] = Seq(NewSet(input.dataType))
  override val updateExpressions: Seq[Expression] = Seq(AddItemToSet(input, items))
  override val mergeExpressions: Seq[Expression] = Seq(CombineSets(items.left, items.right))
  override val evaluateExpression: Expression = function match {
    case f: Count => CountSet(items)
    case f: DeclarativeAggregate => ReduceSetUsingDeclarativeAggregate(items, f)
    case f: ImperativeAggregate => ReduceSetUsingImperativeAggregate(items, f)
  }
}

case class ReduceSetUsingImperativeAggregate(left: Expression, right: ImperativeAggregate)
  extends BinaryExpression with CodegenFallback {

  override def dataType: DataType = right.dataType

  private[this] val single = right.children.size == 1

  // TODO can we assume that the offsets are 0 when we haven't touched them yet?
  private[this] val function = right
    .withNewInputAggBufferOffset(0)
    .withNewMutableAggBufferOffset(0)

  @transient private[this] lazy val buffer =
    new SpecificMutableRow(right.aggBufferAttributes.map(_.dataType))

  @transient private[this] lazy val singleValueInput = new GenericMutableRow(1)

  override def eval(input: InternalRow): Any = {
    val result = left.eval(input).asInstanceOf[OpenHashSet[Any]]
    if (result != null) {
      right.initialize(buffer)
      val iterator = result.iterator
      if (single) {
        while (iterator.hasNext) {
          singleValueInput.update(0, iterator.next())
          function.update(buffer, singleValueInput)
        }
      } else {
        while (iterator.hasNext) {
          function.update(buffer, iterator.next().asInstanceOf[InternalRow])
        }
      }
      function.eval(buffer)
    } else null
  }
}

case class ReduceSetUsingDeclarativeAggregate(left: Expression, right: DeclarativeAggregate)
  extends Expression with CodegenFallback {
  override def children: Seq[Expression] = Seq(left)
  override def nullable: Boolean = right.nullable
  override def dataType: DataType = right.dataType

  private[this] val single = right.children.size == 1

  private[this] val inputOrdinal = right.children.size

  @transient private[this] lazy val initial =
    InterpretedMutableProjection(right.initialValues).target(buffer)

  @transient private[this] lazy val update = {
    val boundRefs = (right.aggBufferAttributes ++ right.children).zipWithIndex.map {
      case (e, i) => (e, new BoundReference(i, e.dataType, e.nullable))
    }.toMap
    val boundExpressions = right.updateExpressions.map(_.transform(boundRefs))
    new InterpretedMutableProjection(boundExpressions).target(buffer)
  }

  @transient private[this] lazy val evaluate =
    BindReferences.bindReference(right.evaluateExpression, right.aggBufferAttributes)

  @transient private[this] lazy val buffer = {
    val singleType = if (single) {
      Seq(right.children.head.dataType)
    } else {
      Seq.empty
    }
    new SpecificMutableRow(right.inputAggBufferAttributes.map(_.dataType) ++ singleType)
  }

  @transient private[this] lazy val joinRow = new JoinedRow

  override def eval(input: InternalRow): Any = {
    val result = left.eval(input).asInstanceOf[OpenHashSet[Any]]
    if (result != null) {
      initial(EmptyRow)
      val iterator = result.iterator
      if (single) {
        while (iterator.hasNext) {
          buffer.update(inputOrdinal, iterator.next())
          update(buffer)
        }
      } else {
        while (iterator.hasNext) {
          joinRow(buffer, iterator.next().asInstanceOf[InternalRow])
          update(joinRow)
        }
      }
      evaluate.eval(buffer)
    } else null
  }
}

/** Operator that drops a row when it contains any nulls. */
case class DropAnyNull(child: Expression) extends UnaryExpression {
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType

  protected override def nullSafeEval(input: Any): InternalRow = {
    val row = input.asInstanceOf[InternalRow]
    if (row.anyNull) {
      null
    } else {
      row
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, eval => {
      s"""
        if ($eval.anyNull) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = $eval
        }
      """
    })
  }
}
