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

import scala.collection.generic.Growable
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * The Collect aggregate function collects all seen expression values into a list of values.
 *
 * The operator is bound to the slower sort based aggregation path because the number of
 * elements (and their memory usage) can not be determined in advance. This also means that the
 * collected elements are stored on heap, and that too many elements can cause GC pauses and
 * eventually Out of Memory Errors.
 */
abstract class Collect extends ImperativeAggregate {

  val child: Expression

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  override def dataType: DataType = ArrayType(child.dataType)

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  // We need to keep track of the expression id of the list because the dataType of the attribute
  // (and the attribute itself) will change when the dataType of the child gets resolved.
  val listExprId = NamedExpression.newExprId

  override def aggBufferAttributes: Seq[AttributeReference] = {
    Seq(AttributeReference("list", dataType, nullable = false)(listExprId))
  }

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override def inputAggBufferAttributes: Seq[AttributeReference] = {
    aggBufferAttributes.map(_.newInstance())
  }

  override def update(buffer: MutableRow, input: InternalRow): Unit = {
    getMutableArray(buffer) += child.eval(input)
  }

  override def merge(buffer: MutableRow, input: InternalRow): Unit = {
    getMutableArray(buffer) ++= input.getArray(inputAggBufferOffset)
  }

  override def eval(input: InternalRow): Any = {
    // TODO return null if there are no elements?
    getMutableArray(input).toFastRandomAccess
  }

  private def getMutableArray(buffer: InternalRow): MutableArrayData = {
    buffer.getArray(mutableAggBufferOffset).asInstanceOf[MutableArrayData]
  }
}

case class CollectList(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends Collect {

  def this(child: Expression) = this(child, 0, 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def initialize(mutableAggBuffer: MutableRow): Unit = {
    mutableAggBuffer.update(mutableAggBufferOffset, ListMutableArrayData())
  }
}

case class CollectSet(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends Collect {

  def this(child: Expression) = this(child, 0, 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def initialize(mutableAggBuffer: MutableRow): Unit = {
    mutableAggBuffer.update(mutableAggBufferOffset, SetMutableArrayData())
  }
}

/**
 * MutableArrayData is an implementation of ArrayData that can be updated in place. This makes
 * the assumption that the buffer holding this object data keeps a reference to this object. This
 * means that this approach is only valid if a GenericInternalRow or a SpecializedInternalRow is
 * used as a buffer.
 */
abstract class MutableArrayData extends ArrayData {
  val buffer: Growable[Any] with Iterable[Any]

  /** Add a single element to the MutableArrayData. */
  def +=(elem: Any): MutableArrayData = {
    buffer += elem
    this
  }

  /** Add another array to the MutableArrayData. */
  def ++=(elems: ArrayData): MutableArrayData = {
    elems match {
      case input: MutableArrayData => buffer ++= input.buffer
      case input => buffer ++= input.array
    }
    this
  }

  /** Return an ArrayData instance with fast random access properties. */
  def toFastRandomAccess: ArrayData = this

  protected def getAs[T](ordinal: Int): T

  /* ArrayData methods. */
  override def numElements(): Int = buffer.size
  override def array: Array[Any] = buffer.toArray
  override def isNullAt(ordinal: Int): Boolean = getAs[AnyRef](ordinal) eq null
  override def get(ordinal: Int, elementType: DataType): AnyRef = getAs(ordinal)
  override def getBoolean(ordinal: Int): Boolean = getAs(ordinal)
  override def getByte(ordinal: Int): Byte = getAs(ordinal)
  override def getShort(ordinal: Int): Short = getAs(ordinal)
  override def getInt(ordinal: Int): Int = getAs(ordinal)
  override def getLong(ordinal: Int): Long = getAs(ordinal)
  override def getFloat(ordinal: Int): Float = getAs(ordinal)
  override def getDouble(ordinal: Int): Double = getAs(ordinal)
  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = getAs(ordinal)
  override def getUTF8String(ordinal: Int): UTF8String = getAs(ordinal)
  override def getBinary(ordinal: Int): Array[Byte] = getAs(ordinal)
  override def getInterval(ordinal: Int): CalendarInterval = getAs(ordinal)
  override def getStruct(ordinal: Int, numFields: Int): InternalRow = getAs(ordinal)
  override def getArray(ordinal: Int): ArrayData = getAs(ordinal)
  override def getMap(ordinal: Int): MapData = getAs(ordinal)
}

case class ListMutableArrayData(
    val buffer: ArrayBuffer[Any] = ArrayBuffer.empty) extends MutableArrayData {
  override protected def getAs[T](ordinal: Int): T = buffer(ordinal).asInstanceOf[T]
  override def copy(): ListMutableArrayData = ListMutableArrayData(buffer.clone())
}

case class SetMutableArrayData(
    val buffer: mutable.HashSet[Any] = mutable.HashSet.empty) extends MutableArrayData {
  override protected def getAs[T](ordinal: Int): T = buffer.toArray.apply(ordinal).asInstanceOf[T]
  override def copy(): SetMutableArrayData = SetMutableArrayData(buffer.clone())
  override def toFastRandomAccess: GenericArrayData = new GenericArrayData(buffer.toArray)
}
