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
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._


/**
 * An interpreted row ordering comparator.
 */
class InterpretedOrdering(ordering: Seq[SortOrder]) extends Ordering[InternalRow] {

  def this(ordering: Seq[SortOrder], inputSchema: Seq[Attribute]) =
    this(ordering.map(BindReferences.bindReference(_, inputSchema)))

  private def compareValue(
      left: Any,
      right: Any,
      dataType: DataType,
      direction: SortDirection): Int = {
    if (left == null && right == null) {
      return 0
    } else if (left == null) {
      return if (direction == Ascending) -1 else 1
    } else if (right == null) {
      return if (direction == Ascending) 1 else -1
    } else {
      dataType match {
        case dt: AtomicType if direction == Ascending =>
          return dt.ordering.asInstanceOf[Ordering[Any]].compare(left, right)
        case dt: AtomicType if direction == Descending =>
          return dt.ordering.asInstanceOf[Ordering[Any]].reverse.compare(left, right)
        case s: StructType if direction == Ascending =>
          return s.interpretedOrdering.asInstanceOf[Ordering[Any]].compare(left, right)
        case s: StructType if direction == Descending =>
          return s.interpretedOrdering.asInstanceOf[Ordering[Any]].reverse.compare(left, right)
        case a: ArrayType =>
          val leftArray = left.asInstanceOf[ArrayData]
          val rightArray = right.asInstanceOf[ArrayData]
          val minLength = scala.math.min(leftArray.numElements(), rightArray.numElements())
          var i = 0
          while (i < minLength) {
            val isNullLeft = leftArray.isNullAt(i)
            val isNullRight = rightArray.isNullAt(i)
            if (isNullLeft && isNullRight) {
              // Do nothing.
            } else if (isNullLeft) {
              return if (direction == Ascending) -1 else 1
            } else if (isNullRight) {
              return if (direction == Ascending) 1 else -1
            } else {
              val comp =
                compareValue(
                  leftArray.get(i, a.elementType),
                  rightArray.get(i, a.elementType),
                  a.elementType,
                  direction)
              if (comp != 0) {
                return comp
              }
            }
            i += 1
          }
          if (leftArray.numElements() < rightArray.numElements()) {
            return if (direction == Ascending) -1 else 1
          } else if (leftArray.numElements() > rightArray.numElements()) {
            return if (direction == Ascending) 1 else -1
          } else {
            return 0
          }
        case other =>
          throw new IllegalArgumentException(s"Type $other does not support ordered operations")
      }
    }
  }

  def compare(a: InternalRow, b: InternalRow): Int = {
    var i = 0
    while (i < ordering.size) {
      val order = ordering(i)
      val left = order.child.eval(a)
      val right = order.child.eval(b)
      val comparison = compareValue(left, right, order.dataType, order.direction)
      if (comparison != 0) {
        return comparison
      }
      i += 1
    }
    return 0
  }
}

object InterpretedOrdering {

  /**
   * Creates a [[InterpretedOrdering]] for the given schema, in natural ascending order.
   */
  def forSchema(dataTypes: Seq[DataType]): InterpretedOrdering = {
    new InterpretedOrdering(dataTypes.zipWithIndex.map {
      case (dt, index) => new SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    })
  }
}

object RowOrdering {

  /**
   * Returns true iff the data type can be ordered (i.e. can be sorted).
   */
  def isOrderable(dataType: DataType): Boolean = dataType match {
    case NullType => true
    case dt: AtomicType => true
    case struct: StructType => struct.fields.forall(f => isOrderable(f.dataType))
    case array: ArrayType => isOrderable(array.elementType)
    case _ => false
  }

  /**
   * Returns true iff outputs from the expressions can be ordered.
   */
  def isOrderable(exprs: Seq[Expression]): Boolean = exprs.forall(e => isOrderable(e.dataType))
}
