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

package org.apache.spark.sql.types

import scala.math.Ordering

import org.json4s.JsonDSL._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.util.ArrayData

object ArrayType extends AbstractDataType {
  /** Construct a [[ArrayType]] object with the given element type. The `containsNull` is true. */
  def apply(elementType: DataType): ArrayType = ArrayType(elementType, containsNull = true)

  override private[sql] def defaultConcreteType: DataType = ArrayType(NullType, containsNull = true)

  override private[sql] def acceptsType(other: DataType): Boolean = {
    other.isInstanceOf[ArrayType]
  }

  override private[sql] def simpleString: String = "array"
}


/**
 * :: DeveloperApi ::
 * The data type for collections of multiple values.
 * Internally these are represented as columns that contain a ``scala.collection.Seq``.
 *
 * Please use [[DataTypes.createArrayType()]] to create a specific instance.
 *
 * An [[ArrayType]] object comprises two fields, `elementType: [[DataType]]` and
 * `containsNull: Boolean`. The field of `elementType` is used to specify the type of
 * array elements. The field of `containsNull` is used to specify if the array has `null` values.
 *
 * @param elementType The data type of values.
 * @param containsNull Indicates if values have `null` values
 */
@DeveloperApi
case class ArrayType(elementType: DataType, containsNull: Boolean) extends DataType {

  /** No-arg constructor for kryo. */
  protected def this() = this(null, false)

  private[sql] def buildFormattedString(prefix: String, builder: StringBuilder): Unit = {
    builder.append(
      s"$prefix-- element: ${elementType.typeName} (containsNull = $containsNull)\n")
    DataType.buildFormattedString(elementType, s"$prefix    |", builder)
  }

  override private[sql] def jsonValue =
    ("type" -> typeName) ~
      ("elementType" -> elementType.jsonValue) ~
      ("containsNull" -> containsNull)

  /**
   * The default size of a value of the ArrayType is 100 * the default size of the element type.
   * (We assume that there are 100 elements).
   */
  override def defaultSize: Int = 100 * elementType.defaultSize

  override def simpleString: String = s"array<${elementType.simpleString}>"

  override def sql: String = s"ARRAY<${elementType.sql}>"

  override private[spark] def asNullable: ArrayType =
    ArrayType(elementType.asNullable, containsNull = true)

  override private[spark] def existsRecursively(f: (DataType) => Boolean): Boolean = {
    f(this) || elementType.existsRecursively(f)
  }

  @transient
  private[sql] lazy val interpretedOrdering: Ordering[ArrayData] = new Ordering[ArrayData] {
    private[this] val elementOrdering: Ordering[Any] = elementType match {
      case dt: AtomicType => dt.ordering.asInstanceOf[Ordering[Any]]
      case a : ArrayType => a.interpretedOrdering.asInstanceOf[Ordering[Any]]
      case s: StructType => s.interpretedOrdering.asInstanceOf[Ordering[Any]]
      case other =>
        throw new IllegalArgumentException(s"Type $other does not support ordered operations")
    }

    def compare(x: ArrayData, y: ArrayData): Int = {
      val leftArray = x
      val rightArray = y
      val minLength = scala.math.min(leftArray.numElements(), rightArray.numElements())
      var i = 0
      while (i < minLength) {
        val isNullLeft = leftArray.isNullAt(i)
        val isNullRight = rightArray.isNullAt(i)
        if (isNullLeft && isNullRight) {
          // Do nothing.
        } else if (isNullLeft) {
          return -1
        } else if (isNullRight) {
          return 1
        } else {
          val comp =
            elementOrdering.compare(
              leftArray.get(i, elementType),
              rightArray.get(i, elementType))
          if (comp != 0) {
            return comp
          }
        }
        i += 1
      }
      if (leftArray.numElements() < rightArray.numElements()) {
        return -1
      } else if (leftArray.numElements() > rightArray.numElements()) {
        return 1
      } else {
        return 0
      }
    }
  }
}
