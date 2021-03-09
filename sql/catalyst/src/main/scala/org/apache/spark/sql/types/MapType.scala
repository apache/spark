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

import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData, TypeUtils}
import org.apache.spark.sql.catalyst.util.StringUtils.StringConcat

/**
 * The data type for Maps. Keys in a map are not allowed to have `null` values.
 *
 * Please use `DataTypes.createMapType()` to create a specific instance.
 *
 * @param keyType The data type of map keys.
 * @param valueType The data type of map values.
 * @param valueContainsNull Indicates if map values have `null` values.
 */
@Stable
case class MapType(
  keyType: DataType,
  valueType: DataType,
  valueContainsNull: Boolean) extends DataType {

  /** No-arg constructor for kryo. */
  def this() = this(null, null, false)

  private[sql] def buildFormattedString(
      prefix: String,
      stringConcat: StringConcat,
      maxDepth: Int = Int.MaxValue): Unit = {
    if (maxDepth > 0) {
      stringConcat.append(s"$prefix-- key: ${keyType.typeName}\n")
      DataType.buildFormattedString(keyType, s"$prefix    |", stringConcat, maxDepth)
      stringConcat.append(s"$prefix-- value: ${valueType.typeName} " +
        s"(valueContainsNull = $valueContainsNull)\n")
      DataType.buildFormattedString(valueType, s"$prefix    |", stringConcat, maxDepth)
    }
  }

  override private[sql] def jsonValue: JValue =
    ("type" -> typeName) ~
      ("keyType" -> keyType.jsonValue) ~
      ("valueType" -> valueType.jsonValue) ~
      ("valueContainsNull" -> valueContainsNull)

  /**
   * The default size of a value of the MapType is
   * (the default size of the key type + the default size of the value type).
   * We assume that there is only 1 element on average in a map. See SPARK-18853.
   */
  override def defaultSize: Int = 1 * (keyType.defaultSize + valueType.defaultSize)

  override def simpleString: String = s"map<${keyType.simpleString},${valueType.simpleString}>"

  override def catalogString: String = s"map<${keyType.catalogString},${valueType.catalogString}>"

  override def sql: String = s"MAP<${keyType.sql}, ${valueType.sql}>"

  override private[spark] def asNullable: MapType =
    MapType(keyType.asNullable, valueType.asNullable, valueContainsNull = true)

  override private[spark] def existsRecursively(f: (DataType) => Boolean): Boolean = {
    f(this) || keyType.existsRecursively(f) || valueType.existsRecursively(f)
  }

  @transient
  private[sql] lazy val keyOrdering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(keyType)

  @transient
  private[sql] lazy val valueOrdering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(valueType)

  @transient
  private[sql] lazy val interpretedOrdering: Ordering[MapData] = new Ordering[MapData] {
    def compare(left: MapData, right: MapData): Int = {
      if (left.numElements() != right.numElements()) {
        return left.numElements() - right.numElements()
      }

      val numElements = left.numElements()
      val leftKeys = left.keyArray()
      val rightKeys = right.keyArray()
      val leftValues = left.valueArray()
      val rightValues = right.valueArray()

      val keyIndexOrdering = (keys: ArrayData) => new Ordering[Int] {
        override def compare(a: Int, b: Int): Int = {
          keyOrdering.compare(keys.get(a, keyType), keys.get(b, keyType))
        }
      }
      val leftSortedKeyIndex = (0 until numElements).toArray.sorted(keyIndexOrdering(leftKeys))
      val rightSortedKeyIndex = (0 until numElements).toArray.sorted(keyIndexOrdering(rightKeys))
      var i = 0
      while (i < numElements) {
        val leftIndex = leftSortedKeyIndex(i)
        val rightIndex = rightSortedKeyIndex(i)

        val leftKey = leftKeys.get(leftIndex, keyType)
        val rightKey = rightKeys.get(rightIndex, keyType)
        val keyComp = keyOrdering.compare(leftKey, rightKey)
        if (keyComp != 0) {
          return keyComp
        } else {
          val leftValueIsNull = leftValues.isNullAt(leftIndex)
          val rightValueIsNull = rightValues.isNullAt(rightIndex)
          if (leftValueIsNull && rightValueIsNull) {
            // do nothing
          } else if (leftValueIsNull) {
            return -1
          } else if (rightValueIsNull) {
            return 1
          } else {
            val leftValue = leftValues.get(leftIndex, valueType)
            val rightValue = rightValues.get(rightIndex, valueType)
            val valueComp = valueOrdering.compare(leftValue, rightValue)
            if (valueComp != 0) {
              return valueComp
            }
          }
        }
        i += 1
      }

      0
    }
  }
}

/**
 * @since 1.3.0
 */
@Stable
object MapType extends AbstractDataType {

  override private[sql] def defaultConcreteType: DataType = apply(NullType, NullType)

  override private[sql] def acceptsType(other: DataType): Boolean = {
    other.isInstanceOf[MapType]
  }

  override private[sql] def simpleString: String = "map"

  /**
   * Construct a [[MapType]] object with the given key type and value type.
   * The `valueContainsNull` is true.
   */
  def apply(keyType: DataType, valueType: DataType): MapType =
    MapType(keyType: DataType, valueType: DataType, valueContainsNull = true)
}
