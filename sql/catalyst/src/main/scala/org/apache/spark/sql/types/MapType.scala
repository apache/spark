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

import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.catalyst.util.{MapData, TypeUtils}

/**
 * The data type for Maps. Keys in a map are not allowed to have `null` values.
 *
 * Please use `DataTypes.createMapType()` to create a specific instance.
 *
 * @param keyType The data type of map keys.
 * @param valueType The data type of map values.
 * @param valueContainsNull Indicates if map values have `null` values.
 */
@InterfaceStability.Stable
case class MapType(
  keyType: DataType,
  valueType: DataType,
  valueContainsNull: Boolean) extends DataType {

  /** No-arg constructor for kryo. */
  def this() = this(null, null, false)

  private[sql] def buildFormattedString(prefix: String, builder: StringBuilder): Unit = {
    builder.append(s"$prefix-- key: ${keyType.typeName}\n")
    DataType.buildFormattedString(keyType, s"$prefix    |", builder)
    builder.append(s"$prefix-- value: ${valueType.typeName} " +
      s"(valueContainsNull = $valueContainsNull)\n")
    DataType.buildFormattedString(valueType, s"$prefix    |", builder)
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

  private[this] class OrderedWrapper {
    var isOrdered: Boolean = false
  }

  private[this] lazy val orderedWrapper: OrderedWrapper = new OrderedWrapper()

  private[sql] def setOrdered(b: Boolean): Unit = {
    orderedWrapper.isOrdered = b
  }

  // Indicates if a map is itself "ordered". It makes sense to compare two
  // maps only when they are themselves "ordered", i.e. entries of the map are sorted.
  // This parameter is used by internal when doing ordering operation, e.g. sort
  // values of `MapType`.
  private[sql] def isOrdered(): Boolean = orderedWrapper.isOrdered

  // This is used to sort the entries of a map.
  @transient
  private[sql] lazy val interpretedKeyOrdering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(keyType)

  @transient
  private[this] lazy val interpretedValueOrdering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(valueType)

  @transient
  private[sql] lazy val interpretedOrdering: Ordering[MapData] = new Ordering[MapData] {
    val keyOrdering = interpretedKeyOrdering
    val valueOrdering = interpretedValueOrdering

    // The approach to compare (left: MapData, right: MapData):
    // 1. The precondition is that entries inside `left` and `right` are already sorted themselves;
    // 2. Compare entries from `left` and `right`, say entryA(keyA, valueA) is from `left` and
    //    entryB(keyB, valueB) is from `right`:
    //    a. entryA is bigger than entryB if keyA is bigger than keyB and vice versa;
    //    b. entryA is bigger than entryB if keyA equals to keyB and valueA is bigger than
    //       valueB and vice versa;
    // 3. If entries from the head equals to each other between `left` and `right`, the `MapData`
    //    with more entries is bigger.
    def compare(left: MapData, right: MapData): Int = {
      val leftKeys = left.keyArray()
      val leftValues = left.valueArray()
      val rightKeys = right.keyArray()
      val rightValues = right.valueArray()
      val minLength = scala.math.min(leftKeys.numElements(), rightKeys.numElements())
      var i = 0
      while (i < minLength) {
        val keyComp = keyOrdering.compare(leftKeys.get(i, keyType), rightKeys.get(i, keyType))
        if (keyComp != 0) {
          return keyComp
        }

        val isNullLeft = leftValues.isNullAt(i)
        val isNullRight = rightValues.isNullAt(i)
        if (isNullLeft && isNullRight) {
          // Do nothing.
        } else if (isNullLeft) {
          return -1
        } else if (isNullRight) {
          return 1
        } else {
          val comp = valueOrdering.compare(
            leftValues.get(i, valueType), rightValues.get(i, valueType))
          if (comp != 0) {
            return comp
          }
        }
        i += 1
      }
      val diff = left.numElements() - right.numElements()
      if (diff < 0) {
        -1
      } else if (diff > 0) {
        1
      } else {
        0
      }
    }
  }

  override def toString: String = {
    s"MapType(${keyType.toString},${valueType.toString},${valueContainsNull.toString})"
  }
}

/**
 * @since 1.3.0
 */
@InterfaceStability.Stable
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
