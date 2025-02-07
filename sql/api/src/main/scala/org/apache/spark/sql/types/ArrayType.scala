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

import org.json4s.JsonDSL._

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.catalyst.util.StringConcat

/**
 * Companion object for ArrayType.
 *
 * @since 1.3.0
 */
@Stable
object ArrayType extends AbstractDataType {

  /**
   * Construct a [[ArrayType]] object with the given element type. The `containsNull` is true.
   */
  def apply(elementType: DataType): ArrayType = ArrayType(elementType, containsNull = true)

  override private[sql] def defaultConcreteType: DataType =
    ArrayType(NullType, containsNull = true)

  override private[sql] def acceptsType(other: DataType): Boolean = {
    other.isInstanceOf[ArrayType]
  }

  override private[spark] def simpleString: String = "array"
}

/**
 * The data type for collections of multiple values. Internally these are represented as columns
 * that contain a ``scala.collection.Seq``.
 *
 * Please use `DataTypes.createArrayType()` to create a specific instance.
 *
 * An [[ArrayType]] object comprises two fields, `elementType: [[DataType]]` and `containsNull:
 * Boolean`. The field of `elementType` is used to specify the type of array elements. The field
 * of `containsNull` is used to specify if the array can have `null` values.
 *
 * @param elementType
 *   The data type of values.
 * @param containsNull
 *   Indicates if the array can have `null` values
 *
 * @since 1.3.0
 */
@Stable
case class ArrayType(elementType: DataType, containsNull: Boolean) extends DataType {

  /** No-arg constructor for kryo. */
  protected def this() = this(null, false)

  private[sql] def buildFormattedString(
      prefix: String,
      stringConcat: StringConcat,
      maxDepth: Int): Unit = {
    if (maxDepth > 0) {
      stringConcat.append(
        s"$prefix-- element: ${elementType.typeName} (containsNull = $containsNull)\n")
      DataType.buildFormattedString(elementType, s"$prefix    |", stringConcat, maxDepth)
    }
  }

  override private[sql] def jsonValue =
    ("type" -> typeName) ~
      ("elementType" -> elementType.jsonValue) ~
      ("containsNull" -> containsNull)

  /**
   * The default size of a value of the ArrayType is the default size of the element type. We
   * assume that there is only 1 element on average in an array. See SPARK-18853.
   */
  override def defaultSize: Int = 1 * elementType.defaultSize

  override def simpleString: String = s"array<${elementType.simpleString}>"

  override def catalogString: String = s"array<${elementType.catalogString}>"

  override def sql: String = s"ARRAY<${elementType.sql}>"

  override private[spark] def asNullable: ArrayType =
    ArrayType(elementType.asNullable, containsNull = true)

  /**
   * Returns the same data type but set all nullability fields are true (`StructField.nullable`,
   * `ArrayType.containsNull`, and `MapType.valueContainsNull`).
   *
   * @since 4.0.0
   */
  def toNullable: ArrayType = asNullable

  override private[spark] def existsRecursively(f: (DataType) => Boolean): Boolean = {
    f(this) || elementType.existsRecursively(f)
  }
}
