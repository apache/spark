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

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * The data type for Maps. Keys in a map are not allowed to have `null` values.
 *
 * Please use [[DataTypes.createMapType()]] to create a specific instance.
 *
 * @param keyType The data type of map keys.
 * @param valueType The data type of map values.
 * @param valueContainsNull Indicates if map values have `null` values.
 */
@DeveloperApi
case class MapType(
  keyType: DataType,
  valueType: DataType,
  valueContainsNull: Boolean) extends DataType {

  /** No-arg constructor for kryo. */
  def this() = this(null, null, false)

  private[sql] def buildFormattedString(prefix: String, builder: StringBuilder): Unit = {
    builder.append(s"$prefix-- key: ${keyType.typeName}\n")
    builder.append(s"$prefix-- value: ${valueType.typeName} " +
      s"(valueContainsNull = $valueContainsNull)\n")
    DataType.buildFormattedString(keyType, s"$prefix    |", builder)
    DataType.buildFormattedString(valueType, s"$prefix    |", builder)
  }

  override private[sql] def jsonValue: JValue =
    ("type" -> typeName) ~
      ("keyType" -> keyType.jsonValue) ~
      ("valueType" -> valueType.jsonValue) ~
      ("valueContainsNull" -> valueContainsNull)

  /**
   * The default size of a value of the MapType is
   * 100 * (the default size of the key type + the default size of the value type).
   * (We assume that there are 100 elements).
   */
  override def defaultSize: Int = 100 * (keyType.defaultSize + valueType.defaultSize)

  override def simpleString: String = s"map<${keyType.simpleString},${valueType.simpleString}>"

  override def catalogString: String = s"map<${keyType.catalogString},${valueType.catalogString}>"

  override def sql: String = s"MAP<${keyType.sql}, ${valueType.sql}>"

  override private[spark] def asNullable: MapType =
    MapType(keyType.asNullable, valueType.asNullable, valueContainsNull = true)

  override private[spark] def existsRecursively(f: (DataType) => Boolean): Boolean = {
    f(this) || keyType.existsRecursively(f) || valueType.existsRecursively(f)
  }
}


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
