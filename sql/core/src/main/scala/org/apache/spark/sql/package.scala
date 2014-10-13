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

package org.apache.spark

import org.apache.spark.annotation.DeveloperApi

/**
 * Allows the execution of relational queries, including those expressed in SQL using Spark.
 *
 *  @groupname dataType Data types
 *  @groupdesc Spark SQL data types.
 *  @groupprio dataType -3
 *  @groupname field Field
 *  @groupprio field -2
 *  @groupname row Row
 *  @groupprio row -1
 */
package object sql {

  /**
   * :: DeveloperApi ::
   *
   * Represents one row of output from a relational operator.
   * @group row
   */
  @DeveloperApi
  type Row = catalyst.expressions.Row

  /**
   * :: DeveloperApi ::
   *
   * A [[Row]] object can be constructed by providing field values. Example:
   * {{{
   * import org.apache.spark.sql._
   *
   * // Create a Row from values.
   * Row(value1, value2, value3, ...)
   * // Create a Row from a Seq of values.
   * Row.fromSeq(Seq(value1, value2, ...))
   * }}}
   *
   * A value of a row can be accessed through both generic access by ordinal,
   * which will incur boxing overhead for primitives, as well as native primitive access.
   * An example of generic access by ordinal:
   * {{{
   * import org.apache.spark.sql._
   *
   * val row = Row(1, true, "a string", null)
   * // row: Row = [1,true,a string,null]
   * val firstValue = row(0)
   * // firstValue: Any = 1
   * val fourthValue = row(3)
   * // fourthValue: Any = null
   * }}}
   *
   * For native primitive access, it is invalid to use the native primitive interface to retrieve
   * a value that is null, instead a user must check `isNullAt` before attempting to retrieve a
   * value that might be null.
   * An example of native primitive access:
   * {{{
   * // using the row from the previous example.
   * val firstValue = row.getInt(0)
   * // firstValue: Int = 1
   * val isNull = row.isNullAt(3)
   * // isNull: Boolean = true
   * }}}
   *
   * Interfaces related to native primitive access are:
   *
   * `isNullAt(i: Int): Boolean`
   *
   * `getInt(i: Int): Int`
   *
   * `getLong(i: Int): Long`
   *
   * `getDouble(i: Int): Double`
   *
   * `getFloat(i: Int): Float`
   *
   * `getBoolean(i: Int): Boolean`
   *
   * `getShort(i: Int): Short`
   *
   * `getByte(i: Int): Byte`
   *
   * `getString(i: Int): String`
   *
   * Fields in a [[Row]] object can be extracted in a pattern match. Example:
   * {{{
   * import org.apache.spark.sql._
   *
   * val pairs = sql("SELECT key, value FROM src").rdd.map {
   *   case Row(key: Int, value: String) =>
   *     key -> value
   * }
   * }}}
   *
   * @group row
   */
  @DeveloperApi
  val Row = catalyst.expressions.Row

  /**
   * :: DeveloperApi ::
   *
   * The base type of all Spark SQL data types.
   *
   * @group dataType
   */
  @DeveloperApi
  type DataType = catalyst.types.DataType

  /**
   * :: DeveloperApi ::
   *
   * The data type representing `String` values
   *
   * @group dataType
   */
  @DeveloperApi
  val StringType = catalyst.types.StringType

  /**
   * :: DeveloperApi ::
   *
   * The data type representing `Array[Byte]` values.
   *
   * @group dataType
   */
  @DeveloperApi
  val BinaryType = catalyst.types.BinaryType

  /**
   * :: DeveloperApi ::
   *
   * The data type representing `Boolean` values.
   *
   *@group dataType
   */
  @DeveloperApi
  val BooleanType = catalyst.types.BooleanType

  /**
   * :: DeveloperApi ::
   *
   * The data type representing `java.sql.Timestamp` values.
   *
   * @group dataType
   */
  @DeveloperApi
  val TimestampType = catalyst.types.TimestampType

  /**
   * :: DeveloperApi ::
   *
   * The data type representing `java.sql.Date` values.
   *
   * @group dataType
   */
  @DeveloperApi
  val DateType = catalyst.types.DateType

  /**
   * :: DeveloperApi ::
   *
   * The data type representing `scala.math.BigDecimal` values.
   *
   * @group dataType
   */
  @DeveloperApi
  val DecimalType = catalyst.types.DecimalType

  /**
   * :: DeveloperApi ::
   *
   * The data type representing `Double` values.
   *
   * @group dataType
   */
  @DeveloperApi
  val DoubleType = catalyst.types.DoubleType

  /**
   * :: DeveloperApi ::
   *
   * The data type representing `Float` values.
   *
   * @group dataType
   */
  @DeveloperApi
  val FloatType = catalyst.types.FloatType

  /**
   * :: DeveloperApi ::
   *
   * The data type representing `Byte` values.
   *
   * @group dataType
   */
  @DeveloperApi
  val ByteType = catalyst.types.ByteType

  /**
   * :: DeveloperApi ::
   *
   * The data type representing `Int` values.
   *
   * @group dataType
   */
  @DeveloperApi
  val IntegerType = catalyst.types.IntegerType

  /**
   * :: DeveloperApi ::
   *
   * The data type representing `Long` values.
   *
   * @group dataType
   */
  @DeveloperApi
  val LongType = catalyst.types.LongType

  /**
   * :: DeveloperApi ::
   *
   * The data type representing `Short` values.
   *
   * @group dataType
   */
  @DeveloperApi
  val ShortType = catalyst.types.ShortType

  /**
   * :: DeveloperApi ::
   *
   * The data type for collections of multiple values.
   * Internally these are represented as columns that contain a ``scala.collection.Seq``.
   *
   * An [[ArrayType]] object comprises two fields, `elementType: [[DataType]]` and
   * `containsNull: Boolean`. The field of `elementType` is used to specify the type of
   * array elements. The field of `containsNull` is used to specify if the array has `null` values.
   *
   * @group dataType
   */
  @DeveloperApi
  type ArrayType = catalyst.types.ArrayType

  /**
   * :: DeveloperApi ::
   *
   * An [[ArrayType]] object can be constructed with two ways,
   * {{{
   * ArrayType(elementType: DataType, containsNull: Boolean)
   * }}} and
   * {{{
   * ArrayType(elementType: DataType)
   * }}}
   * For `ArrayType(elementType)`, the field of `containsNull` is set to `false`.
   *
   * @group dataType
   */
  @DeveloperApi
  val ArrayType = catalyst.types.ArrayType

  /**
   * :: DeveloperApi ::
   *
   * The data type representing `Map`s. A [[MapType]] object comprises three fields,
   * `keyType: [[DataType]]`, `valueType: [[DataType]]` and `valueContainsNull: Boolean`.
   * The field of `keyType` is used to specify the type of keys in the map.
   * The field of `valueType` is used to specify the type of values in the map.
   * The field of `valueContainsNull` is used to specify if values of this map has `null` values.
   * For values of a MapType column, keys are not allowed to have `null` values.
   *
   * @group dataType
   */
  @DeveloperApi
  type MapType = catalyst.types.MapType

  /**
   * :: DeveloperApi ::
   *
   * A [[MapType]] object can be constructed with two ways,
   * {{{
   * MapType(keyType: DataType, valueType: DataType, valueContainsNull: Boolean)
   * }}} and
   * {{{
   * MapType(keyType: DataType, valueType: DataType)
   * }}}
   * For `MapType(keyType: DataType, valueType: DataType)`,
   * the field of `valueContainsNull` is set to `true`.
   *
   * @group dataType
   */
  @DeveloperApi
  val MapType = catalyst.types.MapType

  /**
   * :: DeveloperApi ::
   *
   * The data type representing [[Row]]s.
   * A [[StructType]] object comprises a [[Seq]] of [[StructField]]s.
   *
   * @group dataType
   */
  @DeveloperApi
  type StructType = catalyst.types.StructType

  /**
   * :: DeveloperApi ::
   *
   * A [[StructType]] object can be constructed by
   * {{{
   * StructType(fields: Seq[StructField])
   * }}}
   * For a [[StructType]] object, one or multiple [[StructField]]s can be extracted by names.
   * If multiple [[StructField]]s are extracted, a [[StructType]] object will be returned.
   * If a provided name does not have a matching field, it will be ignored. For the case
   * of extracting a single StructField, a `null` will be returned.
   * Example:
   * {{{
   * import org.apache.spark.sql._
   *
   * val struct =
   *   StructType(
   *     StructField("a", IntegerType, true) ::
   *     StructField("b", LongType, false) ::
   *     StructField("c", BooleanType, false) :: Nil)
   *
   * // Extract a single StructField.
   * val singleField = struct("b")
   * // singleField: StructField = StructField(b,LongType,false)
   *
   * // This struct does not have a field called "d". null will be returned.
   * val nonExisting = struct("d")
   * // nonExisting: StructField = null
   *
   * // Extract multiple StructFields. Field names are provided in a set.
   * // A StructType object will be returned.
   * val twoFields = struct(Set("b", "c"))
   * // twoFields: StructType =
   * //   StructType(List(StructField(b,LongType,false), StructField(c,BooleanType,false)))
   *
   * // Those names do not have matching fields will be ignored.
   * // For the case shown below, "d" will be ignored and
   * // it is treated as struct(Set("b", "c")).
   * val ignoreNonExisting = struct(Set("b", "c", "d"))
   * // ignoreNonExisting: StructType =
   * //   StructType(List(StructField(b,LongType,false), StructField(c,BooleanType,false)))
   * }}}
   *
   * A [[Row]] object is used as a value of the StructType.
   * Example:
   * {{{
   * import org.apache.spark.sql._
   *
   * val innerStruct =
   *   StructType(
   *     StructField("f1", IntegerType, true) ::
   *     StructField("f2", LongType, false) ::
   *     StructField("f3", BooleanType, false) :: Nil)
   *
   * val struct = StructType(
   *   StructField("a", innerStruct, true) :: Nil)
   *
   * // Create a Row with the schema defined by struct
   * val row = Row(Row(1, 2, true))
   * // row: Row = [[1,2,true]]
   * }}}
   *
   * @group dataType
   */
  @DeveloperApi
  val StructType = catalyst.types.StructType

  /**
   * :: DeveloperApi ::
   *
   * A [[StructField]] object represents a field in a [[StructType]] object.
   * A [[StructField]] object comprises three fields, `name: [[String]]`, `dataType: [[DataType]]`,
   * and `nullable: Boolean`. The field of `name` is the name of a `StructField`. The field of
   * `dataType` specifies the data type of a `StructField`.
   * The field of `nullable` specifies if values of a `StructField` can contain `null` values.
   *
   * @group field
   */
  @DeveloperApi
  type StructField = catalyst.types.StructField

  /**
   * :: DeveloperApi ::
   *
   * A [[StructField]] object can be constructed by
   * {{{
   * StructField(name: String, dataType: DataType, nullable: Boolean)
   * }}}
   *
   * @group dataType
   */
  @DeveloperApi
  val StructField = catalyst.types.StructField
}
