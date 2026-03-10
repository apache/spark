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

package org.apache.spark.sql.catalyst.util

import org.apache.datasketches.common._
import org.apache.datasketches.frequencies.ItemsSketch
import org.apache.datasketches.memory.Memory

import org.apache.spark.sql.catalyst.expressions.ArrayOfDecimalsSerDe
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._

object ItemsSketchUtils {

  // Default maxMapSize (power of 2). The effective capacity is 0.75 * maxMapSize.
  final val DEFAULT_MAX_MAP_SIZE = 1 << 12 // 4,096
  // Minimum maxMapSize per DataSketches library requirement (must be power of 2, >= 8).
  final val MIN_MAX_MAP_SIZE = 8
  // Maximum maxMapSize to prevent excessive memory usage.
  final val MAX_MAX_MAP_SIZE = 1 << 26 // 67,108,864

  /**
   * Validates the maxMapSize parameter for ItemsSketch. Must be a power of 2 within bounds.
   *
   * @param maxMapSize the maximum map size for the sketch
   * @param prettyName the display name of the function for error messages
   */
  def checkMaxMapSize(maxMapSize: Int, prettyName: String): Unit = {
    if (maxMapSize < MIN_MAX_MAP_SIZE || maxMapSize > MAX_MAX_MAP_SIZE) {
      throw QueryExecutionErrors.itemsSketchInvalidMaxMapSize(
        function = prettyName,
        min = MIN_MAX_MAP_SIZE,
        max = MAX_MAX_MAP_SIZE,
        value = maxMapSize)
    }
    if ((maxMapSize & (maxMapSize - 1)) != 0) {
      throw QueryExecutionErrors.itemsSketchMaxMapSizeNotPowerOfTwo(
        function = prettyName,
        value = maxMapSize)
    }
  }

  /**
   * Creates a new ItemsSketch instance for the given data type.
   *
   * @param dataType the Spark SQL data type of items
   * @param maxMapSize the maximum map size for the sketch
   * @return a new ItemsSketch instance
   */
  def createItemsSketch(dataType: DataType, maxMapSize: Int): ItemsSketch[Any] = {
    dataType match {
      case _: BooleanType =>
        new ItemsSketch[Boolean](maxMapSize).asInstanceOf[ItemsSketch[Any]]
      case _: ByteType | _: ShortType | _: IntegerType | _: FloatType | _: DateType =>
        new ItemsSketch[Number](maxMapSize).asInstanceOf[ItemsSketch[Any]]
      case _: LongType | _: TimestampType | _: TimestampNTZType =>
        new ItemsSketch[Long](maxMapSize).asInstanceOf[ItemsSketch[Any]]
      case _: DoubleType =>
        new ItemsSketch[Double](maxMapSize).asInstanceOf[ItemsSketch[Any]]
      case _: StringType =>
        new ItemsSketch[String](maxMapSize).asInstanceOf[ItemsSketch[Any]]
      case _: DecimalType =>
        new ItemsSketch[Decimal](maxMapSize).asInstanceOf[ItemsSketch[Any]]
      case _ =>
        throw QueryExecutionErrors.itemsSketchUnsupportedDataType(dataType.typeName)
    }
  }

  /**
   * Returns the appropriate ArrayOfItemsSerDe for the given data type.
   * Follows the same pattern as ApproxTopK.genSketchSerDe.
   *
   * @param dataType the Spark SQL data type of items
   * @return the SerDe instance for serialization/deserialization
   */
  def genSketchSerDe(dataType: DataType): ArrayOfItemsSerDe[Any] = {
    dataType match {
      case _: BooleanType => new ArrayOfBooleansSerDe().asInstanceOf[ArrayOfItemsSerDe[Any]]
      case _: ByteType | _: ShortType | _: IntegerType | _: FloatType | _: DateType =>
        new ArrayOfNumbersSerDe().asInstanceOf[ArrayOfItemsSerDe[Any]]
      case _: LongType | _: TimestampType | _: TimestampNTZType =>
        new ArrayOfLongsSerDe().asInstanceOf[ArrayOfItemsSerDe[Any]]
      case _: DoubleType =>
        new ArrayOfDoublesSerDe().asInstanceOf[ArrayOfItemsSerDe[Any]]
      case _: StringType =>
        new ArrayOfStringsSerDe().asInstanceOf[ArrayOfItemsSerDe[Any]]
      case dt: DecimalType =>
        new ArrayOfDecimalsSerDe(dt).asInstanceOf[ArrayOfItemsSerDe[Any]]
      case _ =>
        throw QueryExecutionErrors.itemsSketchUnsupportedDataType(dataType.typeName)
    }
  }


  /**
   * Checks whether the given data type is supported by ItemsSketch functions.
   */
  def isDataTypeSupported(dataType: DataType): Boolean = {
    dataType match {
      case _: BooleanType | _: ByteType | _: ShortType | _: IntegerType |
           _: LongType | _: FloatType | _: DoubleType | _: DateType |
           _: TimestampType | _: TimestampNTZType | _: StringType | _: DecimalType => true
      case _ => false
    }
  }

  /**
   * Deserializes an ItemsSketch from a byte array.
   *
   * @param bytes the serialized sketch bytes
   * @param dataType the Spark SQL data type of items
   * @param prettyName the display name of the function for error messages
   * @return the deserialized ItemsSketch
   */
  def deserializeSketch(
      bytes: Array[Byte],
      dataType: DataType,
      prettyName: String): ItemsSketch[Any] = {
    try {
      val serDe = genSketchSerDe(dataType)
      ItemsSketch.getInstance(Memory.wrap(bytes), serDe)
        .asInstanceOf[ItemsSketch[Any]]
    } catch {
      case _: Exception =>
        throw QueryExecutionErrors.itemsSketchInvalidInputBuffer(prettyName)
    }
  }
}
