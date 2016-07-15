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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.parquet.io.api.Converter
import org.apache.parquet.schema.Type

import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.types._

private[parquet] object ParquetSchemaCompatibility {

  private val schemaConverter = new ParquetSchemaConverter(writeLegacyParquetFormat = false)

  // The logic for setting and adding a value in `ParquetPrimitiveConverter` are separated
  // into `NumericValueUpdater` and `NumericCompatibleConverter` so that value can be converted
  // to a desired type.
  // `NumericValueUpdater` updates the input `Number` via `ParentContainerUpdater`. This
  // is for updating a value converted for the appropriate value type for `ParentContainerUpdater`
  private type NumericValueUpdater = Number => Unit

  // This is a wrapper for `NumericValueUpdater`. this returns a converter that adds the value
  // from `NumericValueUpdater`.
  private type NumericCompatibleConverter = NumericValueUpdater => ParquetPrimitiveConverter

  private def createCompatiblePrimitiveConverter(
      parquetType: Type,
      catalystType: DataType,
      updater: ParentContainerUpdater): NumericCompatibleConverter = {

    val catalystTypeFromParquet = schemaConverter.convertField(parquetType)

    catalystTypeFromParquet match {
      case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType =>
        (valueUpdater: NumericValueUpdater) =>
          new ParquetPrimitiveConverter(updater) {
            override def addInt(value: Int): Unit = valueUpdater(value)
            override def addLong(value: Long): Unit = valueUpdater(value)
            override def addFloat(value: Float): Unit = valueUpdater(value)
            override def addDouble(value: Double): Unit = valueUpdater(value)
          }

      case _ =>
        throw new RuntimeException(
          s"Unable to create Parquet converter for data type $catalystType " +
            s"whose Parquet type is $parquetType. They are not compatible.")
    }
  }

  def isCompatible(catalystType: DataType, parquetType: Type): Boolean = {
    // Find a compatible type between both numeric types.
    val catalystTypeFromParquet = schemaConverter.convertField(parquetType)
    val compatibleCatalystType =
      TypeCoercion.findTightestCommonTypeOfTwo(catalystType, catalystTypeFromParquet).orNull
    catalystType == compatibleCatalystType
  }

  def newCompatibleConverter(
      parquetType: Type,
      catalystType: DataType,
      updater: ParentContainerUpdater): Converter with HasParentContainerUpdater = {

    val newCompatiblePrimitiveConverter =
      createCompatiblePrimitiveConverter(parquetType, catalystType, updater)

    catalystType match {
      case ByteType =>
        val valueUpdater: NumericValueUpdater = (v: Number) => updater.setByte(v.byteValue())
        newCompatiblePrimitiveConverter(valueUpdater)

      case ShortType =>
        val valueUpdater: NumericValueUpdater = (v: Number) => updater.setShort(v.shortValue())
        newCompatiblePrimitiveConverter(valueUpdater)

      case IntegerType =>
        val valueUpdater: NumericValueUpdater = (v: Number) => updater.setInt(v.intValue())
        newCompatiblePrimitiveConverter(valueUpdater)

      case LongType =>
        val valueUpdater: NumericValueUpdater = (v: Number) => updater.setLong(v.longValue())
        newCompatiblePrimitiveConverter(valueUpdater)

      case FloatType =>
        val valueUpdater: NumericValueUpdater = (v: Number) => updater.setFloat(v.floatValue())
        newCompatiblePrimitiveConverter(valueUpdater)

      case DoubleType =>
        val valueUpdater: NumericValueUpdater = (v: Number) => updater.setDouble(v.doubleValue())
        newCompatiblePrimitiveConverter(valueUpdater)

      case _ =>
        throw new RuntimeException(
          s"Unable to create Parquet converter for data type $catalystType " +
            s"whose Parquet type is $parquetType. They are not compatible.")
    }
  }
}
