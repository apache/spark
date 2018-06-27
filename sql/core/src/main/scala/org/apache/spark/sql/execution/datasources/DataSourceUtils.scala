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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types._


object DataSourceUtils {

  /**
   * Verify if the schema is supported in datasource in write path.
   */
  def verifyWriteSchema(format: FileFormat, schema: StructType): Unit = {
    verifySchema(format, schema, isReadPath = false)
  }

  /**
   * Verify if the schema is supported in datasource in read path.
   */
  def verifyReadSchema(format: FileFormat, schema: StructType): Unit = {
    verifySchema(format, schema, isReadPath = true)
  }

  /**
   * Verify if the schema is supported in datasource. This verification should be done
   * in a driver side, e.g., `prepareWrite`, `buildReader`, and `buildReaderWithPartitionValues`
   * in `FileFormat`.
   *
   * Unsupported data types of csv, json, orc, and parquet are as follows;
   *  csv -> R/W: Interval, Null, Array, Map, Struct
   *  json -> W: Interval
   *  orc -> W: Interval, Null
   *  parquet -> R/W: Interval, Null
   */
  private def verifySchema(format: FileFormat, schema: StructType, isReadPath: Boolean): Unit = {
    def throwUnsupportedException(dataType: DataType): Unit = {
      throw new UnsupportedOperationException(
        s"$format data source does not support ${dataType.simpleString} data type.")
    }

    def verifyType(dataType: DataType): Unit = dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
           StringType | BinaryType | DateType | TimestampType | _: DecimalType =>

      // All the unsupported types for CSV
      case _: NullType | _: CalendarIntervalType | _: StructType | _: ArrayType | _: MapType
          if format.isInstanceOf[CSVFileFormat] =>
        throwUnsupportedException(dataType)

      case st: StructType => st.foreach { f => verifyType(f.dataType) }

      case ArrayType(elementType, _) => verifyType(elementType)

      case MapType(keyType, valueType, _) =>
        verifyType(keyType)
        verifyType(valueType)

      case udt: UserDefinedType[_] => verifyType(udt.sqlType)

      // Interval type not supported in all the write path
      case _: CalendarIntervalType if !isReadPath =>
        throwUnsupportedException(dataType)

      // JSON and ORC don't support an Interval type, but we pass it in read pass
      // for back-compatibility.
      case _: CalendarIntervalType if format.isInstanceOf[JsonFileFormat] ||
        format.isInstanceOf[OrcFileFormat] =>

      // Interval type not supported in the other read path
      case _: CalendarIntervalType =>
        throwUnsupportedException(dataType)

      // For JSON & ORC backward-compatibility
      case _: NullType if format.isInstanceOf[JsonFileFormat] ||
        (isReadPath && format.isInstanceOf[OrcFileFormat]) =>

      // Null type not supported in the other path
      case _: NullType =>
        throwUnsupportedException(dataType)

      // We keep this default case for safeguards
      case _ => throwUnsupportedException(dataType)
    }

    schema.foreach(field => verifyType(field.dataType))
  }
}
