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
package org.apache.spark.sql.execution.columnar

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.types._

object ColumnarDataTypeUtils {
  def toLogicalDataType(dataType: PhysicalDataType): DataType = dataType match {
    case PhysicalNullType => NullType
    case PhysicalBooleanType => BooleanType
    case PhysicalIntegerType => IntegerType
    case PhysicalLongType => LongType
    case PhysicalByteType => ByteType
    case PhysicalShortType => ShortType
    case PhysicalBinaryType => BinaryType
    case PhysicalCalendarIntervalType => CalendarIntervalType
    case PhysicalFloatType => FloatType
    case PhysicalDoubleType => DoubleType
    case PhysicalStringType(collationId) => StringType(collationId)
    case PhysicalDecimalType(precision, scale) => DecimalType(precision, scale)
    case PhysicalArrayType(elementType, containsNull) => ArrayType(elementType, containsNull)
    case PhysicalStructType(fields) => StructType(fields)
    case PhysicalMapType(keyType, valueType, valueContainsNull) =>
      MapType(keyType, valueType, valueContainsNull)
    case unsupportedType => throw new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_3162",
      messageParameters = Map("type" -> unsupportedType.toString))
  }
}
