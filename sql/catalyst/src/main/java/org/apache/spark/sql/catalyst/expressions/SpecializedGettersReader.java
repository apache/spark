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

package org.apache.spark.sql.catalyst.expressions;

import java.util.Map;

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.sql.catalyst.types.*;
import org.apache.spark.sql.types.*;

public final class SpecializedGettersReader {
  private SpecializedGettersReader() {}

  public static Object read(
      SpecializedGetters obj,
      int ordinal,
      DataType dataType,
      boolean handleNull,
      boolean handleUserDefinedType) {
    PhysicalDataType physicalDataType = PhysicalDataType.apply(dataType);
    if (handleNull && (obj.isNullAt(ordinal) || physicalDataType instanceof PhysicalNullType)) {
      return null;
    }
    if (physicalDataType instanceof PhysicalBooleanType) {
      return obj.getBoolean(ordinal);
    }
    if (physicalDataType instanceof PhysicalByteType) {
      return obj.getByte(ordinal);
    }
    if (physicalDataType instanceof PhysicalShortType) {
      return obj.getShort(ordinal);
    }
    if (physicalDataType instanceof PhysicalIntegerType) {
      return obj.getInt(ordinal);
    }
    if (physicalDataType instanceof PhysicalLongType) {
      return obj.getLong(ordinal);
    }
    if (physicalDataType instanceof PhysicalFloatType) {
      return obj.getFloat(ordinal);
    }
    if (physicalDataType instanceof PhysicalDoubleType) {
      return obj.getDouble(ordinal);
    }
    if (physicalDataType instanceof PhysicalStringType) {
      return obj.getUTF8String(ordinal);
    }
    if (physicalDataType instanceof PhysicalDecimalType dt) {
      return obj.getDecimal(ordinal, dt.precision(), dt.scale());
    }
    if (physicalDataType instanceof PhysicalCalendarIntervalType) {
      return obj.getInterval(ordinal);
    }
    if (physicalDataType instanceof PhysicalBinaryType) {
      return obj.getBinary(ordinal);
    }
    if (physicalDataType instanceof PhysicalVariantType) {
      return obj.getVariant(ordinal);
    }
    if (physicalDataType instanceof PhysicalStructType dt) {
      return obj.getStruct(ordinal, dt.fields().length);
    }
    if (physicalDataType instanceof PhysicalArrayType) {
      return obj.getArray(ordinal);
    }
    if (physicalDataType instanceof PhysicalMapType) {
      return obj.getMap(ordinal);
    }
    if (handleUserDefinedType && dataType instanceof UserDefinedType dt) {
      return obj.get(ordinal, dt.sqlType());
    }

    throw new SparkUnsupportedOperationException(
      "_LEGACY_ERROR_TEMP_3131", Map.of("dataType", dataType.simpleString()));
  }
}
