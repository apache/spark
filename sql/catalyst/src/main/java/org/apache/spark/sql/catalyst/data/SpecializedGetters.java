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

package org.apache.spark.sql.catalyst.data;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.CalendarIntervalType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.UserDefinedType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

public interface SpecializedGetters {

  /**
   * Returns whether the value at the given ordinal is NULL.
   *
   * @param ordinal index to return
   * @return true if the value is NULL
   */
  boolean isNullAt(int ordinal);

  /**
   * Return the boolean at the given ordinal, avoiding boxing if possible.
   *
   * @param ordinal index to return
   * @return the boolean value
   */
  boolean getBoolean(int ordinal);

  /**
   * Return the byte at the given ordinal, avoiding boxing if possible.
   *
   * @param ordinal index to return
   * @return the byte value
   */
  byte getByte(int ordinal);

  /**
   * Return the short at the given ordinal, avoiding boxing if possible.
   *
   * @param ordinal index to return
   * @return the short value
   */
  short getShort(int ordinal);

  /**
   * Return the int at the given ordinal, avoiding boxing if possible.
   *
   * @param ordinal index to return
   * @return the int value
   */
  int getInt(int ordinal);

  /**
   * Return the long at the given ordinal, avoiding boxing if possible.
   *
   * @param ordinal index to return
   * @return the long value
   */
  long getLong(int ordinal);

  /**
   * Return the float at the given ordinal, avoiding boxing if possible.
   *
   * @param ordinal index to return
   * @return the float value
   */
  float getFloat(int ordinal);

  /**
   * Return the double at the given ordinal, avoiding boxing if possible.
   *
   * @param ordinal index to return
   * @return the double value
   */
  double getDouble(int ordinal);

  /**
   * Return the Decimal at the given ordinal.
   *
   * @param ordinal index to return
   * @return Decimal value
   */
  Decimal getDecimal(int ordinal, int precision, int scale);

  /**
   * Return the string at the given ordinal.
   *
   * @param ordinal index to return
   * @return value as a UTF8String
   */
  UTF8String getUTF8String(int ordinal);

  /**
   * Return the {@link UTF8String internal string} at the given ordinal as a Java {@link String}.
   * <p>
   * Calling this method copies the string and should be avoided. Use {@link #getUTF8String(int)}
   * instead.
   *
   * @param ordinal index to return
   * @return value as a String
   */
  default String getString(int ordinal) {
    return getUTF8String(ordinal).toString();
  }

  /**
   * Return the byte array at the given ordinal.
   *
   * @param ordinal index to return
   * @return the byte[] value
   */
  byte[] getBinary(int ordinal);

  /**
   * Return the CalendarData at the given ordinal.
   *
   * @param ordinal index to return
   * @return the value as a CalendarInterval
   */
  CalendarInterval getInterval(int ordinal);

  /**
   * Return the InternalRow at the given ordinal.
   *
   * @param ordinal index to return
   * @return the value as an InternalRow
   */
  InternalRow getStruct(int ordinal, int numFields);

  /**
   * Return the ArrayData at the given ordinal.
   *
   * @param ordinal index to return
   * @return the value as ArrayData
   */
  ArrayData getArray(int ordinal);

  /**
   * Return the MapData at the given ordinal.
   *
   * @param ordinal index to return
   * @return the value as MapData
   */
  MapData getMap(int ordinal);

  /**
   * Return the value at the given ordinal.
   *
   * @param ordinal index to return
   * @return the value, boxed if the value is a primitive
   */
  default Object get(int ordinal, DataType dataType) {
    if (isNullAt(ordinal) || dataType instanceof NullType) {
      return null;
    } else if (dataType instanceof BooleanType) {
      return getBoolean(ordinal);
    } else if (dataType instanceof ByteType) {
      return getByte(ordinal);
    } else if (dataType instanceof ShortType) {
      return getShort(ordinal);
    } else if (dataType instanceof IntegerType) {
      return getInt(ordinal);
    } else if (dataType instanceof LongType) {
      return getLong(ordinal);
    } else if (dataType instanceof FloatType) {
      return getFloat(ordinal);
    } else if (dataType instanceof DoubleType) {
      return getDouble(ordinal);
    } else if (dataType instanceof DecimalType) {
      DecimalType dt = (DecimalType) dataType;
      return getDecimal(ordinal, dt.precision(), dt.scale());
    } else if (dataType instanceof DateType) {
      return getInt(ordinal);
    } else if (dataType instanceof TimestampType) {
      return getLong(ordinal);
    } else if (dataType instanceof BinaryType) {
      return getBinary(ordinal);
    } else if (dataType instanceof StringType) {
      return getUTF8String(ordinal);
    } else if (dataType instanceof CalendarIntervalType) {
      return getInterval(ordinal);
    } else if (dataType instanceof StructType) {
      return getStruct(ordinal, ((StructType) dataType).size());
    } else if (dataType instanceof ArrayType) {
      return getArray(ordinal);
    } else if (dataType instanceof MapType) {
      return getMap(ordinal);
    } else if (dataType instanceof UserDefinedType) {
      return get(ordinal, ((UserDefinedType)dataType).sqlType());
    } else {
      throw new UnsupportedOperationException("Unsupported data type " + dataType.simpleString());
    }
  }
}
