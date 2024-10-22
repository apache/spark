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

package org.apache.spark.sql.types;

import java.util.*;

import org.apache.spark.annotation.Stable;

/**
 * To get/create specific data type, users should use singleton objects and factory methods
 * provided by this class.
 *
 * @since 1.3.0
 */
@Stable
public class DataTypes {
  /**
   * Gets the StringType object.
   */
  public static final DataType StringType = StringType$.MODULE$;

  /**
   * Gets the BinaryType object.
   */
  public static final DataType BinaryType = BinaryType$.MODULE$;

  /**
   * Gets the BooleanType object.
   */
  public static final DataType BooleanType = BooleanType$.MODULE$;

  /**
   * Gets the DateType object.
   */
  public static final DataType DateType = DateType$.MODULE$;

  /**
   * Gets the TimestampType object.
   */
  public static final DataType TimestampType = TimestampType$.MODULE$;

  /**
   * Gets the TimestampNTZType object.
   */
  public static final DataType TimestampNTZType = TimestampNTZType$.MODULE$;

  /**
   * Gets the CalendarIntervalType object.
   */
  public static final DataType CalendarIntervalType = CalendarIntervalType$.MODULE$;

  /**
   * Gets the DoubleType object.
   */
  public static final DataType DoubleType = DoubleType$.MODULE$;

  /**
   * Gets the FloatType object.
   */
  public static final DataType FloatType = FloatType$.MODULE$;

  /**
   * Gets the ByteType object.
   */
  public static final DataType ByteType = ByteType$.MODULE$;

  /**
   * Gets the IntegerType object.
   */
  public static final DataType IntegerType = IntegerType$.MODULE$;

  /**
   * Gets the LongType object.
   */
  public static final DataType LongType = LongType$.MODULE$;

  /**
   * Gets the ShortType object.
   */
  public static final DataType ShortType = ShortType$.MODULE$;

  /**
   * Gets the NullType object.
   */
  public static final DataType NullType = NullType$.MODULE$;

  /**
   * Gets the VariantType object.
   */
  public static final DataType VariantType = VariantType$.MODULE$;

  /**
   * Creates an ArrayType by specifying the data type of elements ({@code elementType}).
   * The field of {@code containsNull} is set to {@code true}.
   */
  public static ArrayType createArrayType(DataType elementType) {
    if (elementType == null) {
      throw new IllegalArgumentException("elementType should not be null.");
    }
    return new ArrayType(elementType, true);
  }

  /**
   * Creates an ArrayType by specifying the data type of elements ({@code elementType}) and
   * whether the array contains null values ({@code containsNull}).
   */
  public static ArrayType createArrayType(DataType elementType, boolean containsNull) {
    if (elementType == null) {
      throw new IllegalArgumentException("elementType should not be null.");
    }
    return new ArrayType(elementType, containsNull);
  }

  /**
   * Creates a DecimalType by specifying the precision and scale.
   */
  public static DecimalType createDecimalType(int precision, int scale) {
    return DecimalType$.MODULE$.apply(precision, scale);
  }

  /**
   * Creates a DecimalType with default precision and scale, which are 10 and 0.
   */
  public static DecimalType createDecimalType() {
    return DecimalType$.MODULE$.USER_DEFAULT();
  }

  /**
   * Creates a DayTimeIntervalType by specifying the start and end fields.
   */
  public static DayTimeIntervalType createDayTimeIntervalType(byte startField, byte endField) {
    return DayTimeIntervalType$.MODULE$.apply(startField, endField);
  }

  /**
   * Creates a DayTimeIntervalType with default start and end fields: interval day to second.
   */
  public static DayTimeIntervalType createDayTimeIntervalType() {
    return DayTimeIntervalType$.MODULE$.DEFAULT();
  }

  /**
   * Creates a YearMonthIntervalType by specifying the start and end fields.
   */
  public static YearMonthIntervalType createYearMonthIntervalType(byte startField, byte endField) {
    return YearMonthIntervalType$.MODULE$.apply(startField, endField);
  }

  /**
   * Creates a YearMonthIntervalType with default start and end fields: interval year to month.
   */
  public static YearMonthIntervalType createYearMonthIntervalType() {
    return YearMonthIntervalType$.MODULE$.DEFAULT();
  }

  /**
   * Creates a MapType by specifying the data type of keys ({@code keyType}) and values
   * ({@code keyType}). The field of {@code valueContainsNull} is set to {@code true}.
   */
  public static MapType createMapType(DataType keyType, DataType valueType) {
    if (keyType == null) {
      throw new IllegalArgumentException("keyType should not be null.");
    }
    if (valueType == null) {
      throw new IllegalArgumentException("valueType should not be null.");
    }
    return new MapType(keyType, valueType, true);
  }

  /**
   * Creates a MapType by specifying the data type of keys ({@code keyType}), the data type of
   * values ({@code keyType}), and whether values contain any null value
   * ({@code valueContainsNull}).
   */
  public static MapType createMapType(
      DataType keyType,
      DataType valueType,
      boolean valueContainsNull) {
    if (keyType == null) {
      throw new IllegalArgumentException("keyType should not be null.");
    }
    if (valueType == null) {
      throw new IllegalArgumentException("valueType should not be null.");
    }
    return new MapType(keyType, valueType, valueContainsNull);
  }

  /**
   * Creates a StructField by specifying the name ({@code name}), data type ({@code dataType}) and
   * whether values of this field can be null values ({@code nullable}).
   */
  public static StructField createStructField(
      String name,
      DataType dataType,
      boolean nullable,
      Metadata metadata) {
    if (name == null) {
      throw new IllegalArgumentException("name should not be null.");
    }
    if (dataType == null) {
      throw new IllegalArgumentException("dataType should not be null.");
    }
    if (metadata == null) {
      throw new IllegalArgumentException("metadata should not be null.");
    }
    return new StructField(name, dataType, nullable, metadata);
  }

  /**
   * Creates a StructField with empty metadata.
   *
   * @see #createStructField(String, DataType, boolean, Metadata)
   */
  public static StructField createStructField(String name, DataType dataType, boolean nullable) {
    return createStructField(name, dataType, nullable, (new MetadataBuilder()).build());
  }

  /**
   * Creates a StructType with the given list of StructFields ({@code fields}).
   */
  public static StructType createStructType(List<StructField> fields) {
    return createStructType(fields.toArray(new StructField[fields.size()]));
  }

  /**
   * Creates a StructType with the given StructField array ({@code fields}).
   */
  public static StructType createStructType(StructField[] fields) {
    if (fields == null) {
      throw new IllegalArgumentException("fields should not be null.");
    }
    Set<String> distinctNames = new HashSet<>();
    for (StructField field : fields) {
      if (field == null) {
        throw new IllegalArgumentException(
          "fields should not contain any null.");
      }

      distinctNames.add(field.name());
    }
    if (distinctNames.size() != fields.length) {
      throw new IllegalArgumentException("fields should have distinct names.");
    }

    return StructType$.MODULE$.apply(fields);
  }

  /**
   * Creates a CharType with the given length.
   *
   * @since 4.0.0
   */
  public static CharType createCharType(int length) {
    return new CharType(length);
  }

  /**
   * Creates a VarcharType with the given length.
   *
   * @since 4.0.0
   */
  public static VarcharType createVarcharType(int length) {
    return new VarcharType(length);
  }
}
