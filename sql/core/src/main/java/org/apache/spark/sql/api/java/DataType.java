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

package org.apache.spark.sql.api.java;

import java.util.*;

/**
 * The base type of all Spark SQL data types.
 *
 * To get/create specific data type, users should use singleton objects and factory methods
 * provided by this class.
 */
public abstract class DataType {

  /**
   * Gets the StringType object.
   */
  public static final StringType StringType = new StringType();

  /**
   * Gets the BinaryType object.
   */
  public static final BinaryType BinaryType = new BinaryType();

  /**
   * Gets the BooleanType object.
   */
  public static final BooleanType BooleanType = new BooleanType();

  /**
   * Gets the DateType object.
   */
  public static final DateType DateType = new DateType();

  /**
   * Gets the TimestampType object.
   */
  public static final TimestampType TimestampType = new TimestampType();

  /**
   * Gets the DecimalType object.
   */
  public static final DecimalType DecimalType = new DecimalType();

  /**
   * Gets the DoubleType object.
   */
  public static final DoubleType DoubleType = new DoubleType();

  /**
   * Gets the FloatType object.
   */
  public static final FloatType FloatType = new FloatType();

  /**
   * Gets the ByteType object.
   */
  public static final ByteType ByteType = new ByteType();

  /**
   * Gets the IntegerType object.
   */
  public static final IntegerType IntegerType = new IntegerType();

  /**
   * Gets the LongType object.
   */
  public static final LongType LongType = new LongType();

  /**
   * Gets the ShortType object.
   */
  public static final ShortType ShortType = new ShortType();

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
    return createStructType(fields.toArray(new StructField[0]));
  }

  /**
   * Creates a StructType with the given StructField array ({@code fields}).
   */
  public static StructType createStructType(StructField[] fields) {
    if (fields == null) {
      throw new IllegalArgumentException("fields should not be null.");
    }
    Set<String> distinctNames = new HashSet<String>();
    for (StructField field: fields) {
      if (field == null) {
        throw new IllegalArgumentException(
          "fields should not contain any null.");
      }

      distinctNames.add(field.getName());
    }
    if (distinctNames.size() != fields.length) {
      throw new IllegalArgumentException("fields should have distinct names.");
    }

    return new StructType(fields);
  }
}
