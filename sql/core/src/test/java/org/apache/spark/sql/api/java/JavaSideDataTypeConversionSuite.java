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

import java.util.List;
import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.sql.types.util.DataTypeConversions;

public class JavaSideDataTypeConversionSuite {
  public void checkDataType(DataType javaDataType) {
    org.apache.spark.sql.catalyst.types.DataType scalaDataType =
      DataTypeConversions.asScalaDataType(javaDataType);
    DataType actual = DataTypeConversions.asJavaDataType(scalaDataType);
    Assert.assertEquals(javaDataType, actual);
  }

  @Test
  public void createDataTypes() {
    // Simple DataTypes.
    checkDataType(DataType.StringType);
    checkDataType(DataType.BinaryType);
    checkDataType(DataType.BooleanType);
    checkDataType(DataType.TimestampType);
    checkDataType(DataType.DecimalType);
    checkDataType(DataType.DoubleType);
    checkDataType(DataType.FloatType);
    checkDataType(DataType.ByteType);
    checkDataType(DataType.IntegerType);
    checkDataType(DataType.LongType);
    checkDataType(DataType.ShortType);

    // Simple ArrayType.
    DataType simpleJavaArrayType = DataType.createArrayType(DataType.StringType, true);
    checkDataType(simpleJavaArrayType);

    // Simple MapType.
    DataType simpleJavaMapType = DataType.createMapType(DataType.StringType, DataType.LongType);
    checkDataType(simpleJavaMapType);

    // Simple StructType.
    List<StructField> simpleFields = new ArrayList<StructField>();
    simpleFields.add(DataType.createStructField("a", DataType.DecimalType, false));
    simpleFields.add(DataType.createStructField("b", DataType.BooleanType, true));
    simpleFields.add(DataType.createStructField("c", DataType.LongType, true));
    simpleFields.add(DataType.createStructField("d", DataType.BinaryType, false));
    DataType simpleJavaStructType = DataType.createStructType(simpleFields);
    checkDataType(simpleJavaStructType);

    // Complex StructType.
    List<StructField> complexFields = new ArrayList<StructField>();
    complexFields.add(DataType.createStructField("simpleArray", simpleJavaArrayType, true));
    complexFields.add(DataType.createStructField("simpleMap", simpleJavaMapType, true));
    complexFields.add(DataType.createStructField("simpleStruct", simpleJavaStructType, true));
    complexFields.add(DataType.createStructField("boolean", DataType.BooleanType, false));
    DataType complexJavaStructType = DataType.createStructType(complexFields);
    checkDataType(complexJavaStructType);

    // Complex ArrayType.
    DataType complexJavaArrayType = DataType.createArrayType(complexJavaStructType, true);
    checkDataType(complexJavaArrayType);

    // Complex MapType.
    DataType complexJavaMapType =
      DataType.createMapType(complexJavaStructType, complexJavaArrayType, false);
    checkDataType(complexJavaMapType);
  }

  @Test
  public void illegalArgument() {
    // ArrayType
    try {
      DataType.createArrayType(null, true);
      Assert.fail();
    } catch (IllegalArgumentException expectedException) {
    }

    // MapType
    try {
      DataType.createMapType(null, DataType.StringType);
      Assert.fail();
    } catch (IllegalArgumentException expectedException) {
    }
    try {
      DataType.createMapType(DataType.StringType, null);
      Assert.fail();
    } catch (IllegalArgumentException expectedException) {
    }
    try {
      DataType.createMapType(null, null);
      Assert.fail();
    } catch (IllegalArgumentException expectedException) {
    }

    // StructField
    try {
      DataType.createStructField(null, DataType.StringType, true);
    } catch (IllegalArgumentException expectedException) {
    }
    try {
      DataType.createStructField("name", null, true);
    } catch (IllegalArgumentException expectedException) {
    }
    try {
      DataType.createStructField(null, null, true);
    } catch (IllegalArgumentException expectedException) {
    }

    // StructType
    try {
      List<StructField> simpleFields = new ArrayList<StructField>();
      simpleFields.add(DataType.createStructField("a", DataType.DecimalType, false));
      simpleFields.add(DataType.createStructField("b", DataType.BooleanType, true));
      simpleFields.add(DataType.createStructField("c", DataType.LongType, true));
      simpleFields.add(null);
      DataType.createStructType(simpleFields);
      Assert.fail();
    } catch (IllegalArgumentException expectedException) {
    }
    try {
      List<StructField> simpleFields = new ArrayList<StructField>();
      simpleFields.add(DataType.createStructField("a", DataType.DecimalType, false));
      simpleFields.add(DataType.createStructField("a", DataType.BooleanType, true));
      simpleFields.add(DataType.createStructField("c", DataType.LongType, true));
      DataType.createStructType(simpleFields);
      Assert.fail();
    } catch (IllegalArgumentException expectedException) {
    }
  }
}
