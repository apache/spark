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

package org.apache.spark.sql.types.util

import org.apache.spark.sql._
import org.apache.spark.sql.api.java.types.{DataType => JDataType, StructField => JStructField}

import scala.collection.JavaConverters._

protected[sql] object DataTypeConversions {

  /**
   * Returns the equivalent StructField in Scala for the given StructField in Java.
   */
  def asJavaStructField(scalaStructField: StructField): JStructField = {
    JDataType.createStructField(
      scalaStructField.name,
      asJavaDataType(scalaStructField.dataType),
      scalaStructField.nullable)
  }

  /**
   * Returns the equivalent DataType in Java for the given DataType in Scala.
   */
  def asJavaDataType(scalaDataType: DataType): JDataType = scalaDataType match {
    case StringType => JDataType.StringType
    case BinaryType => JDataType.BinaryType
    case BooleanType => JDataType.BooleanType
    case TimestampType => JDataType.TimestampType
    case DecimalType => JDataType.DecimalType
    case DoubleType => JDataType.DoubleType
    case FloatType => JDataType.FloatType
    case ByteType => JDataType.ByteType
    case IntegerType => JDataType.IntegerType
    case LongType => JDataType.LongType
    case ShortType => JDataType.ShortType

    case arrayType: ArrayType => JDataType.createArrayType(
        asJavaDataType(arrayType.elementType), arrayType.containsNull)
    case mapType: MapType => JDataType.createMapType(
        asJavaDataType(mapType.keyType),
        asJavaDataType(mapType.valueType),
        mapType.valueContainsNull)
    case structType: StructType => JDataType.createStructType(
        structType.fields.map(asJavaStructField).asJava)
  }

  /**
   * Returns the equivalent StructField in Scala for the given StructField in Java.
   */
  def asScalaStructField(javaStructField: JStructField): StructField = {
    StructField(
      javaStructField.getName,
      asScalaDataType(javaStructField.getDataType),
      javaStructField.isNullable)
  }

  /**
   * Returns the equivalent DataType in Scala for the given DataType in Java.
   */
  def asScalaDataType(javaDataType: JDataType): DataType = javaDataType match {
    case stringType: org.apache.spark.sql.api.java.types.StringType =>
      StringType
    case binaryType: org.apache.spark.sql.api.java.types.BinaryType =>
      BinaryType
    case booleanType: org.apache.spark.sql.api.java.types.BooleanType =>
      BooleanType
    case timestampType: org.apache.spark.sql.api.java.types.TimestampType =>
      TimestampType
    case decimalType: org.apache.spark.sql.api.java.types.DecimalType =>
      DecimalType
    case doubleType: org.apache.spark.sql.api.java.types.DoubleType =>
      DoubleType
    case floatType: org.apache.spark.sql.api.java.types.FloatType =>
      FloatType
    case byteType: org.apache.spark.sql.api.java.types.ByteType =>
      ByteType
    case integerType: org.apache.spark.sql.api.java.types.IntegerType =>
      IntegerType
    case longType: org.apache.spark.sql.api.java.types.LongType =>
      LongType
    case shortType: org.apache.spark.sql.api.java.types.ShortType =>
      ShortType

    case arrayType: org.apache.spark.sql.api.java.types.ArrayType =>
      ArrayType(asScalaDataType(arrayType.getElementType), arrayType.isContainsNull)
    case mapType: org.apache.spark.sql.api.java.types.MapType =>
      MapType(
        asScalaDataType(mapType.getKeyType),
        asScalaDataType(mapType.getValueType),
        mapType.isValueContainsNull)
    case structType: org.apache.spark.sql.api.java.types.StructType =>
      StructType(structType.getFields.map(asScalaStructField))
  }
}
