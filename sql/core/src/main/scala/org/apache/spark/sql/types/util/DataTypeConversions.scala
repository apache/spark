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

import java.text.SimpleDateFormat

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.api.java.{DataType => JDataType, StructField => JStructField,
  MetadataBuilder => JMetaDataBuilder, UDTWrappers}
import org.apache.spark.sql.api.java.{DecimalType => JDecimalType}
import org.apache.spark.sql.catalyst.types.decimal.Decimal
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.types.UserDefinedType

protected[sql] object DataTypeConversions {

  /**
   * Returns the equivalent StructField in Scala for the given StructField in Java.
   */
  def asJavaStructField(scalaStructField: StructField): JStructField = {
    JDataType.createStructField(
      scalaStructField.name,
      asJavaDataType(scalaStructField.dataType),
      scalaStructField.nullable,
      (new JMetaDataBuilder).withMetadata(scalaStructField.metadata).build())
  }

  /**
   * Returns the equivalent DataType in Java for the given DataType in Scala.
   */
  def asJavaDataType(scalaDataType: DataType): JDataType = scalaDataType match {
    case udtType: UserDefinedType[_] =>
      UDTWrappers.wrapAsJava(udtType)

    case StringType => JDataType.StringType
    case BinaryType => JDataType.BinaryType
    case BooleanType => JDataType.BooleanType
    case DateType => JDataType.DateType
    case TimestampType => JDataType.TimestampType
    case DecimalType.Fixed(precision, scale) => new JDecimalType(precision, scale)
    case DecimalType.Unlimited => new JDecimalType()
    case DoubleType => JDataType.DoubleType
    case FloatType => JDataType.FloatType
    case ByteType => JDataType.ByteType
    case IntegerType => JDataType.IntegerType
    case LongType => JDataType.LongType
    case ShortType => JDataType.ShortType
    case NullType => JDataType.NullType

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
      javaStructField.isNullable,
      javaStructField.getMetadata)
  }

  /**
   * Returns the equivalent DataType in Scala for the given DataType in Java.
   */
  def asScalaDataType(javaDataType: JDataType): DataType = javaDataType match {
    case udtType: org.apache.spark.sql.api.java.UserDefinedType[_] =>
      UDTWrappers.wrapAsScala(udtType)

    case stringType: org.apache.spark.sql.api.java.StringType =>
      StringType
    case binaryType: org.apache.spark.sql.api.java.BinaryType =>
      BinaryType
    case booleanType: org.apache.spark.sql.api.java.BooleanType =>
      BooleanType
    case dateType: org.apache.spark.sql.api.java.DateType =>
      DateType
    case timestampType: org.apache.spark.sql.api.java.TimestampType =>
      TimestampType
    case decimalType: org.apache.spark.sql.api.java.DecimalType =>
      if (decimalType.isFixed) {
        DecimalType(decimalType.getPrecision, decimalType.getScale)
      } else {
        DecimalType.Unlimited
      }
    case doubleType: org.apache.spark.sql.api.java.DoubleType =>
      DoubleType
    case floatType: org.apache.spark.sql.api.java.FloatType =>
      FloatType
    case byteType: org.apache.spark.sql.api.java.ByteType =>
      ByteType
    case integerType: org.apache.spark.sql.api.java.IntegerType =>
      IntegerType
    case longType: org.apache.spark.sql.api.java.LongType =>
      LongType
    case shortType: org.apache.spark.sql.api.java.ShortType =>
      ShortType

    case arrayType: org.apache.spark.sql.api.java.ArrayType =>
      ArrayType(asScalaDataType(arrayType.getElementType), arrayType.isContainsNull)
    case mapType: org.apache.spark.sql.api.java.MapType =>
      MapType(
        asScalaDataType(mapType.getKeyType),
        asScalaDataType(mapType.getValueType),
        mapType.isValueContainsNull)
    case structType: org.apache.spark.sql.api.java.StructType =>
      StructType(structType.getFields.map(asScalaStructField))
  }

  def stringToTime(s: String): java.util.Date = {
    if (!s.contains('T')) {
      // JDBC escape string
      if (s.contains(' ')) {
        java.sql.Timestamp.valueOf(s)
      } else {
        java.sql.Date.valueOf(s)
      }
    } else if (s.endsWith("Z")) {
      // this is zero timezone of ISO8601
      stringToTime(s.substring(0, s.length - 1) + "GMT-00:00")
    } else if (s.indexOf("GMT") == -1) {
      // timezone with ISO8601
      val inset = "+00.00".length
      val s0 = s.substring(0, s.length - inset)
      val s1 = s.substring(s.length - inset, s.length)
      if (s0.substring(s0.lastIndexOf(':')).contains('.')) {
        stringToTime(s0 + "GMT" + s1)
      } else {
        stringToTime(s0 + ".0GMT" + s1)
      }
    } else {
      // ISO8601 with GMT insert
      val ISO8601GMT: SimpleDateFormat = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSSz" )
      ISO8601GMT.parse(s)
    }
  }

  /** Converts Java objects to catalyst rows / types */
  def convertJavaToCatalyst(a: Any, dataType: DataType): Any = (a, dataType) match {
    case (obj, udt: UserDefinedType[_]) => ScalaReflection.convertToCatalyst(obj, udt) // Scala type
    case (d: java.math.BigDecimal, _) => Decimal(BigDecimal(d))
    case (other, _) => other
  }

  /** Converts Java objects to catalyst rows / types */
  def convertCatalystToJava(a: Any): Any = a match {
    case d: scala.math.BigDecimal => d.underlying()
    case other => other
  }
}
