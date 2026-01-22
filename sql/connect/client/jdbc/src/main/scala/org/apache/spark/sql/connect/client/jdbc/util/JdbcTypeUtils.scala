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

package org.apache.spark.sql.connect.client.jdbc.util

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Array => _, _}

import org.apache.spark.sql.types._

private[jdbc] object JdbcTypeUtils {

  def getColumnType(field: StructField): Int = field.dataType match {
    case NullType => Types.NULL
    case BooleanType => Types.BOOLEAN
    case ByteType => Types.TINYINT
    case ShortType => Types.SMALLINT
    case IntegerType => Types.INTEGER
    case LongType => Types.BIGINT
    case FloatType => Types.FLOAT
    case DoubleType => Types.DOUBLE
    case StringType => Types.VARCHAR
    case _: DecimalType => Types.DECIMAL
    case DateType => Types.DATE
    case TimestampType => Types.TIMESTAMP
    case TimestampNTZType => Types.TIMESTAMP
    case BinaryType => Types.VARBINARY
    case _: TimeType => Types.TIME
    case other =>
      throw new SQLFeatureNotSupportedException(s"DataType $other is not supported yet.")
  }

  def getColumnTypeClassName(field: StructField): String = field.dataType match {
    case NullType => "null"
    case BooleanType => classOf[JBoolean].getName
    case ByteType => classOf[JByte].getName
    case ShortType => classOf[JShort].getName
    case IntegerType => classOf[Integer].getName
    case LongType => classOf[JLong].getName
    case FloatType => classOf[JFloat].getName
    case DoubleType => classOf[JDouble].getName
    case StringType => classOf[String].getName
    case _: DecimalType => classOf[JBigDecimal].getName
    case DateType => classOf[Date].getName
    case TimestampType => classOf[Timestamp].getName
    case TimestampNTZType => classOf[Timestamp].getName
    case BinaryType => classOf[Array[Byte]].getName
    case _: TimeType => classOf[Time].getName
    case other =>
      throw new SQLFeatureNotSupportedException(s"DataType $other is not supported yet.")
  }

  def isSigned(field: StructField): Boolean = field.dataType match {
    case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
         _: DecimalType => true
    case NullType | BooleanType | StringType | DateType | BinaryType | _: TimeType |
         TimestampType | TimestampNTZType => false
    case other =>
      throw new SQLFeatureNotSupportedException(s"DataType $other is not supported yet.")
  }

  def getPrecision(field: StructField): Int = field.dataType match {
    case NullType => 0
    case BooleanType => 1
    case ByteType => 3
    case ShortType => 5
    case IntegerType => 10
    case LongType => 19
    case FloatType => 7
    case DoubleType => 15
    case StringType => Int.MaxValue
    case DecimalType.Fixed(p, _) => p
    case DateType => 10
    case TimestampType => 29
    case TimestampNTZType => 29
    case BinaryType => Int.MaxValue
    // Returns the Spark SQL TIME type precision, even though java.sql.ResultSet.getTime()
    // can only retrieve up to millisecond precision (3) due to java.sql.Time limitations.
    // Users can call getObject(index, classOf[LocalTime]) to access full microsecond
    // precision when the source type is TIME(4) or higher.
    case TimeType(precision) => precision
    case other =>
      throw new SQLFeatureNotSupportedException(s"DataType $other is not supported yet.")
  }

  def getScale(field: StructField): Int = field.dataType match {
    case FloatType => 7
    case DoubleType => 15
    case TimestampType => 6
    case TimestampNTZType => 6
    case NullType | BooleanType | ByteType | ShortType | IntegerType | LongType | StringType |
         DateType | BinaryType | _: TimeType => 0
    case DecimalType.Fixed(_, s) => s
    case other =>
      throw new SQLFeatureNotSupportedException(s"DataType $other is not supported yet.")
  }

  def getDisplaySize(field: StructField): Int = field.dataType match {
    case NullType => 4 // length of `NULL`
    case BooleanType => 5 // `TRUE` or `FALSE`
    case ByteType | ShortType | IntegerType | LongType =>
      getPrecision(field) + 1 // may have leading negative sign
    case FloatType => 14
    case DoubleType => 24
    case StringType =>
      getPrecision(field)
    case DateType => 10 // length of `YYYY-MM-DD`
    case TimestampType => 29 // length of `YYYY-MM-DD HH:MM:SS.SSSSSS`
    case TimestampNTZType => 29 // length of `YYYY-MM-DD HH:MM:SS.SSSSSS`
    case BinaryType => Int.MaxValue
    case TimeType(precision) if precision > 0 => 8 + 1 + precision // length of `HH:MM:SS.ffffff`
    case TimeType(_) => 8 // length of `HH:MM:SS`
    // precision + negative sign + leading zero + decimal point, like DECIMAL(5,5) = -0.12345
    case DecimalType.Fixed(p, s) if p == s => p + 3
    // precision + negative sign, like DECIMAL(5,0) = -12345
    case DecimalType.Fixed(p, s) if s == 0 => p + 1
    // precision + negative sign + decimal point, like DECIMAL(5,2) = -123.45
    case DecimalType.Fixed(p, _) => p + 2
    case other =>
      throw new SQLFeatureNotSupportedException(s"DataType $other is not supported yet.")
  }

  def getDecimalDigits(field: StructField): Integer = field.dataType match {
    case BooleanType | _: IntegralType => 0
    case FloatType => 7
    case DoubleType => 15
    case d: DecimalType => d.scale
    case TimeType(scale) => scale
    case TimestampType | TimestampNTZType => 6
    case _ => null
  }

  def getNumPrecRadix(field: StructField): Integer = field.dataType match {
    case _: NumericType => 10
    case _ => null
  }
}
