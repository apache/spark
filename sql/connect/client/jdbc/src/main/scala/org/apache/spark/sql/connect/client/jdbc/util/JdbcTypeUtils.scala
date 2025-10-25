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
    case other =>
      throw new SQLFeatureNotSupportedException(s"DataType $other is not supported yet.")
  }

  def isSigned(field: StructField): Boolean = field.dataType match {
    case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType => true
    case NullType | BooleanType | StringType => false
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
    case StringType => 255
    case other =>
      throw new SQLFeatureNotSupportedException(s"DataType $other is not supported yet.")
  }

  def getScale(field: StructField): Int = field.dataType match {
    case FloatType => 7
    case DoubleType => 15
    case NullType | BooleanType | ByteType | ShortType | IntegerType | LongType | StringType => 0
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
    case other =>
      throw new SQLFeatureNotSupportedException(s"DataType $other is not supported yet.")
  }
}
