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

package org.apache.spark.sql

import scala.math.BigDecimal

import org.apache.spark.annotation.DeveloperApi

package object util {


  /**
   * :: DeveloperApi ::
   */
  @DeveloperApi
  def toBooleanValue: PartialFunction[Any, BooleanType.JvmType] = {
    case v: BooleanType.JvmType => v
    case v: ByteType.JvmType if v == 1 => true
    case v: ByteType.JvmType if v == 0 => false
    case v: ShortType.JvmType if v == 1 => true
    case v: ShortType.JvmType if v == 0 => false
    case v: IntegerType.JvmType if v == 1 => true
    case v: IntegerType.JvmType if v == 0 => false
    case v: LongType.JvmType if v == 1 => true
    case v: LongType.JvmType if v == 0 => false
    case v: StringType.JvmType if v.toLowerCase == "true" => true
    case v: StringType.JvmType if v.toLowerCase == "false" => false
  }

  /**
   * :: DeveloperApi ::
   */
  @DeveloperApi
  def toStringValue: PartialFunction[Any, StringType.JvmType] = {
    case v => Option(v).map(_.toString).orNull
  }

  /**
   * :: DeveloperApi ::
   */
  @DeveloperApi
  def toByteValue: PartialFunction[Any, ByteType.JvmType] = {
    case v: BooleanType.JvmType => if (v) 1.toByte else 0.toByte
    case v: ByteType.JvmType => v
    case v: StringType.JvmType => v.toByte
  }

  /**
   * :: DeveloperApi ::
   */
  @DeveloperApi
  def toShortValue: PartialFunction[Any, ShortType.JvmType] = {
    case v: BooleanType.JvmType => if (v) 1.toShort else 0.toShort
    case v: ByteType.JvmType => v.toShort
    case v: ShortType.JvmType => v
    case v: StringType.JvmType => v.toShort
  }

  /**
   * :: DeveloperApi ::
   */
  @DeveloperApi
  def toIntegerValue: PartialFunction[Any, IntegerType.JvmType] = {
    case v: BooleanType.JvmType => if (v) 1 else 0
    case v: ByteType.JvmType => v.toInt
    case v: ShortType.JvmType => v.toInt
    case v: IntegerType.JvmType => v
    case v: StringType.JvmType => v.toInt
  }

  /**
   * :: DeveloperApi ::
   */
  @DeveloperApi
  def toLongValue: PartialFunction[Any, LongType.JvmType] = {
    case v: BooleanType.JvmType => if (v) 1.toLong else 0.toLong
    case v: ByteType.JvmType => v.toLong
    case v: ShortType.JvmType => v.toLong
    case v: IntegerType.JvmType => v.toLong
    case v: LongType.JvmType => v
    // We can convert a Timestamp object to a Long because a Long representation of
    // a Timestamp object has a clear meaning
    // (milliseconds since January 1, 1970, 00:00:00 GMT).
    case v: TimestampType.JvmType => v.getTime
    case v: StringType.JvmType => v.toLong
  }

  /**
   * :: DeveloperApi ::
   */
  @DeveloperApi
  def toFloatValue: PartialFunction[Any, FloatType.JvmType] = {
    case v: BooleanType.JvmType => if (v) 1.toFloat else 0.toFloat
    case v: ByteType.JvmType => v.toFloat
    case v: ShortType.JvmType => v.toFloat
    case v: IntegerType.JvmType => v.toFloat
    case v: FloatType.JvmType => v
    case v: StringType.JvmType => v.toFloat
  }

  /**
   * :: DeveloperApi ::
   */
  @DeveloperApi
  def toDoubleValue: PartialFunction[Any, DoubleType.JvmType] = {
    case v: BooleanType.JvmType => if (v) 1.toDouble else 0.toDouble
    case v: ByteType.JvmType => v.toDouble
    case v: ShortType.JvmType => v.toDouble
    case v: IntegerType.JvmType => v.toDouble
    case v: LongType.JvmType => v.toDouble
    case v: FloatType.JvmType => v.toDouble
    case v: DoubleType.JvmType => v
    case v: StringType.JvmType => v.toDouble
  }

  /**
   * :: DeveloperApi ::
   */
  @DeveloperApi
  def toDecimalValue: PartialFunction[Any, DecimalType.JvmType] = {
    case v: BooleanType.JvmType => if (v) BigDecimal(1) else BigDecimal(0)
    case v: ByteType.JvmType => BigDecimal(v)
    case v: ShortType.JvmType => BigDecimal(v)
    case v: IntegerType.JvmType => BigDecimal(v)
    case v: LongType.JvmType => BigDecimal(v)
    case v: FloatType.JvmType => BigDecimal(v)
    case v: DoubleType.JvmType => BigDecimal(v)
    case v: TimestampType.JvmType => BigDecimal(v.getTime)
    case v: StringType.JvmType => BigDecimal(v)
  }

  /**
   * :: DeveloperApi ::
   */
  @DeveloperApi
  def toTimestampValue: PartialFunction[Any, TimestampType.JvmType] = {
    case v: LongType.JvmType => new java.sql.Timestamp(v)
    case v: TimestampType.JvmType => v
    case v: StringType.JvmType => java.sql.Timestamp.valueOf(v)
  }

  /**
   * :: DeveloperApi ::
   */
  @DeveloperApi
  def castToType: PartialFunction[(Any, DataType), Any] = {
    case (null, _) => null
    case (_, NullType) => null
    case (v, BooleanType) => toBooleanValue(v)
    case (v, StringType) => toStringValue(v)
    case (v, ByteType) => toByteValue(v)
    case (v, ShortType) => toShortValue(v)
    case (v, IntegerType) => toIntegerValue(v)
    case (v, LongType) => toLongValue(v)
    case (v, FloatType) => toFloatValue(v)
    case (v, DoubleType) => toDoubleValue(v)
    case (v, DecimalType) => toDecimalValue(v)
    case (v, TimestampType) => toTimestampValue(v)
  }
}
