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
package org.apache.spark.sql.connector.catalog.functions

import java.time.{Instant, LocalDate, ZoneId}
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object UnboundYearsFunction extends UnboundFunction {
  override def bind(inputType: StructType): BoundFunction = {
    if (inputType.size == 1 && isValidType(inputType.head.dataType)) YearsFunction
    else throw new UnsupportedOperationException(
      "'years' only take date or timestamp as input type")
  }

  private def isValidType(dt: DataType): Boolean = dt match {
    case DateType | TimestampType => true
    case _ => false
  }

  override def description(): String = name()
  override def name(): String = "years"
}

object YearsFunction extends ScalarFunction[Long] {
  override def inputTypes(): Array[DataType] = Array(TimestampType)
  override def resultType(): DataType = LongType
  override def name(): String = "years"
  override def canonicalName(): String = name()

  val UTC: ZoneId = ZoneId.of("UTC")
  val EPOCH_LOCAL_DATE: LocalDate = Instant.EPOCH.atZone(UTC).toLocalDate

  def invoke(ts: Long): Long = {
    val localDate = DateTimeUtils.microsToInstant(ts).atZone(UTC).toLocalDate
    ChronoUnit.YEARS.between(EPOCH_LOCAL_DATE, localDate)
  }
}

object DaysFunction extends BoundFunction {
  override def inputTypes(): Array[DataType] = Array(TimestampType)
  override def resultType(): DataType = LongType
  override def name(): String = "days"
  override def canonicalName(): String = name()
}

object UnboundDaysFunction extends UnboundFunction {
  override def bind(inputType: StructType): BoundFunction = {
    if (inputType.size == 1 && isValidType(inputType.head.dataType)) DaysFunction
    else throw new UnsupportedOperationException(
      "'days' only take date or timestamp as input type")
  }

  private def isValidType(dt: DataType): Boolean = dt match {
    case DateType | TimestampType => true
    case _ => false
  }

  override def description(): String = name()
  override def name(): String = "days"
}

object UnboundBucketFunction extends UnboundFunction {
  override def bind(inputType: StructType): BoundFunction = BucketFunction
  override def description(): String = name()
  override def name(): String = "bucket"
}

object BucketFunction extends ScalarFunction[Int] with ReducibleFunction[Int, Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType, LongType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "bucket"
  override def canonicalName(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): Int = {
    (input.getLong(1) % input.getInt(0)).toInt
  }

  override def reducer(
      thisNumBuckets: Int,
      otherFunc: ReducibleFunction[_, _],
      otherNumBuckets: Int): Reducer[Int, Int] = {

    if (otherFunc == BucketFunction) {
      val gcd = this.gcd(thisNumBuckets, otherNumBuckets)
      if (gcd > 1 && gcd != thisNumBuckets) {
        return BucketReducer(gcd)
      }
    }
    null
  }

  private def gcd(a: Int, b: Int): Int = BigInt(a).gcd(BigInt(b)).toInt
}

case class BucketReducer(divisor: Int) extends Reducer[Int, Int] {
  override def reduce(bucket: Int): Int = bucket % divisor
}

object UnboundStringSelfFunction extends UnboundFunction {
  override def bind(inputType: StructType): BoundFunction = StringSelfFunction
  override def description(): String = name()
  override def name(): String = "string_self"
}

object StringSelfFunction extends ScalarFunction[UTF8String] {
  override def inputTypes(): Array[DataType] = Array(StringType)
  override def resultType(): DataType = StringType
  override def name(): String = "string_self"
  override def canonicalName(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): UTF8String = {
    input.getUTF8String(0)
  }
}

object UnboundTruncateFunction extends UnboundFunction {
  override def bind(inputType: StructType): BoundFunction = TruncateFunction
  override def description(): String = name()
  override def name(): String = "truncate"
}

object TruncateFunction extends ScalarFunction[UTF8String] {
  override def inputTypes(): Array[DataType] = Array(StringType, IntegerType)
  override def resultType(): DataType = StringType
  override def name(): String = "truncate"
  override def canonicalName(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): UTF8String = {
    val str = input.getUTF8String(0)
    val length = input.getInt(1)
    str.substring(0, length)
  }
}
