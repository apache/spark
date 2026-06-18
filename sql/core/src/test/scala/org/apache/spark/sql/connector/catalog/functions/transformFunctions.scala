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
import org.apache.spark.sql.connector.expressions.Literal
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

abstract class UnboundYearsFunctionBase extends UnboundFunction {
  protected def isValidType(dt: DataType): Boolean = dt match {
    case DateType | TimestampType => true
    case _ => false
  }

  override def description(): String = name()
  override def name(): String = "years"
}

object UnboundYearsFunction extends UnboundYearsFunctionBase {
  override def bind(inputType: StructType): BoundFunction = {
    if (inputType.size == 1 && isValidType(inputType.head.dataType)) YearsFunction
    else throw new UnsupportedOperationException(
      "'years' only take date or timestamp as input type")
  }
}

object UnboundYearsFunctionWithToYearsReducerWithLongResult extends UnboundYearsFunctionBase {
  override def bind(inputType: StructType): BoundFunction = {
    if (inputType.size == 1 && isValidType(inputType.head.dataType)) {
      YearsFunctionWithToYearsReducerWithLongResult
    } else throw new UnsupportedOperationException(
      "'years' only take date or timestamp as input type")
  }
}

abstract class YearsFunctionBase[O] extends ScalarFunction[Int] with ReducibleFunction[Int, O] {
  override def inputTypes(): Array[DataType] = Array(TimestampType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "years"
  override def canonicalName(): String = name()

  val UTC: ZoneId = ZoneId.of("UTC")
  val EPOCH_LOCAL_DATE: LocalDate = Instant.EPOCH.atZone(UTC).toLocalDate

  protected def doInvoke(ts: Long): Long = {
    val localDate = DateTimeUtils.microsToInstant(ts).atZone(UTC).toLocalDate
    ChronoUnit.YEARS.between(EPOCH_LOCAL_DATE, localDate)
  }
}

// This `years` function reduces `IntegerType` partition keys to `IntegerType` partition keys when
// partitions are reduced to partitions of a `days` function, which produces `DateType` keys.
object YearsFunction extends YearsFunctionBase[Int]  {
  def invoke(ts: Long): Int = doInvoke(ts).toInt
  override def reducer(otherFunction: ReducibleFunction[_, _]): Reducer[Int, Int] = null
}

// This `years` function reduces `IntegerType` partition keys to `LongType` partition keys when
// partitions are reduced to partitions of a `days` function, which produces `DateType` keys.
object YearsFunctionWithToYearsReducerWithLongResult extends YearsFunctionBase[Long] {
  def invoke(ts: Long): Int = doInvoke(ts).toInt
  override def reducer(otherFunction: ReducibleFunction[_, _]): Reducer[Int, Long] = {
    if (otherFunction == DaysFunctionWithToYearsReducerWithLongResult) {
      YearsToYearsReducerWithLongResult()
    } else {
      null
    }
  }
}

case class YearsToYearsReducerWithLongResult() extends Reducer[Int, Long] {
  override def resultType(): DataType = LongType
  override def reduce(days: Int): Long = days.toLong
}

abstract class UnboundDaysFunctionBase extends UnboundFunction {
  protected def isValidType(dt: DataType): Boolean = dt match {
    case DateType | TimestampType => true
    case _ => false
  }

  override def description(): String = name()
  override def name(): String = "days"
}

object UnboundDaysFunction extends UnboundDaysFunctionBase {
  override def bind(inputType: StructType): BoundFunction = {
    if (inputType.size == 1 && isValidType(inputType.head.dataType)) DaysFunction
    else throw new UnsupportedOperationException(
      "'days' only take date or timestamp as input type")
  }
}

object UnboundDaysFunctionWithToYearsReducerWithDateResult extends UnboundDaysFunctionBase {
  override def bind(inputType: StructType): BoundFunction = {
    if (inputType.size == 1 && isValidType(inputType.head.dataType)) {
      DaysFunctionWithToYearsReducerWithDateResult
    } else throw new UnsupportedOperationException(
      "'days' only take date or timestamp as input type")
  }
}

object UnboundDaysFunctionWithToYearsReducerWithLongResult extends UnboundDaysFunctionBase {
  override def bind(inputType: StructType): BoundFunction = {
    if (inputType.size == 1 && isValidType(inputType.head.dataType)) {
      DaysFunctionWithToYearsReducerWithLongResult
    } else throw new UnsupportedOperationException(
      "'days' only take date or timestamp as input type")
  }
}

abstract class DaysFunctionBase[O] extends ScalarFunction[Int] with ReducibleFunction[Int, O] {
  override def inputTypes(): Array[DataType] = Array(TimestampType)
  override def resultType(): DataType = DateType
  override def name(): String = "days"
  override def canonicalName(): String = name()
}

// This `days` function reduces `DateType` partition keys to `IntegerType` partition keys when
// partitions are reduced to partitions of a `years` function, which produces `IntegerType` keys.
object DaysFunction extends DaysFunctionBase[Int] {
  override def reducer(otherFunc: ReducibleFunction[_, _]): Reducer[Int, Int] = {
    if (otherFunc == YearsFunction) {
      DaysToYearsReducer()
    } else {
      null
    }
  }
}

// This `days` function reduces `DateType` partition keys to `DateType` partition keys when
// partitions are reduced to partitions of a `years` function, which produces `IntegerType` keys.
object DaysFunctionWithToYearsReducerWithDateResult extends DaysFunctionBase[Int] {
  override def reducer(otherFunc: ReducibleFunction[_, _]): Reducer[Int, Int] = {
    if (otherFunc == YearsFunction) {
      DaysToYearsReducerWithDateResult()
    } else {
      null
    }
  }
}

// This `days` function reduces `DateType` partition keys to `LongType` partition keys when
// partitions are reduced to partitions of a `years` function, which produces `IntegerType` keys.
object DaysFunctionWithToYearsReducerWithLongResult extends DaysFunctionBase[Long] {
  override def reducer(otherFunc: ReducibleFunction[_, _]): Reducer[Int, Long] = {
    if (otherFunc == YearsFunctionWithToYearsReducerWithLongResult) {
      DaysToYearsReducerWithLongResult()
    } else {
      null
    }
  }
}

abstract class DaysToYearsReducerBase[O] extends Reducer[Int, O] {
  val UTC: ZoneId = ZoneId.of("UTC")
  val EPOCH_LOCAL_DATE: LocalDate = Instant.EPOCH.atZone(UTC).toLocalDate

  protected def doReduce(days: Int): Long = {
    val localDate = EPOCH_LOCAL_DATE.plusDays(days)
    ChronoUnit.YEARS.between(EPOCH_LOCAL_DATE, localDate)
  }
}

case class DaysToYearsReducer() extends DaysToYearsReducerBase[Int] {
  override def resultType(): DataType = IntegerType
  override def reduce(days: Int): Int = doReduce(days).toInt
}

case class DaysToYearsReducerWithDateResult() extends DaysToYearsReducerBase[Int] {
  override def resultType(): DataType = DateType
  override def reduce(days: Int): Int = doReduce(days).toInt
}

case class DaysToYearsReducerWithLongResult() extends DaysToYearsReducerBase[Long] {
  override def resultType(): DataType = LongType
  override def reduce(days: Int): Long = doReduce(days)
}

object UnboundBucketFunction extends UnboundFunction {
  override def bind(inputType: StructType): BoundFunction = BucketFunction
  override def description(): String = name()
  override def name(): String = "bucket"
}

// the result should be consistent with BucketTransform defined at InMemoryBaseTable.scala
object BucketFunction extends ScalarFunction[Int] with ReducibleFunction[Int, Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType, LongType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "bucket"
  override def canonicalName(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): Int = {
    Math.floorMod(input.getLong(1), input.getInt(0))
  }

  override def reducer(
      thisParams: Array[Literal[_]],
      otherFunc: ReducibleFunction[_, _],
      otherParams: Array[Literal[_]]): Reducer[Int, Int] = {

    if (otherFunc == BucketFunction) {
      val thisNumBuckets = thisParams(0).value().asInstanceOf[Int]
      val otherNumBuckets = otherParams(0).value().asInstanceOf[Int]

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
  override def resultType(): DataType = IntegerType
  override def displayName(): String = toString
}

/**
 * A bucket function that only overrides the deprecated `reducer(int, func, int)` method,
 * not the new `reducer(Literal[], func, Literal[])` method.
 *
 * Used to verify that the default implementation of the new method correctly falls back
 * to the deprecated int-based API, so legacy implementations continue to work.
 */
object LegacyBucketFunction extends ScalarFunction[Int] with ReducibleFunction[Int, Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType, LongType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "legacy_bucket"
  override def canonicalName(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): Int = {
    Math.floorMod(input.getLong(1), input.getInt(0))
  }

  override def reducer(
      thisNumBuckets: Int,
      otherFunc: ReducibleFunction[_, _],
      otherNumBuckets: Int): Reducer[Int, Int] = {
    if (otherFunc == LegacyBucketFunction) {
      val gcd = BigInt(thisNumBuckets).gcd(BigInt(otherNumBuckets)).toInt
      if (gcd > 1 && gcd != thisNumBuckets) {
        return BucketReducer(gcd)
      }
    }
    null
  }
}

/**
 * A bucket function that implements BOTH reducer overloads: the deprecated `reducer(int, ..., int)`
 * always returns null (not reducible via the old API), while the new `reducer(Literal[], ...)`
 * returns a GCD-based reducer. Used to verify that the dispatch falls back to the generalized
 * overload when the deprecated one returns null (not only when it throws).
 */
object DualApiBucketFunction extends ScalarFunction[Int] with ReducibleFunction[Int, Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType, LongType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "dual_bucket"
  override def canonicalName(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): Int = {
    Math.floorMod(input.getLong(1), input.getInt(0))
  }

  // Deprecated API: intentionally signals "not reducible" via null (not via an exception).
  override def reducer(
      thisNumBuckets: Int,
      otherFunc: ReducibleFunction[_, _],
      otherNumBuckets: Int): Reducer[Int, Int] = null

  // New API: a real GCD-based reducer.
  override def reducer(
      thisParams: Array[Literal[_]],
      otherFunc: ReducibleFunction[_, _],
      otherParams: Array[Literal[_]]): Reducer[Int, Int] = {
    if (otherFunc == DualApiBucketFunction) {
      val thisNumBuckets = thisParams(0).value().asInstanceOf[Int]
      val otherNumBuckets = otherParams(0).value().asInstanceOf[Int]
      val gcd = BigInt(thisNumBuckets).gcd(BigInt(otherNumBuckets)).toInt
      if (gcd > 1 && gcd != thisNumBuckets) {
        return BucketReducer(gcd)
      }
    }
    null
  }
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
  override def bind(inputType: StructType): BoundFunction = {
    if (inputType.size == 2) {
      inputType.head.dataType match {
        case StringType => TruncateFunction
        case IntegerType => IntegerTruncateFunction
        case _ =>
          throw new UnsupportedOperationException(
            s"'truncate' does not support data type: ${inputType.head.dataType}")
      }
    } else {
      throw new UnsupportedOperationException(
        "'truncate' requires exactly 2 arguments: (column, width)")
    }
  }

  override def description(): String = name()
  override def name(): String = "truncate"
}

/**
 * Truncate transform for String type.
 * Follows Iceberg spec: truncate(str, L) = str[0:L]
 *
 * Implements ReducibleFunction: ANY two different widths are compatible.
 * The reducer uses the smaller width.
 */
object TruncateFunction
    extends ScalarFunction[UTF8String]
    with ReducibleFunction[UTF8String, UTF8String] {
  override def inputTypes(): Array[DataType] = Array(StringType, IntegerType)
  override def resultType(): DataType = StringType
  override def name(): String = "truncate"
  override def canonicalName(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): UTF8String = {
    val str = input.getUTF8String(0)
    val width = input.getInt(1)
    str.substring(0, width)
  }

  override def reducer(
      thisParams: Array[Literal[_]],
      otherFunc: ReducibleFunction[_, _],
      otherParams: Array[Literal[_]]): Reducer[UTF8String, UTF8String] = {

    if (otherFunc == TruncateFunction) {
      val thisWidth = thisParams(0).value().asInstanceOf[Int]
      val otherWidth = otherParams(0).value().asInstanceOf[Int]
      val smallerWidth = math.min(thisWidth, otherWidth)

      if (smallerWidth != thisWidth) {
        return TruncateReducer(smallerWidth)
      }
    }
    null
  }
}

case class TruncateReducer(width: Int) extends Reducer[UTF8String, UTF8String] {
  override def reduce(value: UTF8String): UTF8String = {
    value.substring(0, width)
  }
  override def resultType(): DataType = StringType
  override def displayName(): String = s"truncate($width)"
}

/**
 * Truncate transform for Integer type.
 * Follows Iceberg spec: truncate(value, W) = value - (((value % W) + W) % W), which snaps `value`
 * down to a multiple of `W`.
 *
 * Implements ReducibleFunction: truncate(v, W1) and truncate(v, W2) are always reducible onto a
 * common coarser grid of multiples of lcm(W1, W2). The finer side (whose width does not already
 * equal the lcm) reduces by snapping to that grid; when W2 is a multiple of W1 the lcm is simply
 * the coarser width W2.
 */
object IntegerTruncateFunction
    extends ScalarFunction[Int]
    with ReducibleFunction[Int, Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType, IntegerType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "truncate"
  override def canonicalName(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): Int = {
    val value = input.getInt(0)
    val width = input.getInt(1)
    value - (((value % width) + width) % width)
  }

  override def reducer(
      thisParams: Array[Literal[_]],
      otherFunc: ReducibleFunction[_, _],
      otherParams: Array[Literal[_]]): Reducer[Int, Int] = {
    if (otherFunc == IntegerTruncateFunction) {
      val thisWidth = thisParams(0).value().asInstanceOf[Int]
      val otherWidth = otherParams(0).value().asInstanceOf[Int]
      val common = lcm(thisWidth, otherWidth)
      // Only the finer side reduces; if `common == thisWidth` this side is already the common grid.
      if (common != thisWidth) {
        return IntTruncateReducer(common)
      }
    }
    null
  }

  private def lcm(a: Int, b: Int): Int = {
    val g = BigInt(a).gcd(BigInt(b))
    (BigInt(a) / g * BigInt(b)).toInt
  }
}

case class IntTruncateReducer(width: Int) extends Reducer[Int, Int] {
  override def reduce(value: Int): Int = value - (((value % width) + width) % width)
  override def resultType(): DataType = IntegerType
  override def displayName(): String = s"truncate($width)"
}
