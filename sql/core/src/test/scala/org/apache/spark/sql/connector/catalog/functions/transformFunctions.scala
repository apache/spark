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
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.expressions.{Literal, LiteralValue}
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

/**
 * A parameterless transform that implements only the generalized overload and, as a migration
 * shim, answers a parameterless pair through the parameterless overload.
 */
object GeneralizedShimFunction extends ScalarFunction[Int] with ReducibleFunction[Int, Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "generalized_shim"
  override def canonicalName(): String = name()
  override def produceResult(input: InternalRow): Int = input.getInt(0)

  override def reducer(
      thisParams: Array[Literal[_]],
      otherFunc: ReducibleFunction[_, _],
      otherParams: Array[Literal[_]]): Reducer[Int, Int] = {
    if (thisParams.isEmpty && otherParams.isEmpty) reducer(otherFunc) else null
  }
}

/**
 * A `bucket` like [[BucketFunction]] whose generalized reducer throws when asked from the coarser
 * side, a natural guard for an implementor who expects to be asked only from the finer one.
 */
object DirectionalBucketFunction extends ScalarFunction[Int] with ReducibleFunction[Int, Int] {
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
    val thisNumBuckets = thisParams(0).value().asInstanceOf[Int]
    val otherNumBuckets = otherParams(0).value().asInstanceOf[Int]
    require(thisNumBuckets >= otherNumBuckets, "asked from the coarser side")
    BucketFunction.gcdReducer(thisNumBuckets, otherNumBuckets)
  }
}

object UnboundDirectionalBucketFunction extends UnboundFunction {
  override def bind(inputType: StructType): BoundFunction = DirectionalBucketFunction
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

    if (otherFunc == BucketFunction && thisParams.length == 1 && otherParams.length == 1) {
      val thisNumBuckets = thisParams(0).value().asInstanceOf[Int]
      val otherNumBuckets = otherParams(0).value().asInstanceOf[Int]
      gcdReducer(thisNumBuckets, otherNumBuckets)
    } else {
      null
    }
  }

  /**
   * The reducer from `thisNumBuckets` buckets onto the gcd of the two bucket counts, or null when
   * that gcd is 1 or `thisNumBuckets` itself. The bucket fixtures below share it.
   */
  def gcdReducer(thisNumBuckets: Int, otherNumBuckets: Int): Reducer[Int, Int] = {
    val gcd = BigInt(thisNumBuckets).gcd(BigInt(otherNumBuckets)).toInt
    if (gcd > 1 && gcd != thisNumBuckets) BucketReducer(gcd) else null
  }
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
 * Used to verify that Spark's dispatch falls back to the deprecated int-based API when the new
 * method is not implemented (its default throws), so legacy implementations continue to work.
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
      BucketFunction.gcdReducer(thisNumBuckets, otherNumBuckets)
    } else {
      null
    }
  }
}

object UnboundLegacyBucketFunction extends UnboundFunction {
  override def bind(inputType: StructType): BoundFunction = LegacyBucketFunction
  override def description(): String = name()
  override def name(): String = "bucket"
}

/**
 * Like [[LegacyBucketFunction]], but keeps `canonicalName()`'s default, a fresh random name on
 * every call, so only function equality says that two transforms are of one function.
 */
object DefaultNameLegacyBucketFunction extends ScalarFunction[Int]
    with ReducibleFunction[Int, Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType, LongType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "bucket"
  override def produceResult(input: InternalRow): Int = {
    Math.floorMod(input.getLong(1), input.getInt(0))
  }

  override def reducer(
      thisNumBuckets: Int,
      otherFunc: ReducibleFunction[_, _],
      otherNumBuckets: Int): Reducer[Int, Int] = {
    if (otherFunc == this) BucketFunction.gcdReducer(thisNumBuckets, otherNumBuckets) else null
  }
}

object UnboundDefaultNameLegacyBucketFunction extends UnboundFunction {
  override def bind(inputType: StructType): BoundFunction = DefaultNameLegacyBucketFunction
  override def description(): String = name()
  override def name(): String = "bucket"
}

/**
 * A non-bucket function with a single int parameter in the first position, the same argument
 * layout as `bucket(n, col)`.
 */
object LiteralFirstIntFunction extends ScalarFunction[Int] with ReducibleFunction[Int, Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType, LongType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "literal_first_int"
  override def canonicalName(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): Int = input.getLong(1).toInt
}

/**
 * A bucket function that implements only the deprecated `reducer(int, func, int)`, declaring
 * `declared` as its input types: a count type other than `INT`, or the column alone. The deprecated
 * overload gets the count as an `int` whatever is declared.
 */
class LegacyBucketDeclaring(declared: Array[DataType])
    extends ScalarFunction[Int] with ReducibleFunction[Int, Int] {
  override def inputTypes(): Array[DataType] = declared
  override def resultType(): DataType = IntegerType
  override def name(): String = "legacy_bucket_declaring"
  override def canonicalName(): String = name()
  override def produceResult(input: InternalRow): Int = 0

  override def reducer(
      thisNumBuckets: Int,
      otherFunc: ReducibleFunction[_, _],
      otherNumBuckets: Int): Reducer[Int, Int] = {
    if (otherFunc == this) BucketFunction.gcdReducer(thisNumBuckets, otherNumBuckets) else null
  }
}

/**
 * A `truncate`-shaped function, width last, that inherits a bucket-style implementation of the
 * deprecated `reducer(int, func, int)` and does not check what the other function is.
 */
object BucketLogicTruncateFunction extends ScalarFunction[Int] with ReducibleFunction[Int, Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType, IntegerType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "bucket_logic_truncate"
  override def canonicalName(): String = name()
  override def produceResult(input: InternalRow): Int = input.getInt(0)

  override def reducer(
      thisNumBuckets: Int,
      otherFunc: ReducibleFunction[_, _],
      otherNumBuckets: Int): Reducer[Int, Int] = {
    BucketFunction.gcdReducer(thisNumBuckets, otherNumBuckets)
  }
}

/**
 * A bucket whose generalized reducer overwrites the parameter arrays it is handed, then reduces by
 * gcd. A later call must still see the transform's own parameters.
 */
object ArrayMutatingBucketFunction extends ScalarFunction[Int] with ReducibleFunction[Int, Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType, LongType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "array_mutating_bucket"
  override def canonicalName(): String = name()
  override def produceResult(input: InternalRow): Int = 0

  override def reducer(
      thisParams: Array[Literal[_]],
      otherFunc: ReducibleFunction[_, _],
      otherParams: Array[Literal[_]]): Reducer[Int, Int] = {
    val thisNumBuckets = thisParams(0).value().asInstanceOf[Int]
    val otherNumBuckets = otherParams(0).value().asInstanceOf[Int]
    thisParams(0) = LiteralValue(1, IntegerType)
    otherParams(0) = LiteralValue(1, IntegerType)
    BucketFunction.gcdReducer(thisNumBuckets, otherNumBuckets)
  }
}

/**
 * A bucket function that implements BOTH reducer overloads: the deprecated `reducer(int, ..., int)`
 * always returns null (not reducible via the old API), while the new `reducer(Literal[], ...)`
 * returns a GCD-based reducer. Used to verify that the dispatch uses the generalized overload and
 * never consults the deprecated one.
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
      BucketFunction.gcdReducer(thisNumBuckets, otherNumBuckets)
    } else {
      null
    }
  }
}

/**
 * A function with a complex (ArrayType) literal parameter. Its generalized reducer returns a valid
 * reducer unconditionally, so a test can prove the dispatch refuses to invoke it for a non-scalar
 * literal param (rather than the call happening to fail on a cast).
 */
object ArrayParamFunction extends ScalarFunction[Int] with ReducibleFunction[Int, Int] {
  override def inputTypes(): Array[DataType] = Array(ArrayType(IntegerType), LongType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "array_param"
  override def canonicalName(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): Int = input.getInt(1)

  override def reducer(
      thisParams: Array[Literal[_]],
      otherFunc: ReducibleFunction[_, _],
      otherParams: Array[Literal[_]]): Reducer[Int, Int] = BucketReducer(1)
}

object UnboundNonScalarBucketFunction extends UnboundFunction {
  override def bind(inputType: StructType): BoundFunction = NonScalarBucketFunction
  override def description(): String = name()
  override def name(): String = "bucket"
}

object UnboundInheritedBucketFunction extends UnboundFunction {
  override def bind(inputType: StructType): BoundFunction = InheritedBucketFunction
  override def description(): String = name()
  override def name(): String = "bucket"
}

abstract class BucketFunctionBase extends ScalarFunction[Int] {
  override def produceResult(input: InternalRow): Int = {
    Math.floorMod(input.getLong(1), input.getInt(0))
  }
}

// A `bucket` ScalarFunction that inherits `produceResult`, which Spark does not find when it
// resolves the function, so Spark cannot evaluate it either.
object InheritedBucketFunction extends BucketFunctionBase {
  override def inputTypes(): Array[DataType] = Array(IntegerType, LongType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "bucket"
  override def canonicalName(): String = name()
  override def toString: String = name()
}

object UnboundIntBucketFunction extends UnboundFunction {
  override def bind(inputType: StructType): BoundFunction = IntBucketFunction
  override def description(): String = name()
  override def name(): String = "bucket"
}

// A `bucket` that declares `INT` for the value whatever it is bound to, with a magic `invoke`, as
// Apache Iceberg's does for a `TINYINT`, `SMALLINT` or `INT` column. Consistent with
// BucketTransform in InMemoryBaseTable.scala.
object IntBucketFunction extends ScalarFunction[Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType, IntegerType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "bucket"
  override def canonicalName(): String = name()
  override def toString: String = name()
  def invoke(numBuckets: Int, value: Int): Int = Math.floorMod(value, numBuckets)
}

object UnboundDoubleBucketFunction extends UnboundFunction {
  override def bind(inputType: StructType): BoundFunction = DoubleBucketFunction
  override def description(): String = name()
  override def name(): String = "bucket"
}

// A `bucket` that declares `DOUBLE` for the value whatever it is bound to, with a magic `invoke`
// that Java widens an `INT` into. Consistent with BucketTransform in InMemoryBaseTable.scala.
object DoubleBucketFunction extends ScalarFunction[Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType, DoubleType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "bucket"
  override def canonicalName(): String = name()
  override def toString: String = name()
  def invoke(numBuckets: Int, value: Double): Int =
    Math.floorMod(value.toLong, numBuckets.toLong).toInt
}

object UnboundStringBucketFunction extends UnboundFunction {
  override def bind(inputType: StructType): BoundFunction = StringBucketFunction
  override def description(): String = name()
  override def name(): String = "bucket"
}

// A `bucket` that declares a plain `STRING` for the value whatever it is bound to. Consistent with
// BucketTransform in InMemoryBaseTable.scala, which hashes any string type.
object StringBucketFunction extends ScalarFunction[Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType, StringType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "bucket"
  override def canonicalName(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): Int =
    Math.floorMod(input.getUTF8String(1).hashCode.toLong, input.getInt(0).toLong).toInt
}

class UnboundPhysicalDaysFunction(bound: BoundFunction) extends UnboundFunction {
  override def bind(inputType: StructType): BoundFunction = bound
  override def description(): String = name()
  override def name(): String = "days"
}

// `days` that declares `INT` for a `DATE` column, which is held as the days since the epoch.
// Consistent with DaysTransform in InMemoryBaseTable.scala.
object IntDaysFunction extends ScalarFunction[Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType)
  override def resultType(): DataType = DateType
  override def name(): String = "days"
  override def canonicalName(): String = name()
  override def toString: String = name()
  def invoke(days: Int): Int = days
}

// `days` that declares `BIGINT` for a `TIMESTAMP` column, which is held as the micros since the
// epoch. Computes the days as DaysTransform in InMemoryBaseTable.scala does, before 1970 too.
object LongDaysFunction extends ScalarFunction[Int] {
  override def inputTypes(): Array[DataType] = Array(LongType)
  override def resultType(): DataType = DateType
  override def name(): String = "days"
  override def canonicalName(): String = name()
  override def toString: String = name()
  def invoke(micros: Long): Int =
    ChronoUnit.DAYS.between(Instant.EPOCH, DateTimeUtils.microsToInstant(micros)).toInt
}

object UnboundDateYearsFunction extends UnboundFunction {
  override def bind(inputType: StructType): BoundFunction = DateYearsFunction
  override def description(): String = name()
  override def name(): String = "years"
}

// A `years` that declares `DATE`, with a magic `invoke` over the days since the epoch, as Apache
// Iceberg's does for a `DATE` column. Consistent with YearsTransform in InMemoryBaseTable.scala.
object DateYearsFunction extends ScalarFunction[Int] {
  private val epoch = LocalDate.of(1970, 1, 1)
  override def inputTypes(): Array[DataType] = Array(DateType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "years"
  override def canonicalName(): String = name()
  override def toString: String = name()
  def invoke(days: Int): Int =
    ChronoUnit.YEARS.between(epoch, DateTimeUtils.daysToLocalDate(days)).toInt
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

object UnboundSignedZerosFunction extends UnboundFunction {
  override def bind(inputType: StructType): BoundFunction = {
    if (inputType.size == 1 && inputType.head.dataType == LongType) SignedZerosFunction
    else throw new UnsupportedOperationException("'signed_zeros' only takes a long as input type")
  }
  override def description(): String = name()
  override def name(): String = "signed_zeros"
}

// A transform whose result type (DoubleType) differs from its input type (LongType), mapping
// 1 to -0.0 and 2 to 0.0 so that two distinct inputs produce partition keys that are equal
// under SQL semantics but distinct bitwise. The result should be consistent with the
// "signed_zeros" NamedTransform defined at InMemoryBaseTable.scala.
object SignedZerosFunction extends ScalarFunction[Double] {
  override def inputTypes(): Array[DataType] = Array(LongType)
  override def resultType(): DataType = DoubleType
  override def name(): String = "signed_zeros"
  override def canonicalName(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): Double = input.getLong(0) match {
    case 1L => -0.0d
    case 2L => 0.0d
    case v => v.toDouble
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
  override def canonicalName(): String = "truncate(string)"
  override def toString: String = name()
  override def produceResult(input: InternalRow): UTF8String = {
    // A one-side shuffle onto a truncate layout evaluates this on every row, null keys included.
    if (input.isNullAt(0)) return null
    val str = input.getUTF8String(0)
    val width = input.getInt(1)
    str.substring(0, width)
  }

  override def reducer(
      thisParams: Array[Literal[_]],
      otherFunc: ReducibleFunction[_, _],
      otherParams: Array[Literal[_]]): Reducer[UTF8String, UTF8String] = {

    if (otherFunc == TruncateFunction && thisParams.length == 1 && otherParams.length == 1) {
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
    if (value == null) null else value.substring(0, width)
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
    extends ScalarFunction[Integer]
    with ReducibleFunction[Integer, Integer] {
  override def inputTypes(): Array[DataType] = Array(IntegerType, IntegerType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "truncate"
  override def canonicalName(): String = "truncate(int)"
  override def toString: String = name()
  // A one-side shuffle onto a truncate layout evaluates this on every row, null keys included,
  // and the row Spark passes is reused, so a null must be read as null, not as the last value.
  // The result is boxed so that it can be null.
  override def produceResult(input: InternalRow): Integer =
    if (input.isNullAt(0)) null else truncate(input.getInt(0), input.getInt(1))

  /** `value` snapped down to a multiple of `width`, as the Iceberg spec truncates an integer. */
  def truncate(value: Int, width: Int): Int = value - (((value % width) + width) % width)

  override def reducer(
      thisParams: Array[Literal[_]],
      otherFunc: ReducibleFunction[_, _],
      otherParams: Array[Literal[_]]): Reducer[Integer, Integer] = {
    if (otherFunc == IntegerTruncateFunction && thisParams.length == 1 && otherParams.length == 1) {
      val thisWidth = thisParams(0).value().asInstanceOf[Int]
      val otherWidth = otherParams(0).value().asInstanceOf[Int]
      // Only the finer side reduces; if the common grid is `thisWidth`, this side already is it.
      lcm(thisWidth, otherWidth) match {
        case Some(common) if common != thisWidth => return IntTruncateReducer(common)
        case _ =>
      }
    }
    null
  }

  /** The common grid, or None when it does not fit an Int; then the pair is not reducible. */
  private def lcm(a: Int, b: Int): Option[Int] = {
    val common = BigInt(a) / BigInt(a).gcd(BigInt(b)) * BigInt(b)
    if (common.isValidInt) Some(common.toInt) else None
  }
}

case class IntTruncateReducer(width: Int) extends Reducer[Integer, Integer] {
  override def reduce(value: Integer): Integer =
    if (value == null) null else IntegerTruncateFunction.truncate(value, width)
  override def resultType(): DataType = IntegerType
  override def displayName(): String = s"truncate($width)"
}

/**
 * A transform whose reducer is defined across a zero-parameter vs one-parameter shape, e.g.
 * `zero_or_one(col)` reducing onto `zero_or_one(col, 2)`. Used to verify the dispatch does not
 * globally require equal child counts before invoking the reducer.
 */
object ZeroOrOneParamFunction extends ScalarFunction[Int] with ReducibleFunction[Int, Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "zero_or_one"
  override def canonicalName(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): Int = input.getInt(0)

  override def reducer(
      thisParams: Array[Literal[_]],
      otherFunc: ReducibleFunction[_, _],
      otherParams: Array[Literal[_]]): Reducer[Int, Int] = {
    if (otherFunc == ZeroOrOneParamFunction && thisParams.length != otherParams.length) {
      BucketReducer(1)
    } else {
      null
    }
  }
}

/**
 * A transform with a `CalendarIntervalType` literal parameter (which is non-complex but not an
 * `AtomicType`). Used to verify such a parameter is not rejected as a complex container before its
 * reducer is consulted.
 */
object IntervalParamFunction extends ScalarFunction[Int] with ReducibleFunction[Int, Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType, CalendarIntervalType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "interval_param"
  override def canonicalName(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): Int = input.getInt(0)

  override def reducer(
      thisParams: Array[Literal[_]],
      otherFunc: ReducibleFunction[_, _],
      otherParams: Array[Literal[_]]): Reducer[Int, Int] = {
    if (otherFunc == IntervalParamFunction) BucketReducer(1) else null
  }
}

/** A user type whose UDT serializes to a struct (an [[InternalRow]]). */
class StructBacked(val n: Int) extends Serializable

/**
 * A UDT whose `sqlType` is a [[StructType]]: a literal of this type carries an [[InternalRow]]
 * value, yet its `dataType` is the UDT, not `StructType`. A check for container types alone would
 * miss it, which is why `literalParamsPassable` also refuses [[UserDefinedType]]s.
 */
class StructBackedUDT extends UserDefinedType[StructBacked] {
  override def sqlType: DataType = StructType(Seq(StructField("n", IntegerType, nullable = false)))
  override def serialize(obj: StructBacked): InternalRow = new GenericInternalRow(Array[Any](obj.n))
  override def deserialize(datum: Any): StructBacked = datum match {
    case row: InternalRow => new StructBacked(row.getInt(0))
  }
  override def userClass: Class[StructBacked] = classOf[StructBacked]
}

/**
 * A transform whose declared input type at the literal position is a UDT ([[StructBackedUDT]]).
 * Used to make `literalParamsPassable`' UDT clause load-bearing: a UDT-over-struct literal matches
 * the declared input type (so `literalParamsMatchInputTypes` passes), yet its value is an
 * [[InternalRow]], so only that clause rejects it. The reducer returns unconditionally, so reaching
 * it at all is the leak.
 */
object UdtParamFunction extends ScalarFunction[Int] with ReducibleFunction[Int, Int] {
  override def inputTypes(): Array[DataType] = Array(new StructBackedUDT, LongType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "udt_param"
  override def canonicalName(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): Int = input.getInt(1)

  override def reducer(
      thisParams: Array[Literal[_]],
      otherFunc: ReducibleFunction[_, _],
      otherParams: Array[Literal[_]]): Reducer[Int, Int] = BucketReducer(1)
}

/**
 * A string truncate whose reducer reads its width type-tolerantly (via [[Number]], so it accepts a
 * boxed Short or Integer). Used to verify that the literal-param-type gate -- not an incidental
 * ClassCastException in the connector -- is what makes a mismatched-type (e.g. ShortType) width
 * non-reducible. With the gate removed, this reducer WOULD reduce a ShortType-width pair.
 */
object TypeTolerantTruncateFunction
    extends ScalarFunction[UTF8String]
    with ReducibleFunction[UTF8String, UTF8String] {
  override def inputTypes(): Array[DataType] = Array(StringType, IntegerType)
  override def resultType(): DataType = StringType
  override def name(): String = "tolerant_truncate"
  override def canonicalName(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): UTF8String =
    input.getUTF8String(0).substring(0, input.getInt(1))

  override def reducer(
      thisParams: Array[Literal[_]],
      otherFunc: ReducibleFunction[_, _],
      otherParams: Array[Literal[_]]): Reducer[UTF8String, UTF8String] = {
    if (otherFunc == TypeTolerantTruncateFunction &&
        thisParams.length == 1 && otherParams.length == 1) {
      val thisWidth = thisParams(0).value().asInstanceOf[Number].intValue()
      val otherWidth = otherParams(0).value().asInstanceOf[Number].intValue()
      val smaller = math.min(thisWidth, otherWidth)
      if (smaller != thisWidth) return TruncateReducer(smaller)
    }
    null
  }
}

/**
 * A bucket-like function implementing BOTH reducer overloads: the deprecated int overload succeeds
 * (returns a reducer), while the generalized Literal[] overload throws. Used to verify that a
 * throwing generalized overload is final: it is logged, and the deprecated overload is not tried.
 */
object DeprecatedOkGeneralizedThrowsFunction
    extends ScalarFunction[Int] with ReducibleFunction[Int, Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType, LongType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "deprecated_ok_generalized_throws"
  override def canonicalName(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): Int =
    Math.floorMod(input.getLong(1), input.getInt(0))

  override def reducer(
      thisNumBuckets: Int,
      otherFunc: ReducibleFunction[_, _],
      otherNumBuckets: Int): Reducer[Int, Int] =
    if (otherFunc == DeprecatedOkGeneralizedThrowsFunction) BucketReducer(1) else null

  override def reducer(
      thisParams: Array[Literal[_]],
      otherFunc: ReducibleFunction[_, _],
      otherParams: Array[Literal[_]]): Reducer[Int, Int] =
    throw new RuntimeException("boom from generalized overload")
}

/**
 * The mirror of [[DeprecatedOkGeneralizedThrowsFunction]]: the generalized overload returns a
 * reducer, the deprecated overload throws. Used to verify that under generalized-first dispatch the
 * deprecated overload is NOT probed once the generalized one reduced (no "reducer threw" warning).
 */
object GeneralizedOkDeprecatedThrowsFunction
    extends ScalarFunction[Int] with ReducibleFunction[Int, Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType, LongType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "generalized_ok_deprecated_throws"
  override def canonicalName(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): Int =
    Math.floorMod(input.getLong(1), input.getInt(0))

  override def reducer(
      thisNumBuckets: Int,
      otherFunc: ReducibleFunction[_, _],
      otherNumBuckets: Int): Reducer[Int, Int] =
    throw new RuntimeException("boom from deprecated overload")

  override def reducer(
      thisParams: Array[Literal[_]],
      otherFunc: ReducibleFunction[_, _],
      otherParams: Array[Literal[_]]): Reducer[Int, Int] =
    if (otherFunc == GeneralizedOkDeprecatedThrowsFunction) BucketReducer(1) else null
}

/**
 * A legacy connector: implements ONLY the deprecated int reducer (returns a reducer for any int)
 * and not the generalized overload. Used to verify the isSingleInt null-guard under generalized-
 * first dispatch: a typed-null int param must not reach the deprecated fallback, where
 * null.asInstanceOf[Int] would fabricate a 0 this reducer would accept.
 */
object LegacyIntReducerFunction extends ScalarFunction[Int] with ReducibleFunction[Int, Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType, IntegerType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "legacy_int_reducer"
  override def canonicalName(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): Int = input.getInt(1)

  override def reducer(
      thisNumBuckets: Int,
      otherFunc: ReducibleFunction[_, _],
      otherNumBuckets: Int): Reducer[Int, Int] =
    if (otherFunc == LegacyIntReducerFunction) BucketReducer(1) else null
}

/**
 * A dual-API connector whose GENERALIZED reducer returns null (deliberately not reducible) while
 * its DEPRECATED int reducer WOULD reduce a single-int pair (gcd). Used to pin generalized-first
 * dispatch: the generalized null is authoritative, so Spark must NOT fall back to the deprecated
 * overload (which would otherwise co-partition).
 */
object DualApiGeneralizedNullFunction extends ScalarFunction[Int] with ReducibleFunction[Int, Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType, LongType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "dual_api_generalized_null"
  override def canonicalName(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): Int = {
    Math.floorMod(input.getLong(1), input.getInt(0))
  }

  // Generalized API: deliberately not reducible (returns null, not an exception).
  override def reducer(
      thisParams: Array[Literal[_]],
      otherFunc: ReducibleFunction[_, _],
      otherParams: Array[Literal[_]]): Reducer[Int, Int] = null

  // Deprecated int API: WOULD reduce via gcd -- a deprecated-first order would co-partition.
  override def reducer(
      thisNumBuckets: Int,
      otherFunc: ReducibleFunction[_, _],
      otherNumBuckets: Int): Reducer[Int, Int] = {
    if (otherFunc == DualApiGeneralizedNullFunction) {
      BucketFunction.gcdReducer(thisNumBuckets, otherNumBuckets)
    } else {
      null
    }
  }
}
