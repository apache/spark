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

import java.math.MathContext
import java.sql.{Date, Timestamp}
import java.time.{Duration, Instant, LocalDate, LocalDateTime, Period, ZoneId}
import java.time.temporal.ChronoUnit

import scala.collection.mutable
import scala.util.{Random, Try}

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DayTimeIntervalType._
import org.apache.spark.sql.types.YearMonthIntervalType.YEAR
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.collection.Utils
/**
 * Random data generators for Spark SQL DataTypes. These generators do not generate uniformly random
 * values; instead, they're biased to return "interesting" values (such as maximum / minimum values)
 * with higher probability.
 */
object RandomDataGenerator {

  /**
   * The conditional probability of a non-null value being drawn from a set of "interesting" values
   * instead of being chosen uniformly at random.
   */
  private val PROBABILITY_OF_INTERESTING_VALUE: Float = 0.5f

  /**
   * The probability of the generated value being null
   */
  private val PROBABILITY_OF_NULL: Float = 0.1f

  final val MAX_STR_LEN: Int = 1024
  final val MAX_ARR_SIZE: Int = 128
  final val MAX_MAP_SIZE: Int = 128

  /**
   * Helper function for constructing a biased random number generator which returns "interesting"
   * values with a higher probability.
   */
  private def randomNumeric[T](
      rand: Random,
      uniformRand: Random => T,
      interestingValues: Seq[T]): Some[() => T] = {
    val f = () => {
      if (rand.nextFloat() <= PROBABILITY_OF_INTERESTING_VALUE) {
        interestingValues(rand.nextInt(interestingValues.length))
      } else {
        uniformRand(rand)
      }
    }
    Some(f)
  }

  /**
   * A wrapper of Float.intBitsToFloat to use a unique NaN value for all NaN values.
   * This prevents `checkEvaluationWithUnsafeProjection` from failing due to
   * the difference between `UnsafeRow` binary presentation for NaN.
   * This is visible for testing.
   */
  def intBitsToFloat(bits: Int): Float = {
    val value = java.lang.Float.intBitsToFloat(bits)
    if (value.isNaN) Float.NaN else value
  }

  /**
   * A wrapper of Double.longBitsToDouble to use a unique NaN value for all NaN values.
   * This prevents `checkEvaluationWithUnsafeProjection` from failing due to
   * the difference between `UnsafeRow` binary presentation for NaN.
   * This is visible for testing.
   */
  def longBitsToDouble(bits: Long): Double = {
    val value = java.lang.Double.longBitsToDouble(bits)
    if (value.isNaN) Double.NaN else value
  }

  /**
   * Returns a randomly generated schema, based on the given accepted types.
   *
   * @param numFields the number of fields in this schema
   * @param acceptedTypes types to draw from.
   */
  def randomSchema(rand: Random, numFields: Int, acceptedTypes: Seq[DataType]): StructType = {
    StructType(Seq.tabulate(numFields) { i =>
      val dt = acceptedTypes(rand.nextInt(acceptedTypes.size))
      StructField("col_" + i, dt, nullable = rand.nextBoolean())
    })
  }

  /**
   * Returns a random nested schema. This will randomly generate structs and arrays drawn from
   * acceptedTypes.
   */
  def randomNestedSchema(rand: Random, totalFields: Int, acceptedTypes: Seq[DataType]):
      StructType = {
    val fields = mutable.ArrayBuffer.empty[StructField]
    var i = 0
    var numFields = totalFields
    while (numFields > 0) {
      val v = rand.nextInt(3)
      if (v == 0) {
        // Simple type:
        val dt = acceptedTypes(rand.nextInt(acceptedTypes.size))
        fields += new StructField("col_" + i, dt, rand.nextBoolean())
        numFields -= 1
      } else if (v == 1) {
        // Array
        val dt = acceptedTypes(rand.nextInt(acceptedTypes.size))
        fields += new StructField("col_" + i, ArrayType(dt), rand.nextBoolean())
        numFields -= 1
      } else {
        // Struct
        // TODO: do empty structs make sense?
        val n = Math.max(rand.nextInt(numFields), 1)
        val nested = randomNestedSchema(rand, n, acceptedTypes)
        fields += new StructField("col_" + i, nested, rand.nextBoolean())
        numFields -= n
      }
      i += 1
    }
    StructType(fields.toSeq)
  }

  private def uniformMicrosRand(rand: Random): Long = {
    var milliseconds = rand.nextLong() % 253402329599999L
    // -62135740800000L is the number of milliseconds before January 1, 1970, 00:00:00 GMT
    // for "0001-01-01 00:00:00.000000". We need to find a
    // number that is greater or equals to this number as a valid timestamp value.
    while (milliseconds < -62135740800000L) {
      // 253402329599999L is the number of milliseconds since
      // January 1, 1970, 00:00:00 GMT for "9999-12-31 23:59:59.999999".
      milliseconds = rand.nextLong() % 253402329599999L
    }
    milliseconds * MICROS_PER_MILLIS
  }

  private val specialTs = Seq(
    "0001-01-01 00:00:00", // the fist timestamp of Common Era
    "1582-10-15 23:59:59", // the cutover date from Julian to Gregorian calendar
    "1970-01-01 00:00:00", // the epoch timestamp
    "9999-12-31 23:59:59"  // the last supported timestamp according to SQL standard
  )

  /**
   * Returns a function which generates random values for the given `DataType`, or `None` if no
   * random data generator is defined for that data type. The generated values will use an external
   * representation of the data type; for example, the random generator for `DateType` will return
   * instances of [[java.sql.Date]] and the generator for `StructType` will return a [[Row]].
   * For a `UserDefinedType` for a class X, an instance of class X is returned.
   *
   * @param dataType the type to generate values for
   * @param nullable whether null values should be generated
   * @param rand an optional random number generator
   * @param validJulianDatetime whether to generate dates and timestamps that are valid
   *                            in the Julian calendar.
   * @return a function which can be called to generate random values.
   */
  def forType(
      dataType: DataType,
      nullable: Boolean = true,
      rand: Random = new Random,
      validJulianDatetime: Boolean = false): Option[() => Any] = {
    val valueGenerator: Option[() => Any] = dataType match {
      case StringType => Some(() => rand.nextString(rand.nextInt(MAX_STR_LEN)))
      case BinaryType => Some(() => {
        val arr = new Array[Byte](rand.nextInt(MAX_STR_LEN))
        rand.nextBytes(arr)
        arr
      })
      case BooleanType => Some(() => rand.nextBoolean())
      case DateType =>
        def uniformDaysRand(rand: Random): Int = {
          var milliseconds = rand.nextLong() % 253402329599999L
          // -62135740800000L is the number of milliseconds before January 1, 1970, 00:00:00 GMT
          // for "0001-01-01 00:00:00.000000". We need to find a
          // number that is greater or equals to this number as a valid timestamp value.
          while (milliseconds < -62135740800000L) {
            // 253402329599999L is the number of milliseconds since
            // January 1, 1970, 00:00:00 GMT for "9999-12-31 23:59:59.999999".
            milliseconds = rand.nextLong() % 253402329599999L
          }
          (milliseconds / MILLIS_PER_DAY).toInt
        }
        val specialDates = Seq(
          "0001-01-01", // the fist day of Common Era
          "1582-10-15", // the cutover date from Julian to Gregorian calendar
          "1970-01-01", // the epoch date
          "9999-12-31" // the last supported date according to SQL standard
        )
        def getRandomDate(rand: Random): java.sql.Date = {
          val date = DateTimeUtils.toJavaDate(uniformDaysRand(rand))
          // The generated `date` is based on the hybrid calendar Julian + Gregorian since
          // 1582-10-15 but it should be valid in Proleptic Gregorian calendar too which is used
          // by Spark SQL since version 3.0 (see SPARK-26651). We try to convert `date` to
          // a local date in Proleptic Gregorian calendar to satisfy this requirement. Some
          // years are leap years in Julian calendar but not in Proleptic Gregorian calendar.
          // As the consequence of that, 29 February of such years might not exist in Proleptic
          // Gregorian calendar. When this happens, we shift the date by one day.
          Try { date.toLocalDate; date }.getOrElse(new Date(date.getTime + MILLIS_PER_DAY))
        }
        if (SQLConf.get.getConf(SQLConf.DATETIME_JAVA8API_ENABLED)) {
          randomNumeric[LocalDate](
            rand,
            (rand: Random) => {
              val days = if (validJulianDatetime) {
                DateTimeUtils.fromJavaDate(getRandomDate(rand))
              } else {
                uniformDaysRand(rand)
              }
              LocalDate.ofEpochDay(days)
            },
            specialDates.map(LocalDate.parse))
        } else {
          randomNumeric[java.sql.Date](
            rand,
            getRandomDate,
            specialDates.map(java.sql.Date.valueOf))
        }
      case TimestampType =>
        def getRandomTimestamp(rand: Random): java.sql.Timestamp = {
          // DateTimeUtils.toJavaTimestamp takes microsecond.
          val ts = DateTimeUtils.toJavaTimestamp(uniformMicrosRand(rand))
          // The generated `ts` is based on the hybrid calendar Julian + Gregorian since
          // 1582-10-15 but it should be valid in Proleptic Gregorian calendar too which is used
          // by Spark SQL since version 3.0 (see SPARK-26651). We try to convert `ts` to
          // a local timestamp in Proleptic Gregorian calendar to satisfy this requirement. Some
          // years are leap years in Julian calendar but not in Proleptic Gregorian calendar.
          // As the consequence of that, 29 February of such years might not exist in Proleptic
          // Gregorian calendar. When this happens, we shift the timestamp `ts` by one day.
          Try { ts.toLocalDateTime; ts }.getOrElse(new Timestamp(ts.getTime + MILLIS_PER_DAY))
        }
        if (SQLConf.get.getConf(SQLConf.DATETIME_JAVA8API_ENABLED)) {
          randomNumeric[Instant](
            rand,
            (rand: Random) => {
              val micros = if (validJulianDatetime) {
                DateTimeUtils.fromJavaTimestamp(getRandomTimestamp(rand))
              } else {
                uniformMicrosRand(rand)
              }
              DateTimeUtils.microsToInstant(micros)
            },
            specialTs.map { s =>
              val ldt = LocalDateTime.parse(s.replace(" ", "T"))
              ldt.atZone(ZoneId.systemDefault()).toInstant
            })
        } else {
          randomNumeric[java.sql.Timestamp](
            rand,
            getRandomTimestamp,
            specialTs.map(java.sql.Timestamp.valueOf))
        }
      case TimestampNTZType =>
        randomNumeric[LocalDateTime](
          rand,
          (rand: Random) => {
            DateTimeUtils.microsToLocalDateTime(uniformMicrosRand(rand))
          },
          specialTs.map { s => LocalDateTime.parse(s.replace(" ", "T")) }
        )
      case CalendarIntervalType => Some(() => {
        val months = rand.nextInt(1000)
        val days = rand.nextInt(10000)
        val ns = rand.nextLong()
        new CalendarInterval(months, days, ns)
      })
      case DayTimeIntervalType(_, DAY) =>
        val mircoSeconds = rand.nextLong()
        Some(() => Duration.of(mircoSeconds - mircoSeconds % MICROS_PER_DAY, ChronoUnit.MICROS))
      case DayTimeIntervalType(_, HOUR) =>
        val mircoSeconds = rand.nextLong()
        Some(() => Duration.of(mircoSeconds - mircoSeconds % MICROS_PER_HOUR, ChronoUnit.MICROS))
      case DayTimeIntervalType(_, MINUTE) =>
        val mircoSeconds = rand.nextLong()
        Some(() => Duration.of(mircoSeconds - mircoSeconds % MICROS_PER_MINUTE, ChronoUnit.MICROS))
      case DayTimeIntervalType(_, SECOND) =>
        Some(() => Duration.of(rand.nextLong(), ChronoUnit.MICROS))
      case YearMonthIntervalType(_, YEAR) =>
        Some(() => Period.ofYears(rand.nextInt() / MONTHS_PER_YEAR).normalized())
      case YearMonthIntervalType(_, _) => Some(() => Period.ofMonths(rand.nextInt()).normalized())
      case DecimalType.Fixed(precision, scale) => Some(
        () => BigDecimal.apply(
          rand.nextLong() % math.pow(10, precision).toLong,
          scale,
          new MathContext(precision)).bigDecimal)
      case DoubleType => randomNumeric[Double](
        rand, r => longBitsToDouble(r.nextLong()), Seq(Double.MinValue, Double.MinPositiveValue,
          Double.MaxValue, Double.PositiveInfinity, Double.NegativeInfinity, Double.NaN, 0.0, -0.0))
      case FloatType => randomNumeric[Float](
        rand, r => intBitsToFloat(r.nextInt()), Seq(Float.MinValue, Float.MinPositiveValue,
          Float.MaxValue, Float.PositiveInfinity, Float.NegativeInfinity, Float.NaN, 0.0f, -0.0f))
      case ByteType => randomNumeric[Byte](
        rand, _.nextInt().toByte, Seq(Byte.MinValue, Byte.MaxValue, 0.toByte))
      case IntegerType => randomNumeric[Int](
        rand, _.nextInt(), Seq(Int.MinValue, Int.MaxValue, 0))
      case LongType => randomNumeric[Long](
        rand, _.nextLong(), Seq(Long.MinValue, Long.MaxValue, 0L))
      case ShortType => randomNumeric[Short](
        rand, _.nextInt().toShort, Seq(Short.MinValue, Short.MaxValue, 0.toShort))
      case NullType => Some(() => null)
      case ArrayType(elementType, containsNull) =>
        forType(elementType, nullable = containsNull, rand).map {
          elementGenerator => () => Seq.fill(rand.nextInt(MAX_ARR_SIZE))(elementGenerator())
        }
      case MapType(keyType, valueType, valueContainsNull) =>
        for (
          keyGenerator <- forType(keyType, nullable = false, rand);
          valueGenerator <-
            forType(valueType, nullable = valueContainsNull, rand)
        ) yield {
          () => {
            val length = rand.nextInt(MAX_MAP_SIZE)
            val keys = scala.collection.mutable.HashSet(Seq.fill(length)(keyGenerator()): _*)
            // In case the number of different keys is not enough, set a max iteration to avoid
            // infinite loop.
            var count = 0
            while (keys.size < length && count < MAX_MAP_SIZE) {
              keys += keyGenerator()
              count += 1
            }
            val values = Seq.fill(keys.size)(valueGenerator())
            Utils.toMap(keys, values)
          }
        }
      case StructType(fields) =>
        val maybeFieldGenerators: Seq[Option[() => Any]] = fields.map { field =>
          forType(field.dataType, nullable = field.nullable, rand)
        }
        if (maybeFieldGenerators.forall(_.isDefined)) {
          val fieldGenerators: Seq[() => Any] = maybeFieldGenerators.map(_.get)
          Some(() => Row.fromSeq(fieldGenerators.map(_.apply())))
        } else {
          None
        }
      case udt: UserDefinedType[_] =>
        val maybeSqlTypeGenerator = forType(udt.sqlType, nullable, rand)
        // Because random data generator at here returns scala value, we need to
        // convert it to catalyst value to call udt's deserialize.
        val toCatalystType = CatalystTypeConverters.createToCatalystConverter(udt.sqlType)

        maybeSqlTypeGenerator.map { sqlTypeGenerator =>
          () => {
            val generatedScalaValue = sqlTypeGenerator.apply()
            if (generatedScalaValue == null) {
              null
            } else {
              udt.deserialize(toCatalystType(generatedScalaValue))
            }
          }
        }
      case unsupportedType => None
    }
    // Handle nullability by wrapping the non-null value generator:
    valueGenerator.map { valueGenerator =>
      if (nullable) {
        () => {
          if (rand.nextFloat() <= PROBABILITY_OF_NULL) {
            null
          } else {
            valueGenerator()
          }
        }
      } else {
        valueGenerator
      }
    }
  }

  // Generates a random row for `schema`.
  def randomRow(rand: Random, schema: StructType): Row = {
    val fields = mutable.ArrayBuffer.empty[Any]
    schema.fields.foreach { f =>
      f.dataType match {
        case ArrayType(childType, nullable) =>
          val data = if (f.nullable && rand.nextFloat() <= PROBABILITY_OF_NULL) {
            null
          } else {
            val arr = mutable.ArrayBuffer.empty[Any]
            val n = 1// rand.nextInt(10)
            var i = 0
            val generator = RandomDataGenerator.forType(childType, nullable, rand)
            assert(generator.isDefined, "Unsupported type")
            val gen = generator.get
            while (i < n) {
              arr += gen()
              i += 1
            }
            arr.toSeq
          }
          fields += data
        case StructType(children) =>
          fields += randomRow(rand, StructType(children))
        case _ =>
          val generator = RandomDataGenerator.forType(f.dataType, f.nullable, rand)
          assert(generator.isDefined, "Unsupported type")
          val gen = generator.get
          fields += gen()
      }
    }
    Row.fromSeq(fields.toSeq)
  }
}
