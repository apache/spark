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

package org.apache.spark.sql.catalyst.util

import java.time._
import java.time.chrono.IsoChronology
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, ResolverStyle}
import java.time.temporal.{ChronoField, TemporalAccessor, TemporalQueries}
import java.util
import java.util.{Collections, Date, Locale}

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper._
import org.apache.spark.sql.errors.ExecutionErrors
import org.apache.spark.sql.internal.LegacyBehaviorPolicy._
import org.apache.spark.sql.internal.SqlApiConf

trait DateTimeFormatterHelper {
  private def getOrDefault(accessor: TemporalAccessor, field: ChronoField, default: Int): Int = {
    if (accessor.isSupported(field)) {
      accessor.get(field)
    } else {
      default
    }
  }

  private def verifyLocalDate(
      accessor: TemporalAccessor,
      field: ChronoField,
      candidate: LocalDate): Unit = {
    if (accessor.isSupported(field)) {
      val actual = accessor.get(field)
      val expected = candidate.get(field)
      if (actual != expected) {
        throw ExecutionErrors.fieldDiffersFromDerivedLocalDateError(
          field,
          actual,
          expected,
          candidate)
      }
    }
  }

  protected def toLocalDate(accessor: TemporalAccessor): LocalDate = {
    val localDate = accessor.query(TemporalQueries.localDate())
    // If all the date fields are specified, return the local date directly.
    if (localDate != null) return localDate

    // Users may want to parse only a few datetime fields from a string and extract these fields
    // later, and we should provide default values for missing fields.
    // To be compatible with Spark 2.4, we pick 1970 as the default value of year.
    val year = getOrDefault(accessor, ChronoField.YEAR, 1970)
    if (accessor.isSupported(ChronoField.DAY_OF_YEAR)) {
      val dayOfYear = accessor.get(ChronoField.DAY_OF_YEAR)
      val date = LocalDate.ofYearDay(year, dayOfYear)
      verifyLocalDate(accessor, ChronoField.MONTH_OF_YEAR, date)
      verifyLocalDate(accessor, ChronoField.DAY_OF_MONTH, date)
      date
    } else {
      val month = getOrDefault(accessor, ChronoField.MONTH_OF_YEAR, 1)
      val day = getOrDefault(accessor, ChronoField.DAY_OF_MONTH, 1)
      LocalDate.of(year, month, day)
    }
  }

  protected def toLocalTime(accessor: TemporalAccessor): LocalTime = {
    val localTime = accessor.query(TemporalQueries.localTime())
    // If all the time fields are specified, return the local time directly.
    if (localTime != null) return localTime

    val hour = if (accessor.isSupported(ChronoField.HOUR_OF_DAY)) {
      accessor.get(ChronoField.HOUR_OF_DAY)
    } else if (accessor.isSupported(ChronoField.HOUR_OF_AMPM)) {
      // When we reach here, it means am/pm is not specified. Here we assume it's am.
      // All of CLOCK_HOUR_OF_AMPM(h)/HOUR_OF_DAY(H)/CLOCK_HOUR_OF_DAY(k)/HOUR_OF_AMPM(K) will
      // be resolved to HOUR_OF_AMPM here, we do not need to handle them separately
      accessor.get(ChronoField.HOUR_OF_AMPM)
    } else if (accessor.isSupported(ChronoField.AMPM_OF_DAY) &&
      accessor.get(ChronoField.AMPM_OF_DAY) == 1) {
      // When reach here, the `hour` part is missing, and PM is specified.
      // None of CLOCK_HOUR_OF_AMPM(h)/HOUR_OF_DAY(H)/CLOCK_HOUR_OF_DAY(k)/HOUR_OF_AMPM(K) is
      // specified
      12
    } else {
      0
    }
    val minute = getOrDefault(accessor, ChronoField.MINUTE_OF_HOUR, 0)
    val second = getOrDefault(accessor, ChronoField.SECOND_OF_MINUTE, 0)
    val nanoSecond = getOrDefault(accessor, ChronoField.NANO_OF_SECOND, 0)
    LocalTime.of(hour, minute, second, nanoSecond)
  }

  // Converts the parsed temporal object to ZonedDateTime. It sets time components to zeros
  // if they does not exist in the parsed object.
  protected def toZonedDateTime(accessor: TemporalAccessor, zoneId: ZoneId): ZonedDateTime = {
    val localDate = toLocalDate(accessor)
    val localTime = toLocalTime(accessor)
    ZonedDateTime.of(localDate, localTime, zoneId)
  }

  // Gets a formatter from the cache or creates new one. The buildFormatter method can be called
  // a few times with the same parameters in parallel if the cache does not contain values
  // associated to those parameters. Since the formatter is immutable, it does not matter.
  // In this way, synchronised is intentionally omitted in this method to make parallel calls
  // less synchronised.
  // The Cache.get method is not used here to avoid creation of additional instances of Callable.
  protected def getOrCreateFormatter(
      pattern: String,
      locale: Locale,
      isParsing: Boolean): DateTimeFormatter = {
    val newPattern = convertIncompatiblePattern(pattern, isParsing)
    val useVarLen = isParsing && newPattern.contains('S')
    val key = (newPattern, locale, useVarLen)
    cache.computeIfAbsent(key, _ => buildFormatter(newPattern, locale, useVarLen))
  }

  private def needConvertToSparkUpgradeException(e: Throwable): Boolean = e match {
    case _: DateTimeException if SqlApiConf.get.legacyTimeParserPolicy == EXCEPTION => true
    case _ => false
  }
  // When legacy time parser policy set to EXCEPTION, check whether we will get different results
  // between legacy parser and new parser. If new parser fails but legacy parser works, throw a
  // SparkUpgradeException. On the contrary, if the legacy policy set to CORRECTED,
  // DateTimeParseException will address by the caller side.
  protected def checkParsedDiff[T](
      s: String,
      legacyParseFunc: String => T): PartialFunction[Throwable, T] = {
    case e if needConvertToSparkUpgradeException(e) =>
      try {
        legacyParseFunc(s)
      } catch {
        case _: Throwable => throw e
      }
      throw ExecutionErrors.failToParseDateTimeInNewParserError(s, e)
  }

  // When legacy time parser policy set to EXCEPTION, check whether we will get different results
  // between legacy formatter and new formatter. If new formatter fails but legacy formatter works,
  // throw a SparkUpgradeException. On the contrary, if the legacy policy set to CORRECTED,
  // DateTimeParseException will address by the caller side.
  protected def checkFormattedDiff[T <: Date](
      d: T,
      legacyFormatFunc: T => String): PartialFunction[Throwable, String] = {
    case e if needConvertToSparkUpgradeException(e) =>
      val resultCandidate =
        try {
          legacyFormatFunc(d)
        } catch {
          case _: Throwable => throw e
        }
      throw ExecutionErrors.failToParseDateTimeInNewParserError(resultCandidate, e)
  }

  /**
   * When the new DateTimeFormatter failed to initialize because of invalid datetime pattern, it
   * will throw IllegalArgumentException. If the pattern can be recognized by the legacy formatter
   * it will raise SparkUpgradeException to tell users to restore the previous behavior via LEGACY
   * policy or follow our guide to correct their pattern. Otherwise, the original
   * IllegalArgumentException will be thrown.
   *
   * @param pattern
   *   the date time pattern
   * @param tryLegacyFormatter
   *   a func to capture exception, identically which forces a legacy datetime formatter to be
   *   initialized
   */
  protected def checkLegacyFormatter(
      pattern: String,
      tryLegacyFormatter: => Unit): PartialFunction[Throwable, DateTimeFormatter] = {
    case e: IllegalArgumentException =>
      try {
        tryLegacyFormatter
      } catch {
        case _: Throwable => throw e
      }
      throw ExecutionErrors.failToRecognizePatternAfterUpgradeError(pattern, e)
  }

  protected def checkInvalidPattern(pattern: String): PartialFunction[Throwable, Nothing] = {
    case e: IllegalArgumentException =>
      throw ExecutionErrors.failToRecognizePatternError(pattern, e)
  }
}

private object DateTimeFormatterHelper {
  private type CacheKey = (String, Locale, Boolean)
  private val cache: util.Map[CacheKey, DateTimeFormatter] =
    Collections.synchronizedMap(new util.LinkedHashMap[CacheKey, DateTimeFormatter] {
      override def removeEldestEntry(
          eldest: util.Map.Entry[CacheKey, DateTimeFormatter]): Boolean = {
        size() > 128
      }
    })

  final val extractor = "^([^S]*)(S*)(.*)$".r

  def createBuilder(): DateTimeFormatterBuilder = {
    new DateTimeFormatterBuilder().parseCaseInsensitive()
  }

  def toFormatter(builder: DateTimeFormatterBuilder, locale: Locale): DateTimeFormatter = {
    builder
      .toFormatter(locale)
      .withChronology(IsoChronology.INSTANCE)
      .withResolverStyle(ResolverStyle.STRICT)
  }

  /**
   * Building a formatter for parsing seconds fraction with variable length
   */
  def createBuilderWithVarLengthSecondFraction(pattern: String): DateTimeFormatterBuilder = {
    val builder = createBuilder()
    pattern.split("'").zipWithIndex.foreach {
      // Split string starting with the regex itself which is `'` here will produce an extra empty
      // string at res(0). So when the first element here is empty string we do not need append `'`
      // literal to the DateTimeFormatterBuilder.
      case ("", idx) if idx != 0 => builder.appendLiteral("'")
      case (patternPart, idx) if idx % 2 == 0 =>
        var rest = patternPart
        while (rest.nonEmpty) {
          rest match {
            case extractor(prefix, secondFraction, suffix) =>
              builder.appendPattern(prefix)
              if (secondFraction.nonEmpty) {
                builder
                  .appendFraction(ChronoField.NANO_OF_SECOND, 1, secondFraction.length, false)
              }
              rest = suffix
            case _ =>
              throw new SparkIllegalArgumentException(
                errorClass = "INVALID_DATETIME_PATTERN.SECONDS_FRACTION",
                messageParameters = Map("pattern" -> pattern))
          }
        }
      case (patternPart, _) => builder.appendLiteral(patternPart)
    }
    builder
  }

  def buildFormatter(
      pattern: String,
      locale: Locale,
      varLenEnabled: Boolean): DateTimeFormatter = {
    val builder = if (varLenEnabled) {
      createBuilderWithVarLengthSecondFraction(pattern)
    } else {
      createBuilder().appendPattern(pattern)
    }
    toFormatter(builder, locale)
  }

  lazy val fractionFormatter: DateTimeFormatter = {
    val builder = createBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .appendLiteral(' ')
      .appendValue(ChronoField.HOUR_OF_DAY, 2)
      .appendLiteral(':')
      .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
      .appendLiteral(':')
      .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
      .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
    toFormatter(builder, TimestampFormatter.defaultLocale)
  }

  // SPARK-31892: The week-based date fields are rarely used and really confusing for parsing values
  // to datetime, especially when they are mixed with other non-week-based ones;
  // SPARK-31879: It's also difficult for us to restore the behavior of week-based date fields
  // formatting, in DateTimeFormatter the first day of week for week-based date fields become
  // localized, for the default Locale.US, it uses Sunday as the first day of week, while in Spark
  // 2.4, the SimpleDateFormat uses Monday as the first day of week.
  final val weekBasedLetters = Set('Y', 'W', 'w', 'u', 'e', 'c')
  // SPARK-36970: `select date_format('2018-11-17 13:33:33.333', 'B')` failed with Java 8,
  // but use Java 17 will return `in the afternoon` because 'B' is used to represent
  // `Pattern letters to output a day period` in Java 17. So there manual disabled `B` for
  // compatibility with Java 8 behavior.
  final val unsupportedLetters = Set('A', 'B', 'n', 'N', 'p')
  // The quarter fields will also be parsed strangely, e.g. when the pattern contains `yMd` and can
  // be directly resolved then the `q` do check for whether the month is valid, but if the date
  // fields is incomplete, e.g. `yM`, the checking will be bypassed.
  final val unsupportedLettersForParsing = Set('E', 'F', 'q', 'Q')
  final val unsupportedPatternLengths = {
    // SPARK-31771: Disable Narrow-form TextStyle to avoid silent data change, as it is Full-form in
    // 2.4
    Seq("G", "M", "L", "E", "Q", "q").map(_ * 5) ++
      // SPARK-31867: Disable year pattern longer than 10 which will cause Java time library throw
      // unchecked `ArrayIndexOutOfBoundsException` by the `NumberPrinterParser` for formatting. It
      // makes the call side difficult to handle exceptions and easily leads to silent data change
      // because of the exceptions being suppressed.
      // SPARK-32424: The max year that we can actually handle is 6 digits, otherwise, it will
      // overflow
      Seq("y").map(_ * 7)
  }.toSet

  /**
   * In Spark 3.0, we switch to the Proleptic Gregorian calendar and use DateTimeFormatter for
   * parsing/formatting datetime values. The pattern string is incompatible with the one defined
   * by SimpleDateFormat in Spark 2.4 and earlier. This function converts all incompatible pattern
   * for the new parser in Spark 3.0. See more details in SPARK-31030.
   * @param pattern
   *   The input pattern.
   * @return
   *   The pattern for new parser
   */
  def convertIncompatiblePattern(pattern: String, isParsing: Boolean): String = {
    val eraDesignatorContained =
      pattern.split("'").zipWithIndex.exists { case (patternPart, index) =>
        // Text can be quoted using single quotes, we only check the non-quote parts.
        index % 2 == 0 && patternPart.contains("G")
      }
    (pattern + " ")
      .split("'")
      .zipWithIndex
      .map { case (patternPart, index) =>
        if (index % 2 == 0) {
          for (c <- patternPart if weekBasedLetters.contains(c)) {
            throw new SparkIllegalArgumentException(
              errorClass = "INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_WEEK_BASED_PATTERN",
              messageParameters = Map("c" -> c.toString))
          }
          for (c <- patternPart if unsupportedLetters.contains(c) ||
              (isParsing && unsupportedLettersForParsing.contains(c))) {
            throw new SparkIllegalArgumentException(
              errorClass = "INVALID_DATETIME_PATTERN.ILLEGAL_CHARACTER",
              messageParameters = Map("c" -> c.toString, "pattern" -> pattern))
          }
          for (style <- unsupportedPatternLengths if patternPart.contains(style)) {
            throw new SparkIllegalArgumentException(
              errorClass = "INVALID_DATETIME_PATTERN.LENGTH",
              messageParameters = Map("pattern" -> style))
          }
          // In DateTimeFormatter, 'u' supports negative years. We substitute 'y' to 'u' here for
          // keeping the support in Spark 3.0. If parse failed in Spark 3.0, fall back to 'y'.
          // We only do this substitution when there is no era designator found in the pattern.
          if (!eraDesignatorContained) {
            patternPart.replace("y", "u")
          } else {
            patternPart
          }
        } else {
          patternPart
        }
      }
      .mkString("'")
      .stripSuffix(" ")
  }
}
