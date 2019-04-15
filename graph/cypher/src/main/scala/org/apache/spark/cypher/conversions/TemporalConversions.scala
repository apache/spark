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
 *
 */

package org.apache.spark.cypher.conversions

import java.sql.{Date, Timestamp}
import java.time.temporal.ChronoUnit

import org.apache.spark.cypher.udfs.TemporalUdfs
import org.apache.spark.sql.{Column, functions}
import org.apache.spark.unsafe.types.CalendarInterval
import org.opencypher.okapi.api.value.CypherValue.{CypherInteger, CypherMap, CypherString}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, NotImplementedException, UnsupportedOperationException}
import org.opencypher.okapi.impl.temporal.TemporalTypesHelper._
import org.opencypher.okapi.impl.temporal.{Duration, TemporalConstants}
import org.opencypher.okapi.ir.api.expr.{Expr, MapExpression, NullLit, Param}
import org.opencypher.okapi.impl.temporal.{Duration => DurationValue}

import scala.reflect.runtime.universe.TypeTag

object TemporalConversions {

  implicit class RichDuration(duration: Duration) {

    /**
      * Converts the Okapi representation of a duration into the spark representation.
      *
      * @note This conversion is lossy, as the Sparks [[CalendarInterval]] only has a resolution down to microseconds.
      *       Additionally it uses an approximate representation of days.
      */
    def toCalendarInterval: CalendarInterval = {
      if (duration.nanos % 1000 != 0) {
        throw UnsupportedOperationException("Spark does not support durations with nanosecond resolution.")
      }

      val microseconds = duration.nanos / 1000 +
        duration.seconds * CalendarInterval.MICROS_PER_SECOND +
        duration.days * CalendarInterval.MICROS_PER_DAY

      new CalendarInterval(
        duration.months.toInt,
        microseconds
      )
    }
  }

  /**
    * Converts the Spark representation of a duration into the Okapi representation.
    *
    * @note To ensure compatibility with the reverse operation we estimate the number of days from the given seconds.
    */
  implicit class RichCalendarInterval(calendarInterval: CalendarInterval) {
    def toDuration: Duration = {
      val seconds = calendarInterval.microseconds / CalendarInterval.MICROS_PER_SECOND
      val normalizedDays = seconds / (CalendarInterval.MICROS_PER_DAY / CalendarInterval.MICROS_PER_SECOND)
      val normalizedSeconds = seconds % (CalendarInterval.MICROS_PER_DAY / CalendarInterval.MICROS_PER_SECOND)
      val normalizedNanos = calendarInterval.microseconds % CalendarInterval.MICROS_PER_SECOND * 1000

      Duration(months = calendarInterval.months,
        days = normalizedDays,
        seconds = normalizedSeconds,
        nanoseconds = normalizedNanos
      )
    }

    def toJavaDuration: java.time.Duration = {
      val micros = calendarInterval.microseconds +
        (calendarInterval.months * TemporalConstants.AVG_DAYS_PER_MONTH * CalendarInterval.MICROS_PER_DAY).toLong
      java.time.Duration.of(micros, ChronoUnit.MICROS)
    }
  }

  implicit class TemporalExpression(val expr: Expr) extends AnyVal {

    def resolveTimestamp(implicit parameters: CypherMap): Timestamp = {
      expr.resolveTemporalArgument
        .map(parseLocalDateTime)
        .map(java.sql.Timestamp.valueOf)
        .map {
          case ts if ts.getNanos % 1000 == 0 => ts
          case _ => throw IllegalStateException("Spark does not support nanosecond resolution in 'localdatetime'")
        }
        .orNull
    }

    def resolveDate(implicit parameters: CypherMap): Date = {
      expr.resolveTemporalArgument
        .map(parseDate)
        .map(java.sql.Date.valueOf)
        .orNull
    }

    def resolveInterval(implicit parameters: CypherMap): CalendarInterval = {
      expr.resolveTemporalArgument.map {
        case Left(m) => DurationValue(m.mapValues(_.toLong)).toCalendarInterval
        case Right(s) => DurationValue.parse(s).toCalendarInterval
      }.orNull
    }

    def resolveTemporalArgument(implicit parameters: CypherMap): Option[Either[Map[String, Int], String]] = {
      expr match {
        case MapExpression(inner) =>
          val map = inner.map {
            case (key, Param(name)) => key -> (parameters(name) match {
              case CypherString(s) => s.toInt
              case CypherInteger(i) => i.toInt
              case other => throw IllegalArgumentException("A map value of type CypherString or CypherInteger", other)
            })
            case (key, e) =>
              throw NotImplementedException(s"Parsing temporal values is currently only supported for Literal-Maps, got $key -> $e")
          }

          Some(Left(map))

        case Param(name) =>
          val s = parameters(name) match {
            case CypherString(str) => str
            case other => throw IllegalArgumentException(s"Parameter `$name` to be a CypherString", other)
          }

          Some(Right(s))

        case NullLit => None

        case other =>
          throw NotImplementedException(s"Parsing temporal values is currently only supported for Literal-Maps and String literals, got $other")
      }
    }

  }

  def temporalAccessor[I: TypeTag](temporalColumn: Column, accessor: String): Column = {
    accessor.toLowerCase match {
      case "year" => functions.year(temporalColumn)
      case "quarter" => functions.quarter(temporalColumn)
      case "month" => functions.month(temporalColumn)
      case "week" => functions.weekofyear(temporalColumn)
      case "day" => functions.dayofmonth(temporalColumn)
      case "ordinalday" => functions.dayofyear(temporalColumn)
      case "weekyear" => TemporalUdfs.weekYear[I].apply(temporalColumn)
      case "dayofquarter" => TemporalUdfs.dayOfQuarter[I].apply(temporalColumn)
      case "dayofweek" | "weekday" => TemporalUdfs.dayOfWeek[I].apply(temporalColumn)

      case "hour" => functions.hour(temporalColumn)
      case "minute" => functions.minute(temporalColumn)
      case "second" => functions.second(temporalColumn)
      case "millisecond" => TemporalUdfs.milliseconds[I].apply(temporalColumn)
      case "microsecond" => TemporalUdfs.microseconds[I].apply(temporalColumn)
      case other => throw UnsupportedOperationException(s"Unknown Temporal Accessor: $other")
    }
  }
}

