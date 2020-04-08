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

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.util.{Calendar, TimeZone}

import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.catalyst.util.RebaseDateTime._

class RebaseDateTimeSuite extends SparkFunSuite with Matchers with SQLHelper {

  private def parseToJulianMicros(s: String): Long = {
    val ts = Timestamp.valueOf(s)
    val julianMicros = millisToMicros(ts.getTime) +
      ((ts.getNanos / NANOS_PER_MICROS) % MICROS_PER_MILLIS)
    julianMicros
  }

  private def parseToGregMicros(s: String, zoneId: ZoneId): Long = {
    instantToMicros(LocalDateTime.parse(s).atZone(zoneId).toInstant)
  }

  test("rebase julian to/from gregorian micros") {
    outstandingZoneIds.foreach { zid =>
      withDefaultTimeZone(zid) {
        Seq(
          "0001-01-01 01:02:03.654321",
          "1000-01-01 03:02:01.123456",
          "1582-10-04 00:00:00.000000",
          "1582-10-15 00:00:00.999999", // Gregorian cutover day
          "1883-11-10 00:00:00.000000", // America/Los_Angeles -7:52:58 zone offset
          "1883-11-20 00:00:00.000000", // America/Los_Angeles -08:00 zone offset
          "1969-12-31 11:22:33.000100",
          "1970-01-01 00:00:00.000001", // The epoch day
          "2020-03-14 09:33:01.500000").foreach { ts =>
          withClue(s"time zone = ${zid.getId} ts = $ts") {
            val julianMicros = parseToJulianMicros(ts)
            val gregMicros = parseToGregMicros(ts.replace(' ', 'T'), zid)

            assert(rebaseJulianToGregorianMicros(julianMicros) === gregMicros)
            assert(rebaseGregorianToJulianMicros(gregMicros) === julianMicros)
          }
        }
      }
    }
  }

  // millisToDays() and fromJavaDate() are taken from Spark 2.4
  private def millisToDaysLegacy(millisUtc: Long, timeZone: TimeZone): Int = {
    val millisLocal = millisUtc + timeZone.getOffset(millisUtc)
    Math.floor(millisLocal.toDouble / MILLIS_PER_DAY).toInt
  }
  private def fromJavaDateLegacy(date: Date): Int = {
    millisToDaysLegacy(date.getTime, defaultTimeZone())
  }

  test("rebase gregorian to/from julian days") {
    outstandingZoneIds.foreach { zid =>
      withDefaultTimeZone(zid) {
        Seq(
          "0001-01-01",
          "1000-01-01",
          "1582-10-04",
          "1582-10-15", // Gregorian cutover day
          "1883-11-10", // America/Los_Angeles -7:52:58 zone offset
          "1883-11-20", // America/Los_Angeles -08:00 zone offset
          "1969-12-31",
          "1970-01-01", // The epoch day
          "2020-03-14").foreach { date =>
          val julianDays = fromJavaDateLegacy(Date.valueOf(date))
          val gregorianDays = localDateToDays(LocalDate.parse(date))

          assert(rebaseGregorianToJulianDays(gregorianDays) === julianDays)
          assert(rebaseJulianToGregorianDays(julianDays) === gregorianDays)
        }
      }
    }
  }

  test("rebase julian to gregorian date for leap years") {
    outstandingZoneIds.foreach { zid =>
      withDefaultTimeZone(zid) {
        Seq(
          "1000-02-29" -> "1000-03-01",
          "1600-02-29" -> "1600-02-29",
          "1700-02-29" -> "1700-03-01",
          "2000-02-29" -> "2000-02-29").foreach { case (julianDate, gregDate) =>
          withClue(s"tz = ${zid.getId} julian date = $julianDate greg date = $gregDate") {
            val date = Date.valueOf(julianDate)
            val julianDays = fromJavaDateLegacy(date)
            val gregorianDays = localDateToDays(LocalDate.parse(gregDate))

            assert(rebaseJulianToGregorianDays(julianDays) === gregorianDays)
          }
        }
      }
    }
  }

  test("rebase julian to gregorian timestamp for leap years") {
    outstandingZoneIds.foreach { zid =>
      withDefaultTimeZone(zid) {
        Seq(
          "1000-02-29 01:02:03.123456" -> "1000-03-01T01:02:03.123456",
          "1600-02-29 11:12:13.654321" -> "1600-02-29T11:12:13.654321",
          "1700-02-29 21:22:23.000001" -> "1700-03-01T21:22:23.000001",
          "2000-02-29 00:00:00.999999" -> "2000-02-29T00:00:00.999999"
        ).foreach { case (julianTs, gregTs) =>
          withClue(s"tz = ${zid.getId} julian ts = $julianTs greg ts = $gregTs") {
            val julianMicros = parseToJulianMicros(julianTs)
            val gregorianMicros = parseToGregMicros(gregTs, zid)

            assert(rebaseJulianToGregorianMicros(julianMicros) === gregorianMicros)
          }
        }
      }
    }
  }

  test("optimization of days rebasing - Gregorian to Julian") {
    // Rebasing switch days and diffs `gregJulianDiffSwitchDay` and `gregJulianDiffs`
    // in `RebaseDateTime` was generated by using this function.
    def refRebaseGregorianToJulianDays(days: Int): Int = {
      val localDate = LocalDate.ofEpochDay(days)
      val utcCal = new Calendar.Builder()
        // `gregory` is a hybrid calendar that supports both
        // the Julian and Gregorian calendar systems
        .setCalendarType("gregory")
        .setTimeZone(TimeZoneUTC)
        .setDate(localDate.getYear, localDate.getMonthValue - 1, localDate.getDayOfMonth)
        .build()
      Math.toIntExact(Math.floorDiv(utcCal.getTimeInMillis, MILLIS_PER_DAY))
    }

    val start = localDateToDays(LocalDate.of(1, 1, 1))
    val end = localDateToDays(LocalDate.of(2030, 1, 1))

    var days = start
    while (days < end) {
      assert(rebaseGregorianToJulianDays(days) === refRebaseGregorianToJulianDays(days))
      days += 1
    }
  }

  test("optimization of days rebasing - Julian to Gregorian") {
    // Rebasing switch days and diffs `julianGregDiffSwitchDay` and `julianGregDiffs`
    // in `RebaseDateTime` was generated by using this function.
    def refRebaseJulianToGregorianDays(days: Int): Int = {
      val utcCal = new Calendar.Builder()
        // `gregory` is a hybrid calendar that supports both
        // the Julian and Gregorian calendar systems
        .setCalendarType("gregory")
        .setTimeZone(TimeZoneUTC)
        .setInstant(Math.multiplyExact(days, MILLIS_PER_DAY))
        .build()
      val localDate = LocalDate.of(
        utcCal.get(Calendar.YEAR),
        utcCal.get(Calendar.MONTH) + 1,
        // The number of days will be added later to handle non-existing
        // Julian dates in Proleptic Gregorian calendar.
        // For example, 1000-02-29 exists in Julian calendar because 1000
        // is a leap year but it is not a leap year in Gregorian calendar.
        1)
        .plusDays(utcCal.get(Calendar.DAY_OF_MONTH) - 1)
      Math.toIntExact(localDate.toEpochDay)
    }

    val start = rebaseGregorianToJulianDays(
      localDateToDays(LocalDate.of(1, 1, 1)))
    val end = rebaseGregorianToJulianDays(
      localDateToDays(LocalDate.of(2030, 1, 1)))

    var days = start
    while (days < end) {
      assert(rebaseJulianToGregorianDays(days) === refRebaseJulianToGregorianDays(days))
      days += 1
    }
  }

  test("SPARK-31328: rebasing overlapped timestamps during daylight saving time") {
    Seq(
      LA.getId -> Seq("2019-11-03T08:00:00Z", "2019-11-03T08:30:00Z", "2019-11-03T09:00:00Z"),
      "Europe/Amsterdam" ->
        Seq("2019-10-27T00:00:00Z", "2019-10-27T00:30:00Z", "2019-10-27T01:00:00Z")
    ).foreach { case (tz, ts) =>
      withDefaultTimeZone(getZoneId(tz)) {
        ts.foreach { str =>
          val micros = instantToMicros(Instant.parse(str))
          assert(rebaseGregorianToJulianMicros(micros) === micros)
          assert(rebaseJulianToGregorianMicros(micros) === micros)
        }
      }
    }
  }

  test("validate rebase records in JSON files") {
    Seq(
      "gregorian-julian-rebase-micros.json",
      "julian-gregorian-rebase-micros.json").foreach { json =>
      withClue(s"JSON file = $json") {
        val rebaseRecords = loadRebaseRecords(json)
        rebaseRecords.foreach { case (_, rebaseRecord) =>
          assert(rebaseRecord.switches.size === rebaseRecord.diffs.size)
          // Check ascending order of switches values
          assert(rebaseRecord.switches.toSeq === rebaseRecord.switches.sorted.toSeq)
        }
      }
    }
  }

  test("optimization of micros rebasing - Gregorian to Julian") {
    outstandingZoneIds.foreach { zid =>
      withClue(s"zone id = $zid") {
        withDefaultTimeZone(zid) {
          val start = instantToMicros(LocalDateTime.of(1, 1, 1, 0, 0, 0)
            .atZone(zid)
            .toInstant)
          val end = instantToMicros(LocalDateTime.of(2100, 1, 1, 0, 0, 0)
            .atZone(zid)
            .toInstant)
          var micros = start
          do {
            val rebased = rebaseGregorianToJulianMicros(zid, micros)
            val rebasedAndOptimized = rebaseGregorianToJulianMicros(micros)
            assert(rebasedAndOptimized === rebased)
            micros += (MICROS_PER_MONTH * (0.5 + Math.random())).toLong
          } while (micros <= end)
        }
      }
    }
  }

  test("optimization of micros rebasing - Julian to Gregorian") {
    outstandingZoneIds.foreach { zid =>
      withClue(s"zone id = $zid") {
        withDefaultTimeZone(zid) {
          val start = rebaseGregorianToJulianMicros(
            instantToMicros(LocalDateTime.of(1, 1, 1, 0, 0, 0).atZone(zid).toInstant))
          val end = rebaseGregorianToJulianMicros(
            instantToMicros(LocalDateTime.of(2100, 1, 1, 0, 0, 0).atZone(zid).toInstant))
          var micros = start
          do {
            val rebased = rebaseJulianToGregorianMicros(zid, micros)
            val rebasedAndOptimized = rebaseJulianToGregorianMicros(micros)
            assert(rebasedAndOptimized === rebased)
            micros += (MICROS_PER_MONTH * (0.5 + Math.random())).toLong
          } while (micros <= end)
        }
      }
    }
  }

  private def generateRebaseJson(
      adjustFunc: Long => Long,
      rebaseFunc: (ZoneId, Long) => Long,
      dir: String,
      fileName: String): Unit = {
    import java.nio.file.{Files, Paths}
    import java.nio.file.StandardOpenOption

    import scala.collection.mutable.ArrayBuffer

    import com.fasterxml.jackson.databind.ObjectMapper
    import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}

    case class RebaseRecord(tz: String, switches: Array[Long], diffs: Array[Long])

    val result = new ArrayBuffer[RebaseRecord]()
    // The time zones are excluded because:
    // 1. Julian to Gregorian rebasing doesn't match to the opposite rebasing from
    //    Gregorian to Julian rebasing.
    // 2. Linear searching over switch points might be slow.
    // 3. Results after the end time point 2100-01-01 are wrong.
    // See SPARK-31385
    val blacklist = Set("Asia/Tehran", "Iran", "Africa/Casablanca", "Africa/El_Aaiun")
    ALL_TIMEZONES
      .filterNot(zid => blacklist.contains(zid.getId))
      .sortBy(_.getId)
      .foreach { zid =>
      withDefaultTimeZone(zid) {
        val start = adjustFunc(instantToMicros(LocalDateTime.of(1, 1, 1, 0, 0, 0)
          .atZone(zid)
          .toInstant))
        val end = adjustFunc(instantToMicros(LocalDateTime.of(2100, 1, 1, 0, 0, 0)
          .atZone(zid)
          .toInstant))

        var micros = start
        var diff = Long.MaxValue
        val maxStep = DAYS_PER_WEEK * MICROS_PER_DAY
        var step: Long = MICROS_PER_SECOND
        val switches = new ArrayBuffer[Long]()
        val diffs = new ArrayBuffer[Long]()
        while (micros < end) {
          val rebased = rebaseFunc(zid, micros)
          val curDiff = rebased - micros
          if (curDiff != diff) {
            if (step > MICROS_PER_SECOND) {
              micros -= step
              step = (Math.max(MICROS_PER_SECOND, step / 2) / MICROS_PER_SECOND) * MICROS_PER_SECOND
            } else {
              diff = curDiff
              step = maxStep
              assert(diff % MICROS_PER_SECOND == 0)
              diffs.append(diff / MICROS_PER_SECOND)
              assert(micros % MICROS_PER_SECOND == 0)
              switches.append(micros / MICROS_PER_SECOND)
            }
          }
          micros += step
        }
        result.append(RebaseRecord(zid.getId, switches.toArray, diffs.toArray))
      }
    }
    val mapper = (new ObjectMapper() with ScalaObjectMapper)
      .registerModule(DefaultScalaModule)
      .writerWithDefaultPrettyPrinter()
    mapper.writeValue(
      Files.newOutputStream(
        Paths.get(dir, fileName),
        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING),
      result.toArray)
  }

  ignore("generate 'gregorian-julian-rebase-micros.json'") {
    generateRebaseJson(
      adjustFunc = identity[Long],
      rebaseFunc = rebaseGregorianToJulianMicros,
      dir = "/Users/maximgekk/tmp",
      fileName = "gregorian-julian-rebase-micros.json")
  }

  ignore("generate 'julian-gregorian-rebase-micros.json'") {
    generateRebaseJson(
      adjustFunc = rebaseGregorianToJulianMicros,
      rebaseFunc = rebaseJulianToGregorianMicros,
      dir = "/Users/maximgekk/tmp",
      fileName = "julian-gregorian-rebase-micros.json")
  }
}
