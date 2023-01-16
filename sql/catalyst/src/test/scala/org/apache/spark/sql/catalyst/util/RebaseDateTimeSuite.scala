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
import java.util.TimeZone

import org.scalatest.matchers.must.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.catalyst.util.RebaseDateTime._
import org.apache.spark.util.ThreadUtils

class RebaseDateTimeSuite extends SparkFunSuite with Matchers with SQLHelper {

  private def toJulianMicros(ts: Timestamp): Long = {
    val julianMicros = millisToMicros(ts.getTime) +
      ((ts.getNanos / NANOS_PER_MICROS) % MICROS_PER_MILLIS)
    julianMicros
  }
  private def parseToJulianMicros(s: String): Long = toJulianMicros(Timestamp.valueOf(s))

  private def toGregorianMicros(ldt: LocalDateTime, zoneId: ZoneId): Long = {
    instantToMicros(ldt.atZone(zoneId).toInstant)
  }
  private def parseToGregMicros(s: String, zoneId: ZoneId): Long = {
    toGregorianMicros(LocalDateTime.parse(s), zoneId)
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
    millisToDaysLegacy(date.getTime, TimeZone.getTimeZone(ZoneId.systemDefault()))
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
    val start = localDateToDays(LocalDate.of(1, 1, 1))
    val end = localDateToDays(LocalDate.of(2030, 1, 1))

    var days = start
    while (days < end) {
      assert(rebaseGregorianToJulianDays(days) === localRebaseGregorianToJulianDays(days))
      days += 1
    }
  }

  test("optimization of days rebasing - Julian to Gregorian") {
    val start = rebaseGregorianToJulianDays(
      localDateToDays(LocalDate.of(1, 1, 1)))
    val end = rebaseGregorianToJulianDays(
      localDateToDays(LocalDate.of(2030, 1, 1)))

    var days = start
    while (days < end) {
      assert(rebaseJulianToGregorianDays(days) === localRebaseJulianToGregorianDays(days))
      days += 1
    }
  }

  test("SPARK-31328: rebasing overlapped timestamps during daylight saving time") {
    Seq(
      LA.getId -> Seq("2019-11-03T08:00:00Z", "2019-11-03T08:30:00Z", "2019-11-03T09:00:00Z"),
      "Europe/Brussels" ->
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
        val start = instantToMicros(LocalDateTime.of(1, 1, 1, 0, 0, 0)
          .atZone(zid)
          .toInstant)
        val end = instantToMicros(LocalDateTime.of(2100, 1, 1, 0, 0, 0)
          .atZone(zid)
          .toInstant)
        var micros = start
        do {
          val rebased = rebaseGregorianToJulianMicros(TimeZone.getTimeZone(zid), micros)
          val rebasedAndOptimized = withDefaultTimeZone(zid) {
            rebaseGregorianToJulianMicros(micros)
          }
          assert(rebasedAndOptimized === rebased)
          micros += (MICROS_PER_DAY * 30 * (0.5 + Math.random())).toLong
        } while (micros <= end)
      }
    }
  }

  test("optimization of micros rebasing - Julian to Gregorian") {
    outstandingZoneIds.foreach { zid =>
      withClue(s"zone id = $zid") {
        val start = rebaseGregorianToJulianMicros(
          instantToMicros(LocalDateTime.of(1, 1, 1, 0, 0, 0).atZone(zid).toInstant))
        val end = rebaseGregorianToJulianMicros(
          instantToMicros(LocalDateTime.of(2100, 1, 1, 0, 0, 0).atZone(zid).toInstant))
        var micros = start
        do {
          val rebased = rebaseJulianToGregorianMicros(TimeZone.getTimeZone(zid), micros)
          val rebasedAndOptimized = withDefaultTimeZone(zid) {
            rebaseJulianToGregorianMicros(micros)
          }
          assert(rebasedAndOptimized === rebased)
          micros += (MICROS_PER_DAY * 30 * (0.5 + Math.random())).toLong
        } while (micros <= end)
      }
    }
  }

  private def generateRebaseJson(
      adjustFunc: (TimeZone, Long) => Long,
      rebaseFunc: (TimeZone, Long) => Long,
      dir: String,
      fileName: String): Unit = {
    import java.nio.file.{Files, Paths}
    import java.nio.file.StandardOpenOption

    import scala.collection.mutable.ArrayBuffer

    import com.fasterxml.jackson.databind.ObjectMapper
    import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}

    case class RebaseRecord(tz: String, switches: Array[Long], diffs: Array[Long])
    val rebaseRecords = ThreadUtils.parmap(ALL_TIMEZONES, "JSON-rebase-gen", 16) { zid =>
      withDefaultTimeZone(zid) {
        val tz = TimeZone.getTimeZone(zid)
        val start = adjustFunc(
          tz,
          instantToMicros(LocalDateTime.of(1, 1, 1, 0, 0, 0).atZone(zid).toInstant))
        // sun.util.calendar.ZoneInfo resolves DST after 2037 year incorrectly.
        // See https://bugs.java.com/bugdatabase/view_bug.do?bug_id=8073446
        val end = adjustFunc(
          tz,
          instantToMicros(LocalDateTime.of(2037, 1, 1, 0, 0, 0).atZone(zid).toInstant))

        var micros = start
        var diff = Long.MaxValue
        val maxStep = 30 * MICROS_PER_MINUTE
        var step: Long = MICROS_PER_SECOND
        val switches = new ArrayBuffer[Long]()
        val diffs = new ArrayBuffer[Long]()
        while (micros < end) {
          val rebased = rebaseFunc(tz, micros)
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
        RebaseRecord(zid.getId, switches.toArray, diffs.toArray)
      }
    }
    val result = new ArrayBuffer[RebaseRecord]()
    rebaseRecords.sortBy(_.tz).foreach(result.append(_))
    val mapper = (new ObjectMapper() with ClassTagExtensions)
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
      adjustFunc = (_: TimeZone, micros: Long) => micros,
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

  test("rebase gregorian to/from julian days - BCE era") {
    outstandingZoneIds.foreach { zid =>
      withDefaultTimeZone(zid) {
        Seq(
          (-1100, 1, 1),
          (-1044, 3, 5),
          (-44, 3, 5)).foreach { case (year, month, day) =>
          val julianDays = fromJavaDateLegacy(new Date(year - 1900, month - 1, day))
          val gregorianDays = localDateToDays(LocalDate.of(year, month, day))

          assert(rebaseGregorianToJulianDays(gregorianDays) === julianDays)
          assert(rebaseJulianToGregorianDays(julianDays) === gregorianDays)
        }
      }
    }
  }

  test("rebase gregorian to/from julian micros - BCE era") {
    outstandingZoneIds.foreach { zid =>
      withDefaultTimeZone(zid) {
        Seq(
          (-1100, 1, 1, 1, 2, 3, 0),
          (-1044, 3, 5, 0, 0, 0, 123456000),
          (-44, 3, 5, 23, 59, 59, 99999000)
        ).foreach { case (year, month, day, hour, minute, second, nanos) =>
          val julianMicros = toJulianMicros(new Timestamp(
            year - 1900, month - 1, day, hour, minute, second, nanos))
          val gregorianMicros = toGregorianMicros(LocalDateTime.of(
            year, month, day, hour, minute, second, nanos), zid)

          assert(rebaseGregorianToJulianMicros(gregorianMicros) === julianMicros)
          assert(rebaseJulianToGregorianMicros(julianMicros) === gregorianMicros)
        }
      }
    }
  }

  test("rebase not-existed dates in the hybrid calendar") {
    outstandingZoneIds.foreach { zid =>
      withDefaultTimeZone(zid) {
        Seq(
          "1582-10-04" -> "1582-10-04",
          "1582-10-05" -> "1582-10-15", "1582-10-06" -> "1582-10-15", "1582-10-07" -> "1582-10-15",
          "1582-10-08" -> "1582-10-15", "1582-10-09" -> "1582-10-15", "1582-10-11" -> "1582-10-15",
          "1582-10-12" -> "1582-10-15", "1582-10-13" -> "1582-10-15", "1582-10-14" -> "1582-10-15",
          "1582-10-15" -> "1582-10-15").foreach { case (gregDate, hybridDate) =>
          withClue(s"tz = ${zid.getId} greg date = $gregDate hybrid date = $hybridDate ") {
            val date = Date.valueOf(hybridDate)
            val hybridDays = fromJavaDateLegacy(date)
            val gregorianDays = localDateToDays(LocalDate.parse(gregDate))

            assert(localRebaseGregorianToJulianDays(gregorianDays) === hybridDays)
            assert(rebaseGregorianToJulianDays(gregorianDays) === hybridDays)
          }
        }
      }
    }
  }

  test("rebase not-existed timestamps in the hybrid calendar") {
    outstandingZoneIds.foreach { zid =>
      Seq(
        "1582-10-04T23:59:59.999999" -> "1582-10-04 23:59:59.999999",
        "1582-10-05T00:00:00.000000" -> "1582-10-15 00:00:00.000000",
        "1582-10-06T01:02:03.000001" -> "1582-10-15 01:02:03.000001",
        "1582-10-07T00:00:00.000000" -> "1582-10-15 00:00:00.000000",
        "1582-10-08T23:59:59.999999" -> "1582-10-15 23:59:59.999999",
        "1582-10-09T23:59:59.001001" -> "1582-10-15 23:59:59.001001",
        "1582-10-10T00:11:22.334455" -> "1582-10-15 00:11:22.334455",
        "1582-10-11T11:12:13.111111" -> "1582-10-15 11:12:13.111111",
        "1582-10-12T10:11:12.131415" -> "1582-10-15 10:11:12.131415",
        "1582-10-13T00:00:00.123321" -> "1582-10-15 00:00:00.123321",
        "1582-10-14T23:59:59.999999" -> "1582-10-15 23:59:59.999999",
        "1582-10-15T00:00:00.000000" -> "1582-10-15 00:00:00.000000"
      ).foreach { case (gregTs, hybridTs) =>
        withClue(s"tz = ${zid.getId} greg ts = $gregTs hybrid ts = $hybridTs") {
          val hybridMicros = withDefaultTimeZone(zid) { parseToJulianMicros(hybridTs) }
          val gregorianMicros = parseToGregMicros(gregTs, zid)

          val tz = TimeZone.getTimeZone(zid)
          assert(rebaseGregorianToJulianMicros(tz, gregorianMicros) === hybridMicros)
          withDefaultTimeZone(zid) {
            assert(rebaseGregorianToJulianMicros(gregorianMicros) === hybridMicros)
          }
        }
      }
    }
  }

  test("SPARK-31959: JST -> HKT at Asia/Hong_Kong in 1945") {
    // The 'Asia/Hong_Kong' time zone switched from 'Japan Standard Time' (JST = UTC+9)
    // to 'Hong Kong Time' (HKT = UTC+8). After Sunday, 18 November, 1945 01:59:59 AM,
    // clocks were moved backward to become Sunday, 18 November, 1945 01:00:00 AM.
    // In this way, the overlap happened w/o Daylight Saving Time.
    val hkZid = getZoneId("Asia/Hong_Kong")
    var expected = "1945-11-18 01:30:00.0"
    var ldt = LocalDateTime.of(1945, 11, 18, 1, 30, 0)
    var earlierMicros = instantToMicros(ldt.atZone(hkZid).withEarlierOffsetAtOverlap().toInstant)
    var laterMicros = instantToMicros(ldt.atZone(hkZid).withLaterOffsetAtOverlap().toInstant)
    var overlapInterval = MICROS_PER_HOUR
    if (earlierMicros + overlapInterval != laterMicros) {
      // Old JDK might have an outdated time zone database.
      // See https://bugs.openjdk.java.net/browse/JDK-8228469: "Hong Kong ... Its 1945 transition
      // from JST to HKT was on 11-18 at 02:00, not 09-15 at 00:00"
      expected = "1945-09-14 23:30:00.0"
      ldt = LocalDateTime.of(1945, 9, 14, 23, 30, 0)
      earlierMicros = instantToMicros(ldt.atZone(hkZid).withEarlierOffsetAtOverlap().toInstant)
      laterMicros = instantToMicros(ldt.atZone(hkZid).withLaterOffsetAtOverlap().toInstant)
      // If time zone db doesn't have overlapping at all, set the overlap interval to zero.
      overlapInterval = laterMicros - earlierMicros
    }
    val hkTz = TimeZone.getTimeZone(hkZid)
    val rebasedEarlierMicros = rebaseGregorianToJulianMicros(hkTz, earlierMicros)
    val rebasedLaterMicros = rebaseGregorianToJulianMicros(hkTz, laterMicros)
    assert(rebasedEarlierMicros + overlapInterval === rebasedLaterMicros)
    withDefaultTimeZone(hkZid) {
      def toTsStr(micros: Long): String = toJavaTimestamp(micros).toString
      assert(toTsStr(rebasedEarlierMicros) === expected)
      assert(toTsStr(rebasedLaterMicros) === expected)
      // Check optimized rebasing
      assert(rebaseGregorianToJulianMicros(earlierMicros) === rebasedEarlierMicros)
      assert(rebaseGregorianToJulianMicros(laterMicros) === rebasedLaterMicros)
      // Check reverse rebasing
      assert(rebaseJulianToGregorianMicros(rebasedEarlierMicros) === earlierMicros)
      assert(rebaseJulianToGregorianMicros(rebasedLaterMicros) === laterMicros)
    }
    // Check reverse not-optimized rebasing
    assert(rebaseJulianToGregorianMicros(hkTz, rebasedEarlierMicros) === earlierMicros)
    assert(rebaseJulianToGregorianMicros(hkTz, rebasedLaterMicros) === laterMicros)
  }
}
