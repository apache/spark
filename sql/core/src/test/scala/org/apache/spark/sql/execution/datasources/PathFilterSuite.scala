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

package org.apache.spark.sql.execution.datasources

import java.io.File
import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter

import scala.util.Random

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.util.{stringToFile, DateTimeUtils}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class PathFilterSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("SPARK-31962: modifiedBefore specified" +
      " and sharing same timestamp with file last modified time.") {
    withTempDir { dir =>
      val curTime = LocalDateTime.now(ZoneOffset.UTC)
      executeTest(dir, Seq(curTime), 0, modifiedBefore = Some(formatTime(curTime)))
    }
  }

  test("SPARK-31962: modifiedAfter specified" +
      " and sharing same timestamp with file last modified time.") {
    withTempDir { dir =>
      val curTime = LocalDateTime.now(ZoneOffset.UTC)
      executeTest(dir, Seq(curTime), 0, modifiedAfter = Some(formatTime(curTime)))
    }
  }

  test("SPARK-31962: modifiedBefore and modifiedAfter option" +
      " share same timestamp with file last modified time.") {
    withTempDir { dir =>
      val curTime = LocalDateTime.now(ZoneOffset.UTC)
      val formattedTime = formatTime(curTime)
      executeTest(dir, Seq(curTime), 0, modifiedBefore = Some(formattedTime),
        modifiedAfter = Some(formattedTime))
    }
  }

  test("SPARK-31962: modifiedBefore and modifiedAfter option" +
      " share same timestamp with earlier file last modified time.") {
    withTempDir { dir =>
      val curTime = LocalDateTime.now(ZoneOffset.UTC)
      val fileTime = curTime.minusDays(3)
      val formattedTime = formatTime(curTime)
      executeTest(dir, Seq(fileTime), 0, modifiedBefore = Some(formattedTime),
        modifiedAfter = Some(formattedTime))
    }
  }

  test("SPARK-31962: modifiedBefore and modifiedAfter option" +
      " share same timestamp with later file last modified time.") {
    withTempDir { dir =>
      val curTime = LocalDateTime.now(ZoneOffset.UTC)
      val formattedTime = formatTime(curTime)
      executeTest(dir, Seq(curTime), 0, modifiedBefore = Some(formattedTime),
        modifiedAfter = Some(formattedTime))
    }
  }

  test("SPARK-31962: when modifiedAfter specified with a past date") {
    withTempDir { dir =>
      val curTime = LocalDateTime.now(ZoneOffset.UTC)
      val pastTime = curTime.minusYears(1)
      val formattedTime = formatTime(pastTime)
      executeTest(dir, Seq(curTime), 1, modifiedAfter = Some(formattedTime))
    }
  }

  test("SPARK-31962: when modifiedBefore specified with a future date") {
    withTempDir { dir =>
      val curTime = LocalDateTime.now(ZoneOffset.UTC)
      val futureTime = curTime.plusYears(1)
      val formattedTime = formatTime(futureTime)
      executeTest(dir, Seq(curTime), 1, modifiedBefore = Some(formattedTime))
    }
  }

  test("SPARK-31962: with modifiedBefore option provided using a past date") {
    withTempDir { dir =>
      val curTime = LocalDateTime.now(ZoneOffset.UTC)
      val pastTime = curTime.minusYears(1)
      val formattedTime = formatTime(pastTime)
      executeTest(dir, Seq(curTime), 0, modifiedBefore = Some(formattedTime))
    }
  }

  test("SPARK-31962: modifiedAfter specified with a past date, multiple files, one valid") {
    withTempDir { dir =>
      val fileTime1 = LocalDateTime.now(ZoneOffset.UTC)
      val fileTime2 = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC)
      val pastTime = fileTime1.minusYears(1)
      val formattedTime = formatTime(pastTime)
      executeTest(dir, Seq(fileTime1, fileTime2), 1, modifiedAfter = Some(formattedTime))
    }
  }

  test("SPARK-31962: modifiedAfter specified with a past date, multiple files, both valid") {
    withTempDir { dir =>
      val curTime = LocalDateTime.now(ZoneOffset.UTC)
      val pastTime = curTime.minusYears(1)
      val formattedTime = formatTime(pastTime)
      executeTest(dir, Seq(curTime, curTime), 2, modifiedAfter = Some(formattedTime))
    }
  }

  test("SPARK-31962: modifiedAfter specified with a past date, multiple files, none valid") {
    withTempDir { dir =>
      val fileTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC)
      val pastTime = LocalDateTime.now(ZoneOffset.UTC).minusYears(1)
      val formattedTime = formatTime(pastTime)
      executeTest(dir, Seq(fileTime, fileTime), 0, modifiedAfter = Some(formattedTime))
    }
  }

  test("SPARK-31962: modifiedBefore specified with a future date, multiple files, both valid") {
    withTempDir { dir =>
      val fileTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC)
      val futureTime = LocalDateTime.now(ZoneOffset.UTC).plusYears(1)
      val formattedTime = formatTime(futureTime)
      executeTest(dir, Seq(fileTime, fileTime), 2, modifiedBefore = Some(formattedTime))
    }
  }

  test("SPARK-31962: modifiedBefore specified with a future date, multiple files, one valid") {
    withTempDir { dir =>
      val curTime = LocalDateTime.now(ZoneOffset.UTC)
      val fileTime1 = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC)
      val fileTime2 = curTime.plusDays(3)
      val formattedTime = formatTime(curTime)
      executeTest(dir, Seq(fileTime1, fileTime2), 1, modifiedBefore = Some(formattedTime))
    }
  }

  test("SPARK-31962: modifiedBefore specified with a future date, multiple files, none valid") {
    withTempDir { dir =>
      val fileTime = LocalDateTime.now(ZoneOffset.UTC).minusDays(1)
      val formattedTime = formatTime(fileTime)
      executeTest(dir, Seq(fileTime, fileTime), 0, modifiedBefore = Some(formattedTime))
    }
  }

  test("SPARK-31962: modifiedBefore/modifiedAfter is specified with an invalid date") {
    executeTestWithBadOption(
      Map("modifiedBefore" -> "2024-05+1 01:00:00"),
      Seq("The timestamp provided", "modifiedbefore", "2024-05+1 01:00:00"))

    executeTestWithBadOption(
      Map("modifiedAfter" -> "2024-05+1 01:00:00"),
      Seq("The timestamp provided", "modifiedafter", "2024-05+1 01:00:00"))
  }

  test("SPARK-31962: modifiedBefore/modifiedAfter - empty option") {
    executeTestWithBadOption(
      Map("modifiedBefore" -> ""),
      Seq("The timestamp provided", "modifiedbefore"))

    executeTestWithBadOption(
      Map("modifiedAfter" -> ""),
      Seq("The timestamp provided", "modifiedafter"))
  }

  test("SPARK-31962: modifiedBefore/modifiedAfter filter takes into account local timezone " +
      "when specified as an option.") {
    Seq("modifiedbefore", "modifiedafter").foreach { filterName =>
      // CET = UTC + 1 hour, HST = UTC - 10 hours
      Seq("CET", "HST").foreach { tzId =>
        testModifiedDateFilterWithTimezone(tzId, filterName)
      }
    }
  }

  test("Option pathGlobFilter: filter files correctly") {
    withTempPath { path =>
      val dataDir = path.getCanonicalPath
      Seq("foo").toDS().write.text(dataDir)
      Seq("bar").toDS().write.mode("append").orc(dataDir)
      val df = spark.read.option("pathGlobFilter", "*.txt").text(dataDir)
      checkAnswer(df, Row("foo"))

      // Both glob pattern in option and path should be effective to filter files.
      val df2 = spark.read.option("pathGlobFilter", "*.txt").text(dataDir + "/*.orc")
      checkAnswer(df2, Seq.empty)

      val df3 = spark.read.option("pathGlobFilter", "*.txt").text(dataDir + "/*xt")
      checkAnswer(df3, Row("foo"))
    }
  }

  test("Option pathGlobFilter: simple extension filtering should contains partition info") {
    withTempPath { path =>
      val input = Seq(("foo", 1), ("oof", 2)).toDF("a", "b")
      input.write.partitionBy("b").text(path.getCanonicalPath)
      Seq("bar").toDS().write.mode("append").orc(path.getCanonicalPath + "/b=1")

      // If we use glob pattern in the path, the partition column won't be shown in the result.
      val df = spark.read.text(path.getCanonicalPath + "/*/*.txt")
      checkAnswer(df, input.select("a"))

      val df2 = spark.read.option("pathGlobFilter", "*.txt").text(path.getCanonicalPath)
      checkAnswer(df2, input)
    }
  }

  private def executeTest(
      dir: File,
      fileDates: Seq[LocalDateTime],
      expectedCount: Long,
      modifiedBefore: Option[String] = None,
      modifiedAfter: Option[String] = None): Unit = {
    fileDates.foreach { fileDate =>
      val file = createSingleFile(dir)
      setFileTime(fileDate, file)
    }

    val schema = StructType(Seq(StructField("a", StringType)))

    var dfReader = spark.read.format("csv").option("timeZone", "UTC").schema(schema)
    modifiedBefore.foreach { opt => dfReader = dfReader.option("modifiedBefore", opt) }
    modifiedAfter.foreach { opt => dfReader = dfReader.option("modifiedAfter", opt) }

    if (expectedCount > 0) {
      // without pathGlobFilter
      val df1 = dfReader.load(dir.getCanonicalPath)
      assert(df1.count() === expectedCount)

      // pathGlobFilter matched
      val df2 = dfReader.option("pathGlobFilter", "*.csv").load(dir.getCanonicalPath)
      assert(df2.count() === expectedCount)

      // pathGlobFilter mismatched
      val df3 = dfReader.option("pathGlobFilter", "*.txt").load(dir.getCanonicalPath)
      assert(df3.count() === 0)
    } else {
      val df = dfReader.load(dir.getCanonicalPath)
      assert(df.count() === 0)
    }
  }

  private def executeTestWithBadOption(
      options: Map[String, String],
      expectedMsgParts: Seq[String]): Unit = {
    withTempDir { dir =>
      createSingleFile(dir)
      val exc = intercept[AnalysisException] {
        var dfReader = spark.read.format("csv")
        options.foreach { case (key, value) =>
          dfReader = dfReader.option(key, value)
        }
        dfReader.load(dir.getCanonicalPath)
      }
      expectedMsgParts.foreach { msg => assert(exc.getMessage.contains(msg)) }
    }
  }

  private def testModifiedDateFilterWithTimezone(
      timezoneId: String,
      filterParamName: String): Unit = {
    val curTime = LocalDateTime.now(ZoneOffset.UTC)
    val zoneId: ZoneId = DateTimeUtils.getTimeZone(timezoneId).toZoneId
    val strategyTimeInMicros =
      ModifiedDateFilter.toThreshold(
        curTime.toString,
        timezoneId,
        filterParamName)
    val strategyTimeInSeconds = strategyTimeInMicros / 1000 / 1000

    val curTimeAsSeconds = curTime.atZone(zoneId).toEpochSecond
    withClue(s"timezone: $timezoneId / param: $filterParamName,") {
      assert(strategyTimeInSeconds === curTimeAsSeconds)
    }
  }

  private def createSingleFile(dir: File): File = {
    val file = new File(dir, "temp" + Random.nextInt(1000000) + ".csv")
    stringToFile(file, "text")
  }

  private def setFileTime(time: LocalDateTime, file: File): Boolean = {
    val sameTime = time.toEpochSecond(ZoneOffset.UTC)
    file.setLastModified(sameTime * 1000)
  }

  private def formatTime(time: LocalDateTime): String = {
    time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"))
  }
}
