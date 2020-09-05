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
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import scala.util.Random

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.util.{stringToFile, DateTimeUtils}
import org.apache.spark.sql.test.SharedSparkSession

class PathFilterSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  def createSingleFile(dir: File): File = {
    val file = new File(dir, "temp" + Random.nextInt(1000) + ".csv")
    stringToFile(file, "text")
  }

  def setFileTime(time: LocalDateTime, file: File): Boolean = {
    val sameTime = time.toEpochSecond(ZoneOffset.UTC)
    file.setLastModified(sameTime * 1000)
  }

  def setPlusFileTime(time: LocalDateTime, file: File, interval: Long): Boolean = {
    val sameTime = time.plusDays(interval).toEpochSecond(ZoneOffset.UTC)
    file.setLastModified(sameTime * 1000)
  }

  def setMinusFileTime(time: LocalDateTime, file: File, interval: Long): Boolean = {
    val sameTime = time.minusDays(interval).toEpochSecond(ZoneOffset.UTC)
    file.setLastModified(sameTime * 1000)
  }

  def formatTime(time: LocalDateTime): String = {
    time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"))
  }

  test(
    "SPARK-31962: when modifiedBefore specified" +
      " and sharing same timestamp with file last modified time.") {
    withTempDir { dir =>
      val file = createSingleFile(dir)
      val time = LocalDateTime.now(ZoneOffset.UTC)
      setFileTime(time, file)
      val formattedTime = formatTime(time)

      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedBefore", formattedTime)
          .option("timeZone", "UTC")
          .format("csv")
          .load(dir.getCanonicalPath)
      }.getMessage
      assert(msg.contains("Unable to infer schema for CSV"))
    }
  }

  test(
    "SPARK-31962: when modifiedAfter specified" +
      " and sharing same timestamp with file last modified time.") {
    withTempDir { dir =>
      val file = createSingleFile(dir)
      val time = LocalDateTime.now()
      setFileTime(time, file)
      val formattedTime = formatTime(time)

      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedAfter", formattedTime)
          .option("timeZone", "UTC")
          .format("csv")
          .load(dir.getCanonicalPath)
      }.getMessage
      assert(msg.contains("Unable to infer schema for CSV"))
    }
  }

  test(
    "SPARK-31962: when modifiedBefore and modifiedAfter option" +
      " share same timestamp with file last modified time.") {
    withTempDir { dir =>
      val file = createSingleFile(dir)
      val time = LocalDateTime.now()
      setFileTime(time, file)
      val formattedTime = formatTime(time)

      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedAfter", formattedTime)
          .option("modifiedBefore", formattedTime)
          .format("csv")
          .load(dir.getCanonicalPath)
      }.getMessage
      assert(msg.contains("Unable to infer schema for CSV"))
    }
  }

  test(
    "SPARK-31962: when modifiedBefore and modifiedAfter option" +
      " share same timestamp with earlier file last modified time.") {
    withTempDir { dir =>
      val file = createSingleFile(dir)
      val time = LocalDateTime.now()
      setMinusFileTime(time, file, 3)

      val formattedTime = formatTime(time)
      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedAfter", formattedTime)
          .option("modifiedBefore", formattedTime)
          .format("csv")
          .load(dir.getCanonicalPath)
      }.getMessage
      assert(msg.contains("Unable to infer schema for CSV"))
    }
  }

  test(
    "SPARK-31962: when modifiedBefore and modifiedAfter option" +
      " share same timestamp with later file last modified time.") {
    withTempDir { dir =>
      createSingleFile(dir)
      val time = LocalDateTime.now()
      val formattedTime = formatTime(time)
      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedAfter", formattedTime)
          .option("modifiedBefore", formattedTime)
          .format("csv")
          .load(dir.getCanonicalPath)
      }.getMessage
      assert(msg.contains("Unable to infer schema for CSV"))
    }
  }

  test("SPARK-31962: when modifiedAfter specified with a past date") {
    withTempDir { dir =>
      val file = createSingleFile(dir)
      file.setLastModified(DateTimeUtils.currentTimestamp())
      val df = spark.read
        .option("modifiedAfter", "2019-05-10T01:11:00")
        .format("csv")
        .load(dir.getCanonicalPath)
      assert(df.count() == 1)
    }
  }

  test("SPARK-31962: when modifiedBefore specified with a future date") {
    withTempDir { dir =>
      createSingleFile(dir)
      val afterTime = LocalDateTime.now().plusDays(25)
      val formattedTime = formatTime(afterTime)
      val df = spark.read
        .option("modifiedBefore", formattedTime)
        .format("csv")
        .load(dir.getCanonicalPath)
      assert(df.count() == 1)
    }
  }

  test("SPARK-31962: with modifiedBefore option provided using a past date") {
    withTempDir { dir =>
      val file = createSingleFile(dir)
      file.setLastModified(DateTimeUtils.currentTimestamp())
      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedBefore", "1984-05-01T01:00:00")
          .format("csv")
          .load(dir.getCanonicalPath)
      }.getMessage
      assert(msg.contains("Unable to infer schema for CSV"))
    }
  }

  test("SPARK-31962: when modifiedAfter specified with a past date, multiple files, one valid") {
    withTempDir { dir =>
      val file1 = createSingleFile(dir)
      val file2 = createSingleFile(dir)
      file1.setLastModified(DateTimeUtils.currentTimestamp())
      file2.setLastModified(0)

      val df = spark.read
        .option("modifiedAfter", "2019-05-10T01:11:00")
        .format("csv")
        .load(dir.getCanonicalPath)
      assert(df.count() == 1)
    }
  }

  test("SPARK-31962: when modifiedAfter specified with a past date, multiple files, both valid") {
    withTempDir { dir =>
      val file1 = createSingleFile(dir)
      val file2 = createSingleFile(dir)
      file1.setLastModified(DateTimeUtils.currentTimestamp())
      file2.setLastModified(DateTimeUtils.currentTimestamp())
      val df = spark.read
        .option("modifiedAfter", "2019-05-10T01:11:00")
        .format("csv")
        .load(dir.getCanonicalPath)
      assert(df.count() == 2)
    }
  }

  test("SPARK-31962: when modifiedAfter specified with a past date, multiple files, none valid") {
    withTempDir { dir =>
      val file1 = createSingleFile(dir)
      val file2 = createSingleFile(dir)
      file1.setLastModified(0)
      file2.setLastModified(0)
      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedAfter", "1984-05-01T01:00:00")
          .format("csv")
          .load(dir.getCanonicalPath)
      }.getMessage
      assert(msg.contains("Unable to infer schema for CSV"))
    }
  }

  test(
    "SPARK-31962: when modifiedBefore specified with a future date, " +
      "multiple files, both valid") {
    withTempDir { dir =>
      val file1 = createSingleFile(dir)
      val file2 = createSingleFile(dir)
      file1.setLastModified(0)
      file2.setLastModified(0)

      val time = LocalDateTime.now().plusDays(3)
      val formattedTime = formatTime(time)

      val df = spark.read
        .option("modifiedBefore", formattedTime)
        .format("csv")
        .load(dir.getCanonicalPath)
      assert(df.count() == 2)
    }
  }

  test("SPARK-31962: when modifiedBefore specified with a future date, multiple files, one valid") {
    withTempDir { dir =>
      val file1 = createSingleFile(dir)
      val file2 = createSingleFile(dir)
      file1.setLastModified(0)
      val time = LocalDateTime.now()
      setPlusFileTime(time, file2, 3)

      val formattedTime = formatTime(time)
      val df = spark.read
        .option("modifiedBefore", formattedTime)
        .format("csv")
        .load(dir.getCanonicalPath)
      assert(df.count() == 1)
    }
  }

  test(
    "SPARK-31962: when modifiedBefore specified with a future date, " +
      "multiple files, none valid") {
    withTempDir { dir =>
      val file1 = createSingleFile(dir)
      val file2 = createSingleFile(dir)

      val time = LocalDateTime
        .now()
        .minusDays(1)
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"))

      file1.setLastModified(DateTimeUtils.currentTimestamp())
      file2.setLastModified(DateTimeUtils.currentTimestamp())
      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedBefore", time)
          .format("csv")
          .load(dir.getCanonicalPath)
      }.getMessage
      assert(msg.contains("Unable to infer schema for CSV"))
    }
  }

  test(
    "SPARK-31962: when modifiedAfter specified with a past date and " +
      "pathGlobalFilter returning results") {
    withTempDir { dir =>
      createSingleFile(dir)
      val df = spark.read
        .option("modifiedAfter", "1984-05-10T01:11:00")
        .option("pathGlobFilter", "*.csv")
        .format("csv")
        .load(dir.getCanonicalPath)
      assert(df.count() == 1)
    }
  }

  test(
    "SPARK-31962: when modifiedAfter specified with past date " +
      "and pathGlobFilter filtering results") {
    withTempDir { dir =>
      createSingleFile(dir)
      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedAfter", "1984-05-01T01:00:00")
          .option("pathGlobFilter", "*.txt")
          .format("csv")
          .load(dir.getCanonicalPath)
      }.getMessage
      assert(msg.contains("Unable to infer schema for CSV"))
    }
  }

  test(
    "SPARK-31962: when modifiedAfter specified with future date and " +
      "pathGlobFilter returning results") {
    withTempDir { dir =>
      createSingleFile(dir)

      val time = LocalDateTime
        .now()
        .plusDays(10)
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"))

      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedAfter", time)
          .option("pathGlobFilter", "*.csv")
          .format("csv")
          .load(dir.getCanonicalPath)
      }.getMessage
      assert(msg.contains("Unable to infer schema for CSV"))
    }
  }

  test(
    "SPARK-31962: when modifiedAfter specified with future date and " +
      "pathGlobFilter filtering results") {
    withTempDir { dir =>
      createSingleFile(dir)

      val time = LocalDateTime
        .now()
        .plusDays(10)
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"))

      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedAfter", time)
          .option("pathGlobFilter", "*.txt")
          .format("csv")
          .load(dir.getCanonicalPath)
      }.getMessage
      assert(msg.contains("Unable to infer schema for CSV"))
    }
  }

  test(
    "SPARK-31962: when modifiedBefore and modifiedAfter are specified out of range and " +
      "pathGlobFilter returning results") {
    withTempDir { dir =>
      createSingleFile(dir)

      val time = LocalDateTime.now().plusDays(10)
      val formattedTime = formatTime(time)

      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedAfter", formattedTime)
          .option("modifiedBefore", formattedTime)
          .option("pathGlobFilter", "*.csv")
          .format("csv")
          .load(dir.getCanonicalPath)
      }.getMessage
      assert(msg.contains("Unable to infer schema for CSV"))
    }
  }

  test(
    "SPARK-31962: when modifiedBefore and modifiedAfter are specified in range and " +
      "pathGlobFilter returning results") {
    withTempDir { dir =>
      createSingleFile(dir)

      val beforeTime = LocalDateTime
        .now()
        .minusDays(25)
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"))
      val afterTime = LocalDateTime
        .now()
        .plusDays(25)
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"))

      val df = spark.read
        .option("modifiedAfter", beforeTime)
        .option("modifiedBefore", afterTime)
        .option("pathGlobFilter", "*.csv")
        .format("csv")
        .load(dir.getCanonicalPath)
      assert(df.count() == 1)
    }
  }

  test(
    "SPARK-31962: when modifiedBefore and modifiedAfter are specified in range and " +
      "pathGlobFilter filtering results") {
    withTempDir { dir =>
      createSingleFile(dir)

      val beforeTime = LocalDateTime
        .now()
        .minusDays(25)
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"))
      val afterTime = LocalDateTime
        .now()
        .plusDays(25)
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"))

      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedAfter", beforeTime)
          .option("modifiedBefore", afterTime)
          .option("pathGlobFilter", "*.txt")
          .format("csv")
          .load(dir.getCanonicalPath)
      }.getMessage
      assert(msg.contains("Unable to infer schema for CSV"))
    }
  }

  test("SPARK-31962: when modifiedAfter is specified with an invalid date") {
    withTempDir { dir =>
      createSingleFile(dir)
      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedAfter", "2024-05+1 01:00:00")
          .format("csv")
          .load(dir.getCanonicalPath)
      }.getMessage
      Seq("The timestamp provided", "modifiedafter", "2024-05+1 01:00:00").foreach {
        expectedMsg =>
          assert(msg.contains(expectedMsg))
      }
    }
  }

  test("SPARK-31962: modifiedBefore - empty option") {
    withTempDir { dir =>
      createSingleFile(dir)
      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedBefore", "")
          .format("csv")
          .load(dir.getCanonicalPath)
      }.getMessage
      assert(
        msg.contains("The timestamp provided for")
          && msg.contains("modifiedbefore"))
    }
  }

  test("SPARK-31962: modifiedAfter - empty option") {
    withTempDir { dir =>
      createSingleFile(dir)
      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedAfter", "")
          .format("csv")
          .load(dir.getCanonicalPath)
      }.getMessage
      Seq("The timestamp provided", "modifiedafter").foreach { expectedMsg =>
        assert(msg.contains(expectedMsg))
      }
    }
  }

  test(
    "SPARK-31962: modifiedAfter filter takes into account local timezone " +
      "when specified as an option. After UTC.") {
    withTempDir { dir =>
      createSingleFile(dir)
      val timeZone = DateTimeUtils.getTimeZone("UTC")
      val strategyTime =
        ModifiedDateFilter.toThreshold(
          LocalDateTime.now(timeZone.toZoneId).toString,
          "HST",
          "modifiedafter")

      assert(
        strategyTime - DateTimeUtils
          .getMicroseconds(DateTimeUtils.currentTimestamp(), ZoneOffset.UTC) > 0)
    }
  }

  test(
    "SPARK-31962: modifiedAfter filter takes into account local timezone " +
      "when specified as an option. Before UTC.") {
    withTempDir { dir =>
      createSingleFile(dir)

      val timeZone = DateTimeUtils.getTimeZone("UTC")
      val strategyTime =
        ModifiedDateFilter.toThreshold(
          LocalDateTime.now(timeZone.toZoneId).toString,
          "HST",
          "modifiedafter")
      assert(
        DateTimeUtils
          .getMicroseconds(DateTimeUtils.currentTimestamp(), ZoneOffset.UTC) - strategyTime < 0)
    }
  }

  test(
    "SPARK-31962: modifiedBefore filter takes into account local timezone " +
      "when specified as an option. After UTC.") {
    withTempDir { dir =>
      createSingleFile(dir)
      val timeZone = DateTimeUtils.getTimeZone("UTC")
      val strategyTime =
        ModifiedDateFilter.toThreshold(
          LocalDateTime.now(timeZone.toZoneId).toString,
          "CET",
          "modifiedbefore")
      assert(
        DateTimeUtils
          .getMicroseconds(DateTimeUtils.currentTimestamp(), ZoneOffset.UTC) - strategyTime < 0)
    }
  }

  test(
    "SPARK-31962: modifiedBefore filter takes into account local timezone " +
      "when specified as an option. Before UTC.") {
    withTempDir { dir =>
      createSingleFile(dir)
      val timeZone = DateTimeUtils.getTimeZone("UTC")
      val strategyTime =
        ModifiedDateFilter.toThreshold(
          LocalDateTime.now(timeZone.toZoneId).toString,
          "HST",
          "modifiedbefore")
      assert(
        strategyTime - DateTimeUtils.fromUTCTime(DateTimeUtils.currentTimestamp(), "UTC") > 0)
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
}
