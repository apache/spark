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
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils, stringToFile}
import org.apache.spark.sql.test.SharedSparkSession

class PathFilterSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("SPARK-31962: when modifiedAfter specified with a past date") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val file = new File(dir, "file1.csv")
      stringToFile(file, "text")
      file.setLastModified(DateTimeUtils.currentTimestamp())
      val df = spark.read
        .option("modifiedAfter", "2019-05-10T01:11:00")
        .format("csv")
        .load(path.toString)
      assert(df.count() == 1)
    }
  }

  test("SPARK-31962: when modifiedBefore specified with a future date") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val file = new File(dir, "file1.csv")
      stringToFile(file, "text")
      val df = spark.read
        .option("modifiedBefore", "2090-05-10T01:11:00")
        .format("csv")
        .load(path.toString)
      assert(df.count() == 1)
    }
  }

  test("SPARK-31962: when modifiedBefore specified with a past date") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val file = new File(dir, "file1.csv")
      stringToFile(file, "text")
      file.setLastModified(DateTimeUtils.currentTimestamp())
      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedBefore", "1984-05-01T01:00:00")
          .format("csv")
          .load(path.toString)
      }.getMessage
      assert(msg.contains("Unable to infer schema for CSV"))
    }
  }

  test(
    "SPARK-31962: when modifiedAfter specified with a past date, " +
      "multiple files, one valid") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val file1 = new File(dir, "file1.csv")
      val file2 = new File(dir, "file2.csv")
      stringToFile(file1, "text")
      stringToFile(file2, "text")
      file1.setLastModified(DateTimeUtils.currentTimestamp())
      file2.setLastModified(0)
      val df = spark.read
        .option("modifiedAfter", "2019-05-10T01:11:00")
        .format("csv")
        .load(path.toString)
      assert(df.count() == 1)
    }
  }

  test(
    "SPARK-31962: when modifiedAfter specified with a past date, " +
      "multiple files, both valid") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val file1 = new File(dir, "file1.csv")
      val file2 = new File(dir, "file2.csv")
      stringToFile(file1, "text")
      stringToFile(file2, "text")
      file1.setLastModified(DateTimeUtils.currentTimestamp())
      file2.setLastModified(DateTimeUtils.currentTimestamp())
      val df = spark.read
        .option("modifiedAfter", "2019-05-10T01:11:00")
        .format("csv")
        .load(path.toString)
      assert(df.count() == 2)
    }
  }

  test(
    "SPARK-31962: when modifiedAfter specified with a past date," +
      " multiple files, none valid") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val file1 = new File(dir, "file1.csv")
      val file2 = new File(dir, "file2.csv")
      stringToFile(file1, "text")
      stringToFile(file2, "text")
      file1.setLastModified(0)
      file2.setLastModified(0)
      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedAfter", "1984-05-01T01:00:00")
          .format("csv")
          .load(path.toString)
      }.getMessage
      assert(msg.contains("Unable to infer schema for CSV"))
    }
  }

  test(
    "SPARK-31962: when modifiedBefore specified with a future date," +
      " multiple files, both valid") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val file1 = new File(dir, "file1.csv")
      val file2 = new File(dir, "file2.csv")
      stringToFile(file1, "text")
      stringToFile(file2, "text")
      file1.setLastModified(DateTimeUtils.currentTimestamp())
      file2.setLastModified(DateTimeUtils.currentTimestamp())

      val time = LocalDateTime
        .now()
        .plusDays(1)
        .format(DateTimeFormatter
          .ofPattern("yyyy-MM-dd'T'HH:mm:ss"))

      val df = spark.read
        .option("modifiedBefore", time)
        .format("csv")
        .load(path.toString)
      assert(df.count() == 2)
    }
  }

  test(
    "SPARK-31962: when modifiedBefore specified with a future date," +
      " multiple files, one valid") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val file1 = new File(dir, "file1.csv")
      val file2 = new File(dir, "file2.csv")
      stringToFile(file1, "text")
      stringToFile(file2, "text")

      file1.setLastModified(DateTimeUtils.currentTimestamp())

      val failTime =
        LocalDateTime.now().plusDays(1).toEpochSecond(ZoneOffset.UTC)
      file2.setLastModified(failTime * 1000)

      val time = LocalDateTime
        .now()
        .plusHours(10)
        .format(DateTimeFormatter
          .ofPattern("yyyy-MM-dd'T'HH:mm:ss"))

      val df = spark.read
        .option("modifiedBefore", time)
        .format("csv")
        .load(path.toString)
      assert(df.count() == 1)
    }
  }

  test(
    "SPARK-31962: when modifiedBefore specified with a future date," +
      " multiple files, none valid") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val file1 = new File(dir, "file1.csv")
      val file2 = new File(dir, "file2.csv")
      stringToFile(file1, "text")
      stringToFile(file2, "text")

      val time = LocalDateTime
        .now()
        .minusDays(1)
        .format(DateTimeFormatter
          .ofPattern("yyyy-MM-dd'T'HH:mm:ss"))

      file1.setLastModified(DateTimeUtils.currentTimestamp())
      file2.setLastModified(DateTimeUtils.currentTimestamp())
      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedBefore", time)
          .format("csv")
          .load(path.toString)
      }.getMessage
      assert(msg.contains("Unable to infer schema for CSV"))
    }
  }

  test(
    "SPARK-31962: when modifiedAfter specified " +
      "with a past date and pathGlobalFilter returning results") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val file = new File(dir, "file1.csv")
      stringToFile(file, "text")
      val df = spark.read
        .option("modifiedAfter", "1984-05-10T01:11:00")
        .option("pathGlobFilter", "*.csv")
        .format("csv")
        .load(path.toString)
      assert(df.count() == 1)
    }
  }

  test(
    "SPARK-31962: when modifiedAfter specified " +
      "with past date and pathGlobFilter filtering results") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val file = new File(dir, "file1.csv")
      stringToFile(file, "text")
      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedAfter", "1984-05-01T01:00:00")
          .option("pathGlobFilter", "*.txt")
          .format("csv")
          .load(path.toString)
      }.getMessage
      assert(msg.contains("Unable to infer schema for CSV"))
    }
  }

  test(
    "SPARK-31962: when modifiedAfter specified " +
      "with future date and pathGlobFilter returning results") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val file = new File(dir, "file1.csv")
      stringToFile(file, "text")
      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedAfter", "2050-05-01T01:00:00")
          .option("pathGlobFilter", "*.csv")
          .format("csv")
          .load(path.toString)
      }.getMessage
      assert(msg.contains("Unable to infer schema for CSV"))
    }
  }

  test(
    "SPARK-31962: when modifiedAfter specified " +
      "with future date and pathGlobFilter filtering results") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val file = new File(dir, "file1.csv")
      stringToFile(file, "text")
      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedAfter", "2050-05-01T01:00:00")
          .option("pathGlobFilter", "*.txt")
          .format("csv")
          .load(path.toString)
      }.getMessage
      assert(msg.contains("Unable to infer schema for CSV"))
    }
  }

  test(
    "SPARK-31962: when modifiedBefore and modifiedAfter" +
      "are specified out of range and pathGlobFilter returning results") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val file = new File(dir, "file1.csv")
      stringToFile(file, "text")
      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedAfter", "2050-05-01T01:00:00")
          .option("modifiedBefore", "2050-05-01T01:00:00")
          .option("pathGlobFilter", "*.csv")
          .format("csv")
          .load(path.toString)
      }.getMessage
      assert(msg.contains("Unable to infer schema for CSV"))
    }
  }

  test(
    "SPARK-31962: when modifiedBefore and modifiedAfter" +
      "are specified in range and pathGlobFilter returning results") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val file = new File(dir, "file1.csv")
      stringToFile(file, "text")
      val df = spark.read
        .option("modifiedAfter", "2019-05-01T01:00:00")
        .option("modifiedBefore", "2025-05-01T01:00:00")
        .option("pathGlobFilter", "*.csv")
        .format("csv")
        .load(path.toString)
      assert(df.count() == 1)
    }
  }

  test(
    "SPARK-31962: when modifiedBefore and modifiedAfter" +
      "are specified in range and pathGlobFilter filtering results") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val file = new File(dir, "file1.csv")
      stringToFile(file, "text")
      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedAfter", "2019-05-01T01:00:00")
          .option("modifiedBefore", "2025-05-01T01:00:00")
          .option("pathGlobFilter", "*.txt")
          .format("csv")
          .load(path.toString)
      }.getMessage
      assert(msg.contains("Unable to infer schema for CSV"))
    }
  }

  test("SPARK-31962: when modifiedAfter is specified with an invalid date") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val file = new File(dir, "file1.csv")
      stringToFile(file, "text")

      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedAfter", "2024-05+1 01:00:00")
          .format("csv")
          .load(path.toString)
      }.getMessage
      assert(
        msg.contains("The timestamp provided")
          && msg.contains("modifiedafter")
          && msg.contains("2024-05+1 01:00:00"))
    }
  }

  test("SPARK-31962: PathFilterStrategies - modifiedAfter option") {
    val options =
      CaseInsensitiveMap[String](Map("modifiedAfter" -> "2010-10-01T01:01:00"))
    val strategy = PathFilterFactory.create(spark, spark.sessionState.newHadoopConf(), options)
    assert(strategy.head.isInstanceOf[ModifiedAfterFilter])
    assert(strategy.size == 1)
  }

  test("SPARK-31962: PathFilterStrategies - modifiedBefore option") {
    val options =
      CaseInsensitiveMap[String](Map("modifiedBefore" -> "2020-10-01T01:01:00"))
    val strategy = PathFilterFactory.create(spark, spark.sessionState.newHadoopConf(), options)
    assert(strategy.head.isInstanceOf[ModifiedBeforeFilter])
    assert(strategy.size == 1)
  }

  test("SPARK-31962: PathFilterStrategies - pathGlobFilter option") {
    val options = CaseInsensitiveMap[String](Map("pathGlobFilter" -> "*.txt"))
    val strategy = PathFilterFactory.create(spark, spark.sessionState.newHadoopConf(), options)
    assert(strategy.head.isInstanceOf[PathGlobFilter])
    assert(strategy.size == 1)
  }

  test("SPARK-31962: PathFilterStrategies - multiple options") {
    val options = CaseInsensitiveMap[String](
      Map(
        "pathGlobFilter" -> "*.txt",
        "modifiedAfter" -> "2020-01-01T01:01:01",
        "modifiedBefore" -> "2020-01-01T01:01:01"))
    val strategies =
      PathFilterFactory.create(spark, spark.sessionState.newHadoopConf(), options)
    val classes = Set(
      "class org.apache.spark.sql.execution.datasources.PathGlobFilter",
      "class org.apache.spark.sql.execution.datasources.ModifiedAfterFilter",
      "class org.apache.spark.sql.execution.datasources.ModifiedBeforeFilter")
    val foundClasses = strategies.map(x => x.getClass.toString)
    assert(foundClasses == classes)
    assert(strategies.size == 3)
  }

  test("SPARK-31962: PathFilterStrategies - no options") {
    val options = CaseInsensitiveMap[String](Map.empty)
    val strategy = PathFilterFactory.create(spark, spark.sessionState.newHadoopConf(), options)
    assert(strategy.size == 0)
  }

  test("SPARK-31962: modifiedBefore - empty option") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val file = new File(dir, "file1.csv")
      stringToFile(file, "text")

      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedBefore", "")
          .format("csv")
          .load(path.toString)
      }.getMessage
      assert(
        msg.contains("The timestamp provided for")
          && msg.contains("modifiedbefore"))
    }
  }

  test("SPARK-31962: modifiedAfter - empty option") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val file = new File(dir, "file1.csv")
      stringToFile(file, "text")

      val msg = intercept[AnalysisException] {
        spark.read
          .option("modifiedAfter", "")
          .format("csv")
          .load(path.toString)
      }.getMessage
      assert(
        msg.contains("The timestamp provided for")
          && msg.contains("modifiedafter"))
    }
  }

  test(
    "SPARK-31962: modifiedAfter filter takes into account local " +
      "timezone when specified as an option.  After UTC.") {
    withTempDir { dir =>
      val file = new File(dir, "file1.csv")
      stringToFile(file, "text")

      val timeZone = DateTimeUtils.getTimeZone("UTC")
      val strategy = PathFilterFactory.create(
        spark,
        spark.sessionState.newHadoopConf(),
        CaseInsensitiveMap[String](
          Map(
            "modifiedAfter" -> LocalDateTime.now(timeZone.toZoneId).toString,
            "timeZone" -> "CET")))

      val strategyTime =
        strategy.head.asInstanceOf[ModifiedAfterFilter].thresholdTime
      assert(
        strategyTime - DateTimeUtils
          .getMicroseconds(DateTimeUtils.currentTimestamp, timeZone.toZoneId) > 0)
    }
  }

  test(
    "SPARK-31962: modifiedAfter filter takes into account" +
      " local timezone when specified as an option.  Before UTC.") {
    withTempDir { dir =>
      val file = new File(dir, "file1.csv")
      stringToFile(file, "text")
      val timeZone = DateTimeUtils.getTimeZone("UTC")

      val strategy = PathFilterFactory.create(
        spark,
        spark.sessionState.newHadoopConf(),
        CaseInsensitiveMap[String](
          Map(
            "modifiedAfter" -> LocalDateTime.now(timeZone.toZoneId).toString,
            "timeZone" -> "HST")))

      val strategyTime =
        strategy.head.asInstanceOf[ModifiedAfterFilter].thresholdTime
      assert(
        DateTimeUtils
          .getMicroseconds(DateTimeUtils.currentTimestamp, timeZone.toZoneId) - strategyTime < 0)
    }
  }

  test(
    "SPARK-31962: modifiedBefore filter takes into account" +
      " local timezone when specified as an option.  After UTC.") {
    withTempDir { dir =>
      val file = new File(dir, "file1.csv")
      stringToFile(file, "text")

      val timeZone = DateTimeUtils.getTimeZone("CET")
      val strategy = PathFilterFactory.create(
        spark,
        spark.sessionState.newHadoopConf(),
        CaseInsensitiveMap[String](
          Map(
            "modifiedBefore" -> LocalDateTime.now(timeZone.toZoneId).toString,
            "timeZone" -> "CET")))

      val strategyTime =
        strategy.head.asInstanceOf[ModifiedBeforeFilter].thresholdTime
      assert(
        DateTimeUtils
          .fromUTCTime(DateTimeUtils.currentTimestamp, "UTC") - strategyTime >= 0)
    }
  }

  test(
    "SPARK-31962: modifiedBefore filter takes into account" +
      " local timezone when specified as an option.  Before UTC.") {
    withTempDir { dir =>
      val file = new File(dir, "file1.csv")
      stringToFile(file, "text")
      val timeZone = DateTimeUtils.getTimeZone("UTC")

      val strategy = PathFilterFactory.create(
        spark,
        spark.sessionState.newHadoopConf(),
        CaseInsensitiveMap[String](
          Map(
            "modifiedBefore" -> LocalDateTime.now(timeZone.toZoneId).toString,
            "timeZone" -> "HST")))

      val strategyTime =
        strategy.head.asInstanceOf[ModifiedBeforeFilter].thresholdTime
      assert(strategyTime - DateTimeUtils.fromUTCTime(DateTimeUtils.currentTimestamp, "UTC") > 0)
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
      val df2 =
        spark.read.option("pathGlobFilter", "*.txt").text(dataDir + "/*.orc")
      checkAnswer(df2, Seq.empty)

      val df3 =
        spark.read.option("pathGlobFilter", "*.txt").text(dataDir + "/*xt")
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

      val df2 =
        spark.read.option("pathGlobFilter", "*.txt").text(path.getCanonicalPath)
      checkAnswer(df2, input)
    }
  }
}
