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

package org.apache.spark.sql.execution.datasources.csv

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class CSVTimeFormatSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  /**
   * Helper to create a temporary CSV file with the given content.
   */
  private def createCSVFile(dir: File, content: String): String = {
    val file = new File(dir, "test.csv")
    Files.write(file.toPath, content.getBytes(StandardCharsets.UTF_8))
    file.getAbsolutePath
  }

  // ----------------------------------------------------------------------
  // 1. Default WRITE behavior (using actual file)
  // ----------------------------------------------------------------------

  test("CSV write - default timeFormat to actual file") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath + "/csv"
      val df = Seq("12:30:00").toDF("t")
        .selectExpr("CAST(t AS TIME)")

      df.write.csv(path)

      // Read back as text to verify the raw file content
      val output = spark.read.text(path).collect().map(_.getString(0)).mkString("\n")
      assert(output.contains("12:30:00.000000"))
    }
  }

  test("CSV write - preserves microseconds") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath + "/csv"

      val df = Seq("12:30:00.123456").toDF("t")
        .selectExpr("CAST(t AS TIME)")

      df.write.csv(path)

      val output = spark.read.text(path)
        .collect().map(_.getString(0)).mkString

      assert(output.contains("123456"))
    }
  }

  // ----------------------------------------------------------------------
  // 2. Custom WRITE format (using actual file)
  // ----------------------------------------------------------------------

  test("CSV write - custom timeFormat to actual file") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath + "/csv"
      val df = Seq("12:30:00").toDF("t")
        .selectExpr("CAST(t AS TIME)")

      df.write
        .option("timeFormat", "HH:mm:ss")
        .csv(path)

      // Verify the raw file content
      val output = spark.read.text(path).collect().map(_.getString(0)).mkString("\n")
      assert(output.contains("12:30:00"))
      assert(!output.contains(".000000"))
    }
  }

  // ----------------------------------------------------------------------
  // 3. Custom READ format (from actual file)
  // ----------------------------------------------------------------------

  test("CSV read - custom timeFormat from actual file") {
    withTempDir { dir =>
      val path = createCSVFile(dir, "12-30-00")

      val df = spark.read
        .option("timeFormat", "HH-mm-ss")
        .schema(StructType(Seq(StructField("t", TimeType))))
        .csv(path)

      val row = df.collect().head
      assert(!row.isNullAt(0))
      // Verify it parsed correctly as 12:30:00 (45045000000 micros)
      val localTime = row.get(0).asInstanceOf[java.time.LocalTime]
      assert(localTime.getHour === 12)
      assert(localTime.getMinute === 30)
      assert(localTime.getSecond === 0)
    }
  }

  // ----------------------------------------------------------------------
  // 4. Mismatched format (from actual file)
  // ----------------------------------------------------------------------

  test("CSV read - mismatched timeFormat from actual file") {
    withTempDir { dir =>
      val path = createCSVFile(dir, "12:30:00")

      val df = spark.read
        .option("timeFormat", "HH-mm-ss") // wrong format
        .schema(StructType(Seq(StructField("t", TimeType))))
        .csv(path)

      val row = df.collect().head
      assert(row.isNullAt(0))
    }
  }

  // ----------------------------------------------------------------------
  // 5. Invalid format string (robustness check)
  // ----------------------------------------------------------------------

  test("CSV read - invalid timeFormat pattern with actual file") {
    withTempDir { dir =>
      val path = createCSVFile(dir, "12:30:00")

      // Should fail when the formatter is actually used, or during option validation
      val e = intercept[SparkException] {
        spark.read
          .option("timeFormat", "invalid-format")
          .option("mode", "FAILFAST")
          .schema("t TIME")
          .csv(path)
          .collect()
      }
      assert(e.getMessage.contains("timeFormat"))
    }
  }

  // ----------------------------------------------------------------------
  // 6. Round-trip consistency (Write -> File -> Read)
  // ----------------------------------------------------------------------

  test("CSV roundtrip - custom timeFormat through file") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath + "/csv"
      val original = Seq("12:30:45").toDF("t")
        .selectExpr("CAST(t AS TIME)")

      original.write
        .option("timeFormat", "HH:mm:ss")
        .csv(path)

      val readBack = spark.read
        .option("timeFormat", "HH:mm:ss")
        .schema(StructType(Seq(StructField("t", TimeType))))
        .csv(path)

      checkAnswer(readBack, original)
    }
  }

  // ----------------------------------------------------------------------
  // 7. Edge cases (using actual file)
  // ----------------------------------------------------------------------

  test("CSV read/write - edge cases through file") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath + "/csv"
      val df = Seq(
        "00:00:00",
        "23:59:59.999999"
      ).toDF("t").selectExpr("CAST(t AS TIME)")

      df.write.csv(path)

      val readBack = spark.read
        .schema(StructType(Seq(StructField("t", TimeType))))
        .csv(path)

      val rows = readBack.collect().map(_.get(0).asInstanceOf[java.time.LocalTime]).sorted
      assert(rows.length == 2)
      // Verify correct values
      assert(rows(0) === java.time.LocalTime.MIDNIGHT)
      val expectedMax = java.time.LocalTime.of(23, 59, 59, 999999000)
      assert(rows(1) === expectedMax)
    }
  }

  test("CSV read - null values") {
    withTempDir { dir =>
      // File with a single field that is empty (represented by a space if we don't trim)
      // or just a file that isn't completely empty but has no content for the first column.
      val path = createCSVFile(dir, "null")

      val df = spark.read
        .option("nullValue", "null")
        .schema("t TIME")
        .csv(path)

      val row = df.collect().head
      assert(row.isNullAt(0))
    }
  }

  test("CSV read - whitespace handling") {
    withTempDir { dir =>
      val path = createCSVFile(dir, " 12:30:00 ")

      val df = spark.read
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .schema("t TIME")
        .csv(path)

      val row = df.collect().head
      assert(!row.isNullAt(0))
    }
  }

  test("CSV read - multiple rows") {
    withTempDir { dir =>
      val path = createCSVFile(dir, "12:00:00\n13:00:00")

      val df = spark.read
        .schema("t TIME")
        .csv(path)

      assert(df.count() == 2)
    }
  }

  // ----------------------------------------------------------------------
  // 8. Invalid values (using actual file)
  // ----------------------------------------------------------------------

  test("CSV read - invalid time values from file") {
    withTempDir { dir =>
      val path = createCSVFile(dir, "24:00:00")

      val df = spark.read
        .schema(StructType(Seq(StructField("t", TimeType))))
        .csv(path)

      val row = df.collect().head
      assert(row.isNullAt(0))
    }
  }

  // ----------------------------------------------------------------------
  // 9. In-memory Dataset tests (Unit-style)
  // ----------------------------------------------------------------------

  test("CSV read - in-memory Dataset with custom timeFormat") {
    val ds = Seq("12-30-00").toDS()
    val df = spark.read
      .option("timeFormat", "HH-mm-ss")
      .schema(StructType(Seq(StructField("t", TimeType))))
      .csv(ds)

    val row = df.collect().head
    assert(!row.isNullAt(0))
    val localTime = row.get(0).asInstanceOf[java.time.LocalTime]
    assert(localTime.getHour === 12)
    assert(localTime.getMinute === 30)
  }

  test("CSV read - in-memory Dataset mismatched timeFormat") {
    val ds = Seq("12:30:00").toDS()
    val df = spark.read
      .option("timeFormat", "HH-mm-ss")
      .schema(StructType(Seq(StructField("t", TimeType))))
      .csv(ds)

    val row = df.collect().head
    assert(row.isNullAt(0))
  }

  test("CSV read - in-memory Dataset invalid values") {
    val ds = Seq("24:00:00").toDS()
    val df = spark.read
      .schema(StructType(Seq(StructField("t", TimeType))))
      .csv(ds)

    val row = df.collect().head
    assert(row.isNullAt(0))
  }
}
