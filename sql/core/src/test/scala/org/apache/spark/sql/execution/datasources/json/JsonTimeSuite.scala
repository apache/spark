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

package org.apache.spark.sql.execution.datasources.json

import java.io.File
import java.nio.charset.StandardCharsets

import com.google.common.io.Files

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class JsonTimeSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  private def createJsonFile(dir: File, content: String): String = {
    val file = new File(dir, "test.json")
    Files.write(content.getBytes(StandardCharsets.UTF_8), file)
    file.getAbsolutePath
  }

  // =========================================================
  // 1. JSON WRITE Tests (JacksonGenerator)
  // =========================================================

  test("JSON write - default timeFormat") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath + "/json"

      val df = Seq("12:30:00").toDF("t")
        .selectExpr("CAST(t AS TIME)")

      df.write.json(path)

      val output = spark.read.text(path).collect().map(_.getString(0)).mkString

      assert(output.contains("12:30:00.000000"))
    }
  }

  test("JSON write - preserves microseconds") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath + "/json"

      val df = Seq("12:30:00.123456").toDF("t")
        .selectExpr("CAST(t AS TIME)")

      df.write.json(path)

      val output = spark.read.text(path).collect().map(_.getString(0)).mkString

      assert(output.contains("123456"))
    }
  }

  test("JSON write - custom timeFormat") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath + "/json"

      val df = Seq("12:30:00").toDF("t")
        .selectExpr("CAST(t AS TIME)")

      df.write
        .option("timeFormat", "HH:mm:ss")
        .json(path)

      val output = spark.read.text(path).collect().map(_.getString(0)).mkString

      assert(output.contains("12:30:00"))
      assert(!output.contains(".000000"))
    }
  }

  // =========================================================
  // 2. JSON READ Tests (JacksonParser)
  // =========================================================

  test("JSON read - default time format") {
    withTempDir { dir =>
      val path = createJsonFile(dir, """{"t":"12:30:00"}""")

      val df = spark.read
        .schema("t TIME")
        .json(path)

      val row = df.collect().head
      assert(!row.isNullAt(0))

      val t = row.get(0).asInstanceOf[java.time.LocalTime]
      assert(t.getHour == 12)
      assert(t.getMinute == 30)
    }
  }

  test("JSON read - custom timeFormat") {
    withTempDir { dir =>
      val path = createJsonFile(dir, """{"t":"12-30-00"}""")

      val df = spark.read
        .option("timeFormat", "HH-mm-ss")
        .schema("t TIME")
        .json(path)

      val row = df.collect().head
      val t = row.get(0).asInstanceOf[java.time.LocalTime]

      assert(t.getHour == 12)
      assert(t.getMinute == 30)
    }
  }

  test("JSON read - mismatched timeFormat fallback") {
    withTempDir { dir =>
      val path = createJsonFile(dir, """{"t":"12:30:00"}""")

      val df = spark.read
        .option("timeFormat", "HH-mm-ss") // wrong format
        .schema("t TIME")
        .json(path)

      val row = df.collect().head

      // Should still parse via fallback
      assert(!row.isNullAt(0))
    }
  }

  test("JSON read - invalid time value") {
    withTempDir { dir =>
      val path = createJsonFile(dir, """{"t":"24:00:00"}""")

      val df = spark.read
        .schema("t TIME")
        .json(path)

      val row = df.collect().head
      assert(row.isNullAt(0))
    }
  }

  test("JSON read - null value") {
    withTempDir { dir =>
      val path = createJsonFile(dir, """{"t":null}""")

      val df = spark.read
        .schema("t TIME")
        .json(path)

      val row = df.collect().head
      assert(row.isNullAt(0))
    }
  }

  // =========================================================
  // 3. ROUND TRIP TEST (MOST IMPORTANT)
  // =========================================================

  test("JSON roundtrip - write and read time") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath + "/json"

      val original = Seq("12:30:45").toDF("t")
        .selectExpr("CAST(t AS TIME)")

      original.write
        .option("timeFormat", "HH:mm:ss")
        .json(path)

      val readBack = spark.read
        .option("timeFormat", "HH:mm:ss")
        .schema("t TIME")
        .json(path)

      checkAnswer(readBack, original)
    }
  }
}
