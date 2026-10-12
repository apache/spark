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

package org.apache.spark.sql.execution.datasources.xml

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.functions.from_xml
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class XmlLegacyTimestampSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED, false)

  private val schema = new StructType().add("stamp", TimestampType)

  test("SPARK-60199: LEGACY XML preserves spark-xml timestamps in files and inference") {
    val timestamps = Seq(
      "2024-02-29T12:34:56Z" -> "2024-02-29T12:34:56Z",
      "2024-02-29T12:34:56.1Z" -> "2024-02-29T12:34:56.100Z",
      "2024-02-29T12:34:56.123Z" -> "2024-02-29T12:34:56.123Z",
      "2024-02-29T12:34:56.123456Z" -> "2024-02-29T12:34:56.123456Z",
      "2024-02-29T12:34:56.000001Z" -> "2024-02-29T12:34:56.000001Z",
      "2024-02-29T12:34:56.123456789Z" -> "2024-02-29T12:34:56.123456Z",
      "2024-02-29T12:34:56.123456+02:00" -> "2024-02-29T10:34:56.123456Z",
      "2024-02-29T12:34:56.123456" -> "2024-02-29T12:34:56.123456Z",
      "2024-02-29 12:34:56.123456" -> "2024-02-29T12:34:56.123456Z",
      "1500-01-01T12:34:56.123456Z" -> "1500-01-01T12:34:56.123456Z")

    withSQLConf(
      SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "LEGACY",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      Seq(false, true).foreach { multiLine =>
        withTempPath { path =>
          val rows = timestamps.map { case (input, _) => s"<ROW><stamp>$input</stamp></ROW>" }
          val records = if (multiLine) Seq(rows.mkString("<ROWS>\n", "\n", "\n</ROWS>")) else rows
          records.toDS().coalesce(1).write.text(path.getAbsolutePath)
          for {
            mode <- Seq("PERMISSIVE", "FAILFAST")
            inferSchema <- Seq(false, true)
            legacyXmlParser <- Seq(false, true)
          } {
            withClue(
              s"$mode, infer=$inferSchema, multiLine=$multiLine, legacy=$legacyXmlParser") {
              val reader = spark.read
                .option("rowTag", "ROW")
                .option("mode", mode)
                .option("multiLine", multiLine)
                .option("useLegacyXMLParser", legacyXmlParser)
              if (!inferSchema) {
                reader.schema(schema)
              }
              val result = reader.xml(path.getAbsolutePath)
              assert(result.schema == schema)
              checkAnswer(
                result,
                timestamps.map { case (_, expected) =>
                  Row(Timestamp.from(Instant.parse(expected)))
                })
            }
          }
        }
      }
    }
  }

  test("SPARK-60199: LEGACY XML tries ISO timestamps before custom formats") {
    val cases = Seq(
      ("dd/MM/yyyy HH:mm:ss.SSSSSS", "29/02/2024 12:34:56.123456", "2024-02-29T20:34:56.123456Z"),
      (
        "dd/MM/yyyy HH:mm:ss.SSSSSS XXX",
        "29/02/2024 12:34:56.123456 +02:00",
        "2024-02-29T10:34:56.123456Z"),
      (
        "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
        "2024-02-29T12:34:56.123456Z",
        "2024-02-29T12:34:56.123456Z"))
    withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "LEGACY") {
      cases.foreach { case (format, input, expected) =>
        val xml = Seq(s"<ROW><stamp>$input</stamp></ROW>").toDS()
        val reader = spark.read
          .option("rowTag", "ROW")
          .option("mode", "FAILFAST")
          .option("timestampFormat", format)
          .option("timeZone", "America/Los_Angeles")
        checkAnswer(reader.xml(xml), Row(Timestamp.from(Instant.parse(expected))))
        checkAnswer(reader.schema(schema).xml(xml), Row(Timestamp.from(Instant.parse(expected))))
      }
    }
  }

  test("SPARK-60199: LEGACY XML rejects invalid ISO timestamps instead of normalizing them") {
    withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "LEGACY") {
      Seq("2024-02-30T12:34:56.123Z", "2024-02-29T25:34:56.123Z", "2024-02-29T12:34:56.123Zjunk")
        .foreach { value =>
          val xml = Seq(s"<ROW><stamp>$value</stamp></ROW>").toDS()
          val reader = spark.read.option("rowTag", "ROW").schema(schema)
          checkAnswer(reader.option("mode", "PERMISSIVE").xml(xml), Row(null))
          intercept[SparkException] {
            reader.option("mode", "FAILFAST").xml(xml).collect()
          }
        }
    }
  }

  test("SPARK-60199: LEGACY XML still honors TIMESTAMP_NTZ") {
    withSQLConf(
      SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "LEGACY",
      SQLConf.TIMESTAMP_TYPE.key -> "TIMESTAMP_NTZ") {
      val xml = Seq("<ROW><stamp>2024-02-29T12:34:56.123456</stamp></ROW>").toDS()
      val result = spark.read.option("rowTag", "ROW").xml(xml)
      assert(result.schema == new StructType().add("stamp", TimestampNTZType))
      checkAnswer(result, Row(LocalDateTime.parse("2024-02-29T12:34:56.123456")))
    }
  }

  test("SPARK-60199: XML expressions preserve microseconds under every parser policy") {
    Seq("LEGACY", "CORRECTED", "EXCEPTION").foreach { policy =>
      withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> policy) {
        val xml = Seq("<ROW><stamp>2024-02-29T12:34:56.123456Z</stamp></ROW>").toDF("xml")
        val result = xml.select(from_xml($"xml", schema).as("parsed")).select("parsed.stamp")
        checkAnswer(result, Row(Timestamp.from(Instant.parse("2024-02-29T12:34:56.123456Z"))))
        assert(spark.conf.get(SQLConf.LEGACY_TIME_PARSER_POLICY.key) == policy)
      }
    }
  }
}
