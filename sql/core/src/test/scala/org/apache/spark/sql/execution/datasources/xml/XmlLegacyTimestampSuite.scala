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
import java.time.{Instant, LocalDateTime, ZoneId}

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.withDefaultTimeZone
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions.{from_xml, struct, to_xml}
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
                result.selectExpr("unix_micros(stamp)"),
                timestamps.map { case (_, expected) =>
                  Row(DateTimeUtils.instantToMicros(Instant.parse(expected)))
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

  test("SPARK-60199: LEGACY XML custom formats retain timezone, locale and pattern semantics") {
    val cases = Seq(
      ("dd/MM/yyyy HH:mm:ss", "29/02/2024 12:34:56", Map.empty[String, String],
        "2024-02-29T20:34:56Z"),
      ("yyyy-MM-dd HH:mm:ss", "2024-02-29 12:34:56", Map.empty[String, String],
        "2024-02-29T20:34:56Z"),
      ("yyyy-MM-dd HH:mm:ss", "2024-02-29 12:34:56", Map("timeZone" -> "+8:00"),
        "2024-02-29T04:34:56Z"),
      ("dd/MM/yyyy HH:mm:ss", "29/02/2024 12:34:56", Map("timeZone" -> "PST"),
        "2024-02-29T20:34:56Z"),
      ("dd MMM yyyy HH:mm:ss", "04 mars 2024 12:34:56", Map("locale" -> "fr-FR"),
        "2024-03-04T20:34:56Z"),
      ("dd/MM/yy HH:mm:ss", "01/01/70 00:00:00", Map("timeZone" -> "UTC"),
        "1970-01-01T00:00:00Z"),
      ("dd/MM/yyyy HH:mm:ss 'Z'", "29/02/2024 12:34:56 Z", Map.empty[String, String],
        "2024-02-29T20:34:56Z"),
      ("yyyy-dd-MM HH:mm:ss", "2024-03-04 12:34:56", Map.empty[String, String],
        "2024-04-03T19:34:56Z"))
    withSQLConf(
      SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "LEGACY",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      for {
        (format, input, extraOptions, expected) <- cases
        mode <- Seq("PERMISSIVE", "FAILFAST")
      } {
        val options = extraOptions ++ Map(
          "rowTag" -> "ROW", "timestampFormat" -> format, "mode" -> mode)
        val xml = Seq(s"<ROW><stamp>$input</stamp></ROW>").toDS()
        val reader = spark.read.options(options)
        val expectedMicros = Row(DateTimeUtils.instantToMicros(Instant.parse(expected)))
        val inferred = reader.xml(xml)
        assert(inferred.schema == schema)
        checkAnswer(inferred.selectExpr("unix_micros(stamp)"), expectedMicros)
        checkAnswer(reader.schema(schema).xml(xml).selectExpr("unix_micros(stamp)"), expectedMicros)
        val parsed = xml.toDF("xml").select(from_xml($"xml", schema, options.asJava).as("parsed"))
        checkAnswer(parsed.selectExpr("unix_micros(parsed.stamp)"), expectedMicros)
      }
    }
  }

  test("SPARK-60199: LEGACY XML local ISO fractions retain custom format timezone semantics") {
    val fractions = Seq(".1", ".000001", ".123456", ".123456789")
    withSQLConf(
        SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "LEGACY",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      for {
        separator <- Seq(" ", "T")
        multiLine <- Seq(false, true)
      } {
        val values = fractions.map(fraction => s"2024-02-29${separator}12:34:56$fraction")
        val rows = values.map(value => s"<ROW><stamp>$value</stamp></ROW>")
        val records = if (multiLine) Seq(rows.mkString("<ROWS>\n", "\n", "\n</ROWS>")) else rows
        withTempPath { path =>
          records.toDS().coalesce(1).write.text(path.getAbsolutePath)
          for {
            fractionPattern <- Seq("", ".SSS", ".SSSSSS")
            zone <- Seq(None, Some("UTC"), Some("+8:00"))
            mode <- Seq("PERMISSIVE", "FAILFAST")
          } {
            val dateTimePattern = if (separator == "T") "yyyy-MM-dd'T'" else "yyyy-MM-dd "
            val options = Map(
              "rowTag" -> "ROW",
              "multiLine" -> multiLine.toString,
              "mode" -> mode,
              "timestampFormat" -> s"${dateTimePattern}HH:mm:ss$fractionPattern") ++
              zone.map("timeZone" -> _)
            val expectedZone = zone.map(DateTimeUtils.getZoneId)
              .getOrElse(ZoneId.of("America/Los_Angeles"))
            val expected = fractions.map { fraction =>
              val instant = LocalDateTime.parse(s"2024-02-29T12:34:56$fraction")
                .atZone(expectedZone).toInstant
              Row(DateTimeUtils.instantToMicros(instant))
            }
            for (infer <- Seq(false, true)) {
              val reader = spark.read.options(options)
              if (!infer) {
                reader.schema(schema)
              }
              val result = reader.xml(path.getAbsolutePath)
              assert(result.schema == schema)
              checkAnswer(result.selectExpr("unix_micros(stamp)"), expected)
            }
            val parsed = rows.toDF("xml")
              .select(from_xml($"xml", schema, options.asJava).as("parsed"))
            checkAnswer(parsed.selectExpr("unix_micros(parsed.stamp)"), expected)
          }
        }
      }
    }
  }

  test("SPARK-60199: LEGACY XML local ISO fractions preserve custom dates and literals") {
    val cases = Seq(
      ("yyyy-dd-MM HH:mm:ss.SSS", "2024-03-04 12:34:56.123456",
        "2024-04-03T19:34:56.123456Z"),
      ("yyyy-dd-MM HH:mm:ss", "2024-03-04 12:34:56.123456",
        "2024-04-03T19:34:56.123456Z"),
      ("yyyy-MM-dd HH:mm:ss.SSS", "1969-12-31 15:59:59.999999",
        "1969-12-31T23:59:59.999999Z"),
      ("yyyy-MM-dd HH:mm:ss.SSSSSS", "1969-12-31 15:59:59.000001",
        "1969-12-31T23:59:59.000001Z"),
      ("yyyy-MM-dd HH:mm:ss'.123456'", "2024-02-29 12:34:56.123456",
        "2024-02-29T20:34:56Z"),
      ("yyyy-MM-dd HH:mm:'ss.SSS'", "2024-02-29 12:34:ss.SSS",
        "2024-02-29T20:34:00Z"))
    withSQLConf(
        SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "LEGACY",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      for {
        (format, input, expected) <- cases
        mode <- Seq("PERMISSIVE", "FAILFAST")
      } {
        val options = Map("rowTag" -> "ROW", "mode" -> mode, "timestampFormat" -> format)
        val xml = Seq(s"<ROW><stamp>$input</stamp></ROW>").toDS()
        val expectedMicros = Row(DateTimeUtils.instantToMicros(Instant.parse(expected)))
        val reader = spark.read.options(options)
        checkAnswer(reader.xml(xml).selectExpr("unix_micros(stamp)"), expectedMicros)
        checkAnswer(reader.schema(schema).xml(xml).selectExpr("unix_micros(stamp)"), expectedMicros)
        val parsed = xml.toDF("xml").select(from_xml($"xml", schema, options.asJava).as("parsed"))
        checkAnswer(parsed.selectExpr("unix_micros(parsed.stamp)"), expectedMicros)
      }
    }
  }

  test("SPARK-60199: LEGACY XML short timezone IDs do not corrupt inferred string fields") {
    withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "LEGACY") {
      val xml = Seq("<ROW><label>example</label></ROW>").toDS()
      for (zone <- Seq("PST", "+8:00"); mode <- Seq("PERMISSIVE", "FAILFAST")) {
        val result = spark.read.option("rowTag", "ROW")
          .option("timestampFormat", "dd/MM/yyyy HH:mm:ss")
          .option("timeZone", zone).option("mode", mode).xml(xml)
        assert(result.schema == new StructType().add("label", StringType))
        checkAnswer(result, Row("example"))
      }
    }
  }

  test("SPARK-60199: LEGACY XML historical instants do not depend on the JVM timezone") {
    withSQLConf(
      SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "LEGACY",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      for (zone <- Seq("UTC", "America/Los_Angeles")) {
        withDefaultTimeZone(ZoneId.of(zone)) {
          for (year <- Seq("1500", "1800", "2024")) {
            val value = s"$year-01-01T12:34:56.123456Z"
            val expected = Row(DateTimeUtils.instantToMicros(Instant.parse(value)))
            val xml = Seq(s"<ROW><stamp>$value</stamp></ROW>").toDS()
            val parsed = spark.read.option("rowTag", "ROW").schema(schema).xml(xml)
            checkAnswer(parsed.selectExpr("unix_micros(stamp)"), expected)
            val writeOptions = Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
            val written = parsed.select(to_xml(struct($"stamp"), writeOptions.asJava).as("xml"))
            val roundTrip = written.select(from_xml($"xml", schema).as("parsed"))
            checkAnswer(roundTrip.selectExpr("unix_micros(parsed.stamp)"), expected)
          }
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
