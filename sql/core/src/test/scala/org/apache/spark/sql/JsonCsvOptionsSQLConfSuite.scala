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

package org.apache.spark.sql

import java.sql.{Date, Timestamp}
import java.time.Instant

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class JsonCsvOptionsSQLConfSuite extends QueryTest with SharedSparkSession {
  private val fallbackKey = SQLConf.LEGACY_JSON_ENABLE_DATE_TIME_PARSING_FALLBACK.key

  private def withFallbackConf(value: Option[Boolean])(f: => Unit): Unit = {
    val saved = spark.conf.getOption(fallbackKey)
    def setValue(value: Option[String]): Unit = value match {
      case Some(v) => spark.conf.set(fallbackKey, v)
      case None => spark.conf.unset(fallbackKey)
    }
    try {
      setValue(value.map(_.toString))
      f
    } finally {
      setValue(saved)
    }
  }

  private def withDefinition(
      kind: String,
      body: String,
      returnType: String)(f: String => Unit): Unit = {
    if (kind == "view") {
      withView("captured_options_view") {
        sql(s"CREATE VIEW captured_options_view AS SELECT $body AS value FROM range(1, 3)")
        f("SELECT value FROM captured_options_view")
      }
    } else {
      withUserDefinedFunction("captured_options_fn" -> false) {
        sql(s"CREATE FUNCTION captured_options_fn(id BIGINT) RETURNS $returnType RETURN $body")
        f("SELECT captured_options_fn(id) AS value FROM range(1, 3)")
      }
    }
  }

  for {
    kind <- Seq("view", "function")
    format <- Seq("json", "csv")
    creatorValue <- Seq(true, false)
  } {
    test(s"$kind captures to_$format defaults: $creatorValue, with explicit option precedence") {
      val (key, option) = if (format == "json") {
        (SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key, "IgNoReNuLlFiElDs")
      } else {
        (SQLConf.LEGACY_NULL_VALUE_WRITTEN_AS_QUOTED_EMPTY_STRING_CSV.key,
          "TrEaTnUlLaSeMpTyStRiNg")
      }
      val input = "named_struct('id', id, 'n', CAST(NULL AS INT))"
      val body = s"""named_struct(
                    |  'configured', to_$format($input),
                    |  'enabled', to_$format($input, map('$option', 'true')),
                    |  'disabled', to_$format($input, map('$option', 'false')))""".stripMargin
      def rendered(id: Int, enabled: Boolean): String = {
        if (format == "json") {
          if (enabled) s"""{"id":$id}""" else s"""{"id":$id,"n":null}"""
        } else {
          if (enabled) s"$id,\"\"" else s"$id,"
        }
      }
      val expected = (1 to 2).map { id =>
        Row(Row(rendered(id, creatorValue), rendered(id, true), rendered(id, false)))
      }
      val returnType = "STRUCT<configured: STRING, enabled: STRING, disabled: STRING>"
      withSQLConf(key -> creatorValue.toString) {
        withDefinition(kind, body, returnType) { query =>
          // Analyze a new query for each reader, rather than reuse the creator's DataFrame.
          Seq(creatorValue, !creatorValue).foreach { readerValue =>
            withSQLConf(key -> readerValue.toString) {
              checkAnswer(sql(query), expected)
            }
          }
        }
      }
    }
  }

  for {
    kind <- Seq("view", "function")
    creatorValue <- Seq(None, Some(true), Some(false))
    customFormats <- Seq(true, false)
    policy <- (if (customFormats) Seq("CORRECTED", "LEGACY") else Seq("CORRECTED"))
  } {
    test(s"$kind captures from_json fallback $creatorValue, " +
      s"custom formats: $customFormats, policy: $policy") {
      // Without a custom date format, unset fallback accepts legacy epoch-day strings.
      // This distinguishes unset (enabled by default) from an explicit false.
      val dateInput = if (customFormats) "2024-01-02" else "1"
      val input = s"""concat('{"id":', id, ',"d":"$dateInput","t":"2024-01-02"}')"""
      val schema = "id BIGINT, d DATE, t TIMESTAMP"
      val formats = if (customFormats) {
        Seq("'dateFormat'", "'yyyy/MM/dd'", "'timestampFormat'", "'yyyy/MM/dd HH:mm:ss'")
      } else {
        Nil
      }
      def parsed(explicit: Option[Boolean]): String = {
        val options = formats ++ explicit.toSeq.flatMap { enabled =>
          Seq("'EnAbLeDaTeTiMePaRsInGfAlLbAcK'", s"'$enabled'")
        }
        val argument = if (options.isEmpty) "" else s", map(${options.mkString(", ")})"
        s"from_json($input, '$schema'$argument)"
      }
      val body = s"""named_struct(
                    |  'configured', ${parsed(None)},
                    |  'enabled', ${parsed(Some(true))},
                    |  'disabled', ${parsed(Some(false))})""".stripMargin
      val parsedType = "STRUCT<id: BIGINT, d: DATE, t: TIMESTAMP>"
      val returnType =
        s"STRUCT<configured: $parsedType, enabled: $parsedType, disabled: $parsedType>"
      def result(id: Int, enabled: Boolean): Row = {
        val date = if (enabled) {
          Date.valueOf(if (customFormats) "2024-01-02" else "1970-01-02")
        } else {
          null
        }
        val timestamp = if (!customFormats || enabled) {
          Timestamp.from(Instant.parse("2024-01-02T00:00:00Z"))
        } else {
          null
        }
        Row(id.toLong, date, timestamp)
      }
      val expected = (1 to 2).map { id =>
        val defaultFallback = policy == "LEGACY" || !customFormats
        Row(Row(result(id, creatorValue.getOrElse(defaultFallback)),
          result(id, true), result(id, false)))
      }
      withSQLConf(
          SQLConf.LEGACY_TIME_PARSER_POLICY.key -> policy,
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        withFallbackConf(creatorValue) {
          withDefinition(kind, body, returnType) { query =>
            Seq(None, Some(true), Some(false)).foreach { readerValue =>
              withFallbackConf(readerValue) {
                checkAnswer(sql(query), expected)
              }
            }
          }
        }
      }
    }
  }
}
