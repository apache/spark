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

import org.apache.spark.sql.catalyst.expressions.{EquivalentExpressions, Expression, JsonToStructs, StructsToCsv}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class JsonCsvOptionsSQLConfSuite extends QueryTest with SharedSparkSession {
  private val captureKey = SQLConf.JSON_CSV_EXPRESSION_USE_CAPTURED_OPTION_DEFAULTS.key
  private val fallbackKey = SQLConf.LEGACY_JSON_ENABLE_DATE_TIME_PARSING_FALLBACK.key

  private def withFallbackConf(value: Option[Boolean])(f: => Unit): Unit =
    withBooleanConf(fallbackKey, value)(f)

  private def withBooleanConf(key: String, value: Option[Boolean])(f: => Unit): Unit = {
    val saved = spark.sessionState.conf.getAllConfs.get(key)
    def setValue(value: Option[String]): Unit = value match {
      case Some(v) => spark.conf.set(key, v)
      case None => spark.conf.unset(key)
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
    customDateFormat <- Seq(true, false)
    customTimestampFormat <- Seq(true, false)
    policy <- (if (customDateFormat || customTimestampFormat) {
      Seq("CORRECTED", "LEGACY")
    } else {
      Seq("CORRECTED")
    })
  } {
    test(s"$kind captures from_json fallback $creatorValue, " +
      s"custom date: $customDateFormat, timestamp: $customTimestampFormat, policy: $policy") {
      // Without a custom date format, unset fallback accepts legacy epoch-day strings.
      // This distinguishes unset (enabled by default) from an explicit false.
      val dateInput = if (customDateFormat) "2024-01-02" else "1"
      val input = s"""concat('{"id":', id, ',"d":"$dateInput","t":"2024-01-02"}')"""
      val schema = "id BIGINT, d DATE, t TIMESTAMP"
      val formats = (if (customDateFormat) Seq("'dateFormat'", "'yyyy/MM/dd'") else Nil) ++
        (if (customTimestampFormat) {
          Seq("'timestampFormat'", "'yyyy/MM/dd HH:mm:ss'")
        } else Nil)
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
      def result(id: Int, dateFallback: Boolean, timestampFallback: Boolean): Row = {
        val date = if (dateFallback) {
          Date.valueOf(if (customDateFormat) "2024-01-02" else "1970-01-02")
        } else {
          null
        }
        val timestamp = if ((!customTimestampFormat && policy != "LEGACY") || timestampFallback) {
          Timestamp.from(Instant.parse("2024-01-02T00:00:00Z"))
        } else {
          null
        }
        Row(id.toLong, date, timestamp)
      }
      val expected = (1 to 2).map { id =>
        val dateFallback = creatorValue.getOrElse(policy == "LEGACY" || !customDateFormat)
        val timestampFallback =
          creatorValue.getOrElse(policy == "LEGACY" || !customTimestampFormat)
        Row(Row(result(id, dateFallback, timestampFallback),
          result(id, true, true), result(id, false, false)))
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

  for (format <- Seq("json", "csv")) {
    test(s"SQL functions share $format conversions with identical explicit options") {
      val key = if (format == "json") fallbackKey else {
        SQLConf.LEGACY_NULL_VALUE_WRITTEN_AS_QUOTED_EMPTY_STRING_CSV.key
      }
      val body = if (format == "json") {
        """from_json(concat('{"id":', id, '}'), 'id BIGINT, ts TIMESTAMP',
          |map('enableDateTimeParsingFallback', 'false'))""".stripMargin
      } else {
        """to_csv(named_struct('id', id, 'n', CAST(NULL AS INT)),
          |map('treatNullAsEmptyString', 'false'))""".stripMargin
      }
      val returnType = if (format == "json") "STRUCT<id: BIGINT, ts: TIMESTAMP>" else "STRING"
      withUserDefinedFunction("captured_fn_1" -> false, "captured_fn_2" -> false) {
        Seq("captured_fn_1" -> true, "captured_fn_2" -> false).foreach { case (name, value) =>
          withSQLConf(key -> value.toString) {
            sql(s"CREATE FUNCTION $name(id BIGINT) RETURNS $returnType RETURN $body")
          }
        }
        val result = sql("SELECT captured_fn_1(id), captured_fn_2(id) FROM range(1, 3)")
        val conversions = result.queryExecution.optimizedPlan.flatMap { plan =>
          plan.expressions.flatMap(_.collect {
            case j: JsonToStructs => j: Expression
            case c: StructsToCsv => c: Expression
          })
        }
        assert(conversions.size == 2)
        val equivalents = new EquivalentExpressions
        conversions.foreach(equivalents.addExprTree(_))
        assert(equivalents.getCommonSubexpressions.exists(_.semanticEquals(conversions.head)))
        val expected = (1 to 2).map { id =>
          if (format == "json") Row(Row(id.toLong, null), Row(id.toLong, null))
          else Row(s"$id,", s"$id,")
        }
        checkAnswer(result, expected)
      }
    }
  }

  for {
    kind <- Seq("view", "function")
    format <- Seq("json", "csv")
    creatorValue <- Seq(true, false)
    overlays <- Seq(true, false)
  } {
    test(s"$kind can restore lazy to_$format defaults: $creatorValue, overlays: $overlays") {
      val key = if (format == "json") SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key else {
        SQLConf.LEGACY_NULL_VALUE_WRITTEN_AS_QUOTED_EMPTY_STRING_CSV.key
      }
      withSQLConf(captureKey -> "true", key -> creatorValue.toString) {
        withDefinition(kind, s"to_$format(named_struct('id', id, 'n', CAST(NULL AS INT)))",
            "STRING") { query =>
          withSQLConf(captureKey -> "false", key -> (!creatorValue).toString,
              SQLConf.APPLY_SESSION_CONF_OVERRIDES_TO_FUNCTION_RESOLUTION.key ->
                overlays.toString) {
            val expected = (1 to 2).map { id =>
              if (format == "json") {
                Row(if (creatorValue) s"""{"id":$id,"n":null}""" else s"""{"id":$id}""")
              } else {
                Row(if (creatorValue) s"$id," else s"$id,\"\"")
              }
            }
            checkAnswer(sql(query), expected)
          }
        }
      }
    }
  }

  for (kind <- Seq("view", "function"); creatorValue <- Seq(None, Some(true), Some(false))) {
    test(s"$kind rollback retains live unset/true/false JSON fallback: $creatorValue") {
      val input = """concat('{"id":', id, ',"d":"1","t":"2024-01-02"}')"""
      val schema = "id BIGINT, d DATE, t TIMESTAMP"
      def parsed(explicit: Option[Boolean]): String = {
        val option = explicit.map(value => s", 'enableDateTimeParsingFallback', '$value'")
          .getOrElse("")
        s"from_json($input, '$schema', map('timestampFormat', 'yyyy/MM/dd HH:mm:ss'$option))"
      }
      val body = s"named_struct('configured', ${parsed(None)}, " +
        s"'enabled', ${parsed(Some(true))}, 'disabled', ${parsed(Some(false))})"
      val parsedType = "STRUCT<id: BIGINT, d: DATE, t: TIMESTAMP>"
      val returnType =
        s"STRUCT<configured: $parsedType, enabled: $parsedType, disabled: $parsedType>"
      def result(id: Int, fallback: Option[Boolean]): Row = {
        val date = if (fallback.contains(false)) null else Date.valueOf("1970-01-02")
        val timestamp = if (fallback.contains(true)) {
          Timestamp.from(Instant.parse("2024-01-02T00:00:00Z"))
        } else null
        Row(id.toLong, date, timestamp)
      }
      withSQLConf(captureKey -> "true", SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "CORRECTED",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        withFallbackConf(creatorValue) {
          withDefinition(kind, body, returnType) { query =>
            for (reader <- Seq(None, Some(true), Some(false)); overlays <- Seq(true, false)) {
              withSQLConf(captureKey -> "false",
                  SQLConf.APPLY_SESSION_CONF_OVERRIDES_TO_FUNCTION_RESOLUTION.key ->
                    overlays.toString) {
                withFallbackConf(reader) {
                  val expected = (1 to 2).map { id =>
                    Row(Row(result(id, reader), result(id, Some(true)), result(id, Some(false))))
                  }
                  checkAnswer(sql(query), expected)
                }
              }
            }
          }
        }
      }
    }
  }

  for (kind <- Seq("view", "function"); savedMode <- Seq(true, false)) {
    test(s"$kind nested definitions follow the caller's effective binding mode: $savedMode") {
      val jsonKey = SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key
      val body = "to_json(named_struct('id', id, 'n', CAST(NULL AS INT)))"
      def check(query: String): Unit = {
        for (mode <- Seq(None, Some(true), Some(false)); overlays <- Seq(true, false)) {
          withSQLConf(jsonKey -> "false",
              SQLConf.APPLY_SESSION_CONF_OVERRIDES_TO_FUNCTION_RESOLUTION.key ->
                overlays.toString) {
            withBooleanConf(captureKey, mode) {
              val expected = (1 to 2).map { id =>
                Row(if (mode.contains(false)) s"""{"id":$id,"n":null}""" else s"""{"id":$id}""")
              }
              checkAnswer(sql(query), expected)
            }
          }
        }
      }
      withSQLConf(jsonKey -> "true", captureKey -> savedMode.toString) {
        if (kind == "view") {
          withView("captured_inner", "captured_outer") {
            sql(s"CREATE VIEW captured_inner AS SELECT $body AS value FROM range(1, 3)")
            withSQLConf(captureKey -> (!savedMode).toString) {
              sql("CREATE VIEW captured_outer AS SELECT value FROM captured_inner")
            }
            check("SELECT value FROM captured_outer")
          }
        } else {
          withUserDefinedFunction("captured_inner" -> false, "captured_outer" -> false) {
            sql(s"CREATE FUNCTION captured_inner(id BIGINT) RETURNS STRING RETURN $body")
            withSQLConf(captureKey -> (!savedMode).toString) {
              sql("CREATE FUNCTION captured_outer(id BIGINT) RETURNS STRING " +
                "RETURN captured_inner(id)")
            }
            check("SELECT captured_outer(id) FROM range(1, 3)")
          }
        }
      }
    }
  }
}
