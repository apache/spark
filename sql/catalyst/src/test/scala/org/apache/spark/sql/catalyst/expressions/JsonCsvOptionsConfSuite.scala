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

package org.apache.spark.sql.catalyst.expressions

import java.time.LocalDateTime

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class JsonCsvOptionsConfSuite extends SparkFunSuite with ExpressionEvalHelper {
  private val struct = CreateNamedStruct(Seq(Literal("id"),
    BoundReference(0, LongType, nullable = false), Literal("n"), Literal.create(null, IntegerType)))

  for (ignoreNulls <- Seq(true, false)) {
    test(s"to_json captures ignoreNullFields=$ignoreNulls across copies and serialization") {
      val key = SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key
      val expression = withSQLConf(key -> ignoreNulls.toString) {
        StructsToJson(Map.empty, struct)
      }
      withSQLConf(key -> (!ignoreNulls).toString) {
        val copied = expression.copy().withNewChildren(Seq(struct)).asInstanceOf[StructsToJson]
        val expected = if (ignoreNulls) """{"id":1}""" else """{"id":1,"n":null}"""
        checkEvaluation(copied.withTimeZone("UTC"), expected, create_row(1L))
      }
    }
  }

  for (quoteNulls <- Seq(true, false)) {
    test(s"to_csv captures treatNullAsEmptyString=$quoteNulls across copies and serialization") {
      val key = SQLConf.LEGACY_NULL_VALUE_WRITTEN_AS_QUOTED_EMPTY_STRING_CSV.key
      val expression = withSQLConf(key -> quoteNulls.toString) {
        StructsToCsv(Map.empty, struct)
      }
      withSQLConf(key -> (!quoteNulls).toString) {
        val copied = expression.copy().withNewChildren(Seq(struct)).asInstanceOf[StructsToCsv]
        val expected = if (quoteNulls) "1,\"\"" else "1,"
        checkEvaluation(copied.withTimeZone("UTC"), expected, create_row(1L))
      }
    }
  }

  for (quoteNulls <- Seq(true, false)) {
    test(s"to_csv captures the default for an explicit null option: $quoteNulls") {
      val key = SQLConf.LEGACY_NULL_VALUE_WRITTEN_AS_QUOTED_EMPTY_STRING_CSV.key
      val expression = withSQLConf(key -> quoteNulls.toString) {
        StructsToCsv(Map("TrEaTnUlLaSeMpTyStRiNg" -> null), struct)
      }
      withSQLConf(key -> (!quoteNulls).toString) {
        val expected = if (quoteNulls) "1,\"\"" else "1,"
        checkEvaluation(expression.withTimeZone("UTC"), expected, create_row(1L))
      }
    }
  }

  for (fallback <- Seq(None, Some(true), Some(false))) {
    test(s"from_json captures parsing fallback=$fallback across copies and serialization") {
      val key = SQLConf.LEGACY_JSON_ENABLE_DATE_TIME_PARSING_FALLBACK.key
      withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "CORRECTED", key -> "false") {
        fallback match {
          case Some(value) => SQLConf.get.setConfString(key, value.toString)
          case None => SQLConf.get.unsetConf(key)
        }
        val schema = new StructType().add("ts", TimestampType)
        val expression = JsonToStructs(schema,
          Map("timestampFormat" -> "yyyy/MM/dd HH:mm:ss"),
          BoundReference(0, StringType, nullable = false))
        withSQLConf(key -> (!fallback.contains(true)).toString) {
          val copied = expression.copy().withNewChildren(expression.children)
            .asInstanceOf[JsonToStructs]
          val expected = if (fallback.contains(true)) {
            create_row(DateTimeUtils.localDateTimeToMicros(LocalDateTime.of(2024, 1, 2, 0, 0)))
          } else {
            create_row(null)
          }
          checkEvaluation(copied.withTimeZone("UTC"), expected,
            create_row("""{"ts":"2024-01-02"}"""))
        }
      }
    }
  }
}
