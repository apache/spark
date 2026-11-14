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
  private val captureKey = SQLConf.JSON_CSV_EXPRESSION_USE_CAPTURED_OPTION_DEFAULTS.key

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

  test("to_json preserves explicit options next to ROOT-distinct Unicode keys") {
    val keys = Seq(s"${0x0131.toChar}gnoreNullFields", s"${0x0130.toChar}gnoreNullFields",
      s"ignoreNullField${0x017f.toChar}")
    withSQLConf(SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key -> "true") {
      for (key <- keys; reverse <- Seq(false, true)) {
        val entries = Seq("ignoreNullFields" -> "false", key -> "true")
        val options = (if (reverse) entries.reverse else entries).toMap
        checkEvaluation(StructsToJson(options, struct).withTimeZone("UTC"),
          """{"id":1,"n":null}""", create_row(1L))
      }
    }
  }

  test("ineffective from_json defaults preserve conversion sharing") {
    val input = BoundReference(0, StringType, nullable = false)
    val schema = new StructType().add("ts", TimestampType)
    val explicit = JsonToStructs(schema,
      Map("EnAbLeDaTeTiMePaRsInGfAlLbAcK" -> "false"), input, Some("UTC"))
    val other = explicit.copy(enableDateTimeParsingFallback = Some(true))
    val first = explicit.copy(enableDateTimeParsingFallback = Some(false))
    assert(first.semanticEquals(other))
    val equivalents = new EquivalentExpressions
    equivalents.addExprTree(first)
    equivalents.addExprTree(other)
    assert(equivalents.getCommonSubexpressions.size == 1)

    val independentTypes = Seq(IntegerType, TimestampNTZType, new ExampleBaseTypeUDT)
    independentTypes.foreach { dataType =>
      val nested = new StructType().add("v", ArrayType(MapType(StringType, dataType)))
      val parser = JsonToStructs(nested, Map.empty, input, Some("UTC"))
      assert(parser.copy(enableDateTimeParsingFallback = Some(true)).semanticEquals(
        parser.copy(enableDateTimeParsingFallback = Some(false))))
    }

    class TimestampUDT extends UserDefinedType[java.sql.Timestamp] {
      override def sqlType: DataType = TimestampType
      override def serialize(value: java.sql.Timestamp): Any =
        DateTimeUtils.fromJavaTimestamp(value)
      override def deserialize(value: Any): java.sql.Timestamp =
        DateTimeUtils.toJavaTimestamp(value.asInstanceOf[Long])
      override def userClass: Class[java.sql.Timestamp] = classOf[java.sql.Timestamp]
    }
    Seq(DateType, TimestampType, new TimestampUDT).foreach { dataType =>
      val nested = new StructType().add("v", ArrayType(MapType(StringType, dataType)))
      val parser = JsonToStructs(nested, Map.empty, input, Some("UTC"))
      assert(!parser.copy(enableDateTimeParsingFallback = Some(true)).semanticEquals(
        parser.copy(enableDateTimeParsingFallback = Some(false))))
    }
  }

  test("to_csv shares explicit overrides but keeps missing and null defaults distinct") {
    val expression = StructsToCsv(Map("TrEaTnUlLaSeMpTyStRiNg" -> "false"), struct, Some("UTC"))
    assert(expression.copy(nullValueWrittenAsQuotedEmptyString = true).semanticEquals(
      expression.copy(nullValueWrittenAsQuotedEmptyString = false)))
    val nonEmptyNullValue = expression.copy(options = Map("nullValue" -> "NULL"))
    assert(nonEmptyNullValue.copy(nullValueWrittenAsQuotedEmptyString = true).semanticEquals(
      nonEmptyNullValue.copy(nullValueWrittenAsQuotedEmptyString = false)))
    Seq(Map.empty[String, String], Map("treatNullAsEmptyString" -> null)).foreach { options =>
      val configured = expression.copy(options = options)
      assert(!configured.copy(nullValueWrittenAsQuotedEmptyString = true).semanticEquals(
        configured.copy(nullValueWrittenAsQuotedEmptyString = false)))
    }
    // Comparing invalid options must not initialize a writer or raise its validation error.
    expression.copy(options = Map("treatNullAsEmptyString" -> "invalid")).canonicalized
    JsonToStructs(new StructType().add("ts", TimestampType),
      Map("enableDateTimeParsingFallback" -> "invalid"),
      BoundReference(0, StringType, nullable = false), Some("UTC")).canonicalized
  }

  test("conversion sharing respects the binding mode only when it can affect results") {
    val parser = JsonToStructs(new StructType().add("ts", TimestampType), Map.empty,
      BoundReference(0, StringType, nullable = false), Some("UTC"))
    assert(!parser.copy(useCapturedOptionDefaults = true).semanticEquals(
      parser.copy(useCapturedOptionDefaults = false)))
    val legacy = parser.copy(useCapturedOptionDefaults = false)
    assert(legacy.copy(enableDateTimeParsingFallback = Some(true)).semanticEquals(
      legacy.copy(enableDateTimeParsingFallback = Some(false))))
    val explicit = parser.copy(options = Map("enableDateTimeParsingFallback" -> "false"))
    assert(explicit.copy(useCapturedOptionDefaults = true).semanticEquals(
      explicit.copy(useCapturedOptionDefaults = false)))

    val csv = StructsToCsv(Map.empty, struct, Some("UTC"))
    assert(!csv.copy(useCapturedOptionDefaults = true).semanticEquals(
      csv.copy(useCapturedOptionDefaults = false)))
    val csvLegacy = csv.copy(useCapturedOptionDefaults = false)
    assert(csvLegacy.copy(nullValueWrittenAsQuotedEmptyString = true).semanticEquals(
      csvLegacy.copy(nullValueWrittenAsQuotedEmptyString = false)))
    val csvExplicit = csv.copy(options = Map("treatNullAsEmptyString" -> "true"))
    assert(csvExplicit.copy(useCapturedOptionDefaults = true).semanticEquals(
      csvExplicit.copy(useCapturedOptionDefaults = false)))
  }

  for (creatorValue <- Seq(true, false)) {
    test(s"rollback reads defaults after early evaluator creation: $creatorValue") {
      val jsonKey = SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key
      val csvKey = SQLConf.LEGACY_NULL_VALUE_WRITTEN_AS_QUOTED_EMPTY_STRING_CSV.key
      val fallbackKey = SQLConf.LEGACY_JSON_ENABLE_DATE_TIME_PARSING_FALLBACK.key
      withSQLConf(captureKey -> "false", jsonKey -> creatorValue.toString,
          csvKey -> creatorValue.toString, fallbackKey -> creatorValue.toString,
          SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "CORRECTED") {
        val json = StructsToJson(Map.empty, struct).withTimeZone("UTC")
          .asInstanceOf[StructsToJson].replacement
        val csv = StructsToCsv(Map.empty, struct).withTimeZone("UTC").asInstanceOf[StructsToCsv]
        val parser = JsonToStructs(new StructType().add("ts", TimestampType),
          Map("timestampFormat" -> "yyyy/MM/dd HH:mm:ss"),
          BoundReference(0, StringType, nullable = false), Some("UTC"))
        // This creates the evaluator but leaves its lazy parser uninitialized.
        assert(parser.nullSafeEval(null) == null)
        withSQLConf(captureKey -> "true", jsonKey -> (!creatorValue).toString,
            csvKey -> (!creatorValue).toString, fallbackKey -> (!creatorValue).toString) {
          val jsonExpected = if (creatorValue) """{"id":1,"n":null}""" else """{"id":1}"""
          checkEvaluation(json, jsonExpected, create_row(1L))
          checkEvaluation(csv, if (creatorValue) "1," else "1,\"\"", create_row(1L))
          checkEvaluation(csv.copy(options = Map("treatNullAsEmptyString" -> null)),
            if (creatorValue) "1," else "1,\"\"", create_row(1L))
          val timestamp = if (creatorValue) null else {
            DateTimeUtils.localDateTimeToMicros(LocalDateTime.of(2024, 1, 2, 0, 0))
          }
          checkEvaluation(parser, create_row(timestamp), create_row("""{"ts":"2024-01-02"}"""))
        }
      }
    }
  }
}
