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
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class JsonCsvOptionsConfSuite extends SparkFunSuite with ExpressionEvalHelper {
  private val captureKey = SQLConf.JSON_CSV_EXPRESSION_USE_CAPTURED_OPTION_DEFAULTS.key

  private val struct = CreateNamedStruct(Seq(Literal("id"),
    BoundReference(0, LongType, nullable = false), Literal("n"), Literal.create(null, IntegerType)))

  private def assertShared(first: Expression, second: Expression): Unit = {
    assert(first.semanticEquals(second))
    assert(first.semanticHash() == second.semanticHash())
    val equivalents = new EquivalentExpressions
    equivalents.addExprTree(first)
    equivalents.addExprTree(second)
    assert(equivalents.getCommonSubexpressions.exists(_.semanticEquals(first)))
  }

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
    assert(!nonEmptyNullValue.copy(nullValueWrittenAsQuotedEmptyString = true).semanticEquals(
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

  test("CSV null markers preserve result-affecting captured defaults and modes") {
    val input = CreateNamedStruct(Seq(Literal("n"), Literal.create(null, StringType),
      Literal("x"), BoundReference(0, StringType, nullable = false)))
    for (marker <- Seq(" ", "\"\"")) {
      val options = Map("nullValue" -> marker)
      val quoted = StructsToCsv(options, input, Some("UTC"),
        nullValueWrittenAsQuotedEmptyString = true)
      val unquoted = quoted.copy(nullValueWrittenAsQuotedEmptyString = false)
      assert(quoted.eval(create_row("x")) != unquoted.eval(create_row("x")))
      assert(!quoted.semanticEquals(unquoted))
      assert(!quoted.semanticEquals(quoted.copy(useCapturedOptionDefaults = false)))
    }
    checkEvaluation(StructsToCsv(Map("nullValue" -> " "), input, Some("UTC"),
      nullValueWrittenAsQuotedEmptyString = true), "\"\",x", create_row("x"))
    checkEvaluation(StructsToCsv(Map("nullValue" -> " "), input, Some("UTC"),
      nullValueWrittenAsQuotedEmptyString = false), ",x", create_row("x"))
  }

  test("CSV conversions share when every top-level field is nonnullable") {
    val schemas = Seq(new StructType().add("id", LongType, nullable = false),
      new StructType().add("nested", ArrayType(IntegerType), nullable = false))
    schemas.foreach { schema =>
      val csv = StructsToCsv(Map.empty, BoundReference(0, schema, nullable = false), Some("UTC"))
      assertShared(csv.copy(nullValueWrittenAsQuotedEmptyString = true),
        csv.copy(nullValueWrittenAsQuotedEmptyString = false))
      assertShared(csv.copy(useCapturedOptionDefaults = true),
        csv.copy(useCapturedOptionDefaults = false))
    }
    // Identity checks must remain safe before resolution/type checking and option validation.
    StructsToCsv(Map.empty, UnresolvedAttribute("s")).canonicalized
    StructsToCsv(Map.empty, BoundReference(0, IntegerType, nullable = true)).canonicalized
    StructsToCsv(Map("quoteAll" -> "invalid"),
      BoundReference(0, schemas.head, nullable = false)).canonicalized
  }

  test("format-free captured unset and true share parsing but false remains distinct") {
    for (policy <- Seq("CORRECTED", "LEGACY", "EXCEPTION")) {
      withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> policy) {
        val parser = JsonToStructs(new StructType().add("d", DateType).add("ts", TimestampType),
          Map.empty, BoundReference(0, StringType, nullable = false), Some("UTC"),
          enableDateTimeParsingFallback = None)
        assertShared(parser, parser.copy(enableDateTimeParsingFallback = Some(true)))
        assert(!parser.semanticEquals(parser.copy(enableDateTimeParsingFallback = Some(false))))
        assert(!parser.semanticEquals(parser.copy(useCapturedOptionDefaults = false)))
        for (key <- Seq("DaTeFoRmAt", "TiMeStAmPfOrMaT"); value <- Seq(null, "invalid")) {
          val configured = parser.copy(options = Map(key -> value))
          assert(!configured.semanticEquals(
            configured.copy(enableDateTimeParsingFallback = Some(true))))
        }
      }
    }
    withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "CORRECTED") {
      val parser = JsonToStructs(new StructType().add("d", DateType), Map.empty,
        BoundReference(0, StringType, nullable = false), Some("UTC"),
        enableDateTimeParsingFallback = None)
      checkEvaluation(parser, create_row(1), create_row("""{"d":"1"}"""))
      checkEvaluation(parser.copy(enableDateTimeParsingFallback = Some(false)),
        create_row(null), create_row("""{"d":"1"}"""))
    }
  }

  test("JSON arrays and maps without structs share serialization across captured defaults") {
    val types = Seq(ArrayType(LongType), MapType(StringType, LongType),
      ArrayType(MapType(StringType, ArrayType(LongType))))
    types.foreach { dataType =>
      val json = StructsToJson(Map.empty,
        BoundReference(0, dataType, nullable = false), Some("UTC"))
      assertShared(json.copy(ignoreNullFields = true).replacement,
        json.copy(ignoreNullFields = false).replacement)
      assertShared(json.copy(useCapturedOptionDefaults = true).replacement,
        json.copy(useCapturedOptionDefaults = false).replacement)
    }
    val withFields = Seq(ArrayType(new StructType().add("n", LongType)),
      MapType(StringType, new StructType().add("n", LongType)),
      ArrayType(new ExampleBaseTypeUDT))
    withFields.foreach { dataType =>
      val json = StructsToJson(Map.empty,
        BoundReference(0, dataType, nullable = false), Some("UTC"))
      assert(!json.copy(ignoreNullFields = true).replacement.semanticEquals(
        json.copy(ignoreNullFields = false).replacement))
    }
    for (ignore <- Seq(true, false)) {
      checkEvaluation(StructsToJson(Map.empty,
        BoundReference(0, ArrayType(LongType), nullable = false), Some("UTC"),
        ignoreNullFields = ignore), "[1,null]", create_row(Seq[java.lang.Long](1L, null)))
      checkEvaluation(StructsToJson(Map.empty,
        BoundReference(0, MapType(StringType, LongType), nullable = false), Some("UTC"),
        ignoreNullFields = ignore), """{"a":null}""", create_row(create_map("a" -> null)))
    }
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
          val input = create_row("""{"ts":"2024-01-02"}""")
          // Preserve the exact evaluator created above, before testing fresh serialized copies.
          checkEvaluationWithoutCodegen(parser, create_row(timestamp), input)
          checkEvaluation(parser, create_row(timestamp), input)
        }
      }
    }
  }
}
