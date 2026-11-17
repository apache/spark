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

import java.time.{LocalDate, LocalDateTime}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
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

  for (wrapped <- Seq(false, true)) {
    test(s"to_json prepares options with at most one normalization: wrapped=$wrapped") {
      val normalizations = new AtomicInteger
      class CountingMap[V](delegate: Map[String, V]) extends Map[String, V] {
        override def get(key: String): Option[V] = delegate.get(key)
        override def iterator: Iterator[(String, V)] = delegate.iterator
        override def removed(key: String): Map[String, V] =
          new CountingMap(delegate.removed(key))
        override def updated[V1 >: V](key: String, value: V1): Map[String, V1] =
          new CountingMap(delegate.updated(key, value))
        override def filter(p: ((String, V)) => Boolean): Map[String, V] =
          new CountingMap(delegate.filter(p))
        override def filterNot(p: ((String, V)) => Boolean): Map[String, V] =
          new CountingMap(delegate.filterNot(p))
        override def map[K2, V2](f: ((String, V)) => (K2, V2)): Map[K2, V2] = {
          normalizations.incrementAndGet()
          delegate.map(f)
        }
      }
      for (mode <- Seq(true, false); explicit <- Seq(None, Some(true), Some(false))) {
        val entries = (0 until 32).map(i => s"unused$i" -> i.toString).toMap ++
          explicit.map(v => "IgNoReNuLlFiElDs" -> v.toString)
        val raw = new CountingMap(entries)
        val options = if (wrapped) CaseInsensitiveMap(raw) else raw
        val before = normalizations.get()
        val expression = StructsToJson(options, struct, Some("UTC"),
          ignoreNullFields = false, useCapturedOptionDefaults = mode)
        withSQLConf(SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key -> "true") {
          val ignored = explicit.getOrElse(!mode)
          val expected = if (ignored) """{"id":1}""" else """{"id":1,"n":null}"""
          assert(expression.replacement.eval(create_row(1L)).toString == expected)
        }
        val limit = if (wrapped && (!mode || explicit.isDefined)) 0 else 1
        assert(normalizations.get() - before <= limit)
      }
    }
  }

  for (wrapped <- Seq(false, true); mode <- Seq(true, false)) {
    test(s"to_json rejects malformed explicit options: wrapped=$wrapped, capture=$mode") {
      val entries = Map("IgNoReNuLlFiElDs" -> "invalid")
      val options = if (wrapped) CaseInsensitiveMap(entries) else entries
      for (ignoreNulls <- Seq(true, false)) {
        val expression = StructsToJson(options, struct, Some("UTC"),
          ignoreNullFields = ignoreNulls, useCapturedOptionDefaults = mode)
        withSQLConf(SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key -> (!ignoreNulls).toString) {
          val error = intercept[IllegalArgumentException] {
            expression.replacement.eval(create_row(1L))
          }
          assert(error.getMessage.contains("invalid"))
        }
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
    assertShared(nonEmptyNullValue.copy(nullValueWrittenAsQuotedEmptyString = true),
      nonEmptyNullValue.copy(nullValueWrittenAsQuotedEmptyString = false))
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

  test("CSV conversions share plain null markers with default writer settings") {
    val input = CreateNamedStruct(Seq(Literal("n"), Literal.create(null, StringType),
      Literal("x"), BoundReference(0, StringType, nullable = false)))
    for (marker <- Seq("NULL", "NA", "0", "null", "Z9")) {
      val expression = StructsToCsv(Map("NuLlVaLuE" -> marker), input, Some("UTC"),
        nullValueWrittenAsQuotedEmptyString = true)
      val opposite = expression.copy(nullValueWrittenAsQuotedEmptyString = false)
      val rollback = expression.copy(useCapturedOptionDefaults = false)
      withSQLConf(SQLConf.LEGACY_NULL_VALUE_WRITTEN_AS_QUOTED_EMPTY_STRING_CSV.key -> "false") {
        Seq(expression, opposite, rollback).foreach { csv =>
          checkEvaluation(csv, s"$marker,x", create_row("x"))
        }
      }
      assertShared(expression, opposite)
      assertShared(expression, rollback)
    }
  }

  test("CSV custom quote interaction keeps result-sensitive null marker identities") {
    val input = CreateNamedStruct(Seq(Literal("n"), Literal.create(null, StringType),
      Literal("x"), BoundReference(0, StringType, nullable = false)))
    val options = Map("nullValue" -> "NN", "quote" -> "N")
    // A marker bracketed by the quote character is treated differently from a literal string.
    val literalMarker = """N\N\NN,x"""
    val actualNull = "NN,x"
    for (reader <- Seq(true, false)) {
      withSQLConf(SQLConf.LEGACY_NULL_VALUE_WRITTEN_AS_QUOTED_EMPTY_STRING_CSV.key ->
          reader.toString) {
        val quoted = StructsToCsv(options, input, Some("UTC"),
          nullValueWrittenAsQuotedEmptyString = true)
        val unquoted = quoted.copy(nullValueWrittenAsQuotedEmptyString = false)
        val rollback = quoted.copy(useCapturedOptionDefaults = false)
        checkEvaluation(quoted, literalMarker, create_row("x"))
        checkEvaluation(unquoted, actualNull, create_row("x"))
        checkEvaluation(rollback, if (reader) literalMarker else actualNull, create_row("x"))
        assert(!quoted.semanticEquals(unquoted))
        assert(!quoted.semanticEquals(rollback))
        assert(!unquoted.semanticEquals(rollback))
      }
    }
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
      MapType(StringType, new StructType().add("n", LongType)))
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

  test("JSON serialization shares exactly when serialized fields cannot observe null omission") {
    val nonnullable = new StructType().add("id", LongType, nullable = false)
    val independent = Seq(nonnullable,
      new StructType().add("nested", ArrayType(nonnullable), nullable = false),
      VariantType, MapType(new StructType().add("key", LongType), LongType),
      ArrayType(new ExampleBaseTypeUDT), ArrayType(new ExampleIntRowUDT(1)))
    independent.foreach { dataType =>
      val json = StructsToJson(Map.empty,
        BoundReference(0, dataType, nullable = false), Some("UTC"))
      assertShared(json.copy(ignoreNullFields = true).replacement,
        json.copy(ignoreNullFields = false).replacement)
      assertShared(json.copy(useCapturedOptionDefaults = true).replacement,
        json.copy(useCapturedOptionDefaults = false).replacement)
    }
    val nullable = new StructType().add("n", LongType)
    val udt = new ExampleIntRowUDT(1) {
      override def sqlType: DataType = nullable
    }
    val sensitive = Seq(nullable,
      new StructType().add("nested", nullable, nullable = false),
      ArrayType(nullable), MapType(StringType, nullable), ArrayType(udt))
    sensitive.foreach { dataType =>
      val json = StructsToJson(Map.empty,
        BoundReference(0, dataType, nullable = false), Some("UTC"))
      assert(!json.copy(ignoreNullFields = true).replacement.semanticEquals(
        json.copy(ignoreNullFields = false).replacement))
    }
    val nested = CreateNamedStruct(Seq(Literal("nested"), struct))
    for (ignore <- Seq(true, false)) {
      val expected = if (ignore) """{"nested":{"id":1}}""" else {
        """{"nested":{"id":1,"n":null}}"""
      }
      checkEvaluation(StructsToJson(Map.empty, nested, Some("UTC"), ignoreNullFields = ignore),
        expected, create_row(1L))
    }
  }

  test("JSON parser sharing ignores only formats for absent fallback-sensitive types") {
    val input = BoundReference(0, StringType, nullable = false)
    val types = Seq((DateType, "TiMeStAmPfOrMaT", "DaTeFoRmAt"),
      (TimestampType, "DaTeFoRmAt", "TiMeStAmPfOrMaT"))
    for ((dataType, unrelated, own) <- types) {
      val udt = new ExampleIntRowUDT(1) {
        override def sqlType: DataType = new StructType().add("v", dataType)
      }
      val schemas = Seq(new StructType().add("v", dataType),
        new StructType().add("nested", ArrayType(MapType(StringType, dataType))),
        new StructType().add("udt", udt))
      for (schema <- schemas; value <- Seq(null, "yyyy/MM/dd")) {
        val parser = JsonToStructs(schema, Map(unrelated -> value), input, Some("UTC"),
          enableDateTimeParsingFallback = None)
        assertShared(parser, parser.copy(enableDateTimeParsingFallback = Some(true)))
        assert(!parser.semanticEquals(parser.copy(enableDateTimeParsingFallback = Some(false))))
        assert(!parser.semanticEquals(parser.copy(useCapturedOptionDefaults = false)))
        val relevantFormat = parser.copy(options = Map(own -> value))
        assert(!relevantFormat.semanticEquals(
          relevantFormat.copy(enableDateTimeParsingFallback = Some(true))))
      }
    }
    for ((first, last, format) <- Seq((DateType, TimestampType, "timestampFormat"),
        (TimestampType, DateType, "dateFormat"))) {
      val middle = (0 until 128).map(i => StructField(s"n$i", LongType))
      val schema = StructType(Seq(StructField("first", first)) ++ middle :+
        StructField("last", last))
      val parser = JsonToStructs(schema, Map(format -> "yyyy/MM/dd"), input, Some("UTC"),
        enableDateTimeParsingFallback = None)
      assert(!parser.semanticEquals(parser.copy(enableDateTimeParsingFallback = Some(true))))
    }
  }

  test("unchanged JSON canonicalization avoids cloning a wide nullable schema") {
    val fields = (0 until 128).map { i =>
      StructField(s"n$i", ArrayType(MapType(StringType, LongType)))
    }
    val schema = new StructType().add("payload", ArrayType(StructType(fields)))
    val parser = JsonToStructs(schema, Map("enableDateTimeParsingFallback" -> "false"),
      BoundReference(0, StringType, nullable = false), Some("UTC"),
      enableDateTimeParsingFallback = None, useCapturedOptionDefaults = true)
    assert(parser.canonicalized eq parser)
    assert(parser.canonicalized eq parser.canonicalized)

    val equalAttribute = AttributeReference("none", StringType)()
    assert(equalAttribute.canonicalized ne equalAttribute)
    assert(equalAttribute.canonicalized.fastEquals(equalAttribute))
    val equalChild = parser.copy(child = equalAttribute)
    assert(equalChild.canonicalized eq equalChild)
    assert(equalChild.child eq equalAttribute)

    val attribute = AttributeReference("json", StringType, nullable = false)(exprId = ExprId(42))
    assert(attribute.canonicalized ne attribute)
    val changedChild = parser.copy(child = attribute)
    val canonicalChild = changedChild.canonicalized.asInstanceOf[JsonToStructs]
    assert(canonicalChild ne changedChild)
    assert(canonicalChild.child eq attribute.canonicalized)
    assert(changedChild.child eq attribute)

    val captured = parser.copy(enableDateTimeParsingFallback = Some(true))
    val canonicalCapture = captured.canonicalized.asInstanceOf[JsonToStructs]
    assert(canonicalCapture ne captured)
    assert(canonicalCapture.enableDateTimeParsingFallback.isEmpty)
    assert(captured.enableDateTimeParsingFallback.contains(true))
    assert(canonicalCapture.semanticEquals(parser))
    assert(canonicalCapture.semanticHash() == parser.semanticHash())

    val legacy = parser.copy(useCapturedOptionDefaults = false)
    val canonicalMode = legacy.canonicalized.asInstanceOf[JsonToStructs]
    assert(canonicalMode ne legacy)
    assert(canonicalMode.useCapturedOptionDefaults)
    assert(!legacy.useCapturedOptionDefaults)
    assert(canonicalMode.semanticEquals(parser))
  }

  for (temporalType <- Seq(DateType, TimestampType)) {
    test(s"metadata-only JSON canonicalization defers nullable schema copies: $temporalType") {
      val copies = new AtomicInteger
      val payload = StructType((0 until 128).map { i =>
        StructField(s"n$i", ArrayType(LongType, containsNull = false), nullable = false)
      })
      val fields = Array(StructField("t", temporalType, nullable = false),
        StructField("payload", ArrayType(payload, containsNull = false), nullable = false))
      val schema = new StructType(fields) {
        override private[spark] def asNullable: StructType = {
          copies.incrementAndGet()
          super.asNullable
        }
      }
      val parser = JsonToStructs(schema, Map.empty,
        BoundReference(0, StringType, nullable = false), Some("UTC"),
        enableDateTimeParsingFallback = None, useCapturedOptionDefaults = true)
      val enabled = parser.copy(enableDateTimeParsingFallback = Some(true))
      val disabled = parser.copy(enableDateTimeParsingFallback = Some(false))
      val explicit = enabled.copy(options = Map("enableDateTimeParsingFallback" -> "false"),
        useCapturedOptionDefaults = false)
      val explicitCapture = explicit.copy(useCapturedOptionDefaults = true)
      Seq(parser, enabled, disabled, explicit, explicitCapture).foreach(_.dataType)
      val beforeComparison = copies.get()

      assertShared(parser, enabled)
      assertShared(explicit, explicitCapture)
      assert(!parser.semanticEquals(disabled))
      val canonical = parser.canonicalized.asInstanceOf[JsonToStructs]
      assert(canonical ne parser)
      assert(copies.get() == beforeComparison)
      assert(parser.enableDateTimeParsingFallback.isEmpty)
      assert(canonical.enableDateTimeParsingFallback.contains(true))

      val nullableType = StructType(fields).asNullable
      assert(canonical.dataType == nullableType)
      val afterTypeInspection = copies.get()
      assert(canonical.dataType == nullableType)
      assert(copies.get() == afterTypeInspection)
      val executable = parser.copy(schema = StructType(fields))
      val (input, value) = if (temporalType == DateType) {
        ("1", 1)
      } else {
        ("2024-01-02", DateTimeUtils.localDateTimeToMicros(LocalDateTime.of(2024, 1, 2, 0, 0)))
      }
      withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "CORRECTED",
          SQLConf.LEGACY_JSON_ENABLE_DATE_TIME_PARSING_FALLBACK.key -> "false") {
        checkEvaluation(executable, create_row(value, null), create_row(s"""{"t":"$input"}"""))
      }
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

  for ((initialPolicy, parserPolicy) <- Seq("CORRECTED" -> "LEGACY", "LEGACY" -> "CORRECTED")) {
    test(s"captured unset fallback follows lazy parser policy: $initialPolicy to $parserPolicy") {
      val fallbackKey = SQLConf.LEGACY_JSON_ENABLE_DATE_TIME_PARSING_FALLBACK.key
      val policyKey = SQLConf.LEGACY_TIME_PARSER_POLICY.key
      withSQLConf(captureKey -> "true", fallbackKey -> "false", policyKey -> initialPolicy) {
        SQLConf.get.unsetConf(fallbackKey)
        val parser = JsonToStructs(new StructType().add("d", DateType).add("ts", TimestampType),
          Map("dateFormat" -> "yyyy/MM/dd", "timestampFormat" -> "yyyy/MM/dd HH:mm:ss"),
          BoundReference(0, StringType, nullable = false), Some("UTC"))
        assert(parser.enableDateTimeParsingFallback.isEmpty)
        // Initialize this evaluator without initializing its lazy parser.
        assert(parser.nullSafeEval(null) == null)
        withSQLConf(policyKey -> parserPolicy, fallbackKey -> "false") {
          val expected = if (parserPolicy == "LEGACY") {
            create_row(DateTimeUtils.localDateToDays(LocalDate.of(2024, 1, 2)),
              DateTimeUtils.localDateTimeToMicros(LocalDateTime.of(2024, 1, 2, 0, 0)))
          } else {
            create_row(null, null)
          }
          val input = create_row("""{"d":"2024-01-02","ts":"2024-01-02"}""")
          // First assert on the evaluator created before the policy switch, without serialization.
          checkEvaluationWithoutCodegen(parser, expected, input)
          checkEvaluation(parser, expected, input)
        }
      }
    }
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
