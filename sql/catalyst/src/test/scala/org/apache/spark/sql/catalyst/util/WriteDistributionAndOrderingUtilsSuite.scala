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

package org.apache.spark.sql.catalyst.util

import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.{Duration, Instant, LocalDate, LocalDateTime, LocalTime, Period}

import scala.util.Try

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Literal => CatalystLiteral}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.logical.CreateTable
import org.apache.spark.sql.connector.catalog.WriteDistributionMode
import org.apache.spark.sql.connector.expressions.{Cast, Expression, Expressions, Extract, FieldReference, GeneralScalarExpression, GetArrayItem, IdentityTransform, NamedReference, NullOrdering, SortDirection, SortOrder, Transform, UserDefinedScalarFunc, VariantGet}
import org.apache.spark.sql.connector.expressions.LogicalExpressions.{apply => transform, literal, sort}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class WriteDistributionAndOrderingUtilsSuite extends SparkFunSuite with SQLHelper {
  import WriteDistributionAndOrdering._

  private class ConnectorReference(names: String*) extends NamedReference {
    override def fieldNames(): Array[String] = names.toArray
  }

  private val id = FieldReference("id")

  private def f(arg: Expression): Transform = transform("f", id, arg)

  private def key(e: Expression): SortOrder =
    sort(e, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)

  private val schema = StructType.fromDDL(
    "id INT, `order` INT, `order-id` INT, s STRUCT<x: INT>, a ARRAY<STRUCT<x: INT>>")

  private def replay(clauses: String): Option[(WriteDistributionMode, Seq[SortOrder])] = {
    Try(CatalystSqlParser.parsePlan(replayStatement(clauses))).toOption
      .collect { case c: CreateTable => (c.writeDistributionMode, c.writeOrdering) }
  }

  private def emitted(e: Expression): Option[String] = {
    writeClausesSQL(WriteDistributionMode.RANGE, Seq(key(e)), Seq.empty, schema, replay)
  }

  private val spellable: Seq[(String, Expression)] = Seq(
    "reference" -> id,
    "nested field" -> FieldReference(Seq("s", "x")),
    "connector reference" -> transform("f", new ConnectorReference("order-id")),
    "identity" -> IdentityTransform(id),
    "byte" -> f(literal(1.toByte)),
    "short" -> f(literal(1.toShort)),
    "int" -> f(literal(1)),
    "long" -> f(literal(1L)),
    "float" -> f(literal(1.5F)),
    "double" -> f(literal(1.5D)),
    "decimal" -> f(literal(new BigDecimal("1.5"))),
    "string" -> f(literal("x")),
    "string with escapes" -> f(literal("it's a \\")),
    "binary" -> f(literal(Array[Byte](1, 2))),
    "date" -> f(literal(LocalDate.of(1970, 1, 1))),
    "timestamp" -> f(literal(Instant.parse("2020-11-01T09:30:00Z"))),
    "timestamp_ntz" -> f(literal(LocalDateTime.of(2020, 1, 1, 10, 0))),
    "time" -> f(literal(LocalTime.of(12, 0))),
    "time(7) with trailing zeros" -> f(literal(LocalTime.of(12, 0, 0, 100000000), TimeType(7))),
    "time(9) with trailing zeros" -> f(literal(LocalTime.of(12, 0, 0, 100000000), TimeType(9))),
    "time(9) without trailing zeros" ->
      f(literal(LocalTime.of(12, 0, 0, 123456789), TimeType(9))),
    "day-time interval" -> f(literal(Duration.ofHours(1))),
    "year-month interval" -> f(literal(Period.ofMonths(14))),
    "bucket" -> transform("bucket", literal(16), id),
    "bucket without columns" -> transform("bucket", literal(16)),
    "Spark's bucket" -> Expressions.bucket(16, "id"),
    "days" -> transform("days", id),
    "unparsed name" -> transform("Days", id, literal("UTC")))

  private val unspellable: Seq[(String, Expression)] = Seq(
    "nested transform" -> transform("f", transform("g", id)),
    "nested identity" -> transform("f", IdentityTransform(id)),
    "true" -> f(literal(true)),
    "false" -> f(literal(false)),
    "null" -> f(literal(null)),
    "typed null" -> f(literal(null, IntegerType)),
    "decimal with trailing zeros" ->
      f(literal(new BigDecimal("1.50"), DecimalType(10, 2))),
    "decimal in exponent form" ->
      f(literal(new BigDecimal("0.0000001"), DecimalType(10, 7))),
    "char" -> f(literal("x", CharType(3))),
    "varchar" -> f(literal("x", VarcharType(3))),
    "collated string" -> f(literal("x", StringType("UTF8_LCASE"))),
    "NaN" -> f(literal(Float.NaN)),
    "infinity" -> f(literal(Double.PositiveInfinity)),
    "maximal float" -> f(literal(Float.MaxValue)),
    "time(6) holding a nanosecond" -> f(literal(LocalTime.ofNanoOfDay(1))),
    "year interval not in whole years" ->
      f(literal(13, YearMonthIntervalType(YearMonthIntervalType.YEAR))),
    "hour interval not in whole hours" ->
      f(literal(Duration.ofMinutes(90).toNanos / 1000, DayTimeIntervalType(
        DayTimeIntervalType.HOUR))),
    "negative-scale decimal" -> f(Expressions.literal(new BigDecimal("1E+3"))),
    "int value with long type" -> f(literal(5, LongType)),
    "byte buffer value" -> f(literal(ByteBuffer.wrap(Array[Byte](1, 2)), BinaryType)),
    "bucket with columns first" -> transform("bucket", id, literal(16)),
    "bucket with long count" -> transform("bucket", literal(16L), id),
    "bucket with literal column" -> transform("bucket", literal(16), literal(1)),
    "days with two arguments" -> transform("days", id, literal("UTC")),
    "hours of a literal" -> transform("hours", literal(1)),
    "missing column" -> FieldReference("missing"),
    "missing column inside a nested identity" ->
      transform("f", Expressions.identity("missing")),
    "column in a different case" -> FieldReference("ID"),
    "field of an array element" -> FieldReference(Seq("a", "x")),
    "reference with no field names" -> new ConnectorReference())

  test("a sort key is emitted iff its rendering replays as the same key") {
    assert(spellable.filter { case (_, e) =>
      !emitted(e).contains(s"ORDERED BY (${describeSortOrder(key(e))})")
    }.map(_._1) === Seq.empty)
    assert(unspellable.filter { case (_, e) => emitted(e).isDefined }.map(_._1) === Seq.empty)
  }

  test("a sort key is emitted iff it replays as the same key under parser confs") {
    val timestamp = f(literal(Instant.parse("2020-11-01T09:30:00Z")))
    val wrong = Seq(
      (SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles", timestamp, true),
      (SQLConf.TIMESTAMP_TYPE.key -> "TIMESTAMP_NTZ", timestamp, true),
      (SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true", f(literal(Duration.ofHours(1))), false),
      (SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true", f(literal(Period.ofMonths(14))), false),
      (SQLConf.ESCAPED_STRING_LITERALS.key -> "true", f(literal("it's")), false),
      (SQLConf.ESCAPED_STRING_LITERALS.key -> "true", f(literal("a\\b")), false),
      (SQLConf.ESCAPED_STRING_LITERALS.key -> "true", f(literal("x")), true),
      (SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "LEGACY",
        f(literal(LocalDate.of(-1, 1, 1))), false),
      (SQLConf.LEGACY_FROM_DAYTIME_STRING.key -> "true",
        f(literal(Duration.ofHours(26), DayTimeIntervalType(
          DayTimeIntervalType.DAY, DayTimeIntervalType.HOUR))), false)
    ).filter { case (conf, e, spelled) => withSQLConf(conf)(emitted(e).isDefined != spelled) }
    assert(wrong.map(_._2.describe) === Seq.empty)
  }

  test("a name that is a reserved keyword is emitted quoted only where the parser needs it") {
    val order = FieldReference("order")
    assert(emitted(order) === Some("ORDERED BY (order ASC NULLS FIRST)"))
    withSQLConf(
        SQLConf.ANSI_ENABLED.key -> "true",
        SQLConf.ENFORCE_RESERVED_KEYWORDS.key -> "true") {
      assert(emitted(order) === Some("ORDERED BY (`order` ASC NULLS FIRST)"))
      assert(emitted(transform("bucket", literal(4), order)) ===
        Some("ORDERED BY (`bucket`(4, `order`) ASC NULLS FIRST)"))
    }
  }

  test("each mode with a clause form is emitted as that clause") {
    val keys = Seq(key(id))
    val partitioning = Seq(Expressions.identity("p"))
    Seq(
      (WriteDistributionMode.HASH, keys,
        "DISTRIBUTED BY PARTITION ORDERED BY (id ASC NULLS FIRST)"),
      (WriteDistributionMode.HASH, Seq.empty, "DISTRIBUTED BY PARTITION"),
      (WriteDistributionMode.RANGE, keys, "ORDERED BY (id ASC NULLS FIRST)"),
      (WriteDistributionMode.NONE, keys, "LOCALLY ORDERED BY (id ASC NULLS FIRST)"),
      (WriteDistributionMode.NONE, Seq.empty, "UNORDERED")
    ).foreach { case (mode, ordering, clauses) =>
      assert(writeClausesSQL(mode, ordering, partitioning, schema, replay) === Some(clauses),
        s"for $mode $ordering")
    }
  }

  test("a key that does not parse back omits the pair in every mode") {
    val keys = Seq(key(transform("f", transform("g", id))))
    Seq(WriteDistributionMode.HASH, WriteDistributionMode.RANGE, WriteDistributionMode.NONE)
      .foreach { mode =>
        assert(writeClausesSQL(mode, keys, Seq(Expressions.identity("p")), schema, replay).isEmpty,
          s"for $mode")
      }
  }

  test("the pairs with no clause form are not emitted") {
    val keys = Seq(key(id))
    Seq(
      (WriteDistributionMode.HASH, keys, Seq.empty[Transform]),
      (WriteDistributionMode.HASH, Seq.empty, Seq(Expressions.apply("cluster_by", id))),
      (WriteDistributionMode.RANGE, Seq.empty, Seq.empty[Transform]),
      (null, keys, Seq.empty[Transform])
    ).foreach { case (mode, ordering, partitioning) =>
      val anyClauses = (_: String) => Some((mode, ordering))
      assert(writeClausesSQL(mode, ordering, partitioning, schema, anyClauses).isEmpty,
        s"for $mode")
    }
  }

  test("a nanosecond TIMESTAMP_LTZ renders in UTC with an explicit offset") {
    withSQLConf(
        SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      val parsed = CatalystSqlParser.parseExpression(
        "TIMESTAMP_LTZ '2020-11-01 01:30:00.123456789-08:00'").asInstanceOf[CatalystLiteral]
      val e = f(literal(parsed.value, parsed.dataType))
      assert(emitted(e) ===
        Some("ORDERED BY (f(id, TIMESTAMP_LTZ '2020-11-01 09:30:00.123456789Z') ASC NULLS FIRST)"))

      val ntz = CatalystSqlParser.parseExpression("TIMESTAMP_NTZ '2020-01-01 10:00:00.100000000'")
        .asInstanceOf[CatalystLiteral]
      assert(emitted(f(literal(ntz.value, ntz.dataType))).isDefined)
    }
  }

  test("a timestamp renders in UTC with an explicit offset") {
    val e = f(literal(Instant.parse("2020-11-01T09:30:00Z")))
    Seq("UTC", "America/Los_Angeles", "Asia/Tokyo").foreach { tz =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        assert(describeSortOrder(key(e)) ===
          "f(id, TIMESTAMP_LTZ '2020-11-01 09:30:00Z') ASC NULLS FIRST", s"in $tz")
      }
    }
  }

  test("DESCRIBE renders a literal Catalyst cannot represent with describe") {
    Seq(
      Expressions.literal(new BigDecimal("1E+3")) -> "1E+3",
      literal(5, LongType) -> "5"
    ).foreach { case (lit, rendered) =>
      assert(describeSortOrder(key(f(lit))) === s"f(id, $rendered) ASC NULLS FIRST")
    }
    val buffer = ByteBuffer.wrap(Array[Byte](1, 2))
    assert(describeSortOrder(key(f(literal(buffer, BinaryType)))) ===
      s"f(id, $buffer) ASC NULLS FIRST")
  }

  test("DESCRIBE renders a connector expression as describe does") {
    Seq(
      new Extract("YEAR", id) -> "EXTRACT(YEAR FROM id)",
      new UserDefinedScalarFunc("my_udf", "my_udf", Array[Expression](id)) -> "my_udf(id)",
      new GeneralScalarExpression("+", Array[Expression](id, literal(1))) -> "id + 1"
    ).foreach { case (e, rendered) =>
      assert(describeSortOrder(key(transform("f", e))) === s"f($rendered) ASC NULLS FIRST")
    }
  }

  test("DESCRIBE renders a connector expression describe cannot build from its children") {
    val truncated = new GeneralScalarExpression("DATE_TRUNC", Array[Expression](id))
    Seq(
      truncated -> "DATE_TRUNC(id)",
      new UserDefinedScalarFunc("my_udf", "my_udf", Array[Expression](truncated)) ->
        "my_udf(DATE_TRUNC(id))",
      new Cast(truncated, IntegerType) -> "CAST(DATE_TRUNC(id) AS integer)",
      new GetArrayItem(truncated, literal(0), true) -> "DATE_TRUNC(id)[0]",
      new VariantGet(truncated, "$.a", IntegerType, true, null) ->
        "variant_get(DATE_TRUNC(id), '$.a', int)",
      sort(truncated, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST) ->
        "DATE_TRUNC(id) ASC NULLS FIRST",
      new GeneralScalarExpression("+", Array[Expression](id, null)) -> "+(id, null)"
    ).foreach { case (e, rendered) =>
      assert(describeSortOrder(key(transform("f", e))) === s"f($rendered) ASC NULLS FIRST")
      assert(emitted(transform("f", e)).isEmpty, rendered)
    }
  }

  test("DESCRIBE renders an identity transform inside a key as a transform") {
    assert(describeSortOrder(key(transform("f", IdentityTransform(id)))) ===
      "f(identity(id)) ASC NULLS FIRST")
    assert(describeSortOrder(key(IdentityTransform(id))) === "id ASC NULLS FIRST")
  }

  test("DESCRIBE renders a literal inside a connector expression with its type") {
    val date = literal(LocalDate.of(1970, 1, 1))
    val e = new GeneralScalarExpression("+", Array[Expression](id, date))
    assert(describeSortOrder(key(transform("f", e))) ===
      "f(id + DATE '1970-01-01') ASC NULLS FIRST")
  }

  test("a reference renders its quoted field names") {
    assert(describeSortOrder(key(new ConnectorReference("a", "order-id"))) ===
      "a.`order-id` ASC NULLS FIRST")
    assert(describeSortOrder(key(transform("f", new ConnectorReference("order-id")))) ===
      "f(`order-id`) ASC NULLS FIRST")
  }
}
