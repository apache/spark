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
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.logical.CreateTable
import org.apache.spark.sql.connector.catalog.WriteDistributionMode
import org.apache.spark.sql.connector.expressions.{Expression, Expressions, FieldReference, GeneralScalarExpression, IdentityTransform, NamedReference, NullOrdering, SortDirection, SortOrder, Transform}
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

  private def replay(clauses: String): Option[(WriteDistributionMode, Seq[SortOrder])] = {
    Try(CatalystSqlParser.parsePlan(s"CREATE TABLE t (id INT) USING foo $clauses")).toOption
      .collect { case c: CreateTable => (c.writeDistributionMode, c.writeOrdering) }
  }

  private def emitted(e: Expression): Option[String] = {
    writeClausesSQL(WriteDistributionMode.RANGE, Seq(key(e)), Seq.empty, replay)
  }

  private val spellable: Seq[(String, Expression)] = Seq(
    "reference" -> id,
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
    "day-time interval" -> f(literal(Duration.ofHours(1))),
    "year-month interval" -> f(literal(Period.ofMonths(14))),
    "bucket" -> transform("bucket", literal(16), id),
    "bucket without columns" -> transform("bucket", literal(16)),
    "Spark's bucket" -> Expressions.bucket(16, "id"),
    "days" -> transform("days", id),
    "unparsed name" -> transform("Days", id, literal("UTC")))

  private val unspellable: Seq[(String, Expression)] = Seq(
    "nested transform" -> transform("f", transform("g", id)),
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
    "time with nanoseconds" -> f(literal(LocalTime.of(12, 0, 0, 100000000), TimeType(7))),
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
    "hours of a literal" -> transform("hours", literal(1)))

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
    }
  }

  test("the pairs with no clause form are not emitted") {
    val keys = Seq(key(id))
    Seq(
      (WriteDistributionMode.HASH, keys, Seq.empty[Transform]),
      (WriteDistributionMode.HASH, Seq.empty, Seq(Expressions.apply("cluster_by", id))),
      (WriteDistributionMode.RANGE, Seq.empty, Seq.empty[Transform])
    ).foreach { case (mode, ordering, partitioning) =>
      assert(writeClausesSQL(mode, ordering, partitioning, replay).isEmpty, s"for $mode")
    }
  }

  test("only references to columns of the schema count as existing") {
    val schema = StructType.fromDDL("id INT, s STRUCT<x: INT>, a ARRAY<STRUCT<x: INT>>")
    Seq(
      "id" -> true,
      "s.x" -> true,
      "ID" -> false,
      "missing" -> false,
      "a.x" -> false
    ).foreach { case (name, exists) =>
      assert(referencesExist(schema, Seq(key(FieldReference(name)))) === exists, s"for $name")
    }
    assert(!referencesExist(schema, Seq(key(new ConnectorReference()))))
    assert(referencesExist(schema, Seq(key(transform("f", id, literal(1))))))
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

  test("DESCRIBE renders a connector expression from its children") {
    val truncated = new GeneralScalarExpression("DATE_TRUNC", Array[Expression](id))
    assert(describeSortOrder(key(transform("f", truncated))) ===
      "f(DATE_TRUNC(id)) ASC NULLS FIRST")
  }

  test("a reference renders its quoted field names") {
    assert(describeSortOrder(key(new ConnectorReference("a", "order-id"))) ===
      "a.`order-id` ASC NULLS FIRST")
    assert(describeSortOrder(key(transform("f", new ConnectorReference("order-id")))) ===
      "f(`order-id`) ASC NULLS FIRST")
  }
}
