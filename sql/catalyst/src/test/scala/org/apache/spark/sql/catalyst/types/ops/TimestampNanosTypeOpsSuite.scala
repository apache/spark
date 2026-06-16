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

package org.apache.spark.sql.catalyst.types.ops

import java.time.{Instant, LocalDateTime, ZoneOffset}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, ExpressionEncoder}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{InstantNanosEncoder, LocalDateTimeNanosEncoder, OptionEncoder}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Literal, MutableTimestampNanos, SpecificInternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.types.{PhysicalDataType, PhysicalTimestampLTZNanosType, PhysicalTimestampNTZNanosType}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, TimestampLTZNanosType, TimestampNTZNanosType}
import org.apache.spark.unsafe.types.{TimestampNanosVal, UTF8String}

/**
 * Tests for the Types Framework wiring of the nanosecond timestamp types (SPARK-57207).
 *
 * Verifies that TimestampNTZNanosType and TimestampLTZNanosType route physical representation,
 * literals, row accessors, mutable values, codegen class selection, conversions, and encoders
 * through TypeOps/TypeApiOps. The Types Framework is the sole integration path for these types.
 */
class TimestampNanosTypeOpsSuite extends SparkFunSuite with SQLHelper {

  private val precisions = Seq(7, 8, 9)

  private val ntzVal = TimestampNanosVal.fromParts(1234567890123L, 42.toShort)
  private val ltzVal = TimestampNanosVal.fromParts(-98765L, 999.toShort)

  // (dataType, expected physical type, sample value) tuples covering NTZ and LTZ for p in {7,8,9}.
  private def ntzCases: Seq[(DataType, PhysicalDataType, TimestampNanosVal)] =
    precisions.map(p => (TimestampNTZNanosType(p), PhysicalTimestampNTZNanosType, ntzVal))

  private def ltzCases: Seq[(DataType, PhysicalDataType, TimestampNanosVal)] =
    precisions.map(p => (TimestampLTZNanosType(p), PhysicalTimestampLTZNanosType, ltzVal))

  private def allCases: Seq[(DataType, PhysicalDataType, TimestampNanosVal)] = ntzCases ++ ltzCases

  private def checkPhysicalAndLiteralAndCodegen(
      dt: DataType,
      physical: PhysicalDataType): Unit = {
    assert(PhysicalDataType(dt) === physical, s"physical type for $dt")
    val default = Literal.default(dt)
    assert(default.dataType === dt, s"default literal type for $dt")
    assert(default.value === TimestampNanosVal.ZERO, s"default literal value for $dt")
    assert(CodeGenerator.javaClass(dt) === classOf[TimestampNanosVal], s"javaClass for $dt")
  }

  private def checkRowRoundtrip(dt: DataType, value: TimestampNanosVal): Unit = {
    val accessor = InternalRow.getAccessor(dt)
    val writer = InternalRow.getWriter(0, dt)

    val genericRow = new GenericInternalRow(Array[Any](null, null))
    writer(genericRow, value)
    assert(accessor(genericRow, 0) === value, s"GenericInternalRow roundtrip for $dt")
    assert(accessor(new GenericInternalRow(Array[Any](null, null)), 0) === null)

    val specificRow = new SpecificInternalRow(Seq(dt))
    specificRow.update(0, value)
    assert(accessor(specificRow, 0) === value, s"SpecificInternalRow roundtrip for $dt")
    specificRow.update(0, null)
    assert(accessor(specificRow, 0) === null)
  }

  test("TypeOps and TypeApiOps are registered when the framework is enabled") {
    allCases.foreach { case (dt, _, _) =>
      assert(TypeOps(dt).isDefined, s"TypeOps should be defined for $dt")
      assert(TypeApiOps(dt).isDefined, s"TypeApiOps should be defined for $dt")
    }
  }

  test("physical type, default literal, and codegen class (framework enabled)") {
    allCases.foreach { case (dt, physical, _) =>
      checkPhysicalAndLiteralAndCodegen(dt, physical)
    }
  }

  test("InternalRow and SpecificInternalRow roundtrip (framework enabled)") {
    allCases.foreach { case (dt, _, value) =>
      checkRowRoundtrip(dt, value)
    }
  }

  test("SpecificInternalRow uses a dedicated MutableTimestampNanos holder") {
    allCases.foreach { case (dt, _, _) =>
      val row = new SpecificInternalRow(Seq(dt))
      assert(row.values(0).isInstanceOf[MutableTimestampNanos],
        s"expected MutableTimestampNanos for $dt")
    }
  }

  test("getEncoder returns the SPARK-57033 nanos encoder (matches the legacy RowEncoder path)") {
    precisions.foreach { p =>
      assert(TypeApiOps(TimestampNTZNanosType(p)).get.getEncoder === LocalDateTimeNanosEncoder(p))
      assert(TypeApiOps(TimestampLTZNanosType(p)).get.getEncoder === InstantNanosEncoder(p))
    }
  }

  test("getEncoder honors the timestampNanosTypes.enabled gate") {
    withSQLConf(SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "false") {
      allCases.foreach { case (dt, _, _) =>
        val e = intercept[org.apache.spark.SparkException](TypeApiOps(dt).get.getEncoder)
        assert(e.getCondition === "FEATURE_NOT_ENABLED")
      }
    }
  }

  // A sample with sub-micro digits so precision truncation is exercised.
  private val sampleLocalDateTime = LocalDateTime.parse("2019-02-26T16:56:00.123456789")
  private val sampleInstant = Instant.parse("2019-02-26T16:56:00.123456789Z")

  private def externalValue(dt: DataType): Any = dt match {
    case _: TimestampNTZNanosType => sampleLocalDateTime
    case _: TimestampLTZNanosType => sampleInstant
  }

  test("CatalystTypeConverters convert java.time values (matches the legacy converter path)") {
    allCases.foreach { case (dt, _, _) =>
      val external = externalValue(dt)
      val expectedCatalyst = dt match {
        case t: TimestampNTZNanosType =>
          DateTimeUtils.localDateTimeToTimestampNanos(sampleLocalDateTime, t.precision)
        case t: TimestampLTZNanosType =>
          DateTimeUtils.instantToTimestampNanos(sampleInstant, t.precision)
      }

      // toScala over the truncated catalyst value, i.e. what a lossless roundtrip yields.
      val expectedScala = dt match {
        case _: TimestampNTZNanosType =>
          DateTimeUtils.timestampNanosToLocalDateTime(expectedCatalyst)
        case _: TimestampLTZNanosType =>
          DateTimeUtils.timestampNanosToInstant(expectedCatalyst)
      }

      val catalyst = CatalystTypeConverters.createToCatalystConverter(dt)(external)
      assert(catalyst === expectedCatalyst, s"toCatalyst for $dt")
      assert(catalyst.isInstanceOf[TimestampNanosVal], s"toCatalyst must not be identity for $dt")

      val scala = CatalystTypeConverters.createToScalaConverter(dt)(catalyst)
      assert(scala === expectedScala, s"toScala roundtrip for $dt")
    }
  }

  // SPARK-57285: a value with sub-precision (sub-nano) digits so flooring is exercised; built from
  // a UTC wall clock so the rendered NTZ/LTZ strings are predictable.
  private val nanosLdt = LocalDateTime.of(2020, 1, 1, 0, 0, 0, 123456789)

  // Expected fractional second after flooring to the given nanos precision (trailing zeros are
  // trimmed by the formatter, but 123456789 has none to trim at p in {7,8,9}).
  private def expectedFraction(precision: Int): String = precision match {
    case 7 => "1234567"
    case 8 => "12345678"
    case 9 => "123456789"
  }

  test("SPARK-57285: NTZ format renders zone-independently (no session zone needed)") {
    precisions.foreach { p =>
      val ops = TypeApiOps(TimestampNTZNanosType(p)).get
      val v = DateTimeUtils.localDateTimeToTimestampNanos(nanosLdt, p)
      val expected = s"2020-01-01 00:00:00.${expectedFraction(p)}"
      // NTZ is zone-independent: it renders the same regardless of the session zone.
      assert(ops.format(v) === expected, s"NTZ format for precision $p")
      assert(ops.formatUTF8(v) === UTF8String.fromString(expected))
      assert(ops.toSQLValue(v) === s"TIMESTAMP_NTZ '$expected'")
    }
  }

  test("SPARK-57285: LTZ format renders in the zone carried by the ops instance") {
    precisions.foreach { p =>
      val t = TimestampLTZNanosType(p)
      // Physical value is the epoch instant of the UTC wall clock above.
      val v = DateTimeUtils.instantToTimestampNanos(nanosLdt.toInstant(ZoneOffset.UTC), p)
      val frac = expectedFraction(p)
      // UTC zone: the rendered wall clock matches the UTC instant.
      val utcOps = new TimestampLTZNanosTypeApiOps(t, ZoneOffset.UTC)
      assert(utcOps.format(v) === s"2020-01-01 00:00:00.$frac")
      assert(utcOps.formatUTF8(v) === UTF8String.fromString(s"2020-01-01 00:00:00.$frac"))
      assert(utcOps.toSQLValue(v) === s"TIMESTAMP_LTZ '2020-01-01 00:00:00.$frac'")
      // A +05:00 zone shifts the rendered wall clock; the fraction is unaffected.
      val offsetOps = new TimestampLTZNanosTypeApiOps(t, ZoneOffset.ofHours(5))
      assert(offsetOps.format(v) === s"2020-01-01 05:00:00.$frac")
    }
  }

  test("SPARK-57285: LTZ zone-less lookup renders in the session-local time zone config") {
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      precisions.foreach { p =>
        val dt = TimestampLTZNanosType(p)
        // TypeApiOps.apply has no cast zone, so it defaults to the session-local time zone config.
        val ops = TypeApiOps(dt).get
        val v = DateTimeUtils.instantToTimestampNanos(nanosLdt.toInstant(ZoneOffset.UTC), p)
        val frac = expectedFraction(p)
        assert(ops.format(v) === s"2020-01-01 00:00:00.$frac")
        assert(ops.toSQLValue(v) === s"TIMESTAMP_LTZ '2020-01-01 00:00:00.$frac'")
      }
    }
  }

  test("SPARK-57285: NTZ formatExternal renders LocalDateTime at the column precision") {
    precisions.foreach { p =>
      val ops = TypeApiOps(TimestampNTZNanosType(p)).get
      // The encoder floors to column precision before handing the value to Row JSON;
      // round-trip through TimestampNanosVal to reproduce that flooring, then format.
      val v = DateTimeUtils.localDateTimeToTimestampNanos(nanosLdt, p)
      val ldt = DateTimeUtils.timestampNanosToLocalDateTime(v)
      val expected = s"2020-01-01 00:00:00.${expectedFraction(p)}"
      assert(ops.formatExternal(ldt) === Some(expected), s"NTZ formatExternal for precision $p")
    }
  }

  test("SPARK-57285: LTZ formatExternal renders Instant at the column precision in the ops zone") {
    precisions.foreach { p =>
      val t = TimestampLTZNanosType(p)
      val v = DateTimeUtils.instantToTimestampNanos(nanosLdt.toInstant(ZoneOffset.UTC), p)
      val instant = DateTimeUtils.timestampNanosToInstant(v)
      val frac = expectedFraction(p)
      // UTC zone: rendered wall clock matches the UTC instant.
      val utcOps = new TimestampLTZNanosTypeApiOps(t, ZoneOffset.UTC)
      assert(utcOps.formatExternal(instant) === Some(s"2020-01-01 00:00:00.$frac"),
        s"LTZ formatExternal for precision $p")
      // A +05:00 zone shifts the rendered wall clock; the fraction is unaffected.
      val offsetOps = new TimestampLTZNanosTypeApiOps(t, ZoneOffset.ofHours(5))
      assert(offsetOps.formatExternal(instant) === Some(s"2020-01-01 05:00:00.$frac"),
        s"LTZ formatExternal for precision $p with +05:00")
    }
  }

  private def checkOptionRoundtrip[T](
      enc: AgnosticEncoder[Option[T]],
      values: Seq[Option[T]]): Unit = {
    val encoder = ExpressionEncoder(enc).resolveAndBind()
    val toRow = encoder.createSerializer()
    val fromRow = encoder.createDeserializer()
    values.foreach(v => assert(fromRow(toRow(v)) === v, s"roundtrip for $enc with $v"))
  }

  test("Option-wrapped nanos encoders round-trip (wrapper unwrapped before framework dispatch)") {
    // Regression for the framework serde dispatch keying off enc.dataType: OptionEncoder proxies
    // dataType to the wrapped nanos leaf, so the wrapper must be handled (UnwrapOption/WrapOption)
    // before the TypeOps leaf dispatch. Precision 9 keeps the roundtrip lossless (SPARK-57207).
    checkOptionRoundtrip(
      OptionEncoder(LocalDateTimeNanosEncoder(9)),
      Seq(Some(LocalDateTime.parse("2019-02-26T16:56:00.123456789")), None))
    checkOptionRoundtrip(
      OptionEncoder(InstantNanosEncoder(9)),
      Seq(Some(Instant.parse("2019-02-26T16:56:00.123456789Z")), None))
  }
}
