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

import java.time.{Instant, LocalDateTime}

import org.apache.spark.{SparkFunSuite, SparkIllegalArgumentException}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{InstantNanosEncoder, LocalDateTimeNanosEncoder}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Literal, MutableTimestampNanos, SpecificInternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.types.{PhysicalDataType, PhysicalTimestampLTZNanosType, PhysicalTimestampNTZNanosType}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, TimestampLTZNanosType, TimestampNTZNanosType}
import org.apache.spark.sql.types.ops.TypeApiOps
import org.apache.spark.unsafe.types.TimestampNanosVal

/**
 * Tests for the Types Framework wiring of the nanosecond timestamp types (SPARK-57207).
 *
 * Verifies that TimestampNTZNanosType and TimestampLTZNanosType route physical representation,
 * literals, row accessors, mutable values, codegen class selection, conversions, and encoders
 * through TypeOps/TypeApiOps. The Types Framework is the sole integration path for these types, so
 * the suite runs with spark.sql.types.framework.enabled = true (the default under tests).
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

  test("format and toSQLValue raise an internal error (formatting not implemented yet)") {
    allCases.foreach { case (dt, _, value) =>
      val ops = TypeApiOps(dt).get
      Seq[() => Any](() => ops.format(value), () => ops.toSQLValue(value)).foreach { call =>
        val e = intercept[org.apache.spark.SparkException](call())
        assert(e.getCondition === "INTERNAL_ERROR", s"condition for $dt")
        assert(e.getMessage.contains("TimestampFormatter for the type"), s"message for $dt")
        assert(e.getMessage.contains("is not implemented yet"), s"message for $dt")
      }
    }
  }

  test("framework disabled leaves the nanos types unsupported (no legacy fallback)") {
    withSQLConf(SQLConf.TYPES_FRAMEWORK_ENABLED.key -> "false") {
      allCases.foreach { case (dt, _, _) =>
        assert(TypeOps(dt).isEmpty, s"TypeOps should be empty for $dt when disabled")
        assert(TypeApiOps(dt).isEmpty, s"TypeApiOps should be empty for $dt when disabled")
      }
    }
  }

  test("enabling the nanos types requires the Types Framework to be enabled") {
    withSQLConf(SQLConf.TYPES_FRAMEWORK_ENABLED.key -> "false") {
      checkError(
        exception = intercept[SparkIllegalArgumentException] {
          SQLConf.get.setConfString(SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key, "true")
        },
        condition = "INVALID_CONF_VALUE.REQUIREMENT",
        parameters = Map(
          "confName" -> SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key,
          "confValue" -> "true",
          "confRequirement" ->
            (s"'${SQLConf.TYPES_FRAMEWORK_ENABLED.key}' must be true to enable the nanosecond " +
              "timestamp types.")))
    }
  }
}
