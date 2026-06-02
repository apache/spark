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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Literal, MutableTimestampNanos, SpecificInternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.types.{PhysicalDataType, PhysicalTimestampLTZNanosType, PhysicalTimestampNTZNanosType}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, TimestampLTZNanosType, TimestampNTZNanosType}
import org.apache.spark.sql.types.ops.TypeApiOps
import org.apache.spark.unsafe.types.TimestampNanosVal

/**
 * Tests for the Types Framework wiring of the nanosecond timestamp types (SPARK-57207).
 *
 * Verifies that TimestampNTZNanosType and TimestampLTZNanosType route physical representation,
 * literals, row accessors, mutable values, and codegen class selection through TypeOps/TypeApiOps
 * when spark.sql.types.framework.enabled is true, and that disabling the flag falls back to the
 * legacy paths with identical results.
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

  test("getEncoder is unsupported until encoders are wired (SPARK-57033)") {
    allCases.foreach { case (dt, _, _) =>
      val e = intercept[AnalysisException](TypeApiOps(dt).get.getEncoder)
      assert(e.getCondition === "UNSUPPORTED_DATA_TYPE_FOR_ENCODER")
    }
  }

  test("toSQLValue uses the NTZ/LTZ literal prefix") {
    val ntzOps = TypeApiOps(TimestampNTZNanosType(9)).get
    assert(ntzOps.toSQLValue(ntzVal).startsWith("TIMESTAMP_NTZ '"))
    val ltzOps = TypeApiOps(TimestampLTZNanosType(9)).get
    assert(ltzOps.toSQLValue(ltzVal).startsWith("TIMESTAMP_LTZ '"))
  }

  test("framework disabled falls back to identical legacy behavior") {
    withSQLConf(SQLConf.TYPES_FRAMEWORK_ENABLED.key -> "false") {
      allCases.foreach { case (dt, physical, value) =>
        assert(TypeOps(dt).isEmpty, s"TypeOps should be empty for $dt when disabled")
        assert(TypeApiOps(dt).isEmpty, s"TypeApiOps should be empty for $dt when disabled")
        checkPhysicalAndLiteralAndCodegen(dt, physical)
        checkRowRoundtrip(dt, value)
      }
    }
  }
}
