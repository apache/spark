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

package org.apache.spark.sql.connect.planner

import java.time.{Instant, LocalDateTime, LocalTime}

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

import org.apache.spark.SparkException
import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.{expressions, CatalystTypeConverters}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.catalyst.util.SparkDateTimeUtils
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, InvalidPlanInput}
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter.ToLiteralProtoOptions
import org.apache.spark.sql.connect.planner.LiteralExpressionProtoConverter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.TimestampNanosVal

class LiteralExpressionProtoConverterSuite extends AnyFunSuite { // scalastyle:ignore funsuite

  private def withTimestampNanosTypesEnabled(enabled: Boolean = true)(f: => Unit): Unit = {
    val conf = new SQLConf()
    conf.setConfString(SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key, enabled.toString)
    SQLConf.withExistingConf(conf)(f)
  }

  private def toLiteralProto(v: Any): proto.Expression.Literal = {
    LiteralValueProtoConverter
      .toLiteralProtoWithOptions(
        v,
        None,
        ToLiteralProtoOptions(useDeprecatedDataTypeFields = false))
  }

  private def toLiteralProto(v: Any, t: DataType): proto.Expression.Literal = {
    LiteralValueProtoConverter
      .toLiteralProtoWithOptions(
        v,
        Some(t),
        ToLiteralProtoOptions(useDeprecatedDataTypeFields = false))
  }

  test("basic proto value and catalyst value conversion") {
    val values = Array(null, true, 1.toByte, 1.toShort, 1, 1L, 1.1d, 1.1f, "spark")
    for (v <- values) {
      assertResult(v)(LiteralValueProtoConverter.toScalaValue(toLiteralProto(v)))
    }
  }

  test("SPARK-57566: TIME literal proto and catalyst value round-trip") {
    val times =
      Seq(LocalTime.of(0, 0, 0), LocalTime.of(12, 13, 14), LocalTime.of(23, 59, 59, 999999999))
    for (t <- times) {
      val literalProto = toLiteralProto(t, TimeType())
      // The literal carries the TIME proto type with the expected precision.
      assert(literalProto.getTime.getPrecision == TimeType.DEFAULT_PRECISION)
      // Proto -> Scala value round-trips back to the original LocalTime.
      assertResult(t)(LiteralValueProtoConverter.toScalaValue(literalProto))
      // Proto -> Catalyst expression matches a directly-built catalyst literal.
      val convert = CatalystTypeConverters.createToCatalystConverter(TimeType())
      val expected = expressions.Literal(convert(t), TimeType())
      assertResult(expected)(LiteralExpressionProtoConverter.toCatalystExpression(literalProto))
    }
  }

  test("SPARK-57566: TIME literal proto propagates a non-default precision") {
    val literalProto = toLiteralProto(LocalTime.of(1, 2, 3), TimeType(3))
    assert(literalProto.getTime.getPrecision == 3)
    assertResult(LocalTime.of(1, 2, 3))(LiteralValueProtoConverter.toScalaValue(literalProto))
  }

  test("SPARK-57161: timestamp nanos data type proto round-trip") {
    withTimestampNanosTypesEnabled() {
      val cases: Seq[(Int => DataType, proto.DataType.KindCase)] = Seq(
        (p => TimestampNTZNanosType(p), proto.DataType.KindCase.TIMESTAMP_NTZ_NANOS),
        (p => TimestampLTZNanosType(p), proto.DataType.KindCase.TIMESTAMP_LTZ_NANOS))

      cases.foreach { case (factory, kindCase) =>
        TimestampNTZNanosType.MIN_PRECISION to TimestampNTZNanosType.MAX_PRECISION foreach { p =>
          val dataType = factory(p)
          val protoType = DataTypeProtoConverter.toConnectProtoType(dataType)
          assert(protoType.getKindCase === kindCase)
          val precision = kindCase match {
            case proto.DataType.KindCase.TIMESTAMP_NTZ_NANOS =>
              protoType.getTimestampNtzNanos.getPrecision
            case proto.DataType.KindCase.TIMESTAMP_LTZ_NANOS =>
              protoType.getTimestampLtzNanos.getPrecision
            case _ => fail(s"Unexpected kind case: $kindCase")
          }
          assert(precision === p)
          assert(DataTypeProtoConverter.toCatalystType(protoType) === dataType)
        }
      }

      val defaultNtzProto = proto.DataType
        .newBuilder()
        .setTimestampNtzNanos(proto.DataType.TimestampNTZNanos.newBuilder().build())
        .build()
      val defaultLtzProto = proto.DataType
        .newBuilder()
        .setTimestampLtzNanos(proto.DataType.TimestampLTZNanos.newBuilder().build())
        .build()
      assert(DataTypeProtoConverter.toCatalystType(defaultNtzProto) === TimestampNTZNanosType())
      assert(DataTypeProtoConverter.toCatalystType(defaultLtzProto) === TimestampLTZNanosType())
    }
  }

  test("SPARK-57161: timestamp nanos literal proto and catalyst value round-trip") {
    withTimestampNanosTypesEnabled() {
      val ntzValues = Seq(
        LocalDateTime.parse("1969-12-31T23:59:59.999999789"),
        LocalDateTime.parse("1970-01-01T00:00:00.000000999"))
      val ltzValues = Seq(
        Instant.parse("1969-12-31T23:59:59.999999789Z"),
        Instant.parse("1970-01-01T00:00:00.000000999Z"))

      TimestampNTZNanosType.MIN_PRECISION to TimestampNTZNanosType.MAX_PRECISION foreach { p =>
        ntzValues.foreach { value =>
          val dataType = TimestampNTZNanosType(p)
          val expected = SparkDateTimeUtils.localDateTimeToTimestampNanos(value, p)
          val literalProto = toLiteralProto(value, dataType)
          assert(
            literalProto.getLiteralTypeCase ===
              proto.Expression.Literal.LiteralTypeCase.TIMESTAMP_NTZ_NANOS)
          assert(literalProto.getTimestampNtzNanos.getPrecision === p)
          assert(literalProto.getTimestampNtzNanos.getEpochMicros === expected.epochMicros)
          assert(
            literalProto.getTimestampNtzNanos.getNanosWithinMicro ===
              expected.nanosWithinMicro.toInt)
          assert(LiteralValueProtoConverter.getDataType(literalProto) === dataType)
          assert(
            LiteralValueProtoConverter.toScalaValue(literalProto) ===
              SparkDateTimeUtils.timestampNanosToLocalDateTime(expected))

          val literal = LiteralExpressionProtoConverter.toCatalystExpression(literalProto)
          assert(literal.dataType === dataType)
          assert(literal.value === expected)
        }

        ltzValues.foreach { value =>
          val dataType = TimestampLTZNanosType(p)
          val expected = SparkDateTimeUtils.instantToTimestampNanos(value, p)
          val literalProto = toLiteralProto(value, dataType)
          assert(
            literalProto.getLiteralTypeCase ===
              proto.Expression.Literal.LiteralTypeCase.TIMESTAMP_LTZ_NANOS)
          assert(literalProto.getTimestampLtzNanos.getPrecision === p)
          assert(literalProto.getTimestampLtzNanos.getEpochMicros === expected.epochMicros)
          assert(
            literalProto.getTimestampLtzNanos.getNanosWithinMicro ===
              expected.nanosWithinMicro.toInt)
          assert(LiteralValueProtoConverter.getDataType(literalProto) === dataType)
          assert(
            LiteralValueProtoConverter.toScalaValue(literalProto) ===
              SparkDateTimeUtils.timestampNanosToInstant(expected))

          val literal = LiteralExpressionProtoConverter.toCatalystExpression(literalProto)
          assert(literal.dataType === dataType)
          assert(literal.value === expected)
        }
      }
    }
  }

  test("SPARK-57161: schema-less TimestampNanosVal literal defaults to NTZ nanos") {
    withTimestampNanosTypesEnabled() {
      val value = TimestampNanosVal.fromParts(-1L, 999.toShort)
      val literalProto = toLiteralProto(value)
      assert(
        literalProto.getLiteralTypeCase ===
          proto.Expression.Literal.LiteralTypeCase.TIMESTAMP_NTZ_NANOS)
      assert(LiteralValueProtoConverter.getDataType(literalProto) === TimestampNTZNanosType())
      assert(
        LiteralValueProtoConverter.toScalaValue(literalProto) ===
          SparkDateTimeUtils.timestampNanosToLocalDateTime(value))
    }
  }

  test("SPARK-57161: timestamp nanos literal compatibility keeps NTZ and LTZ distinct") {
    withTimestampNanosTypesEnabled() {
      val ntzLiteralWithLtzType = proto.Expression.Literal
        .newBuilder()
        .setTimestampNtzNanos(
          proto.Expression.Literal.TimestampNTZNanos
            .newBuilder()
            .setEpochMicros(0L)
            .setNanosWithinMicro(1)
            .setPrecision(TimestampNTZNanosType.DEFAULT_PRECISION)
            .build())
        .setDataType(DataTypeProtoConverter.toConnectProtoType(TimestampLTZNanosType()))
        .build()
      val ntzError = intercept[InvalidPlanInput] {
        LiteralValueProtoConverter.getProtoDataType(ntzLiteralWithLtzType)
      }
      assert(ntzError.getCondition === "CONNECT_INVALID_PLAN.INCOMPATIBLE_LITERAL_DATA_TYPE")

      val ltzLiteralWithNtzType = proto.Expression.Literal
        .newBuilder()
        .setTimestampLtzNanos(
          proto.Expression.Literal.TimestampLTZNanos
            .newBuilder()
            .setEpochMicros(0L)
            .setNanosWithinMicro(1)
            .setPrecision(TimestampLTZNanosType.DEFAULT_PRECISION)
            .build())
        .setDataType(DataTypeProtoConverter.toConnectProtoType(TimestampNTZNanosType()))
        .build()
      val ltzError = intercept[InvalidPlanInput] {
        LiteralValueProtoConverter.getProtoDataType(ltzLiteralWithNtzType)
      }
      assert(ltzError.getCondition === "CONNECT_INVALID_PLAN.INCOMPATIBLE_LITERAL_DATA_TYPE")
    }
  }

  test("SPARK-57161: timestamp nanos proto conversion honors the feature flag") {
    withTimestampNanosTypesEnabled(enabled = false) {
      val dataTypeProto = proto.DataType
        .newBuilder()
        .setTimestampNtzNanos(
          proto.DataType.TimestampNTZNanos
            .newBuilder()
            .setPrecision(TimestampNTZNanosType.DEFAULT_PRECISION)
            .build())
        .build()
      val dataTypeError = intercept[SparkException] {
        DataTypeProtoConverter.toCatalystType(dataTypeProto)
      }
      assert(dataTypeError.getCondition === "FEATURE_NOT_ENABLED")

      val connectProtoError = intercept[SparkException] {
        DataTypeProtoConverter.toConnectProtoType(TimestampLTZNanosType())
      }
      assert(connectProtoError.getCondition === "FEATURE_NOT_ENABLED")

      val literalProto = proto.Expression.Literal
        .newBuilder()
        .setTimestampNtzNanos(
          proto.Expression.Literal.TimestampNTZNanos
            .newBuilder()
            .setEpochMicros(0L)
            .setNanosWithinMicro(1)
            .setPrecision(TimestampNTZNanosType.DEFAULT_PRECISION)
            .build())
        .build()
      val literalError = intercept[SparkException] {
        LiteralExpressionProtoConverter.toCatalystExpression(literalProto)
      }
      assert(literalError.getCondition === "FEATURE_NOT_ENABLED")
    }
  }

  // The goal of this test is to check that converting a Scala value -> Proto -> Catalyst value
  // is equivalent to converting a Scala value directly to a Catalyst value.
  Seq[(Any, DataType)](
    (Array[String](null, "a", null), ArrayType(StringType)),
    (Map[String, String]("a" -> null, "b" -> null), MapType(StringType, StringType)),
    (
      (1, "string", true),
      StructType(
        Seq(
          StructField("a", IntegerType),
          StructField("b", StringType),
          StructField("c", BooleanType)))),
    (
      Array((1, "string", true), (2, "string", false), (3, "string", true)),
      ArrayType(
        StructType(
          Seq(
            StructField("a", IntegerType),
            StructField("b", StringType),
            StructField("c", BooleanType))))),
    (
      (1, (2, 3)),
      StructType(
        Seq(
          StructField("a", IntegerType),
          StructField(
            "b",
            StructType(Seq(StructField("c", IntegerType), StructField("d", IntegerType))))))),
    (Array(true, false, true), ArrayType(BooleanType)),
    (Array(1.toShort, 2.toShort, 3.toShort), ArrayType(ShortType)),
    (Array(1, 2, 3), ArrayType(IntegerType)),
    (Array(1L, 2L, 3L), ArrayType(LongType)),
    (Array(1.1d, 2.1d, 3.1d), ArrayType(DoubleType)),
    (Array(1.1f, 2.1f, 3.1f), ArrayType(FloatType)),
    (Array(Array[Int](), Array(1, 2, 3), Array(4, 5, 6)), ArrayType(ArrayType(IntegerType))),
    (Array(Array(1, 2, 3), Array(4, 5, 6), Array[Int]()), ArrayType(ArrayType(IntegerType))),
    (
      Array(Array(Array(Array(Array(Array(1, 2, 3)))))),
      ArrayType(ArrayType(ArrayType(ArrayType(ArrayType(ArrayType(IntegerType))))))),
    (Map[String, String]("1" -> "2", "3" -> "4"), MapType(StringType, StringType)),
    (Map[String, Boolean]("1" -> true, "2" -> false), MapType(StringType, BooleanType)),
    (Map[Int, Int](), MapType(IntegerType, IntegerType)),
    (Map(1 -> 2, 3 -> 4, 5 -> 6), MapType(IntegerType, IntegerType))).zipWithIndex.foreach {
    case ((v, t), idx) =>
      val convert = CatalystTypeConverters.createToCatalystConverter(t)
      val expected = expressions.Literal(convert(v), t)
      test(s"complex proto value and catalyst value conversion #$idx") {
        assertResult(expected)(
          LiteralExpressionProtoConverter.toCatalystExpression(
            LiteralValueProtoConverter.toLiteralProtoWithOptions(
              v,
              Some(t),
              ToLiteralProtoOptions(useDeprecatedDataTypeFields = false))))
      }

      test(s"complex proto value and catalyst value conversion #$idx - backward compatibility") {
        assertResult(expected)(
          LiteralExpressionProtoConverter.toCatalystExpression(
            LiteralValueProtoConverter.toLiteralProtoWithOptions(
              v,
              Some(t),
              ToLiteralProtoOptions(useDeprecatedDataTypeFields = true))))
      }
  }

  test("backward compatibility for array literal proto") {
    // Test the old way of defining arrays with elementType field and elements
    val literalProto = LiteralValueProtoConverter.toLiteralProtoWithOptions(
      Seq(1, 2, 3),
      Some(ArrayType(IntegerType, containsNull = false)),
      ToLiteralProtoOptions(useDeprecatedDataTypeFields = true))
    assert(!literalProto.hasDataType)
    assert(literalProto.getArray.getElementsList.size == 3)
    assert(literalProto.getArray.getElementType.hasInteger)

    val literal = LiteralExpressionProtoConverter.toCatalystExpression(literalProto)
    assert(literal.dataType.isInstanceOf[ArrayType])
    assert(literal.dataType.asInstanceOf[ArrayType].elementType == IntegerType)
    // The containsNull field is always set to true when using the old way of defining arrays.
    assert(literal.dataType.asInstanceOf[ArrayType].containsNull)

    val arrayData = literal.value.asInstanceOf[org.apache.spark.sql.catalyst.util.ArrayData]
    assert(arrayData.numElements() == 3)
    assert(arrayData.getInt(0) == 1)
    assert(arrayData.getInt(1) == 2)
    assert(arrayData.getInt(2) == 3)
  }

  test("backward compatibility for map literal proto") {
    // Test the old way of defining maps with keyType and valueType fields
    val literalProto = LiteralValueProtoConverter.toLiteralProtoWithOptions(
      Map[String, Int]("a" -> 1, "b" -> 2),
      Some(MapType(StringType, IntegerType, valueContainsNull = false)),
      ToLiteralProtoOptions(useDeprecatedDataTypeFields = true))
    assert(!literalProto.hasDataType)
    assert(literalProto.getMap.getKeysList.size == 2)
    assert(literalProto.getMap.getValuesList.size == 2)
    assert(literalProto.getMap.getKeyType.hasString)
    assert(literalProto.getMap.getValueType.hasInteger)

    val literal = LiteralExpressionProtoConverter.toCatalystExpression(literalProto)
    assert(literal.dataType.isInstanceOf[MapType])
    assert(literal.dataType.asInstanceOf[MapType].keyType == StringType)
    assert(literal.dataType.asInstanceOf[MapType].valueType == IntegerType)
    // The valueContainsNull field is always set to true when using the old way of defining maps.
    assert(literal.dataType.asInstanceOf[MapType].valueContainsNull)

    val mapData = literal.value.asInstanceOf[org.apache.spark.sql.catalyst.util.MapData]
    assert(mapData.numElements() == 2)
    val keys = mapData.keyArray()
    val values = mapData.valueArray()
    assert(keys.getUTF8String(0).toString == "a")
    assert(values.getInt(0) == 1)
    assert(keys.getUTF8String(1).toString == "b")
    assert(values.getInt(1) == 2)
  }

  test("backward compatibility for struct literal proto") {
    // Test the old way of defining structs with structType field and elements
    val structProto = LiteralValueProtoConverter.toLiteralProtoWithOptions(
      (1, "test"),
      Some(
        StructType(
          Seq(
            StructField("a", IntegerType, nullable = true),
            StructField("b", StringType, nullable = false)))),
      ToLiteralProtoOptions(useDeprecatedDataTypeFields = true))
    assert(!structProto.hasDataType)
    assert(structProto.getStruct.getElementsList.size == 2)
    val structTypeProto = structProto.getStruct.getStructType.getStruct
    assert(structTypeProto.getFieldsList.size == 2)
    assert(structTypeProto.getFieldsList.get(0).getName == "a")
    assert(structTypeProto.getFieldsList.get(0).getDataType.hasInteger)
    assert(structTypeProto.getFieldsList.get(1).getName == "b")
    assert(structTypeProto.getFieldsList.get(1).getDataType.hasString)

    val result = LiteralValueProtoConverter.toScalaValue(structProto)
    val resultType = LiteralValueProtoConverter.getProtoDataType(structProto)

    // Verify the result is a GenericRowWithSchema with correct values
    assert(result.isInstanceOf[GenericRowWithSchema])
    val row = result.asInstanceOf[GenericRowWithSchema]
    assert(row.length == 2)
    assert(row.get(0) == 1)
    assert(row.get(1) == "test")

    // Verify the returned struct type matches the original
    assert(resultType.getKindCase == proto.DataType.KindCase.STRUCT)
    val structType = resultType.getStruct
    assert(structType.getFieldsCount == 2)
    assert(structType.getFields(0).getName == "a")
    assert(structType.getFields(0).getDataType.hasInteger)
    assert(structType.getFields(0).getNullable)
    assert(structType.getFields(1).getName == "b")
    assert(structType.getFields(1).getDataType.hasString)
    assert(!structType.getFields(1).getNullable)
  }

  test("an invalid array literal") {
    val literalProto = proto.Expression.Literal
      .newBuilder()
      .setArray(proto.Expression.Literal.Array.newBuilder())
      .build()
    intercept[InvalidPlanInput] {
      LiteralValueProtoConverter.toScalaValue(literalProto)
    }
  }

  test("an invalid map literal") {
    val literalProto = proto.Expression.Literal
      .newBuilder()
      .setMap(proto.Expression.Literal.Map.newBuilder())
      .build()
    intercept[InvalidPlanInput] {
      LiteralValueProtoConverter.toScalaValue(literalProto)
    }
  }
}
