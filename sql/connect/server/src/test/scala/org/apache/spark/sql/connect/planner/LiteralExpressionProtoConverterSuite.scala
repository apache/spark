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

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter
import org.apache.spark.sql.types._

class LiteralExpressionProtoConverterSuite extends AnyFunSuite { // scalastyle:ignore funsuite

  test("basic proto value and catalyst value conversion") {
    val values = Array(null, true, 1.toByte, 1.toShort, 1, 1L, 1.1d, 1.1f, "spark")
    for (v <- values) {
      assertResult(v)(
        LiteralValueProtoConverter.toCatalystValue(LiteralValueProtoConverter.toLiteralProto(v)))
    }
  }

  Seq(
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
            StructType(
              Seq(StructField("c", IntegerType), StructField("d", IntegerType)))))))).zipWithIndex
    .foreach { case ((v, t), idx) =>
      test(s"complex proto value and catalyst value conversion #$idx") {
        assertResult(v)(
          LiteralValueProtoConverter.toCatalystValue(
            LiteralValueProtoConverter.toLiteralProto(v, t)))
      }
    }

  test("backward compatibility for struct literal proto") {
    // Test the old way of defining structs with structType field and elements
    val structTypeProto = proto.DataType.Struct
      .newBuilder()
      .addFields(
        proto.DataType.StructField
          .newBuilder()
          .setName("a")
          .setDataType(proto.DataType
            .newBuilder()
            .setInteger(proto.DataType.Integer.newBuilder())
            .build())
          .setNullable(true)
          .build())
      .addFields(
        proto.DataType.StructField
          .newBuilder()
          .setName("b")
          .setDataType(proto.DataType
            .newBuilder()
            .setString(proto.DataType.String.newBuilder())
            .build())
          .setNullable(false)
          .build())
      .build()

    val structProto = proto.Expression.Literal.Struct
      .newBuilder()
      .setStructType(proto.DataType.newBuilder().setStruct(structTypeProto).build())
      .addElements(LiteralValueProtoConverter.toLiteralProto(1))
      .addElements(LiteralValueProtoConverter.toLiteralProto("test"))
      .build()

    val (result, resultType) = LiteralValueProtoConverter.toCatalystStruct(structProto)

    // Verify the result is a tuple with correct values
    assert(result.isInstanceOf[Product])
    val product = result.asInstanceOf[Product]
    assert(product.productArity == 2)
    assert(product.productElement(0) == 1)
    assert(product.productElement(1) == "test")

    // Verify the returned struct type matches the original
    assert(resultType.getFieldsCount == 2)
    assert(resultType.getFields(0).getName == "a")
    assert(resultType.getFields(0).getDataType.hasInteger)
    assert(resultType.getFields(0).getNullable)
    assert(resultType.getFields(1).getName == "b")
    assert(resultType.getFields(1).getDataType.hasString)
    assert(!resultType.getFields(1).getNullable)
  }

  test("data types of struct fields are not set for inferable types") {
    val literalProto = LiteralValueProtoConverter.toLiteralProto(
      (1, 2.0, true, (1, 2)),
      StructType(
        Seq(
          StructField("a", IntegerType),
          StructField("b", DoubleType),
          StructField("c", BooleanType),
          StructField(
            "d",
            StructType(Seq(StructField("e", IntegerType), StructField("f", IntegerType)))))))
    assert(!literalProto.getStruct.getDataTypeStruct.getFieldsList.get(0).hasDataType)
    assert(!literalProto.getStruct.getDataTypeStruct.getFieldsList.get(1).hasDataType)
    assert(!literalProto.getStruct.getDataTypeStruct.getFieldsList.get(2).hasDataType)
    assert(!literalProto.getStruct.getDataTypeStruct.getFieldsList.get(3).hasDataType)
  }

  test("data types of struct fields are set for non-inferable types") {
    val literalProto = LiteralValueProtoConverter.toLiteralProto(
      ("string", Decimal(1)),
      StructType(Seq(StructField("a", StringType), StructField("b", DecimalType(10, 2)))))
    assert(literalProto.getStruct.getDataTypeStruct.getFieldsList.get(0).hasDataType)
    assert(literalProto.getStruct.getDataTypeStruct.getFieldsList.get(1).hasDataType)
  }

  test("nullable and metadata fields are set for struct literal proto") {
    val literalProto = LiteralValueProtoConverter.toLiteralProto(
      ("string", Decimal(1)),
      StructType(Seq(
        StructField("a", StringType, nullable = true, Metadata.fromJson("""{"key": "value"}""")),
        StructField("b", DecimalType(10, 2), nullable = false))))
    val structFields = literalProto.getStruct.getDataTypeStruct.getFieldsList
    assert(structFields.get(0).getNullable)
    assert(structFields.get(0).hasMetadata)
    assert(structFields.get(0).getMetadata == """{"key":"value"}""")
    assert(!structFields.get(1).getNullable)
    assert(!structFields.get(1).hasMetadata)

    val (_, structTypeProto) = LiteralValueProtoConverter.toCatalystStruct(literalProto.getStruct)
    assert(structTypeProto.getFieldsList.get(0).getNullable)
    assert(structTypeProto.getFieldsList.get(0).hasMetadata)
    assert(structTypeProto.getFieldsList.get(0).getMetadata == """{"key":"value"}""")
    assert(!structTypeProto.getFieldsList.get(1).getNullable)
    assert(!structTypeProto.getFieldsList.get(1).hasMetadata)
  }
}
