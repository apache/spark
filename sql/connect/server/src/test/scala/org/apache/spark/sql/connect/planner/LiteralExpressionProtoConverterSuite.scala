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
import org.apache.spark.sql.connect.common.InvalidPlanInput
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter.ToLiteralProtoOptions
import org.apache.spark.sql.connect.planner.LiteralExpressionProtoConverter
import org.apache.spark.sql.types._

class LiteralExpressionProtoConverterSuite extends AnyFunSuite { // scalastyle:ignore funsuite

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
            StructType(Seq(StructField("c", IntegerType), StructField("d", IntegerType))))))),
    (Array(true, false, true), ArrayType(BooleanType)),
    (Array(1.toByte, 2.toByte, 3.toByte), ArrayType(ByteType)),
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
    (Array(Map(1 -> 2)), ArrayType(MapType(IntegerType, IntegerType))),
    (Map[String, String]("1" -> "2", "3" -> "4"), MapType(StringType, StringType)),
    (Map[String, Boolean]("1" -> true, "2" -> false), MapType(StringType, BooleanType)),
    (Map[Int, Int](), MapType(IntegerType, IntegerType)),
    (Map(1 -> 2, 3 -> 4, 5 -> 6), MapType(IntegerType, IntegerType))).zipWithIndex.foreach {
    case ((v, t), idx) =>
      test(s"complex proto value and catalyst value conversion #$idx") {
        assertResult(v)(
          LiteralValueProtoConverter.toScalaValue(
            LiteralValueProtoConverter.toLiteralProtoWithOptions(
              v,
              Some(t),
              ToLiteralProtoOptions(useDeprecatedDataTypeFields = false))))
      }

      test(s"complex proto value and catalyst value conversion #$idx - backward compatibility") {
        assertResult(v)(
          LiteralValueProtoConverter.toScalaValue(
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
    assert(!literalProto.getArray.hasDataType)
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
    assert(!literalProto.getMap.hasDataType)
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
    assert(!structProto.getStruct.hasDataTypeStruct)
    assert(structProto.getStruct.getElementsList.size == 2)
    val structTypeProto = structProto.getStruct.getStructType.getStruct
    assert(structTypeProto.getFieldsList.size == 2)
    assert(structTypeProto.getFieldsList.get(0).getName == "a")
    assert(structTypeProto.getFieldsList.get(0).getDataType.hasInteger)
    assert(structTypeProto.getFieldsList.get(1).getName == "b")
    assert(structTypeProto.getFieldsList.get(1).getDataType.hasString)

    val result = LiteralValueProtoConverter.toScalaStruct(structProto.getStruct)
    val resultType = LiteralValueProtoConverter.getProtoStructType(structProto.getStruct)

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
    val literalProto = toLiteralProto(
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
    val literalProto = toLiteralProto(
      ("string", Decimal(1)),
      StructType(Seq(StructField("a", StringType), StructField("b", DecimalType(10, 2)))))
    assert(literalProto.getStruct.getDataTypeStruct.getFieldsList.get(0).hasDataType)
    assert(literalProto.getStruct.getDataTypeStruct.getFieldsList.get(1).hasDataType)
  }

  test("nullable and metadata fields are set for struct literal proto") {
    val literalProto = toLiteralProto(
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

    val structTypeProto = LiteralValueProtoConverter.getProtoStructType(literalProto.getStruct)
    assert(structTypeProto.getFieldsList.get(0).getNullable)
    assert(structTypeProto.getFieldsList.get(0).hasMetadata)
    assert(structTypeProto.getFieldsList.get(0).getMetadata == """{"key":"value"}""")
    assert(!structTypeProto.getFieldsList.get(1).getNullable)
    assert(!structTypeProto.getFieldsList.get(1).hasMetadata)
  }

  test("element type of array literal is set for an empty array") {
    val literalProto =
      toLiteralProto(Array[Int](), ArrayType(IntegerType))
    assert(literalProto.getArray.getDataType.hasElementType)
  }

  test("element type of array literal is set for a non-empty array with non-inferable type") {
    val literalProto = toLiteralProto(Array[String]("1", "2", "3"), ArrayType(StringType))
    assert(literalProto.getArray.getDataType.hasElementType)
  }

  test("element type of array literal is not set for a non-empty array with inferable type") {
    val literalProto =
      toLiteralProto(Array(1, 2, 3), ArrayType(IntegerType))
    assert(!literalProto.getArray.getDataType.hasElementType)
  }

  test("key and value type of map literal are set for an empty map") {
    val literalProto = toLiteralProto(Map[Int, Int](), MapType(IntegerType, IntegerType))
    assert(literalProto.getMap.getDataType.hasKeyType)
    assert(literalProto.getMap.getDataType.hasValueType)
  }

  test("key type of map literal is set for a non-empty map with non-inferable key type") {
    val literalProto = toLiteralProto(
      Map[String, Int]("1" -> 1, "2" -> 2, "3" -> 3),
      MapType(StringType, IntegerType))
    assert(literalProto.getMap.getDataType.hasKeyType)
    assert(!literalProto.getMap.getDataType.hasValueType)
  }

  test("value type of map literal is set for a non-empty map with non-inferable value type") {
    val literalProto = toLiteralProto(
      Map[Int, String](1 -> "1", 2 -> "2", 3 -> "3"),
      MapType(IntegerType, StringType))
    assert(!literalProto.getMap.getDataType.hasKeyType)
    assert(literalProto.getMap.getDataType.hasValueType)
  }

  test("key and value type of map literal are not set for a non-empty map with inferable types") {
    val literalProto =
      toLiteralProto(Map(1 -> 2, 3 -> 4, 5 -> 6), MapType(IntegerType, IntegerType))
    assert(!literalProto.getMap.getDataType.hasKeyType)
    assert(!literalProto.getMap.getDataType.hasValueType)
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
