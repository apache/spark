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
          LiteralValueProtoConverter.toCatalystValue(
            LiteralValueProtoConverter.toLiteralProto(v, t)))
      }
  }

  test("element type of array literal is set for an empty array") {
    val literalProto =
      LiteralValueProtoConverter.toLiteralProto(Array[Int](), ArrayType(IntegerType))
    assert(literalProto.getArray.hasElementType)
  }

  test("element type of array literal is set for a non-empty array with non-inferable type") {
    val literalProto = LiteralValueProtoConverter.toLiteralProto(
      Array[String]("1", "2", "3"),
      ArrayType(StringType))
    assert(literalProto.getArray.hasElementType)
  }

  test("element type of array literal is not set for a non-empty array with inferable type") {
    val literalProto =
      LiteralValueProtoConverter.toLiteralProto(Array(1, 2, 3), ArrayType(IntegerType))
    assert(!literalProto.getArray.hasElementType)
  }

  test("key and value type of map literal are set for an empty map") {
    val literalProto = LiteralValueProtoConverter.toLiteralProto(
      Map[Int, Int](),
      MapType(IntegerType, IntegerType))
    assert(literalProto.getMap.hasKeyType)
    assert(literalProto.getMap.hasValueType)
  }

  test("key type of map literal is set for a non-empty map with non-inferable key type") {
    val literalProto = LiteralValueProtoConverter.toLiteralProto(
      Map[String, Int]("1" -> 1, "2" -> 2, "3" -> 3),
      MapType(StringType, IntegerType))
    assert(literalProto.getMap.hasKeyType)
    assert(!literalProto.getMap.hasValueType)
  }

  test("value type of map literal is set for a non-empty map with non-inferable value type") {
    val literalProto = LiteralValueProtoConverter.toLiteralProto(
      Map[Int, String](1 -> "1", 2 -> "2", 3 -> "3"),
      MapType(IntegerType, StringType))
    assert(!literalProto.getMap.hasKeyType)
    assert(literalProto.getMap.hasValueType)
  }

  test("key and value type of map literal are not set for a non-empty map with inferable types") {
    val literalProto = LiteralValueProtoConverter.toLiteralProto(
      Map(1 -> 2, 3 -> 4, 5 -> 6),
      MapType(IntegerType, IntegerType))
    assert(!literalProto.getMap.hasKeyType)
    assert(!literalProto.getMap.hasValueType)
  }

  test("an invalid array literal") {
    val literalProto = proto.Expression.Literal
      .newBuilder()
      .setArray(proto.Expression.Literal.Array.newBuilder())
      .build()
    intercept[InvalidPlanInput] {
      LiteralValueProtoConverter.toCatalystValue(literalProto)
    }
  }

  test("an invalid map literal") {
    val literalProto = proto.Expression.Literal
      .newBuilder()
      .setMap(proto.Expression.Literal.Map.newBuilder())
      .build()
    intercept[InvalidPlanInput] {
      LiteralValueProtoConverter.toCatalystValue(literalProto)
    }
  }
}
