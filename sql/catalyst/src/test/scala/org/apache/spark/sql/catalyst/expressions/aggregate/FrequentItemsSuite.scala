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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckFailure}
import org.apache.spark.sql.catalyst.expressions.{Abs, ApproxFrequentItemsEstimate, BoundReference, Literal}
import org.apache.spark.sql.catalyst.expressions.Cast.ordinalNumber
import org.apache.spark.sql.types.{ArrayType, BinaryType, IntegerType, LongType, MapType, StringType, StructField, StructType}

class FrequentItemsSuite extends SparkFunSuite {

  /////////////////////////////////
  // ApproxFrequentItems tests
  /////////////////////////////////

  test("Accepts literal and foldable inputs") {
    val agg = new ApproxFrequentItems(
      expr = BoundReference(0, IntegerType, nullable = true),
      k = Abs(Literal(10)),
      maxItemsTracked = Abs(Literal(-10))
    )
    assert(agg.checkInputDataTypes().isSuccess)
  }

  test("Fail if parameters are not foldable") {
    val badAgg = new ApproxFrequentItems(
      expr = BoundReference(0, IntegerType, nullable = true),
      k = Sum(BoundReference(1, IntegerType, nullable = true)),
      maxItemsTracked = Literal(10)
    )
    assert(badAgg.checkInputDataTypes().isFailure)
    assert(badAgg.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(1),
          "requiredType" -> "\"INT\"",
          "inputSql" -> "\"sum(boundreference())\"",
          "inputType" -> "\"BIGINT\""
        )
      )
    )

    val badAgg2 = new ApproxFrequentItems(
      expr = BoundReference(0, IntegerType, nullable = true),
      k = Literal(10),
      maxItemsTracked = Sum(BoundReference(1, IntegerType, nullable = true))
    )
    assert(badAgg2.checkInputDataTypes().isFailure)
    assert(badAgg2.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(2),
          "requiredType" -> "\"INT\"",
          "inputSql" -> "\"sum(boundreference())\"",
          "inputType" -> "\"BIGINT\""
        )
      )
    )
  }

  gridTest("invalid ApproxFrequentItems with unsupported item types")(
    Seq(
      ("array", ArrayType(IntegerType)),
      ("map", MapType(StringType, IntegerType)),
      ("struct", StructType(Seq(StructField("a", IntegerType)))),
      ("binary", BinaryType)
    )) { unSupportedType =>
    val (typeName, dataType) = unSupportedType
    val agg = new ApproxFrequentItems(
      expr = BoundReference(0, dataType, nullable = true),
      k = Literal(10),
      maxItemsTracked = Literal(10000)
    )
    assert(agg.checkInputDataTypes().isFailure)
    assert(agg.checkInputDataTypes() == TypeCheckFailure(s"$typeName columns are not supported"))
  }

  ///////////////////////////////////////////
  // ApproxFrequentItemsAccumulate tests
  ///////////////////////////////////////////

  gridTest("invalid accumulate if item type is not supported")(
    Seq(
      ("array", ArrayType(IntegerType)),
      ("map", MapType(StringType, IntegerType)),
      ("struct", StructType(Seq(StructField("a", IntegerType)))),
      ("binary", BinaryType)
    )) { unSupportedType =>
    val (typeName, dataType) = unSupportedType
    val badAccumulate = ApproxFrequentItemsAccumulate(
      expr = BoundReference(0, dataType, nullable = true),
      maxItemsTracked = Literal(10)
    )
    assert(badAccumulate.checkInputDataTypes().isFailure)
    assert(badAccumulate.checkInputDataTypes() ==
      TypeCheckFailure(s"$typeName columns are not supported"))
  }

  test("invalid accumulate if maxItemsTracked are not foldable") {
    val badAccumulate = ApproxFrequentItemsAccumulate(
      expr = BoundReference(0, IntegerType, nullable = true),
      maxItemsTracked = Sum(BoundReference(1, IntegerType, nullable = true))
    )
    assert(badAccumulate.checkInputDataTypes().isFailure)
    assert(badAccumulate.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(1),
          "requiredType" -> "\"INT\"",
          "inputSql" -> "\"sum(boundreference())\"",
          "inputType" -> "\"BIGINT\""
        )
      )
    )
  }

  /////////////////////////////////////////
  // ApproxFrequentItemsEstimate tests
  /////////////////////////////////////////

  test("invalid estimate if k are not foldable") {
    val badEstimate = ApproxFrequentItemsEstimate(
      state = BoundReference(0, StructType(Seq(
        StructField("sketch", BinaryType),
        StructField("maxItemsTracked", IntegerType),
        StructField("itemDataType", IntegerType),
        StructField("itemDataTypeDDL", StringType)
      )), nullable = false),
      k = Sum(BoundReference(1, IntegerType, nullable = true))
    )
    assert(badEstimate.checkInputDataTypes().isFailure)
    assert(badEstimate.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(1),
          "requiredType" -> "\"INT\"",
          "inputSql" -> "\"sum(boundreference())\"",
          "inputType" -> "\"BIGINT\""
        )
      )
    )
  }

  test("invalid estimate if state is not a struct") {
    val badEstimate = ApproxFrequentItemsEstimate(
      state = BoundReference(0, IntegerType, nullable = false),
      k = Literal(5)
    )
    assert(badEstimate.checkInputDataTypes().isFailure)
    assert(badEstimate.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> "\"STRUCT\"",
          "inputSql" -> "\"boundreference()\"",
          "inputType" -> "\"INT\""
        )
      )
    )
  }

  test("invalid estimate if state struct length is not 4") {
    val invalidState = StructType(Seq(
      StructField("sketch", BinaryType),
      StructField("maxItemsTracked", IntegerType)
      // Missing "itemDataType", "itemDataTypeDDL" fields
    ))
    val badEstimate = ApproxFrequentItemsEstimate(
      state = BoundReference(0, invalidState, nullable = false),
      k = Literal(5)
    )
    assert(badEstimate.checkInputDataTypes().isFailure)
    assert(badEstimate.checkInputDataTypes() ==
      TypeCheckFailure("State must be a struct with 4 fields. " +
        "Expected struct: " +
        "struct<sketch:binary,maxItemsTracked:int,itemDataType:any,itemDataTypeDDL:string>. " +
        "Got: struct<sketch:binary,maxItemsTracked:int>"))
  }

  test("invalid estimate if state struct's first field is not binary") {
    val invalidState = StructType(Seq(
      StructField("notSketch", IntegerType), // Should be BinaryType
      StructField("maxItemsTracked", IntegerType),
      StructField("itemDataType", IntegerType),
      StructField("itemDataTypeDDL", StringType)
    ))
    val badEstimate = ApproxFrequentItemsEstimate(
      state = BoundReference(0, invalidState, nullable = false),
      k = Literal(5)
    )
    assert(badEstimate.checkInputDataTypes().isFailure)
    assert(badEstimate.checkInputDataTypes() ==
      TypeCheckFailure("State struct must have the first field to be binary. Got: int"))
  }

  test("invalid estimate if state struct's second field is not int") {
    val invalidState = StructType(Seq(
      StructField("sketch", BinaryType),
      StructField("maxItemsTracked", LongType), // Should be IntegerType
      StructField("itemDataType", IntegerType),
      StructField("itemDataTypeDDL", StringType)
    ))
    val badEstimate = ApproxFrequentItemsEstimate(
      state = BoundReference(0, invalidState, nullable = false),
      k = Literal(5)
    )
    assert(badEstimate.checkInputDataTypes().isFailure)
    assert(badEstimate.checkInputDataTypes() ==
      TypeCheckFailure("State struct must have the second field to be int. Got: bigint"))
  }

  gridTest("invalid estimate if state struct's third field is not supported")(
    Seq(
      ("array<int>", ArrayType(IntegerType)),
      ("map<string,int>", MapType(StringType, IntegerType)),
      ("struct<a:int>", StructType(Seq(StructField("a", IntegerType)))),
      ("binary", BinaryType)
    )) { unSupportedType =>
    val (typeName, dataType) = unSupportedType
    val invalidState = StructType(Seq(
      StructField("sketch", BinaryType),
      StructField("maxItemsTracked", IntegerType),
      StructField("itemDataType", dataType),
      StructField("itemDataTypeDDL", StringType)
    ))
    val badEstimate = ApproxFrequentItemsEstimate(
      state = BoundReference(0, invalidState, nullable = false),
      k = Literal(5)
    )
    assert(badEstimate.checkInputDataTypes().isFailure)
    assert(badEstimate.checkInputDataTypes() ==
      TypeCheckFailure(s"State struct must have the third field to be a supported data type. " +
        s"Got: $typeName"))
  }

  test("invalid estimate if state struct's fourth field is not string") {
    val invalidState = StructType(Seq(
      StructField("sketch", BinaryType),
      StructField("maxItemsTracked", IntegerType),
      StructField("itemDataType", IntegerType),
      StructField("itemDataTypeDDL", BinaryType) // Should be StringType
    ))
    val badEstimate = ApproxFrequentItemsEstimate(
      state = BoundReference(0, invalidState, nullable = false),
      k = Literal(5)
    )
    assert(badEstimate.checkInputDataTypes().isFailure)
    assert(badEstimate.checkInputDataTypes() ==
      TypeCheckFailure("State struct must have the fourth field to be string. Got: binary"))
  }

  ////////////////////////////////////////
  // ApproxFrequentItemsCombine tests
  ////////////////////////////////////////

  test("invalid combine if maxItemsTracked is not foldable") {
    val badCombine = ApproxFrequentItemsCombine(
      state = BoundReference(0, StructType(Seq(
        StructField("sketch", BinaryType),
        StructField("maxItemsTracked", IntegerType),
        StructField("itemDataType", IntegerType),
        StructField("itemDataTypeDDL", StringType)
      )), nullable = false),
      maxItemsTracked = Sum(BoundReference(1, IntegerType, nullable = true))
    )
    assert(badCombine.checkInputDataTypes().isFailure)
    assert(badCombine.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(1),
          "requiredType" -> "\"INT\"",
          "inputSql" -> "\"sum(boundreference())\"",
          "inputType" -> "\"BIGINT\""
        )
      )
    )
  }

  test("invalid combine if state is not a struct") {
    val badCombine = ApproxFrequentItemsCombine(
      state = BoundReference(0, IntegerType, nullable = false),
      maxItemsTracked = Literal(10)
    )
    assert(badCombine.checkInputDataTypes().isFailure)
    assert(badCombine.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> "\"STRUCT\"",
          "inputSql" -> "\"boundreference()\"",
          "inputType" -> "\"INT\""
        )
      )
    )
  }

  test("invalid combine if state struct length is not 4") {
    val invalidState = StructType(Seq(
      StructField("sketch", BinaryType),
      StructField("maxItemsTracked", IntegerType)
      // Missing "itemDataType", "itemDataTypeDDL" fields
    ))
    val badCombine = ApproxFrequentItemsCombine(
      state = BoundReference(0, invalidState, nullable = false),
      maxItemsTracked = Literal(10)
    )
    assert(badCombine.checkInputDataTypes().isFailure)
    assert(badCombine.checkInputDataTypes() ==
      TypeCheckFailure("State must be a struct with 4 fields. " +
        "Expected struct: " +
        "struct<sketch:binary,maxItemsTracked:int,itemDataType:any,itemDataTypeDDL:string>. " +
        "Got: struct<sketch:binary,maxItemsTracked:int>"))
  }

  test("invalid combine if state struct's first field is not binary") {
    val invalidState = StructType(Seq(
      StructField("sketch", IntegerType), // Should be BinaryType
      StructField("maxItemsTracked", IntegerType),
      StructField("itemDataType", IntegerType),
      StructField("itemDataTypeDDL", StringType)
    ))
    val badCombine = ApproxFrequentItemsCombine(
      state = BoundReference(0, invalidState, nullable = false),
      maxItemsTracked = Literal(10)
    )
    assert(badCombine.checkInputDataTypes().isFailure)
    assert(badCombine.checkInputDataTypes() ==
      TypeCheckFailure("State struct must have the first field to be binary. Got: int"))
  }

  test("invalid combine if state struct's second field is not int") {
    val invalidState = StructType(Seq(
      StructField("sketch", BinaryType),
      StructField("maxItemsTracked", LongType), // Should be IntegerType
      StructField("itemDataType", IntegerType),
      StructField("itemDataTypeDDL", StringType)
    ))
    val badCombine = ApproxFrequentItemsCombine(
      state = BoundReference(0, invalidState, nullable = false),
      maxItemsTracked = Literal(10)
    )
    assert(badCombine.checkInputDataTypes().isFailure)
    assert(badCombine.checkInputDataTypes() ==
      TypeCheckFailure("State struct must have the second field to be int. Got: bigint"))
  }

  gridTest("invalid combine if state struct's third field is not supported")(
    Seq(
      ("array<int>", ArrayType(IntegerType)),
      ("map<string,int>", MapType(StringType, IntegerType)),
      ("struct<a:int>", StructType(Seq(StructField("a", IntegerType)))),
      ("binary", BinaryType)
    )) { unSupportedType =>
    val (typeName, dataType) = unSupportedType
    val invalidState = StructType(Seq(
      StructField("sketch", BinaryType),
      StructField("maxItemsTracked", IntegerType),
      StructField("itemDataType", dataType),
      StructField("itemDataTypeDDL", StringType)
    ))
    val badCombine = ApproxFrequentItemsCombine(
      state = BoundReference(0, invalidState, nullable = false),
      maxItemsTracked = Literal(10)
    )
    assert(badCombine.checkInputDataTypes().isFailure)
    assert(badCombine.checkInputDataTypes() ==
      TypeCheckFailure(s"State struct must have the third field to be a supported data type. " +
        s"Got: $typeName"))
  }

  test("invalid combine if state struct's fourth field is not string") {
    val invalidState = StructType(Seq(
      StructField("sketch", BinaryType),
      StructField("maxItemsTracked", IntegerType),
      StructField("itemDataType", IntegerType),
      StructField("itemDataTypeDDL", BinaryType) // Should be StringType
    ))
    val badCombine = ApproxFrequentItemsCombine(
      state = BoundReference(0, invalidState, nullable = false),
      maxItemsTracked = Literal(10)
    )
    assert(badCombine.checkInputDataTypes().isFailure)
    assert(badCombine.checkInputDataTypes() ==
      TypeCheckFailure("State struct must have the fourth field to be string. Got: binary"))
  }
}
