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
import org.apache.spark.sql.catalyst.expressions.{Abs, ApproxTopKEstimate, BoundReference, Literal}
import org.apache.spark.sql.catalyst.expressions.Cast.ordinalNumber
import org.apache.spark.sql.types.{ArrayType, BinaryType, IntegerType, LongType, MapType, StringType, StructField, StructType}

class ApproxTopKSuite extends SparkFunSuite {

  /////////////////////////////
  // ApproxTopK tests
  /////////////////////////////

  test("SPARK-52515: Accepts literal and foldable inputs") {
    val agg = new ApproxTopK(
      expr = BoundReference(0, IntegerType, nullable = true),
      k = Abs(Literal(10)),
      maxItemsTracked = Abs(Literal(-10))
    )
    assert(agg.checkInputDataTypes().isSuccess)
  }

  test("SPARK-52515: Fail if parameters are not foldable") {
    val badAgg = new ApproxTopK(
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

    val badAgg2 = new ApproxTopK(
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

  gridTest("SPARK-52515: invalid ApproxTopK with unsupported item types")(
    Seq(
      ("array", ArrayType(IntegerType)),
      ("map", MapType(StringType, IntegerType)),
      ("struct", StructType(Seq(StructField("a", IntegerType)))),
      ("binary", BinaryType)
    )) { unSupportedType =>
    val (typeName, dataType) = unSupportedType
    val agg = new ApproxTopK(
      expr = BoundReference(0, dataType, nullable = true),
      k = Literal(10),
      maxItemsTracked = Literal(10000)
    )
    assert(agg.checkInputDataTypes().isFailure)
    assert(agg.checkInputDataTypes() == TypeCheckFailure(s"$typeName columns are not supported"))
  }

  /////////////////////////////
  // ApproxTopKAccumulate tests
  /////////////////////////////

  gridTest("SPARK-52588: invalid accumulate if item type is not supported")(
    Seq(
      ("array", ArrayType(IntegerType)),
      ("map", MapType(StringType, IntegerType)),
      ("struct", StructType(Seq(StructField("a", IntegerType)))),
      ("binary", BinaryType)
    )) { unSupportedType =>
    val (typeName, dataType) = unSupportedType
    val badAccumulate = ApproxTopKAccumulate(
      expr = BoundReference(0, dataType, nullable = true),
      maxItemsTracked = Literal(10)
    )
    assert(badAccumulate.checkInputDataTypes().isFailure)
    assert(badAccumulate.checkInputDataTypes() ==
      TypeCheckFailure(s"$typeName columns are not supported"))
  }

  test("SPARK-52588: invalid accumulate if maxItemsTracked are not foldable") {
    val badAccumulate = ApproxTopKAccumulate(
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

  /////////////////////////////
  // ApproxTopKEstimate tests
  /////////////////////////////

  val stateStructType: StructType = StructType(Seq(
    StructField("sketch", BinaryType),
    StructField("itemDataType", IntegerType),
    StructField("maxItemsTracked", IntegerType)
  ))

  test("SPARK-52588: invalid estimate if k are not foldable") {
    val badEstimate = ApproxTopKEstimate(
      state = BoundReference(0, StructType(Seq(
        StructField("sketch", BinaryType),
        StructField("itemDataType", IntegerType),
        StructField("maxItemsTracked", IntegerType))), nullable = false),
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

  test("SPARK-52588: invalid estimate if state is not a struct") {
    val badEstimate = ApproxTopKEstimate(
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

  test("SPARK-52588: invalid estimate if state struct length is not 3") {
    val invalidState = StructType(Seq(
      StructField("sketch", BinaryType),
      StructField("itemDataType", IntegerType)
      // Missing "maxItemsTracked"
    ))
    val badEstimate = ApproxTopKEstimate(
      state = BoundReference(0, invalidState, nullable = false),
      k = Literal(5)
    )
    assert(badEstimate.checkInputDataTypes().isFailure)
    assert(badEstimate.checkInputDataTypes() ==
      TypeCheckFailure("State must be a struct with 3 fields. " +
        "Expected struct: struct<sketch:binary,itemDataType:any,maxItemsTracked:int>. " +
        "Got: struct<sketch:binary,itemDataType:int>"))
  }

  test("SPARK-52588: invalid estimate if state struct's first field is not binary") {
    val invalidState = StructType(Seq(
      StructField("notSketch", IntegerType), // Should be BinaryType
      StructField("itemDataType", IntegerType),
      StructField("maxItemsTracked", IntegerType)
    ))
    val badEstimate = ApproxTopKEstimate(
      state = BoundReference(0, invalidState, nullable = false),
      k = Literal(5)
    )
    assert(badEstimate.checkInputDataTypes().isFailure)
    assert(badEstimate.checkInputDataTypes() ==
      TypeCheckFailure("State struct must have the first field to be binary. Got: int"))
  }

  gridTest("SPARK-52588: invalid estimate if state struct's second field is not supported")(
    Seq(
      ("array<int>", ArrayType(IntegerType)),
      ("map<string,int>", MapType(StringType, IntegerType)),
      ("struct<a:int>", StructType(Seq(StructField("a", IntegerType)))),
      ("binary", BinaryType)
    )) { unSupportedType =>
    val (typeName, dataType) = unSupportedType
    val invalidState = StructType(Seq(
      StructField("sketch", BinaryType),
      StructField("itemDataType", dataType),
      StructField("maxItemsTracked", IntegerType)
    ))
    val badEstimate = ApproxTopKEstimate(
      state = BoundReference(0, invalidState, nullable = false),
      k = Literal(5)
    )
    assert(badEstimate.checkInputDataTypes().isFailure)
    assert(badEstimate.checkInputDataTypes() ==
      TypeCheckFailure(s"State struct must have the second field to be a supported data type. " +
        s"Got: $typeName"))
  }

  test("SPARK-52588: invalid estimate if state struct's third field is not int") {
    val invalidState = StructType(Seq(
      StructField("sketch", BinaryType),
      StructField("itemDataType", IntegerType),
      StructField("maxItemsTracked", LongType) // Should be IntegerType
    ))
    val badEstimate = ApproxTopKEstimate(
      state = BoundReference(0, invalidState, nullable = false),
      k = Literal(5)
    )
    assert(badEstimate.checkInputDataTypes().isFailure)
    assert(badEstimate.checkInputDataTypes() ==
      TypeCheckFailure("State struct must have the third field to be int. Got: bigint"))
  }
}
