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

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.catalyst.expressions.{Abs, ApproxTopKEstimate, BoundReference, Literal}
import org.apache.spark.sql.types.{ArrayType, BinaryType, IntegerType, MapType, StringType, StructField, StructType}

class ApproxTopKSuite extends SparkFunSuite {

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

    val badAgg2 = new ApproxTopK(
      expr = BoundReference(0, IntegerType, nullable = true),
      k = Literal(10),
      maxItemsTracked = Sum(BoundReference(1, IntegerType, nullable = true))
    )
    assert(badAgg2.checkInputDataTypes().isFailure)
  }

  test("SPARK-52515: invalid item types") {
    Seq(
      ArrayType(IntegerType),
      MapType(StringType, IntegerType),
      StructType(Seq(StructField("a", IntegerType), StructField("b", StringType))),
      BinaryType
    ).foreach { unsupportedType =>
      val agg = new ApproxTopK(
        expr = BoundReference(0, unsupportedType, nullable = true),
        k = Literal(10),
        maxItemsTracked = Literal(10000)
      )
      assert(agg.checkInputDataTypes().isFailure)
    }
  }

  test("SPARK-52588: invalid accumulate if item type is not supported") {
    Seq(
      ArrayType(IntegerType),
      MapType(StringType, IntegerType),
      StructType(Seq(StructField("a", IntegerType), StructField("b", StringType))),
      BinaryType
    ).foreach {
      unsupportedType =>
        val badAccumulate = ApproxTopKAccumulate(
          expr = BoundReference(0, unsupportedType, nullable = true),
          maxItemsTracked = Literal(10)
        )
        assert(badAccumulate.checkInputDataTypes().isFailure)
    }
  }

  test("SPARK-52588: invalid accumulate if maxItemsTracked are not foldable") {
    val badAccumulate = ApproxTopKAccumulate(
      expr = BoundReference(0, IntegerType, nullable = true),
      maxItemsTracked = Sum(BoundReference(1, IntegerType, nullable = true))
    )
    assert(badAccumulate.checkInputDataTypes().isFailure)
  }

  test("SPARK-52588: invalid accumulate if maxItemsTracked less than or equal to 0") {
    Seq(0, -1).foreach { invalidInput =>
      val badAccumulate = ApproxTopKAccumulate(
        expr = BoundReference(0, IntegerType, nullable = true),
        maxItemsTracked = Literal(invalidInput)
      )
      checkError(
        exception = intercept[SparkRuntimeException] {
          badAccumulate.createAggregationBuffer()
        },
        condition = "APPROX_TOP_K_NON_POSITIVE_ARG",
        parameters = Map("argName" -> "`maxItemsTracked`", "argValue" -> invalidInput.toString)
      )
    }
  }

  test("SPARK-52588: invalid estimate if k are not foldable") {
    val badEstimate = ApproxTopKEstimate(
      state = BoundReference(0, IntegerType, nullable = false),
      k = Sum(BoundReference(1, IntegerType, nullable = true))
    )
    assert(badEstimate.checkInputDataTypes().isFailure)
  }

  test("SPARK-combine: invalid combine if maxItemsTracked are not foldable") {
    val badCombine = ApproxTopKCombine(
      expr = BoundReference(0, BinaryType, nullable = false),
      maxItemsTracked = Sum(BoundReference(1, IntegerType, nullable = true))
    )
    assert(badCombine.checkInputDataTypes().isFailure)
  }
}
