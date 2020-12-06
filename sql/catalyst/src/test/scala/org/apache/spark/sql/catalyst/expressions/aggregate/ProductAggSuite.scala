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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.DoubleType


class ProductAggSuite extends SparkFunSuite {
  val input = AttributeReference("product", DoubleType, nullable = true)()
  val evaluator = DeclarativeAggregateEvaluator(Product(input), Seq(input))
  val evaluatorWithScale = DeclarativeAggregateEvaluator(Product(input, 2.0), Seq(input))

  test("empty buffer") {
    assert(evaluator.initialize() === InternalRow(null, false))
  }

  test("update") {
    val result = evaluator.update(
      InternalRow(-2.0),
      InternalRow(3.0),
      InternalRow(-5.0),
      InternalRow(7.0) )
    assert(result === InternalRow(210.0, true))
  }

  test("update - with scale") {
    val result = evaluatorWithScale.update(
      InternalRow(-2.0),
      InternalRow(3.0),
      InternalRow(-5.0),
      InternalRow(7.0) )
    assert(result === InternalRow(210.0 * (1 << 4), true))
  }

  test("update - ignore nulls") {
    val result1 = evaluator.update(
      InternalRow(null),
      InternalRow(11.0),
      InternalRow(null),
      InternalRow(13.0) )
    assert(result1 === InternalRow(143.0, true))

    val result2 = evaluator.update(
      InternalRow(null),
      InternalRow(null))
    assert(result2 === InternalRow(null, false))

    val result3 = evaluatorWithScale.update(
      InternalRow(null),
      InternalRow(17.0))
    assert(result2 === InternalRow(34.0, false))
  }

  // FIXME - add tests for merge & extraction methods
}
