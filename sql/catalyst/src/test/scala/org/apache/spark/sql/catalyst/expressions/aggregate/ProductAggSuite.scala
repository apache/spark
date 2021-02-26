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

  test("empty buffer") {
    assert(evaluator.initialize() === InternalRow(null))
  }

  test("update") {
    val result = evaluator.update(
      InternalRow(-2.0),
      InternalRow(3.0),
      InternalRow(-5.0),
      InternalRow(7.0))
    assert(result === InternalRow(210.0))
  }

  test("update - with nulls") {
    val result1 = evaluator.update(
      InternalRow(null),
      InternalRow(11.0),
      InternalRow(null),
      InternalRow(13.0))
    assert(result1 === InternalRow(143.0))

    val result2 = evaluator.update(
      InternalRow(null),
      InternalRow(null))
    assert(result2 === InternalRow(null))
  }

  test("update - with specials") {
    val result1 = evaluator.update(
      InternalRow(Double.NaN),
      InternalRow(2.0))
    assert(result1 === InternalRow(Double.NaN))

    val result2 = evaluator.update(
      InternalRow(3.0),
      InternalRow(Double.PositiveInfinity))
    assert(result2 === InternalRow(Double.PositiveInfinity))

    val result3 = evaluator.update(
      InternalRow(Double.NegativeInfinity),
      InternalRow(5.0))
    assert(result3 === InternalRow(Double.NegativeInfinity))

    val result4 = evaluator.update(
      InternalRow(7.0),
      InternalRow(Double.PositiveInfinity),
      InternalRow(null))
    assert(result4 === InternalRow(Double.PositiveInfinity))

    val result5 = evaluator.update(
      InternalRow(Double.NaN),
      InternalRow(Double.PositiveInfinity),
      InternalRow(1.0))
    assert(result5 === InternalRow(Double.NaN))

    val result6 = evaluator.update(
      InternalRow(Double.NegativeInfinity),
      InternalRow(2.0),
      InternalRow(Double.NaN))
    assert(result6 === InternalRow(Double.NaN))
  }

  test("merge") {
    // Empty
    val p0 = evaluator.initialize()
    assert(evaluator.merge(p0) === InternalRow(null))

    // Singleton
    val p1 = evaluator.update(InternalRow(7.0), InternalRow(11.0))
    assert(evaluator.merge(p1) === p1)

    // Pair
    val p2 = evaluator.update(InternalRow(17.0), InternalRow(19.0))
    assert(evaluator.merge(p1, p2) === InternalRow((7 * 11 * 17 * 19).toDouble))
    assert(evaluator.merge(p1, p2) === evaluator.merge(p2, p1))

    // Mixtures with empty
    assert(evaluator.merge(p1, p0, p2) === evaluator.merge(p1, p2))
    assert(evaluator.merge(p0, p2, p1) === evaluator.merge(p1, p2))
  }

  test("merge - with nulls") {
    val p0 = evaluator.update(InternalRow(null), InternalRow(null))
    val p1 = evaluator.update(InternalRow(5.0), InternalRow(null))
    val p2 = evaluator.update(InternalRow(null), InternalRow(7.0))

    assert(evaluator.merge(p0, p0) === p0)
    assert(evaluator.merge(p0, p1) === p1)
    assert(evaluator.merge(p2, p0) === p2)

    assert(evaluator.merge(p2, p1, p0) === InternalRow((5 * 7).toDouble))
  }

  test("merge - with specials") {
    val p0 = evaluator.update(InternalRow(Double.NaN), InternalRow(1.0))
    val p1 = evaluator.update(InternalRow(Double.PositiveInfinity), InternalRow(1.0))
    val p2 = evaluator.update(InternalRow(Double.NegativeInfinity), InternalRow(1.0))
    val p3 = evaluator.update(InternalRow(null), InternalRow(1.0))

    assert(evaluator.merge(p0, p0) === p0)
    assert(evaluator.merge(p0, p1) === p0)
    assert(evaluator.merge(p0, p2) === p0)
    assert(evaluator.merge(p0, p3) === p0)

    assert(evaluator.merge(p1, p1) === p1)
    assert(evaluator.merge(p1, p2) === p2)
    assert(evaluator.merge(p1, p3) === p1)

    assert(evaluator.merge(p2, p2) === p1)
    assert(evaluator.merge(p2, p3) === p2)

    assert(evaluator.merge(p3, p3) === p3)
  }

  test("eval") {
    // Null
    assert(evaluator.eval(InternalRow(null)) === InternalRow(null))

    // Empty
    assert(evaluator.eval(evaluator.initialize()) === InternalRow(null))

    // Non-trivial
    val p1 = evaluator.update(InternalRow(2.0), InternalRow(3.0))
    val p2 = evaluator.update(InternalRow(5.0), InternalRow(7.0), InternalRow(11.0))
    val m12 = evaluator.merge(p1, p2)
    assert(evaluator.eval(m12) === InternalRow(2.0 * 3 * 5 * 7 * 11))
  }
}
