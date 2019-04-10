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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

class CountIfSuite extends SparkFunSuite {
  val input: Attribute = AttributeReference("input", BooleanType, nullable = false)()
  val evaluator: DeclarativeAggregateEvaluator =
    DeclarativeAggregateEvaluator(CountIf(input), Seq(input))

  test("empty") {
    val result: InternalRow = evaluator.initialize()
    assert(result === InternalRow(0L))
  }

  test("update") {
    // update with non-nulls
    val result1: InternalRow = evaluator.update(
      InternalRow(true),
      InternalRow(false),
      InternalRow(true))
    assert(result1 === InternalRow(2L))

    // update with nulls
    val result2: InternalRow = evaluator.update(
      InternalRow(null),
      InternalRow(null),
      InternalRow(null))
    assert(result2 === InternalRow(0L))

    // update with non-nulls and nulls
    val result: InternalRow = evaluator.update(
      InternalRow(null),
      InternalRow(true),
      InternalRow(null),
      InternalRow(false))
    assert(result === InternalRow(1L))
  }

  test("merge") {
    // merge empty
    val partition0: InternalRow = evaluator.initialize()
    assert(evaluator.merge(partition0) === InternalRow(0L))

    // merge single
    val partition1: InternalRow = evaluator.update(
      InternalRow(true),
      InternalRow(false),
      InternalRow(true))
    assert(evaluator.merge(partition1) === InternalRow(2L))

    // merge multiples
    val partition2: InternalRow = evaluator.update(
      InternalRow(false),
      InternalRow(true),
      InternalRow(null))
    assert(evaluator.merge(partition0, partition1) === InternalRow(2L))
    assert(evaluator.merge(partition0, partition2) === InternalRow(1L))
    assert(evaluator.merge(partition1, partition2) === InternalRow(3L))
    assert(evaluator.merge(partition0, partition1, partition2) === InternalRow(3L))
  }

  test("eval") {
    // eval null
    assert(evaluator.eval(InternalRow(null)) === InternalRow(0L))

    // eval empty
    val partition0: InternalRow = evaluator.initialize()
    assert(evaluator.eval(partition0) === InternalRow(0L))

    // eval after update
    val partition1: InternalRow = evaluator.update(
      InternalRow(true),
      InternalRow(false),
      InternalRow(true))
    assert(evaluator.eval(partition1) === InternalRow(2L))

    // eval after update and merge
    val partition2: InternalRow = evaluator.update(
      InternalRow(false),
      InternalRow(true),
      InternalRow(null))
    val merge: InternalRow = evaluator.merge(partition0, partition1, partition2)
    assert(evaluator.eval(merge) === InternalRow(3L))
  }
}
