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

package org.apache.spark.sql.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

class ReduceAggregatorSuite extends SparkFunSuite {

  test("zero value") {
    val encoder: ExpressionEncoder[Int] = ExpressionEncoder()
    val func = (v1: Int, v2: Int) => v1 + v2
    val aggregator: ReduceAggregator[Int] = new ReduceAggregator(func)(Encoders.scalaInt)
    assert(aggregator.zero == (false, null))
  }

  test("reduce, merge and finish") {
    val encoder: ExpressionEncoder[Int] = ExpressionEncoder()
    val func = (v1: Int, v2: Int) => v1 + v2
    val aggregator: ReduceAggregator[Int] = new ReduceAggregator(func)(Encoders.scalaInt)

    val firstReduce = aggregator.reduce(aggregator.zero, 1)
    assert(firstReduce == (true, 1))

    val secondReduce = aggregator.reduce(firstReduce, 2)
    assert(secondReduce == (true, 3))

    val thirdReduce = aggregator.reduce(secondReduce, 3)
    assert(thirdReduce == (true, 6))

    val mergeWithZero1 = aggregator.merge(aggregator.zero, firstReduce)
    assert(mergeWithZero1 == (true, 1))

    val mergeWithZero2 = aggregator.merge(secondReduce, aggregator.zero)
    assert(mergeWithZero2 == (true, 3))

    val mergeTwoReduced = aggregator.merge(firstReduce, secondReduce)
    assert(mergeTwoReduced == (true, 4))

    assert(aggregator.finish(firstReduce)== 1)
    assert(aggregator.finish(secondReduce) == 3)
    assert(aggregator.finish(thirdReduce) == 6)
    assert(aggregator.finish(mergeWithZero1) == 1)
    assert(aggregator.finish(mergeWithZero2) == 3)
    assert(aggregator.finish(mergeTwoReduced) == 4)
  }

  test("requires at least one input row") {
    val encoder: ExpressionEncoder[Int] = ExpressionEncoder()
    val func = (v1: Int, v2: Int) => v1 + v2
    val aggregator: ReduceAggregator[Int] = new ReduceAggregator(func)(Encoders.scalaInt)

    intercept[IllegalStateException] {
      aggregator.finish(aggregator.zero)
    }
  }
}
