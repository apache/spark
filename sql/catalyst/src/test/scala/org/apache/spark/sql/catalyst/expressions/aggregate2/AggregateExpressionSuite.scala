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

package org.apache.spark.sql.catalyst.expressions.aggregate2

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, BoundReference, Literal, ExpressionEvalHelper}
import org.apache.spark.sql.types._

class AggregateExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("Average") {
    val inputValues = Array(Int.MaxValue, null, 1000, Int.MinValue, 2)
    val avg = Average(child = BoundReference(0, IntegerType, true)).withBufferOffset(2)
    val inputRow = new GenericMutableRow(1)
    val buffer = new GenericMutableRow(4)
    avg.initialize(buffer)

    // We there is no input data, average should return null.
    assert(avg.eval(buffer) === null)
    // When input values are all nulls, average should return null.
    var i = 0
    while (i < 10) {
      inputRow.update(0, null)
      avg.update(inputRow, buffer)
      i += 1
    }
    assert(avg.eval(buffer) === null)

    // Add some values.
    i = 0
    while (i < inputValues.length) {
      inputRow.update(0, inputValues(i))
      avg.update(buffer, inputRow)
      i += 1
    }
    assert(avg.eval(buffer) === 1001 / 4.0)

    // eval should not reset the buffer
    assert(buffer(2) === 1001L)
    assert(buffer(3) === 4L)
    assert(avg.eval(buffer) === 1001 / 4.0)

    // Merge with a just initialized buffer.
    val inputBuffer = new GenericMutableRow(4)
    avg.initialize(inputBuffer)
    avg.merge(buffer, inputBuffer)
    assert(buffer(2) === 1001L)
    assert(buffer(3) === 4L)
    assert(avg.eval(buffer) === 1001 / 4.0)

    // Merge with a buffer containing partial results.
    inputBuffer.update(2, 2000.0)
    inputBuffer.update(3, 10L)
    avg.merge(buffer, inputBuffer)
    assert(buffer(2) === 3001L)
    assert(buffer(3) === 14L)
    assert(avg.eval(buffer) === 3001 / 14.0)
  }
}
