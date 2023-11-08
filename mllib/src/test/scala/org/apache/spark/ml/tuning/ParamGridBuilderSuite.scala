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

package org.apache.spark.ml.tuning

import scala.collection.mutable

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.{ParamMap, TestParams}

class ParamGridBuilderSuite extends SparkFunSuite {

  val solver = new TestParams()
  import solver.{inputCol, maxIter}

  test("param grid builder") {
    def validateGrid(maps: Array[ParamMap], expected: mutable.Set[(Int, String)]): Unit = {
      assert(maps.size === expected.size)
      maps.foreach { m =>
        val tuple = (m(maxIter), m(inputCol))
        assert(expected.contains(tuple))
        expected.remove(tuple)
      }
      assert(expected.isEmpty)
    }

    val maps0 = new ParamGridBuilder()
      .baseOn(maxIter -> 10)
      .addGrid(inputCol, Array("input0", "input1"))
      .build()
    val expected0 = mutable.Set(
      (10, "input0"),
      (10, "input1"))
    validateGrid(maps0, expected0)

    val maps1 = new ParamGridBuilder()
      .baseOn(ParamMap(maxIter -> 5, inputCol -> "input")) // will be overwritten
      .addGrid(maxIter, Array(10, 20))
      .addGrid(inputCol, Array("input0", "input1"))
      .build()
    val expected1 = mutable.Set(
      (10, "input0"),
      (20, "input0"),
      (10, "input1"),
      (20, "input1"))
    validateGrid(maps1, expected1)
  }
}
