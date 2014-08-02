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

package org.apache.spark.mllib.stat

import org.scalatest.FunSuite

import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class HypothesisTestSuite extends FunSuite with LocalSparkContext {
  test("chi squared") {
    val x = sc.parallelize(Array(2.0, 23.0, 53.0))
    val y = sc.parallelize(Array(53.0, 76.0, 1.0))
    val c = Statistics.chiSquared(x, y)
    assert(c.statistic ~= 120.2546 absTol 1e-3)

    val bad = sc.parallelize(Array(2.0, -23.0, 53.0))
    intercept[Exception](Statistics.chiSquared(bad, y))
  }
}
