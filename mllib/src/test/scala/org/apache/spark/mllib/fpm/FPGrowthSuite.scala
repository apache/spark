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
package org.apache.spark.mllib.fpm

import org.scalatest.FunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext

class FPGrowthSuite  extends FunSuite with MLlibTestSparkContext {

  test("test FPGrowth algorithm")
  {
    val arr = FPGrowthSuite.createTestData()

    assert(arr.length === 6)
    val dataSet = sc.parallelize(arr)
    assert(dataSet.count() == 6)
    val rdd = dataSet.map(line => line.split(" "))
    assert(rdd.count() == 6)

    val algorithm = new FPGrowth()
    algorithm.setMinSupport(0.9)
    assert(algorithm.run(rdd).frequentPattern.length == 0)
    algorithm.setMinSupport(0.8)
    assert(algorithm.run(rdd).frequentPattern.length == 1)
    algorithm.setMinSupport(0.7)
    assert(algorithm.run(rdd).frequentPattern.length == 1)
    algorithm.setMinSupport(0.6)
    assert(algorithm.run(rdd).frequentPattern.length == 2)
    algorithm.setMinSupport(0.5)
    assert(algorithm.run(rdd).frequentPattern.length == 18)
    algorithm.setMinSupport(0.4)
    assert(algorithm.run(rdd).frequentPattern.length == 18)
    algorithm.setMinSupport(0.3)
    assert(algorithm.run(rdd).frequentPattern.length == 54)
    algorithm.setMinSupport(0.2)
    assert(algorithm.run(rdd).frequentPattern.length == 54)
    algorithm.setMinSupport(0.1)
    assert(algorithm.run(rdd).frequentPattern.length == 625)
  }
}

object FPGrowthSuite
{
  /**
   * Create test data set
   */
    def createTestData():Array[String] =
    {
      val arr = Array[String](
        "r z h k p",
        "z y x w v u t s",
        "s x o n r",
        "x z y m t s q e",
        "z",
        "x z y r q t p")
      arr
    }
}
