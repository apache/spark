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
package org.apache.spark.mllib.fim

import org.scalatest.FunSuite
import org.apache.spark.mllib.util.LocalSparkContext

/**
 * scala test unit
 * using Practical Machine Learning Book data test the FPGrowth algorithm result by minSupport from 0.9 to 0.1
 */
class FPGrowthSuite  extends FunSuite with LocalSparkContext {

  test("test FIM with FPGrowth")
  {
    val arr = AprioriSuite.createFIMDataSet2()

    assert(arr.length === 6)
    val dataSet = sc.parallelize(arr)
    assert(dataSet.count() == 6)
    val rdd = dataSet.map(line => line.split(" "))
    assert(rdd.count() == 6)

    //check frenquent item set length
    val fimWithFPGrowth = new FPGrowth()
    println(fimWithFPGrowth.FPGrowth(rdd,0.1,sc).length)

    assert(fimWithFPGrowth.FPGrowth(rdd,0.9,sc).length == 0)

    assert(fimWithFPGrowth.FPGrowth(rdd,0.8,sc).length == 1)

    assert(fimWithFPGrowth.FPGrowth(rdd,0.7,sc).length == 1)

    assert(fimWithFPGrowth.FPGrowth(rdd,0.6,sc).length == 2)

    assert(fimWithFPGrowth.FPGrowth(rdd,0.5,sc).length == 18)

    assert(fimWithFPGrowth.FPGrowth(rdd,0.4,sc).length == 18)

    assert(fimWithFPGrowth.FPGrowth(rdd,0.3,sc).length == 54)

    assert(fimWithFPGrowth.FPGrowth(rdd,0.2,sc).length == 54)

    assert(fimWithFPGrowth.FPGrowth(rdd,0.1,sc).length == 625)

  }
}

/**
 * create dataset
 */
object FPGrowthSuite
{
  /**
   * create dataset using Practical Machine Learning Book data
   * @return dataset
   */
    def createFIMDataSet():Array[String] =
    {
      val arr = Array[String]("r z h j p",
        "z y x w v u t s",
        "z",
        "r x n o s",
        "y r x z q t p",
        "y z x e q s t m")
      return arr
    }
}
