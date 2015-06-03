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

package org.apache.spark.mllib.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.util.MLlibTestSparkContext

class RandomProjectionSuite extends SparkFunSuite with MLlibTestSparkContext {

  var rp: RandomProjection = _
  var map: ParamMap = _
  val intrinsicDimension = 20

  override def beforeAll(): Unit = {
    super.beforeAll()
    rp = new RandomProjection(intrinsicDimension)
    map = new ParamMap()
  }


  test("calculate correct density") {
    val density = 0.22360679774997896
    assert(rp.getDensity == density)
  }

  test("create random, non-repeating list") {
    val range = 0 until 50
    val maxValue = 200
    val items = rp.drawNonZeroIndices(range, maxValue)
    assert(items.max <= maxValue)
    val list = items.toList
    assert(items.length == items.toList.distinct.length)
  }

  test("draw a list of random -1 or 1 values") {
    val limit = 20
    val range = 0 until limit
    val items = rp.drawNonZeroValues(range)

    assert(items.length == limit)
    val expLimit = items.count { value => value == -1.0 || value == 1.0 }
    assert(expLimit == limit)
  }

  test("scale non zero values accordingly") {
    val value = 0.2
    assert(rp.scaleNonZeroRandomValue(0.4, 0.2, 20) == value)
  }

  test("ensure the RP matrix has the correct dimension") {
    val origDimension = 200
    val density = 0.2
    val reduced = rp.computeRPRows(origDimension, density)

    // at least one column index must match the max dimension
    val countA = reduced.count {
      _.i == origDimension - 1
    }
    assert(countA >= 1)

    val countB = reduced.count {
      _.j == intrinsicDimension - 1
    }
    assert(countB >= 1)
  }

}
