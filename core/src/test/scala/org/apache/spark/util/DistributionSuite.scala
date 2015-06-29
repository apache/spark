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

package org.apache.spark.util

import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite

/**
 *
 */

class DistributionSuite extends SparkFunSuite with Matchers {
  test("summary") {
    val d = new Distribution((1 to 100).toArray.map{_.toDouble})
    val stats = d.statCounter
    stats.count should be (100)
    stats.mean should be (50.5)
    stats.sum should be (50 * 101)

    val quantiles = d.getQuantiles()
    quantiles(0) should be (1)
    quantiles(1) should be (26)
    quantiles(2) should be (51)
    quantiles(3) should be (76)
    quantiles(4) should be (100)
  }
}
