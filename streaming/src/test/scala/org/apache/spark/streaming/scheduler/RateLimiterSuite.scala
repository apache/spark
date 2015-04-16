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

package org.apache.spark.streaming.scheduler

import org.apache.spark.streaming.TestSuiteBase

class RateLimiterSuite extends TestSuiteBase {

  test("FixedRateLimiter with expected rate") {
    var fakeRate = 0.0
    val fixedRateLimiter = new FixedRateLimiter {
      override def defaultRate: Double = fakeRate
      override def isDriver: Boolean = true
    }

    assert(fixedRateLimiter.effectiveRate == Int.MaxValue.toDouble)

    fakeRate = 10.0
    assert(fixedRateLimiter.effectiveRate == 10.0)

    fixedRateLimiter.computeEffectiveRate(1000, 10000)
    assert(fixedRateLimiter.effectiveRate == 10.0)
  }

  test("DynamicRateLimiter with expected rate") {
    var fakeRate = 0.0
    val slowStartRate = 10.0

    val dynamicRateLimiter = new DynamicRateLimiter {
      override def slowStartInitialRate: Double = slowStartRate
      override def defaultRate: Double = fakeRate
      override def isDriver: Boolean = true
    }

    assert(dynamicRateLimiter.effectiveRate == slowStartRate)
    fakeRate = 1.0
    assert(dynamicRateLimiter.effectiveRate == 10.0)
    assert(dynamicRateLimiter.effectiveRate == 10.0)

    dynamicRateLimiter.computeEffectiveRate(11, 10000)
    assert(dynamicRateLimiter.effectiveRate == 1.0)

    dynamicRateLimiter.computeEffectiveRate(9, 10000)
    assert(dynamicRateLimiter.effectiveRate == 0.9)

    dynamicRateLimiter.computeEffectiveRate(0, 10000)
    assert(dynamicRateLimiter.effectiveRate == 0.9)
  }
}
