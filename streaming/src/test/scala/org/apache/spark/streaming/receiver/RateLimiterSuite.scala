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

package org.apache.spark.streaming.receiver

import org.apache.spark.SparkConf
import org.apache.spark.SparkFunSuite

/** Testsuite for testing the network receiver behavior */
class RateLimiterSuite extends SparkFunSuite {

  test("rate limiter initializes even without a maxRate set") {
    val conf = new SparkConf()
    val rateLimiter = new RateLimiter(conf){}
    rateLimiter.updateRate(105)
    assert(rateLimiter.getCurrentLimit == 105)
  }

  test("rate limiter updates when below maxRate") {
    val conf = new SparkConf().set("spark.streaming.receiver.maxRate", "110")
    val rateLimiter = new RateLimiter(conf){}
    rateLimiter.updateRate(105)
    assert(rateLimiter.getCurrentLimit == 105)
  }

  test("rate limiter stays below maxRate despite large updates") {
    val conf = new SparkConf().set("spark.streaming.receiver.maxRate", "100")
    val rateLimiter = new RateLimiter(conf){}
    rateLimiter.updateRate(105)
    assert(rateLimiter.getCurrentLimit === 100)
  }
}
