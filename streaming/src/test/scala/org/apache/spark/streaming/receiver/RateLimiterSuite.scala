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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.util.SystemClock

/** Testsuite for testing the network receiver behavior */
class RateLimiterSuite extends SparkFunSuite {

  test("rate limiter initializes even without a maxRate set") {
    val conf = new SparkConf()
    val rateLimiter = new RateLimiter(conf, new SystemClock){}
    rateLimiter.updateRate(105)
    assert(rateLimiter.getCurrentLimit == 105)
  }

  test("rate limiter updates when below maxRate") {
    val conf = new SparkConf().set("spark.streaming.receiver.maxRate", "110")
    val rateLimiter = new RateLimiter(conf, new SystemClock){}
    rateLimiter.updateRate(105)
    assert(rateLimiter.getCurrentLimit == 105)
  }

  test("rate limiter stays below maxRate despite large updates") {
    val conf = new SparkConf().set("spark.streaming.receiver.maxRate", "100")
    val rateLimiter = new RateLimiter(conf, new SystemClock){}
    rateLimiter.updateRate(105)
    assert(rateLimiter.getCurrentLimit === 100)
  }

  test("historySumThenTrim() returns expected numRecordsLimit") {
    val conf = new SparkConf().set("spark.streaming.receiver.maxRate", "100")
                              .set("spark.streaming.blockInterval", "500ms")
    val rateLimiter = new RateLimiter(conf, new SystemClock){}

    // Make sure that rateLimitHistory starts with a special initial snapshot
    assert(rateLimiter.rateLimitHistory(0).limit == 100)
    assert(rateLimiter.rateLimitHistory(0).ts == -1)

    // Test if sumHistoryThenTrim() works well with the first batch which
    // contains a special initial snapshot
    {
      rateLimiter.appendLimitToHistory(10, 1100)
      rateLimiter.appendLimitToHistory(20, 1200)
      rateLimiter.appendLimitToHistory(30, 1300)
      rateLimiter.appendLimitToHistory(40, 1400)
      val sum = rateLimiter.sumHistoryThenTrim(1500)
      val expectedInMillis = 100 * (1100 - (1500 - 500)) +
                              10 * (1200 - 1100) +
                              20 * (1300 - 1200) +
                              30 * (1400 - 1300) +
                              40 * (1500 - 1400)
      val expected = expectedInMillis / 1000
      assert(sum == expected)
    }

    assert(rateLimiter.rateLimitHistory.length == 1)
    assert(rateLimiter.rateLimitHistory(0).limit == 40)
    assert(rateLimiter.rateLimitHistory(0).ts == 1500)

    {
      val sum = rateLimiter.sumHistoryThenTrim(2000)
      val expectedInMillis = 40 * (2000 - 1500)
      val expected = expectedInMillis / 1000
      assert(sum == expected)
    }

    assert(rateLimiter.rateLimitHistory.length == 1)
    assert(rateLimiter.rateLimitHistory(0).limit == 40)
    assert(rateLimiter.rateLimitHistory(0).ts == 2000)

    {
      rateLimiter.appendLimitToHistory(50, 2100)
      val sum = rateLimiter.sumHistoryThenTrim(2500)
      val expectedInMillis = 40 * (2100 - 2000) +
                             50 * (2500 - 2100)
      val expected = expectedInMillis / 1000
      assert(sum == expected)
    }

    assert(rateLimiter.rateLimitHistory.length == 1)
    assert(rateLimiter.rateLimitHistory(0).limit == 50)
    assert(rateLimiter.rateLimitHistory(0).ts == 2500)
  }

  test("sumRateLimits() works good for normal cases") {
    // if rateLimits contains some None, then sumRateLimits() should return None
    var rateLimits: Seq[Option[Long]] = Seq(Some(1L), None)
    assert(RateLimiterHelper.sumRateLimits(rateLimits) == None)

    // normal case
    rateLimits = Seq(Some(10L))
    var sum: Option[Long] = RateLimiterHelper.sumRateLimits(rateLimits)
    assert(sum.isDefined && sum.get == 10L)
    
    // another normal case
    rateLimits = Seq(Some(10L), Some(20L), Some(30L), Some(40L))
    sum = RateLimiterHelper.sumRateLimits(rateLimits)
    assert(sum.isDefined && sum.get == 100L)
  }

  test("sumRateLimits() works good for overflow cases") {
    // overflow case
    var rateLimits: Seq[Option[Long]] = Seq(Some(Long.MaxValue), Some(1L))
    var sum = RateLimiterHelper.sumRateLimits(rateLimits)
    assert(sum.isDefined && sum.get == Long.MaxValue)

    // another overflow case
    rateLimits = Seq(Some(Long.MaxValue), Some(Long.MaxValue), Some(Long.MaxValue))
    sum = RateLimiterHelper.sumRateLimits(rateLimits)
    assert(sum.isDefined && sum.get == Long.MaxValue)
  }

}
