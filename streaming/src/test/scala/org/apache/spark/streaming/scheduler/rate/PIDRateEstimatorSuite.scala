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

package org.apache.spark.streaming.scheduler.rate

import scala.util.Random

import org.scalatest.Inspectors.forAll
import org.scalatest.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.streaming.Seconds

class PIDRateEstimatorSuite extends SparkFunSuite with Matchers {

  test("the right estimator is created") {
    val conf = new SparkConf
    conf.set("spark.streaming.backpressure.rateEstimator", "pid")
    val pid = RateEstimator.create(conf, Seconds(1))
    pid.getClass should equal(classOf[PIDRateEstimator])
  }

  test("estimator checks ranges") {
    intercept[IllegalArgumentException] {
      new PIDRateEstimator(0, 1, 2, 3)
    }
    intercept[IllegalArgumentException] {
      new PIDRateEstimator(100, -1, 2, 3)
    }
    intercept[IllegalArgumentException] {
      new PIDRateEstimator(100, 0, -1, 3)
    }
    intercept[IllegalArgumentException] {
      new PIDRateEstimator(100, 0, 0, -1)
    }
  }

  private def createDefaultEstimator: PIDRateEstimator = {
    new PIDRateEstimator(20, 1D, 0D, 0D)
  }

  test("first bound is None") {
    val p = createDefaultEstimator
    p.compute(0, 10, 10, 0) should equal(None)
  }

  test("second bound is rate") {
    val p = createDefaultEstimator
    p.compute(0, 10, 10, 0)
    // 1000 elements / s
    p.compute(10, 10, 10, 0) should equal(Some(1000))
  }

  test("works even with no time between updates") {
    val p = createDefaultEstimator
    p.compute(0, 10, 10, 0)
    p.compute(10, 10, 10, 0)
    p.compute(10, 10, 10, 0) should equal(None)
  }

  test("bound is never negative") {
    val p = new PIDRateEstimator(20, 1D, 1D, 0D)
    // prepare a series of batch updates, one every 20ms, 0 processed elements, 2ms of processing
    // this might point the estimator to try and decrease the bound, but we test it never
    // goes below zero, which would be nonsensical.
    val times = List.tabulate(50)(x => x * 20) // every 20ms
    val elements = List.fill(50)(0) // no processing
    val proc = List.fill(50)(20) // 20ms of processing
    val sched = List.fill(50)(100) // strictly positive accumulation
    val res = for (i <- List.range(0, 50)) yield p.compute(times(i), elements(i), proc(i), sched(i))
    res.head should equal(None)
    res.tail should equal(List.fill(49)(Some(0D)))
  }

  test("with no accumulated or positive error, |I| > 0, follow the processing speed") {
    val p = new PIDRateEstimator(20, 1D, 1D, 0D)
    // prepare a series of batch updates, one every 20ms with an increasing number of processed
    // elements in each batch, but constant processing time, and no accumulated error. Even though
    // the integral part is non-zero, the estimated rate should follow only the proportional term
    val times = List.tabulate(50)(x => x * 20) // every 20ms
    val elements = List.tabulate(50)(x => x * 20) // increasing
    val proc = List.fill(50)(20) // 20ms of processing
    val sched = List.fill(50)(0)
    val res = for (i <- List.range(0, 50)) yield p.compute(times(i), elements(i), proc(i), sched(i))
    res.head should equal(None)
    res.tail should equal(List.tabulate(50)(x => Some(x * 1000D)).tail)
  }

  test("with no accumulated but some positive error, |I| > 0, follow the processing speed") {
    val p = new PIDRateEstimator(20, 1D, 1D, 0D)
    // prepare a series of batch updates, one every 20ms with an decreasing number of processed
    // elements in each batch, but constant processing time, and no accumulated error. Even though
    // the integral part is non-zero, the estimated rate should follow only the proportional term,
    // asking for less and less elements
    val times = List.tabulate(50)(x => x * 20) // every 20ms
    val elements = List.tabulate(50)(x => (50 - x) * 20) // decreasing
    val proc = List.fill(50)(20) // 20ms of processing
    val sched = List.fill(50)(0)
    val res = for (i <- List.range(0, 50)) yield p.compute(times(i), elements(i), proc(i), sched(i))
    res.head should equal(None)
    res.tail should equal(List.tabulate(50)(x => Some((50 - x) * 1000D)).tail)
  }

  test("with some accumulated and some positive error, |I| > 0, stay below the processing speed") {
    val p = new PIDRateEstimator(20, 1D, .01D, 0D)
    val times = List.tabulate(50)(x => x * 20) // every 20ms
    val rng = new Random()
    val elements = List.tabulate(50)(x => rng.nextInt(1000))
    val procDelayMs = 20
    val proc = List.fill(50)(procDelayMs) // 20ms of processing
    val sched = List.tabulate(50)(x => rng.nextInt(19)) // random wait
    val speeds = elements map ((x) => x.toDouble / procDelayMs * 1000)

    val res = for (i <- List.range(0, 50)) yield p.compute(times(i), elements(i), proc(i), sched(i))
    res.head should equal(None)
    forAll(List.range(1, 50)) { (n) =>
      res(n) should not be None
      if (res(n).get > 0 && sched(n) > 0) {
        res(n).get should be < speeds(n)
      }
    }
  }
}
