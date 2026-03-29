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

package org.apache.spark.streaming.util

import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.Eventually._

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.ManualClock

class RecurringTimerSuite extends SparkFunSuite with PrivateMethodTester {

  test("basic") {
    val clock = new ManualClock()
    val results = new ConcurrentLinkedQueue[Long]()
    val timer = new RecurringTimer(clock, 100, time => {
      results.add(time)
    }, "RecurringTimerSuite-basic")
    timer.start(0)
    eventually(timeout(10.seconds), interval(10.millis)) {
      assert(results.asScala.toSeq === Seq(0L))
    }
    clock.advance(100)
    eventually(timeout(10.seconds), interval(10.millis)) {
      assert(results.asScala.toSeq === Seq(0L, 100L))
    }
    clock.advance(200)
    eventually(timeout(10.seconds), interval(10.millis)) {
      assert(results.asScala.toSeq === Seq(0L, 100L, 200L, 300L))
    }
    assert(timer.stop(interruptTimer = true) === 300L)
  }

  test("SPARK-10224: call 'callback' after stopping") {
    val clock = new ManualClock()
    val results = new ConcurrentLinkedQueue[Long]
    val timer = new RecurringTimer(clock, 100, time => {
      results.add(time)
    }, "RecurringTimerSuite-SPARK-10224")
    timer.start(0)
    eventually(timeout(10.seconds), interval(10.millis)) {
      assert(results.asScala.toSeq === Seq(0L))
    }
    @volatile var lastTime = -1L
    // Now RecurringTimer is waiting for the next interval
    val thread = new Thread {
      override def run(): Unit = {
        lastTime = timer.stop(interruptTimer = false)
      }
    }
    thread.start()
    val stopped = PrivateMethod[RecurringTimer](Symbol("stopped"))
    // Make sure the `stopped` field has been changed
    eventually(timeout(10.seconds), interval(10.millis)) {
      assert(timer.invokePrivate(stopped()) === true)
    }
    clock.advance(200)
    // When RecurringTimer is awake from clock.waitTillTime, it will call `callback` once.
    // Then it will find `stopped` is true and exit the loop, but it should call `callback` again
    // before exiting its internal thread.
    thread.join()
    assert(results.asScala.toSeq === Seq(0L, 100L, 200L))
    assert(lastTime === 200L)
  }
}
