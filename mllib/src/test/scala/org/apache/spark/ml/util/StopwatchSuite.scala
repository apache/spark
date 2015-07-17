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

package org.apache.spark.ml.util

import java.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext

class StopwatchSuite extends SparkFunSuite with MLlibTestSparkContext {

  import StopwatchSuite._

  private def testStopwatchOnDriver(sw: Stopwatch): Unit = {
    assert(sw.name === "sw")
    assert(sw.elapsed() === 0L)
    assert(!sw.isRunning)
    intercept[AssertionError] {
      sw.stop()
    }
    val ubStart = now
    sw.start()
    val lbStart = now
    runTask()
    val lb = now - lbStart
    val duration = sw.stop()
    val ub = now - ubStart
    assert(duration >= lb && duration <= ub)
    val elapsed = sw.elapsed()
    assert(elapsed === duration)
    val ubStart2 = now
    sw.start()
    val lbStart2 = now
    runTask()
    val lb2 = now - lbStart2
    val duration2 = sw.stop()
    val ub2 = now - ubStart2
    assert(duration2 >= lb2 && duration2 <= ub2)
    val elapsed2 = sw.elapsed()
    assert(elapsed2 === duration + duration2)
    assert(sw.toString === s"sw: ${elapsed2}ms")
    sw.start()
    assert(sw.isRunning)
    intercept[AssertionError] {
      sw.start()
    }
  }

  test("LocalStopwatch") {
    val sw = new LocalStopwatch("sw")
    testStopwatchOnDriver(sw)
  }

  test("DistributedStopwatch on driver") {
    val sw = new DistributedStopwatch(sc, "sw")
    testStopwatchOnDriver(sw)
  }

  test("DistributedStopwatch on executors") {
    val sw = new DistributedStopwatch(sc, "sw")
    val rdd = sc.parallelize(0 until 4, 4)
    val ubAcc = sc.accumulator(0L)
    val lbAcc = sc.accumulator(0L)
    rdd.foreach { i =>
      val ubStart = now
      sw.start()
      val lbStart = now
      runTask()
      val lb = now - lbStart
      sw.stop()
      val ub = now - ubStart
      lbAcc += lb
      ubAcc += ub
    }
    assert(!sw.isRunning)
    val elapsed = sw.elapsed()
    assert(elapsed >= lbAcc.value && elapsed <= ubAcc.value)
  }

  test("MultiStopwatch") {
    val sw = new MultiStopwatch(sc)
      .addLocal("local")
      .addDistributed("spark")
    assert(sw("local").name === "local")
    assert(sw("spark").name === "spark")
    intercept[NoSuchElementException] {
      sw("some")
    }
    assert(sw.toString === "{\n  local: 0ms,\n  spark: 0ms\n}")
    val localUbStart = now
    sw("local").start()
    val localLbStart = now
    val sparkUbStart = now
    sw("spark").start()
    val sparkLbStart = now
    runTask()
    val localLb = now - localLbStart
    sw("local").stop()
    val localUb = now - localUbStart
    runTask()
    val sparkLb = now - sparkLbStart
    sw("spark").stop()
    val sparkUb = now - sparkUbStart
    val localElapsed = sw("local").elapsed()
    val sparkElapsed = sw("spark").elapsed()
    assert(localElapsed >= localLb && localElapsed <= localUb)
    assert(sparkElapsed >= sparkLb && sparkElapsed <= sparkUb)
    assert(sw.toString ===
      s"{\n  local: ${localElapsed}ms,\n  spark: ${sparkElapsed}ms\n}")
    val rdd = sc.parallelize(0 until 4, 4)
    val lbAcc = sc.accumulator(0L)
    val ubAcc = sc.accumulator(0L)
    rdd.foreach { i =>
      sw("local").start()
      val ubStart = now
      sw("spark").start()
      val lbStart = now
      runTask()
      val lb = now - lbStart
      sw("spark").stop()
      val ub = now - ubStart
      sw("local").stop()
      lbAcc += lb
      ubAcc += ub
    }
    val localElapsed2 = sw("local").elapsed()
    assert(localElapsed2 === localElapsed)
    val sparkElapsed2 = sw("spark").elapsed()
    assert(sparkElapsed2 >= sparkElapsed + lbAcc.value
      && sparkElapsed2 <= sparkElapsed + ubAcc.value)
  }
}

private object StopwatchSuite {

  /** Runs a task that takes a random time. */
  def runTask(): Unit = {
    Thread.sleep(new Random().nextInt(10))
  }

  /** The current time in milliseconds. */
  def now: Long = System.currentTimeMillis()
}
