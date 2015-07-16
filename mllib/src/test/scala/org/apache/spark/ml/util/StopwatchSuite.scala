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

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext

class StopwatchSuite extends SparkFunSuite with MLlibTestSparkContext {

  private def testStopwatchOnDriver(sw: Stopwatch): Unit = {
    assert(sw.name === "sw")
    assert(sw.elapsed() === 0L)
    assert(!sw.isRunning)
    intercept[AssertionError] {
      sw.stop()
    }
    sw.start()
    Thread.sleep(50)
    val duration = sw.stop()
    assert(duration >= 50 && duration < 100) // using a loose upper bound
    val elapsed = sw.elapsed()
    assert(elapsed === duration)
    sw.start()
    Thread.sleep(50)
    val duration2 = sw.stop()
    assert(duration2 >= 50 && duration2 < 100)
    val elapsed2 = sw.elapsed()
    assert(elapsed2 === duration + duration2)
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
    rdd.foreach { i =>
      sw.start()
      Thread.sleep(50)
      sw.stop()
    }
    assert(!sw.isRunning)
    val elapsed = sw.elapsed()
    assert(elapsed >= 200 && elapsed < 400) // using a loose upper bound
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
    sw("local").start()
    sw("spark").start()
    Thread.sleep(50)
    sw("local").stop()
    Thread.sleep(50)
    sw("spark").stop()
    val localElapsed = sw("local").elapsed()
    val sparkElapsed = sw("spark").elapsed()
    assert(localElapsed >= 50 && localElapsed < 100)
    assert(sparkElapsed >= 100 && sparkElapsed < 200)
    assert(sw.toString ===
      s"{\n  local: ${localElapsed}ms,\n  spark: ${sparkElapsed}ms\n}")
    val rdd = sc.parallelize(0 until 4, 4)
    rdd.foreach { i =>
      sw("local").start()
      sw("spark").start()
      Thread.sleep(50)
      sw("spark").stop()
      sw("local").stop()
    }
    val localElapsed2 = sw("local").elapsed()
    assert(localElapsed2 === localElapsed)
    val sparkElapsed2 = sw("spark").elapsed()
    assert(sparkElapsed2 >= 300 && sparkElapsed2 < 600)
  }
}
