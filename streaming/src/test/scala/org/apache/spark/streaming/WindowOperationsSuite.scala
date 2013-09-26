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

package org.apache.spark.streaming

import org.apache.spark.streaming.StreamingContext._
import collection.mutable.ArrayBuffer

class WindowOperationsSuite extends TestSuiteBase {

  System.setProperty("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")

  override def framework = "WindowOperationsSuite"

  override def maxWaitTimeMillis = 20000

  override def batchDuration = Seconds(1)

  after {
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }

  val largerSlideInput = Seq(
    Seq(("a", 1)),
    Seq(("a", 2)),  // 1st window from here
    Seq(("a", 3)),
    Seq(("a", 4)),  // 2nd window from here
    Seq(("a", 5)),
    Seq(("a", 6)),  // 3rd window from here
    Seq(),
    Seq()           // 4th window from here
  )

  val largerSlideReduceOutput = Seq(
    Seq(("a", 3)),
    Seq(("a", 10)),
    Seq(("a", 18)),
    Seq(("a", 11))
  )


  val bigInput = Seq(
    Seq(("a", 1)),
    Seq(("a", 1), ("b", 1)),
    Seq(("a", 1), ("b", 1), ("c", 1)),
    Seq(("a", 1), ("b", 1)),
    Seq(("a", 1)),
    Seq(),
    Seq(("a", 1)),
    Seq(("a", 1), ("b", 1)),
    Seq(("a", 1), ("b", 1), ("c", 1)),
    Seq(("a", 1), ("b", 1)),
    Seq(("a", 1)),
    Seq()
  )

  val bigGroupByOutput = Seq(
    Seq(("a", Seq(1))),
    Seq(("a", Seq(1, 1)), ("b", Seq(1))),
    Seq(("a", Seq(1, 1)), ("b", Seq(1, 1)), ("c", Seq(1))),
    Seq(("a", Seq(1, 1)), ("b", Seq(1, 1)), ("c", Seq(1))),
    Seq(("a", Seq(1, 1)), ("b", Seq(1))),
    Seq(("a", Seq(1))),
    Seq(("a", Seq(1))),
    Seq(("a", Seq(1, 1)), ("b", Seq(1))),
    Seq(("a", Seq(1, 1)), ("b", Seq(1, 1)), ("c", Seq(1))),
    Seq(("a", Seq(1, 1)), ("b", Seq(1, 1)), ("c", Seq(1))),
    Seq(("a", Seq(1, 1)), ("b", Seq(1))),
    Seq(("a", Seq(1)))
  )


  val bigReduceOutput = Seq(
    Seq(("a", 1)),
    Seq(("a", 2), ("b", 1)),
    Seq(("a", 2), ("b", 2), ("c", 1)),
    Seq(("a", 2), ("b", 2), ("c", 1)),
    Seq(("a", 2), ("b", 1)),
    Seq(("a", 1)),
    Seq(("a", 1)),
    Seq(("a", 2), ("b", 1)),
    Seq(("a", 2), ("b", 2), ("c", 1)),
    Seq(("a", 2), ("b", 2), ("c", 1)),
    Seq(("a", 2), ("b", 1)),
    Seq(("a", 1))
  )

  /*
  The output of the reduceByKeyAndWindow with inverse function but without a filter
  function will be different from the naive reduceByKeyAndWindow, as no keys get
  eliminated from the ReducedWindowedDStream even if the value of a key becomes 0.
  */

  val bigReduceInvOutput = Seq(
    Seq(("a", 1)),
    Seq(("a", 2), ("b", 1)),
    Seq(("a", 2), ("b", 2), ("c", 1)),
    Seq(("a", 2), ("b", 2), ("c", 1)),
    Seq(("a", 2), ("b", 1), ("c", 0)),
    Seq(("a", 1), ("b", 0), ("c", 0)),
    Seq(("a", 1), ("b", 0), ("c", 0)),
    Seq(("a", 2), ("b", 1), ("c", 0)),
    Seq(("a", 2), ("b", 2), ("c", 1)),
    Seq(("a", 2), ("b", 2), ("c", 1)),
    Seq(("a", 2), ("b", 1), ("c", 0)),
    Seq(("a", 1), ("b", 0), ("c", 0))
  )

  // Testing window operation

  testWindow(
    "basic window",
    Seq( Seq(0), Seq(1), Seq(2), Seq(3), Seq(4), Seq(5)),
    Seq( Seq(0), Seq(0, 1), Seq(1, 2), Seq(2, 3), Seq(3, 4), Seq(4, 5))
  )

  testWindow(
    "tumbling window",
    Seq( Seq(0), Seq(1), Seq(2), Seq(3), Seq(4), Seq(5)),
    Seq( Seq(0, 1), Seq(2, 3), Seq(4, 5)),
    Seconds(2),
    Seconds(2)
  )

  testWindow(
    "larger window",
    Seq( Seq(0), Seq(1), Seq(2), Seq(3), Seq(4), Seq(5)),
    Seq( Seq(0, 1), Seq(0, 1, 2, 3), Seq(2, 3, 4, 5), Seq(4, 5)),
    Seconds(4),
    Seconds(2)
  )

  testWindow(
    "non-overlapping window",
    Seq( Seq(0), Seq(1), Seq(2), Seq(3), Seq(4), Seq(5)),
    Seq( Seq(1, 2), Seq(4, 5)),
    Seconds(2),
    Seconds(3)
  )

  // Testing naive reduceByKeyAndWindow (without invertible function)

  testReduceByKeyAndWindow(
    "basic reduction",
    Seq( Seq(("a", 1), ("a", 3)) ),
    Seq( Seq(("a", 4)) )
  )

  testReduceByKeyAndWindow(
    "key already in window and new value added into window",
    Seq( Seq(("a", 1)), Seq(("a", 1)) ),
    Seq( Seq(("a", 1)), Seq(("a", 2)) )
  )

  testReduceByKeyAndWindow(
    "new key added into window",
    Seq( Seq(("a", 1)), Seq(("a", 1), ("b", 1)) ),
    Seq( Seq(("a", 1)), Seq(("a", 2), ("b", 1)) )
  )

  testReduceByKeyAndWindow(
    "key removed from window",
    Seq( Seq(("a", 1)), Seq(("a", 1)), Seq(), Seq() ),
    Seq( Seq(("a", 1)), Seq(("a", 2)), Seq(("a", 1)), Seq() )
  )

  testReduceByKeyAndWindow(
    "larger slide time",
    largerSlideInput,
    largerSlideReduceOutput,
    Seconds(4),
    Seconds(2)
  )

  testReduceByKeyAndWindow("big test", bigInput, bigReduceOutput)

  // Testing reduceByKeyAndWindow (with invertible reduce function)

  testReduceByKeyAndWindowWithInverse(
    "basic reduction",
    Seq(Seq(("a", 1), ("a", 3)) ),
    Seq(Seq(("a", 4)) )
  )

  testReduceByKeyAndWindowWithInverse(
    "key already in window and new value added into window",
    Seq( Seq(("a", 1)), Seq(("a", 1)) ),
    Seq( Seq(("a", 1)), Seq(("a", 2)) )
  )

  testReduceByKeyAndWindowWithInverse(
    "new key added into window",
    Seq( Seq(("a", 1)), Seq(("a", 1), ("b", 1)) ),
    Seq( Seq(("a", 1)), Seq(("a", 2), ("b", 1)) )
  )

  testReduceByKeyAndWindowWithInverse(
    "key removed from window",
    Seq( Seq(("a", 1)), Seq(("a", 1)), Seq(), Seq() ),
    Seq( Seq(("a", 1)), Seq(("a", 2)), Seq(("a", 1)), Seq(("a", 0)) )
  )

  testReduceByKeyAndWindowWithInverse(
    "larger slide time",
    largerSlideInput,
    largerSlideReduceOutput,
    Seconds(4),
    Seconds(2)
  )

  testReduceByKeyAndWindowWithInverse("big test", bigInput, bigReduceInvOutput)

  testReduceByKeyAndWindowWithFilteredInverse("big test", bigInput, bigReduceOutput)

  test("groupByKeyAndWindow") {
    val input = bigInput
    val expectedOutput = bigGroupByOutput.map(_.map(x => (x._1, x._2.toSet)))
    val windowDuration = Seconds(2)
    val slideDuration = Seconds(1)
    val numBatches = expectedOutput.size * (slideDuration / batchDuration).toInt
    val operation = (s: DStream[(String, Int)]) => {
      s.groupByKeyAndWindow(windowDuration, slideDuration)
       .map(x => (x._1, x._2.toSet))
       .persist()
    }
    testOperation(input, operation, expectedOutput, numBatches, true)
  }

  test("countByWindow") {
    val input = Seq(Seq(1), Seq(1), Seq(1, 2), Seq(0), Seq(), Seq() )
    val expectedOutput = Seq( Seq(1), Seq(2), Seq(3), Seq(3), Seq(1), Seq(0))
    val windowDuration = Seconds(2)
    val slideDuration = Seconds(1)
    val numBatches = expectedOutput.size * (slideDuration / batchDuration).toInt
    val operation = (s: DStream[Int]) => {
      s.countByWindow(windowDuration, slideDuration).map(_.toInt)
    }
    testOperation(input, operation, expectedOutput, numBatches, true)
  }

  test("countByValueAndWindow") {
    val input = Seq(Seq("a"), Seq("b", "b"), Seq("a", "b"))
    val expectedOutput = Seq( Seq(("a", 1)), Seq(("a", 1), ("b", 2)), Seq(("a", 1), ("b", 3)))
    val windowDuration = Seconds(2)
    val slideDuration = Seconds(1)
    val numBatches = expectedOutput.size * (slideDuration / batchDuration).toInt
    val operation = (s: DStream[String]) => {
      s.countByValueAndWindow(windowDuration, slideDuration).map(x => (x._1, x._2.toInt))
    }
    testOperation(input, operation, expectedOutput, numBatches, true)
  }


  // Helper functions

  def testWindow(
    name: String,
    input: Seq[Seq[Int]],
    expectedOutput: Seq[Seq[Int]],
    windowDuration: Duration = Seconds(2),
    slideDuration: Duration = Seconds(1)
    ) {
    test("window - " + name) {
      val numBatches = expectedOutput.size * (slideDuration / batchDuration).toInt
      val operation = (s: DStream[Int]) => s.window(windowDuration, slideDuration)
      testOperation(input, operation, expectedOutput, numBatches, true)
    }
  }

  def testReduceByKeyAndWindow(
    name: String,
    input: Seq[Seq[(String, Int)]],
    expectedOutput: Seq[Seq[(String, Int)]],
    windowDuration: Duration = Seconds(2),
    slideDuration: Duration = Seconds(1)
    ) {
    test("reduceByKeyAndWindow - " + name) {
      logInfo("reduceByKeyAndWindow - " + name)
      val numBatches = expectedOutput.size * (slideDuration / batchDuration).toInt
      val operation = (s: DStream[(String, Int)]) => {
        s.reduceByKeyAndWindow((x: Int, y: Int) => x + y, windowDuration, slideDuration)
      }
      testOperation(input, operation, expectedOutput, numBatches, true)
    }
  }

  def testReduceByKeyAndWindowWithInverse(
    name: String,
    input: Seq[Seq[(String, Int)]],
    expectedOutput: Seq[Seq[(String, Int)]],
    windowDuration: Duration = Seconds(2),
    slideDuration: Duration = Seconds(1)
  ) {
    test("reduceByKeyAndWindow with inverse function - " + name) {
      logInfo("reduceByKeyAndWindow with inverse function - " + name)
      val numBatches = expectedOutput.size * (slideDuration / batchDuration).toInt
      val operation = (s: DStream[(String, Int)]) => {
        s.reduceByKeyAndWindow(_ + _, _ - _, windowDuration, slideDuration)
         .checkpoint(Seconds(100)) // Large value to avoid effect of RDD checkpointing
      }
      testOperation(input, operation, expectedOutput, numBatches, true)
    }
  }

  def testReduceByKeyAndWindowWithFilteredInverse(
      name: String,
      input: Seq[Seq[(String, Int)]],
      expectedOutput: Seq[Seq[(String, Int)]],
      windowDuration: Duration = Seconds(2),
      slideDuration: Duration = Seconds(1)
    ) {
    test("reduceByKeyAndWindow with inverse and filter functions - " + name) {
      logInfo("reduceByKeyAndWindow with inverse and filter functions - " + name)
      val numBatches = expectedOutput.size * (slideDuration / batchDuration).toInt
      val filterFunc = (p: (String, Int)) => p._2 != 0
      val operation = (s: DStream[(String, Int)]) => {
        s.reduceByKeyAndWindow(_ + _, _ - _, windowDuration, slideDuration, filterFunc = filterFunc)
          .persist()
          .checkpoint(Seconds(100)) // Large value to avoid effect of RDD checkpointing
      }
      testOperation(input, operation, expectedOutput, numBatches, true)
    }
  }
}
