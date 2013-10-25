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

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import util.ManualClock

class BasicOperationsSuite extends TestSuiteBase {

  override def framework() = "BasicOperationsSuite"

  before {
    System.setProperty("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
  }

  after {
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }

  test("map") {
    val input = Seq(1 to 4, 5 to 8, 9 to 12)
    testOperation(
      input,
      (r: DStream[Int]) => r.map(_.toString),
      input.map(_.map(_.toString))
    )
  }

  test("flatMap") {
    val input = Seq(1 to 4, 5 to 8, 9 to 12)
    testOperation(
      input,
      (r: DStream[Int]) => r.flatMap(x => Seq(x, x * 2)),
      input.map(_.flatMap(x => Array(x, x * 2)))
    )
  }

  test("filter") {
    val input = Seq(1 to 4, 5 to 8, 9 to 12)
    testOperation(
      input,
      (r: DStream[Int]) => r.filter(x => (x % 2 == 0)),
      input.map(_.filter(x => (x % 2 == 0)))
    )
  }

  test("glom") {
    assert(numInputPartitions === 2, "Number of input partitions has been changed from 2")
    val input = Seq(1 to 4, 5 to 8, 9 to 12)
    val output = Seq(
      Seq( Seq(1, 2), Seq(3, 4) ),
      Seq( Seq(5, 6), Seq(7, 8) ),
      Seq( Seq(9, 10), Seq(11, 12) )
    )
    val operation = (r: DStream[Int]) => r.glom().map(_.toSeq)
    testOperation(input, operation, output)
  }

  test("mapPartitions") {
    assert(numInputPartitions === 2, "Number of input partitions has been changed from 2")
    val input = Seq(1 to 4, 5 to 8, 9 to 12)
    val output = Seq(Seq(3, 7), Seq(11, 15), Seq(19, 23))
    val operation = (r: DStream[Int]) => r.mapPartitions(x => Iterator(x.reduce(_ + _)))
    testOperation(input, operation, output, true)
  }

  test("repartition (more partitions)") {
    val input = Seq(1 to 100, 101 to 200, 201 to 300)
    val operation = (r: DStream[Int]) => r.repartition(5)
    val ssc = setupStreams(input, operation, 2)
    val output = runStreamsWithPartitions(ssc, 3, 3)
    assert(output.size === 3)
    val first = output(0)
    val second = output(1)
    val third = output(2)

    assert(first.size === 5)
    assert(second.size === 5)
    assert(third.size === 5)

    assert(first.flatten.toSet === (1 to 100).toSet)
    assert(second.flatten.toSet === (101 to 200).toSet)
    assert(third.flatten.toSet === (201 to 300).toSet)
  }

  test("repartition (fewer partitions)") {
    val input = Seq(1 to 100, 101 to 200, 201 to 300)
    val operation = (r: DStream[Int]) => r.repartition(2)
    val ssc = setupStreams(input, operation, 5)
    val output = runStreamsWithPartitions(ssc, 3, 3)
    assert(output.size === 3)
    val first = output(0)
    val second = output(1)
    val third = output(2)

    assert(first.size === 2)
    assert(second.size === 2)
    assert(third.size === 2)

    assert(first.flatten.toSet === (1 to 100).toSet)
    assert(second.flatten.toSet === (101 to 200).toSet)
    assert(third.flatten.toSet === (201 to 300).toSet)
  }

  test("groupByKey") {
    testOperation(
      Seq( Seq("a", "a", "b"), Seq("", ""), Seq() ),
      (s: DStream[String]) => s.map(x => (x, 1)).groupByKey(),
      Seq( Seq(("a", Seq(1, 1)), ("b", Seq(1))), Seq(("", Seq(1, 1))), Seq() ),
      true
    )
  }

  test("reduceByKey") {
    testOperation(
      Seq( Seq("a", "a", "b"), Seq("", ""), Seq() ),
      (s: DStream[String]) => s.map(x => (x, 1)).reduceByKey(_ + _),
      Seq( Seq(("a", 2), ("b", 1)), Seq(("", 2)), Seq() ),
      true
    )
  }

  test("reduce") {
    testOperation(
      Seq(1 to 4, 5 to 8, 9 to 12),
      (s: DStream[Int]) => s.reduce(_ + _),
      Seq(Seq(10), Seq(26), Seq(42))
    )
  }

  test("count") {
    testOperation(
      Seq(Seq(), 1 to 1, 1 to 2, 1 to 3, 1 to 4),
      (s: DStream[Int]) => s.count(),
      Seq(Seq(0L), Seq(1L), Seq(2L), Seq(3L), Seq(4L))
    )
  }

  test("countByValue") {
    testOperation(
      Seq(1 to 1, Seq(1, 1, 1), 1 to 2, Seq(1, 1, 2, 2)),
      (s: DStream[Int]) => s.countByValue(),
      Seq(Seq((1, 1L)), Seq((1, 3L)), Seq((1, 1L), (2, 1L)), Seq((2, 2L), (1, 2L))),
      true
    )
  }

  test("mapValues") {
    testOperation(
      Seq( Seq("a", "a", "b"), Seq("", ""), Seq() ),
      (s: DStream[String]) => s.map(x => (x, 1)).reduceByKey(_ + _).mapValues(_ + 10),
      Seq( Seq(("a", 12), ("b", 11)), Seq(("", 12)), Seq() ),
      true
    )
  }

  test("flatMapValues") {
    testOperation(
      Seq( Seq("a", "a", "b"), Seq("", ""), Seq() ),
      (s: DStream[String]) => s.map(x => (x, 1)).reduceByKey(_ + _).flatMapValues(x => Seq(x, x + 10)),
      Seq( Seq(("a", 2), ("a", 12), ("b", 1), ("b", 11)), Seq(("", 2), ("", 12)), Seq() ),
      true
    )
  }

  test("union") {
    val input = Seq(1 to 4, 101 to 104, 201 to 204)
    val output = Seq(1 to 8, 101 to 108, 201 to 208)
    testOperation(
      input,
      (s: DStream[Int]) => s.union(s.map(_ + 4)) ,
      output
    )
  }

  test("StreamingContext.union") {
    val input = Seq(1 to 4, 101 to 104, 201 to 204)
    val output = Seq(1 to 12, 101 to 112, 201 to 212)
    // union over 3 DStreams
    testOperation(
      input,
      (s: DStream[Int]) => s.context.union(Seq(s, s.map(_ + 4), s.map(_ + 8))),
      output
    )
  }

  test("transform") {
    val input = Seq(1 to 4, 5 to 8, 9 to 12)
    testOperation(
      input,
      (r: DStream[Int]) => r.transform(rdd => rdd.map(_.toString)),   // RDD.map in transform
      input.map(_.map(_.toString))
    )
  }

  test("transformWith") {
    val inputData1 = Seq( Seq("a", "b"), Seq("a", ""), Seq(""), Seq() )
    val inputData2 = Seq( Seq("a", "b"), Seq("b", ""), Seq(), Seq("")   )
    val outputData = Seq(
      Seq( ("a", (1, "x")), ("b", (1, "x")) ),
      Seq( ("", (1, "x")) ),
      Seq(  ),
      Seq(  )
    )
    val operation = (s1: DStream[String], s2: DStream[String]) => {
      val t1 = s1.map(x => (x, 1))
      val t2 = s2.map(x => (x, "x"))
      t1.transformWith(           // RDD.join in transform
        t2,
        (rdd1: RDD[(String, Int)], rdd2: RDD[(String, String)]) => rdd1.join(rdd2)
      )
    }
    testOperation(inputData1, inputData2, operation, outputData, true)
  }

  test("StreamingContext.transform") {
    val input = Seq(1 to 4, 101 to 104, 201 to 204)
    val output = Seq(1 to 12, 101 to 112, 201 to 212)

    // transform over 3 DStreams by doing union of the 3 RDDs
    val operation = (s: DStream[Int]) => {
      s.context.transform(
        Seq(s, s.map(_ + 4), s.map(_ + 8)),   // 3 DStreams
        (rdds: Seq[RDD[_]], time: Time) =>
          rdds.head.context.union(rdds.map(_.asInstanceOf[RDD[Int]]))  // union of RDDs
      )
    }

    testOperation(input, operation, output)
  }

  test("cogroup") {
    val inputData1 = Seq( Seq("a", "a", "b"), Seq("a", ""), Seq(""), Seq() )
    val inputData2 = Seq( Seq("a", "a", "b"), Seq("b", ""), Seq(), Seq()   )
    val outputData = Seq(
      Seq( ("a", (Seq(1, 1), Seq("x", "x"))), ("b", (Seq(1), Seq("x"))) ),
      Seq( ("a", (Seq(1), Seq())), ("b", (Seq(), Seq("x"))), ("", (Seq(1), Seq("x"))) ),
      Seq( ("", (Seq(1), Seq())) ),
      Seq(  )
    )
    val operation = (s1: DStream[String], s2: DStream[String]) => {
      s1.map(x => (x,1)).cogroup(s2.map(x => (x, "x")))
    }
    testOperation(inputData1, inputData2, operation, outputData, true)
  }

  test("join") {
    val inputData1 = Seq( Seq("a", "b"), Seq("a", ""), Seq(""), Seq() )
    val inputData2 = Seq( Seq("a", "b"), Seq("b", ""), Seq(), Seq("")   )
    val outputData = Seq(
      Seq( ("a", (1, "x")), ("b", (1, "x")) ),
      Seq( ("", (1, "x")) ),
      Seq(  ),
      Seq(  )
    )
    val operation = (s1: DStream[String], s2: DStream[String]) => {
      s1.map(x => (x, 1)).join(s2.map(x => (x, "x")))
    }
    testOperation(inputData1, inputData2, operation, outputData, true)
  }

  test("leftOuterJoin") {
    val inputData1 = Seq( Seq("a", "b"), Seq("a", ""), Seq(""), Seq() )
    val inputData2 = Seq( Seq("a", "b"), Seq("b", ""), Seq(), Seq("")   )
    val outputData = Seq(
      Seq( ("a", (1, Some("x"))), ("b", (1, Some("x"))) ),
      Seq( ("", (1, Some("x"))), ("a", (1, None)) ),
      Seq( ("", (1, None)) ),
      Seq(  )
    )
    val operation = (s1: DStream[String], s2: DStream[String]) => {
      s1.map(x => (x, 1)).leftOuterJoin(s2.map(x => (x, "x")))
    }
    testOperation(inputData1, inputData2, operation, outputData, true)
  }

  test("rightOuterJoin") {
    val inputData1 = Seq( Seq("a", "b"), Seq("a", ""), Seq(""), Seq() )
    val inputData2 = Seq( Seq("a", "b"), Seq("b", ""), Seq(), Seq("")   )
    val outputData = Seq(
      Seq( ("a", (Some(1), "x")), ("b", (Some(1), "x")) ),
      Seq( ("", (Some(1), "x")), ("b", (None, "x")) ),
      Seq(  ),
      Seq( ("", (None, "x")) )
    )
    val operation = (s1: DStream[String], s2: DStream[String]) => {
      s1.map(x => (x, 1)).rightOuterJoin(s2.map(x => (x, "x")))
    }
    testOperation(inputData1, inputData2, operation, outputData, true)
  }

  test("updateStateByKey") {
    val inputData =
      Seq(
        Seq("a"),
        Seq("a", "b"),
        Seq("a", "b", "c"),
        Seq("a", "b"),
        Seq("a"),
        Seq()
      )

    val outputData =
      Seq(
        Seq(("a", 1)),
        Seq(("a", 2), ("b", 1)),
        Seq(("a", 3), ("b", 2), ("c", 1)),
        Seq(("a", 4), ("b", 3), ("c", 1)),
        Seq(("a", 5), ("b", 3), ("c", 1)),
        Seq(("a", 5), ("b", 3), ("c", 1))
      )

    val updateStateOperation = (s: DStream[String]) => {
      val updateFunc = (values: Seq[Int], state: Option[Int]) => {
        Some(values.foldLeft(0)(_ + _) + state.getOrElse(0))
      }
      s.map(x => (x, 1)).updateStateByKey[Int](updateFunc)
    }

    testOperation(inputData, updateStateOperation, outputData, true)
  }

  test("updateStateByKey - object lifecycle") {
    val inputData =
      Seq(
        Seq("a","b"),
        null,
        Seq("a","c","a"),
        Seq("c"),
        null,
        null
      )

    val outputData =
      Seq(
        Seq(("a", 1), ("b", 1)),
        Seq(("a", 1), ("b", 1)),
        Seq(("a", 3), ("c", 1)),
        Seq(("a", 3), ("c", 2)),
        Seq(("c", 2)),
        Seq()
      )

    val updateStateOperation = (s: DStream[String]) => {
      class StateObject(var counter: Int = 0, var expireCounter: Int = 0) extends Serializable

      // updateFunc clears a state when a StateObject is seen without new values twice in a row
      val updateFunc = (values: Seq[Int], state: Option[StateObject]) => {
        val stateObj = state.getOrElse(new StateObject)
        values.foldLeft(0)(_ + _) match {
          case 0 => stateObj.expireCounter += 1 // no new values
          case n => { // has new values, increment and reset expireCounter
            stateObj.counter += n
            stateObj.expireCounter = 0
          }
        }
        stateObj.expireCounter match {
          case 2 => None // seen twice with no new values, give it the boot
          case _ => Option(stateObj)
        }
      }
      s.map(x => (x, 1)).updateStateByKey[StateObject](updateFunc).mapValues(_.counter)
    }

    testOperation(inputData, updateStateOperation, outputData, true)
  }

  test("slice") {
    val ssc = new StreamingContext("local[2]", "BasicOperationSuite", Seconds(1))
    val input = Seq(Seq(1), Seq(2), Seq(3), Seq(4))
    val stream = new TestInputStream[Int](ssc, input, 2)
    ssc.registerInputStream(stream)
    stream.foreach(_ => {})  // Dummy output stream
    ssc.start()
    Thread.sleep(2000)
    def getInputFromSlice(fromMillis: Long, toMillis: Long) = {
      stream.slice(new Time(fromMillis), new Time(toMillis)).flatMap(_.collect()).toSet
    }

    assert(getInputFromSlice(0, 1000) == Set(1))
    assert(getInputFromSlice(0, 2000) == Set(1, 2))
    assert(getInputFromSlice(1000, 2000) == Set(1, 2))
    assert(getInputFromSlice(2000, 4000) == Set(2, 3, 4))
    ssc.stop()
    Thread.sleep(1000)
  }

  test("forgetting of RDDs - map and window operations") {
    assert(batchDuration === Seconds(1), "Batch duration has changed from 1 second")

    val input = (0 until 10).map(x => Seq(x, x + 1)).toSeq
    val rememberDuration = Seconds(3)

    assert(input.size === 10, "Number of inputs have changed")

    def operation(s: DStream[Int]): DStream[(Int, Int)] = {
      s.map(x => (x % 10, 1))
       .window(Seconds(2), Seconds(1))
       .window(Seconds(4), Seconds(2))
    }

    val ssc = setupStreams(input, operation _)
    ssc.remember(rememberDuration)
    runStreams[(Int, Int)](ssc, input.size, input.size / 2)

    val windowedStream2 = ssc.graph.getOutputStreams().head.dependencies.head
    val windowedStream1 = windowedStream2.dependencies.head
    val mappedStream = windowedStream1.dependencies.head

    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    assert(clock.time === Seconds(10).milliseconds)

    // IDEALLY
    // WindowedStream2 should remember till 7 seconds: 10, 8,
    // WindowedStream1 should remember till 4 seconds: 10, 9, 8, 7, 6, 5
    // MappedStream should remember till 7 seconds:    10, 9, 8, 7, 6, 5, 4, 3,

    // IN THIS TEST
    // WindowedStream2 should remember till 7 seconds: 10, 8,
    // WindowedStream1 should remember till 4 seconds: 10, 9, 8, 7, 6, 5, 4
    // MappedStream should remember till 7 seconds:    10, 9, 8, 7, 6, 5, 4, 3, 2

    // WindowedStream2
    assert(windowedStream2.generatedRDDs.contains(Time(10000)))
    assert(windowedStream2.generatedRDDs.contains(Time(8000)))
    assert(!windowedStream2.generatedRDDs.contains(Time(6000)))

    // WindowedStream1
    assert(windowedStream1.generatedRDDs.contains(Time(10000)))
    assert(windowedStream1.generatedRDDs.contains(Time(4000)))
    assert(!windowedStream1.generatedRDDs.contains(Time(3000)))

    // MappedStream
    assert(mappedStream.generatedRDDs.contains(Time(10000)))
    assert(mappedStream.generatedRDDs.contains(Time(2000)))
    assert(!mappedStream.generatedRDDs.contains(Time(1000)))
  }
}
