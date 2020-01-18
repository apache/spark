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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable
import scala.reflect.ClassTag

import org.scalatest.concurrent.Eventually.eventually

import org.apache.spark.{HashPartitioner, SparkConf, SparkException}
import org.apache.spark.rdd.{BlockRDD, RDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, WindowedDStream}
import org.apache.spark.util.{Clock, ManualClock}

class BasicOperationsSuite extends TestSuiteBase {
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
    val operation = (r: DStream[Int]) => r.mapPartitions(x => Iterator(x.sum))
    testOperation(input, operation, output, true)
  }

  test("repartition (more partitions)") {
    val input = Seq(1 to 100, 101 to 200, 201 to 300)
    val operation = (r: DStream[Int]) => r.repartition(5)
    withStreamingContext(setupStreams(input, operation, 2)) { ssc =>
      val output = runStreamsWithPartitions(ssc, 3, 3)
      assert(output.size === 3)
      val outputArray = output.toArray
      val first = outputArray(0)
      val second = outputArray(1)
      val third = outputArray(2)

      assert(first.size === 5)
      assert(second.size === 5)
      assert(third.size === 5)

      assert(first.flatten.toSet.equals((1 to 100).toSet))
      assert(second.flatten.toSet.equals((101 to 200).toSet))
      assert(third.flatten.toSet.equals((201 to 300).toSet))
    }
  }

  test("repartition (fewer partitions)") {
    val input = Seq(1 to 100, 101 to 200, 201 to 300)
    val operation = (r: DStream[Int]) => r.repartition(2)
    withStreamingContext(setupStreams(input, operation, 5)) { ssc =>
      val output = runStreamsWithPartitions(ssc, 3, 3)
      assert(output.size === 3)
      val outputArray = output.toArray
      val first = outputArray(0)
      val second = outputArray(1)
      val third = outputArray(2)

      assert(first.size === 2)
      assert(second.size === 2)
      assert(third.size === 2)

      assert(first.flatten.toSet.equals((1 to 100).toSet))
      assert(second.flatten.toSet.equals((101 to 200).toSet))
      assert(third.flatten.toSet.equals((201 to 300).toSet))
    }
  }

  test("groupByKey") {
    testOperation(
      Seq( Seq("a", "a", "b"), Seq("", ""), Seq() ),
      (s: DStream[String]) => s.map(x => (x, 1)).groupByKey().mapValues(_.toSeq),
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
      (s: DStream[String]) => {
        s.map(x => (x, 1)).reduceByKey(_ + _).flatMapValues(x => Seq(x, x + 10))
      },
      Seq( Seq(("a", 2), ("a", 12), ("b", 1), ("b", 11)), Seq(("", 2), ("", 12)), Seq() ),
      true
    )
  }

  test("union") {
    val input = Seq(1 to 4, 101 to 104, 201 to 204)
    val output = Seq(1 to 8, 101 to 108, 201 to 208)
    testOperation(
      input,
      (s: DStream[Int]) => s.union(s.map(_ + 4)),
      output
    )
  }

  test("union with input stream return None") {
    val input = Seq(1 to 4, 101 to 104, 201 to 204, null)
    val output = Seq(1 to 8, 101 to 108, 201 to 208)
    intercept[SparkException] {
      testOperation(
        input,
        (s: DStream[Int]) => s.union(s.map(_ + 4)),
        output,
        input.length,
        false
      )
    }
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

  test("transform with NULL") {
    val input = Seq(1 to 4)
    intercept[SparkException] {
      testOperation(
        input,
        (r: DStream[Int]) => r.transform(rdd => null.asInstanceOf[RDD[Int]]),
        Seq(Seq()),
        1,
        false
      )
    }
  }

  test("transform with input stream return None") {
    val input = Seq(1 to 4, 5 to 8, null)
    intercept[SparkException] {
      testOperation(
        input,
        (r: DStream[Int]) => r.transform(rdd => rdd.map(_.toString)),
        input.filterNot(_ == null).map(_.map(_.toString)),
        input.length,
        false
      )
    }
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

  test("transformWith with input stream return None") {
    val inputData1 = Seq( Seq("a", "b"), Seq("a", ""), Seq(""), null )
    val inputData2 = Seq( Seq("a", "b"), Seq("b", ""), Seq(), null )
    val outputData = Seq(
      Seq("a", "b", "a", "b"),
      Seq("a", "b", "", ""),
      Seq("")
    )

    val operation = (s1: DStream[String], s2: DStream[String]) => {
      s1.transformWith(           // RDD.join in transform
        s2,
        (rdd1: RDD[String], rdd2: RDD[String]) => rdd1.union(rdd2)
      )
    }

    intercept[SparkException] {
      testOperation(inputData1, inputData2, operation, outputData, inputData1.length, true)
    }
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

  test("StreamingContext.transform with input stream return None") {
    val input = Seq(1 to 4, 101 to 104, 201 to 204, null)
    val output = Seq(1 to 12, 101 to 112, 201 to 212)

    // transform over 3 DStreams by doing union of the 3 RDDs
    val operation = (s: DStream[Int]) => {
      s.context.transform(
        Seq(s, s.map(_ + 4), s.map(_ + 8)),   // 3 DStreams
        (rdds: Seq[RDD[_]], time: Time) =>
          rdds.head.context.union(rdds.map(_.asInstanceOf[RDD[Int]]))  // union of RDDs
      )
    }

    intercept[SparkException] {
      testOperation(input, operation, output, input.length, false)
    }
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
      s1.map(x => (x, 1)).cogroup(s2.map(x => (x, "x"))).mapValues(x => (x._1.toSeq, x._2.toSeq))
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

  test("fullOuterJoin") {
    val inputData1 = Seq( Seq("a", "b"), Seq("a", ""), Seq(""), Seq() )
    val inputData2 = Seq( Seq("a", "b"), Seq("b", ""), Seq(), Seq("")   )
    val outputData = Seq(
      Seq( ("a", (Some(1), Some("x"))), ("b", (Some(1), Some("x"))) ),
      Seq( ("", (Some(1), Some("x"))), ("a", (Some(1), None)), ("b", (None, Some("x"))) ),
      Seq( ("", (Some(1), None)) ),
      Seq( ("", (None, Some("x"))) )
    )
    val operation = (s1: DStream[String], s2: DStream[String]) => {
      s1.map(x => (x, 1)).fullOuterJoin(s2.map(x => (x, "x")))
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
        Some(values.sum + state.getOrElse(0))
      }
      s.map(x => (x, 1)).updateStateByKey[Int](updateFunc)
    }

    testOperation(inputData, updateStateOperation, outputData, true)
  }

  test("updateStateByKey - simple with initial value RDD") {
    val initial = Seq(("a", 1), ("c", 2))

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
        Seq(("a", 2), ("c", 2)),
        Seq(("a", 3), ("b", 1), ("c", 2)),
        Seq(("a", 4), ("b", 2), ("c", 3)),
        Seq(("a", 5), ("b", 3), ("c", 3)),
        Seq(("a", 6), ("b", 3), ("c", 3)),
        Seq(("a", 6), ("b", 3), ("c", 3))
      )

    val updateStateOperation = (s: DStream[String]) => {
      val initialRDD = s.context.sparkContext.makeRDD(initial)
      val updateFunc = (values: Seq[Int], state: Option[Int]) => {
        Some(values.sum + state.getOrElse(0))
      }
      s.map(x => (x, 1)).updateStateByKey[Int](updateFunc,
        new HashPartitioner (numInputPartitions), initialRDD)
    }

    testOperation(inputData, updateStateOperation, outputData, true)
  }

  test("updateStateByKey - testing time stamps as input") {
    type StreamingState = Long
    val initial: Seq[(String, StreamingState)] = Seq(("a", 0L), ("c", 0L))

    val inputData =
      Seq(
        Seq("a"),
        Seq("a", "b"),
        Seq("a", "b", "c"),
        Seq("a", "b"),
        Seq("a"),
        Seq()
      )

    // a -> 1000, 3000, 6000, 10000, 15000, 15000
    // b -> 0, 2000, 5000, 9000, 9000, 9000
    // c -> 1000, 1000, 3000, 3000, 3000, 3000

    val outputData: Seq[Seq[(String, StreamingState)]] = Seq(
        Seq(
          ("a", 1000L),
          ("c", 0L)), // t = 1000
        Seq(
          ("a", 3000L),
          ("b", 2000L),
          ("c", 0L)), // t = 2000
        Seq(
          ("a", 6000L),
          ("b", 5000L),
          ("c", 3000L)), // t = 3000
        Seq(
          ("a", 10000L),
          ("b", 9000L),
          ("c", 3000L)), // t = 4000
        Seq(
          ("a", 15000L),
          ("b", 9000L),
          ("c", 3000L)), // t = 5000
        Seq(
          ("a", 15000L),
          ("b", 9000L),
          ("c", 3000L)) // t = 6000
      )

    val updateStateOperation = (s: DStream[String]) => {
      val initialRDD = s.context.sparkContext.makeRDD(initial)
      val updateFunc = (time: Time,
                        key: String,
                        values: Seq[Int],
                        state: Option[StreamingState]) => {
        // Update only if we receive values for this key during the batch.
        if (values.nonEmpty) {
          Option(time.milliseconds + state.getOrElse(0L))
        } else {
          Option(state.getOrElse(0L))
        }
      }
      s.map(x => (x, 1)).updateStateByKey[StreamingState](updateFunc = updateFunc,
        partitioner = new HashPartitioner (numInputPartitions), rememberPartitioner = false,
        initialRDD = Option(initialRDD))
    }

    testOperation(input = inputData, operation = updateStateOperation,
      expectedOutput = outputData, useSet = true)
  }

  test("updateStateByKey - with initial value RDD") {
    val initial = Seq(("a", 1), ("c", 2))

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
        Seq(("a", 2), ("c", 2)),
        Seq(("a", 3), ("b", 1), ("c", 2)),
        Seq(("a", 4), ("b", 2), ("c", 3)),
        Seq(("a", 5), ("b", 3), ("c", 3)),
        Seq(("a", 6), ("b", 3), ("c", 3)),
        Seq(("a", 6), ("b", 3), ("c", 3))
      )

    val updateStateOperation = (s: DStream[String]) => {
      val initialRDD = s.context.sparkContext.makeRDD(initial)
      val updateFunc = (values: Seq[Int], state: Option[Int]) => {
        Some(values.sum + state.getOrElse(0))
      }
      val newUpdateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
        iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
      }
      s.map(x => (x, 1)).updateStateByKey[Int](newUpdateFunc,
        new HashPartitioner (numInputPartitions), true, initialRDD)
    }

    testOperation(inputData, updateStateOperation, outputData, true)
  }

  test("updateStateByKey - object lifecycle") {
    val inputData =
      Seq(
        Seq("a", "b"),
        null,
        Seq("a", "c", "a"),
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
      // updateFunc clears a state when a StateObject is seen without new values twice in a row
      val updateFunc = (values: Seq[Int], state: Option[StateObject]) => {
        val stateObj = state.getOrElse(new StateObject)
        values.sum match {
          case 0 => stateObj.expireCounter += 1 // no new values
          case n => // has new values, increment and reset expireCounter
            stateObj.counter += n
            stateObj.expireCounter = 0
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
    withStreamingContext(new StreamingContext(conf, Seconds(1))) { ssc =>
      val input = Seq(Seq(1), Seq(2), Seq(3), Seq(4))
      val stream = new TestInputStream[Int](ssc, input, 2)
      stream.foreachRDD(_ => {})  // Dummy output stream
      ssc.start()
      Thread.sleep(2000)
      def getInputFromSlice(fromMillis: Long, toMillis: Long): Set[Int] = {
        stream.slice(new Time(fromMillis), new Time(toMillis)).flatMap(_.collect()).toSet
      }

      assert(getInputFromSlice(0, 1000) == Set(1))
      assert(getInputFromSlice(0, 2000) == Set(1, 2))
      assert(getInputFromSlice(1000, 2000) == Set(1, 2))
      assert(getInputFromSlice(2000, 4000) == Set(2, 3, 4))
    }
  }
  test("slice - has not been initialized") {
    withStreamingContext(new StreamingContext(conf, Seconds(1))) { ssc =>
      val input = Seq(Seq(1), Seq(2), Seq(3), Seq(4))
      val stream = new TestInputStream[Int](ssc, input, 2)
      val thrown = intercept[SparkException] {
        stream.slice(new Time(0), new Time(1000))
      }
      assert(thrown.getMessage.contains("has not been initialized"))
    }
  }

  val cleanupTestInput = (0 until 10).map(x => Seq(x, x + 1)).toSeq

  test("rdd cleanup - map and window") {
    val rememberDuration = Seconds(3)
    def operation(s: DStream[Int]): DStream[(Int, Int)] = {
      s.map(x => (x % 10, 1))
       .window(Seconds(2), Seconds(1))
       .window(Seconds(4), Seconds(2))
    }

    runCleanupTest(
        conf,
        operation,
        numExpectedOutput = cleanupTestInput.size / 2,
        rememberDuration = Seconds(3)) { operatedStream =>
      eventually(eventuallyTimeout) {
        val windowedStream2 = operatedStream.asInstanceOf[WindowedDStream[Int]]
        val windowedStream1 = windowedStream2.dependencies.head.asInstanceOf[WindowedDStream[Int]]
        val mappedStream = windowedStream1.dependencies.head

        // Checkpoint remember durations
        assert(windowedStream2.rememberDuration === rememberDuration)
        assert(
          windowedStream1.rememberDuration === rememberDuration + windowedStream2.windowDuration)
        assert(mappedStream.rememberDuration ===
          rememberDuration + windowedStream2.windowDuration + windowedStream1.windowDuration)

        // WindowedStream2 should remember till 7 seconds: 10, 9, 8, 7
        // WindowedStream1 should remember till 4 seconds: 10, 9, 8, 7, 6, 5, 4
        // MappedStream should remember till 2 seconds:    10, 9, 8, 7, 6, 5, 4, 3, 2

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
  }

  test("rdd cleanup - updateStateByKey") {
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      Some(values.sum + state.getOrElse(0))
    }
    runCleanupTest(
      conf, _.map(_ -> 1).updateStateByKey(updateFunc).checkpoint(Seconds(3))) { stateStream =>
      eventually(eventuallyTimeout) {
        assert(stateStream.rememberDuration === stateStream.checkpointDuration * 2)
        assert(stateStream.generatedRDDs.contains(Time(10000)))
        assert(!stateStream.generatedRDDs.contains(Time(4000)))
      }
    }
  }

  test("rdd cleanup - input blocks and persisted RDDs") {
    // Actually receive data over through receiver to create BlockRDDs

    withTestServer(new TestServer()) { testServer =>
      withStreamingContext(new StreamingContext(conf, batchDuration)) { ssc =>
        testServer.start()

        val batchCounter = new BatchCounter(ssc)

        // Set up the streaming context and input streams
        val networkStream =
          ssc.socketTextStream("localhost", testServer.port, StorageLevel.MEMORY_AND_DISK)
        val mappedStream = networkStream.map(_ + ".").persist()
        val outputQueue = new ConcurrentLinkedQueue[Seq[String]]
        val outputStream = new TestOutputStream(mappedStream, outputQueue)

        outputStream.register()
        ssc.start()

        // Feed data to the server to send to the network receiver
        val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
        val input = Seq(1, 2, 3, 4, 5, 6)

        val blockRdds = new mutable.HashMap[Time, BlockRDD[_]]
        val persistentRddIds = new mutable.HashMap[Time, Int]

        def collectRddInfo(): Unit = { // get all RDD info required for verification
          networkStream.generatedRDDs.foreach { case (time, rdd) =>
            blockRdds(time) = rdd.asInstanceOf[BlockRDD[_]]
          }
          mappedStream.generatedRDDs.foreach { case (time, rdd) =>
            persistentRddIds(time) = rdd.id
          }
        }

        Thread.sleep(200)
        for (i <- 0 until input.size) {
          testServer.send(input(i).toString + "\n")
          Thread.sleep(200)
          val numCompletedBatches = batchCounter.getNumCompletedBatches
          clock.advance(batchDuration.milliseconds)
          if (!batchCounter.waitUntilBatchesCompleted(numCompletedBatches + 1, 5000)) {
            fail("Batch took more than 5 seconds to complete")
          }
          collectRddInfo()
        }

        Thread.sleep(200)
        collectRddInfo()
        logInfo("Stopping server")
        testServer.stop()

        // verify data has been received
        assert(!outputQueue.isEmpty)
        assert(blockRdds.size > 0)
        assert(persistentRddIds.size > 0)

        import Time._

        val latestPersistedRddId = persistentRddIds(persistentRddIds.keySet.max)
        val earliestPersistedRddId = persistentRddIds(persistentRddIds.keySet.min)
        val latestBlockRdd = blockRdds(blockRdds.keySet.max)
        val earliestBlockRdd = blockRdds(blockRdds.keySet.min)
        // verify that the latest mapped RDD is persisted but the earliest one has been unpersisted
        assert(ssc.sparkContext.persistentRdds.contains(latestPersistedRddId))
        assert(!ssc.sparkContext.persistentRdds.contains(earliestPersistedRddId))

        // verify that the latest input blocks are present but the earliest blocks have been removed
        assert(latestBlockRdd.isValid)
        assert(latestBlockRdd.collect != null)
        assert(!earliestBlockRdd.isValid)
        earliestBlockRdd.blockIds.foreach { blockId =>
          assert(!ssc.sparkContext.env.blockManager.master.contains(blockId))
        }
      }
    }
  }

  /**
   * Test cleanup of RDDs in DStream metadata. `assertCleanup` is the function that asserts the
   * cleanup of RDDs is successful.
   */
  def runCleanupTest[T: ClassTag](
      conf2: SparkConf,
      operation: DStream[Int] => DStream[T],
      numExpectedOutput: Int = cleanupTestInput.size,
      rememberDuration: Duration = null
    )(assertCleanup: (DStream[T]) => Unit): DStream[T] = {

    // Setup the stream computation
    assert(batchDuration === Seconds(1),
      "Batch duration has changed from 1 second, check cleanup tests")
    withStreamingContext(setupStreams(cleanupTestInput, operation)) { ssc =>
      val operatedStream =
        ssc.graph.getOutputStreams().head.dependencies.head.asInstanceOf[DStream[T]]
      if (rememberDuration != null) ssc.remember(rememberDuration)
      val output = runStreams[(Int, Int)](
        ssc,
        cleanupTestInput.size,
        numExpectedOutput,
        () => assertCleanup(operatedStream))
      val clock = ssc.scheduler.clock.asInstanceOf[Clock]
      assert(clock.getTimeMillis() === Seconds(10).milliseconds)
      assert(output.size === numExpectedOutput)
      operatedStream
    }
  }
}

class StateObject(var counter: Int = 0, var expireCounter: Int = 0) extends Serializable
