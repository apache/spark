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

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Time, Duration, StreamingContext}
import org.apache.spark.streaming.dstream.{ReceiverInputDStream, InputDStream}
import org.apache.spark.streaming.receiver.Receiver

class InputInfoTrackerSuite extends FunSuite with BeforeAndAfter {

  private var ssc: StreamingContext = _

  before {
    val conf = new SparkConf().setMaster("local[2]").setAppName("DirectStreamTacker")
    if (ssc == null) {
      ssc = new StreamingContext(conf, Duration(1000))
    }
  }

  after {
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }
  }

  test("test track the number of input stream") {
    class TestInputDStream extends InputDStream[String](ssc) {
      def start() { }
      def stop() { }
      def compute(validTime: Time): Option[RDD[String]] = ???
    }

    class TestReceiverInputDStream extends ReceiverInputDStream[String](ssc) {
      def getReceiver: Receiver[String] = ???
    }

    // Register input streams
    val receiverInputStreams = Array(new TestReceiverInputDStream, new TestReceiverInputDStream)
    val inputStreams = Array(new TestInputDStream, new TestInputDStream, new TestInputDStream)

    assert(ssc.graph.getInputStreams().length == 5)
    assert(ssc.graph.getReceiverInputStreams().length == 2)
    assert(ssc.graph.getReceiverInputStreams() === receiverInputStreams)
    assert(ssc.graph.getInputStreams().map(_.id) === Array(0, 1, 2, 3, 4))
    assert(receiverInputStreams.map(_.id) === Array(0, 1))
  }

  test("test report and get InputInfo from InputInfoTracker") {
    val inputInfoTracker = new InputInfoTracker(ssc)

    val streamId1 = 0
    val streamId2 = 1
    val time = Time(0L)
    val inputInfo1 = InputInfo(time, streamId1, 100L)
    val inputInfo2 = InputInfo(time, streamId2, 300L)
    inputInfoTracker.reportInfo(inputInfo1.batchTime, inputInfo1)
    inputInfoTracker.reportInfo(inputInfo2.batchTime, inputInfo2)

    val batchTimeToInputInfos = inputInfoTracker.getInfo(time)
    assert(batchTimeToInputInfos.size == 2)
    assert(batchTimeToInputInfos.keys === Set(streamId1, streamId2))
    assert(batchTimeToInputInfos(streamId1) === inputInfo1)
    assert(batchTimeToInputInfos(streamId2) === inputInfo2)
    assert(inputInfoTracker.getInfoOfBatchAndStream(time, streamId1) === Some(inputInfo1))
  }

  test("test cleanup InputInfo from InputInfoTracker") {
    val inputInfoTracker = new InputInfoTracker(ssc)

    val streamId1 = 0
    val inputInfo1 = InputInfo(Time(0), streamId1, 100L)
    val inputInfo2 = InputInfo(Time(1), streamId1, 300L)
    inputInfoTracker.reportInfo(inputInfo1.batchTime, inputInfo1)
    inputInfoTracker.reportInfo(inputInfo2.batchTime, inputInfo2)

    inputInfoTracker.cleanup(Time(0))
    assert(inputInfoTracker.getInfoOfBatchAndStream(Time(0), streamId1) === Some(inputInfo1))
    assert(inputInfoTracker.getInfoOfBatchAndStream(Time(1), streamId1) === Some(inputInfo2))

    inputInfoTracker.cleanup(Time(1))
    assert(inputInfoTracker.getInfoOfBatchAndStream(Time(0), streamId1) === None)
    assert(inputInfoTracker.getInfoOfBatchAndStream(Time(1), streamId1) === Some(inputInfo2))
  }
}
