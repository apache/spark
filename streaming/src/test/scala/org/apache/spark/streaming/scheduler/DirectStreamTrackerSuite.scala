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
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Time, Duration, StreamingContext}

class DirectStreamTrackerSuite extends FunSuite with BeforeAndAfter {

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

  test("test direct stream track the number of direct stream") {
    abstract class TestInputDStram extends InputDStream[String](ssc) {
      def start() { }
      def stop() { }
      def compute(validTime: Time): Option[RDD[String]] = ???
    }

    val directStream1 = new TestInputDStram {
      private[streaming] override def isDirectInputStream: Boolean = true
    }
    val directStream2 = new TestInputDStram {
      private[streaming] override def isDirectInputStream: Boolean = true
    }
    val receiverStream = new TestInputDStram {
      private[streaming] override def isDirectInputStream: Boolean = false
    }

    assert(ssc.graph.getDirectInputStreams().length == 2)
    assert(ssc.graph.getDirectInputStreams() === Array(directStream1, directStream2))
    assert(ssc.graph.getDirectInputStreams().map(_.id) === Array(0, 1))
    assert(receiverStream.id == 2)

    // Build a fake DAG
    ssc.union(Seq(directStream1, directStream2, receiverStream)).foreachRDD(_.foreach(_ => Unit))
    ssc.start()

    assert(ssc.scheduler.directStreamTracker.directInputStreams === Array(directStream1,
      directStream2))
    assert(ssc.scheduler.directStreamTracker.directInputStreamIds === Array(0, 1))
  }

  test("test direct block addition") {
    val directStreamTracker = new DirectStreamTracker(ssc)

    val streamId1 = 0
    val streamId2 = 1
    val time = Time(0L)
    val directBlockInfos = Seq(
      DirectBlockInfo(0, 100, 0, 100),
      DirectBlockInfo(0, 200, 100, 300))

    directStreamTracker.addBlock(time, streamId1, directBlockInfos)
    directStreamTracker.addBlock(time, streamId2, directBlockInfos)

    val blocksOfBatch = directStreamTracker.getBlocksOfBatch(time)
    assert(blocksOfBatch.size == 2)
    assert(blocksOfBatch.keys === Set(streamId1, streamId2))
    assert(blocksOfBatch(streamId1) === directBlockInfos)
    assert(blocksOfBatch(streamId2) === directBlockInfos)
    assert(directStreamTracker.getBlocksOfBatchAndStream(time, streamId1) === directBlockInfos)
  }

  test("test direct block cleanup") {
    val directStreamTracker = new DirectStreamTracker(ssc)

    val streamId1 = 0
    val directBlockInfos = Seq(
      DirectBlockInfo(0, 100, 0, 100),
      DirectBlockInfo(0, 200, 100, 300))

    directStreamTracker.addBlock(Time(0), streamId1, directBlockInfos)
    directStreamTracker.addBlock(Time(1), streamId1, directBlockInfos)

    directStreamTracker.cleanupOldBatches(Time(0))
    assert(directStreamTracker.getBlocksOfBatchAndStream(Time(0), streamId1)
      === directBlockInfos)
    assert(directStreamTracker.getBlocksOfBatchAndStream(Time(1), streamId1)
      === directBlockInfos)

    directStreamTracker.cleanupOldBatches(Time(1))
    assert(directStreamTracker.getBlocksOfBatchAndStream(Time(0), streamId1) === Seq.empty)
    assert(directStreamTracker.getBlocksOfBatchAndStream(Time(1), streamId1)
      === directBlockInfos)
  }
}
