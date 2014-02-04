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

import org.apache.spark.streaming.scheduler._
import scala.collection.mutable.ArrayBuffer
import org.scalatest.matchers.ShouldMatchers
import org.apache.spark.streaming.dstream.DStream

class StreamingListenerSuite extends TestSuiteBase with ShouldMatchers {

  val input = (1 to 4).map(Seq(_)).toSeq
  val operation = (d: DStream[Int]) => d.map(x => x)

  // To make sure that the processing start and end times in collected
  // information are different for successive batches
  override def batchDuration = Milliseconds(100)
  override def actuallyWait = true

  test("basic BatchInfo generation") {
    val ssc = setupStreams(input, operation)
    val collector = new BatchInfoCollector
    ssc.addStreamingListener(collector)
    runStreams(ssc, input.size, input.size)
    val batchInfos = collector.batchInfos
    batchInfos should have size 4

    batchInfos.foreach(info => {
      info.schedulingDelay should not be None
      info.processingDelay should not be None
      info.totalDelay should not be None
      info.schedulingDelay.get should be >= 0L
      info.processingDelay.get should be >= 0L
      info.totalDelay.get should be >= 0L
    })

    isInIncreasingOrder(batchInfos.map(_.submissionTime)) should be (true)
    isInIncreasingOrder(batchInfos.map(_.processingStartTime.get)) should be (true)
    isInIncreasingOrder(batchInfos.map(_.processingEndTime.get)) should be (true)
  }

  /** Check if a sequence of numbers is in increasing order */
  def isInIncreasingOrder(seq: Seq[Long]): Boolean = {
    for(i <- 1 until seq.size) {
      if (seq(i - 1) > seq(i)) return false
    }
    true
  }

  /** Listener that collects information on processed batches */
  class BatchInfoCollector extends StreamingListener {
    val batchInfos = new ArrayBuffer[BatchInfo]
    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      batchInfos += batchCompleted.batchInfo
    }
  }
}
