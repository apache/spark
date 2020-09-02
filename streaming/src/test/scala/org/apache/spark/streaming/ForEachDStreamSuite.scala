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

import scala.collection.mutable.ArrayBuffer

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.{Seconds => ScalaTestSeconds, Span}

import org.apache.spark.streaming.scheduler.{BatchInfo, StreamingListener, StreamingListenerBatchSubmitted}
import org.apache.spark.util.ManualClock

class ForEachDStreamSuite extends TestSuiteBase with BeforeAndAfterAll {
  val submittedBatch = ArrayBuffer[BatchInfo]()
  var ssc: StreamingContext = _

  override def batchDuration: Duration = Seconds(1)

  override def beforeEach(): Unit = {
    super.beforeEach()
    ssc = new StreamingContext(conf, batchDuration)
    val listener = new StreamingListener {
      override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
        submittedBatch += batchSubmitted.batchInfo
      }
    }
    ssc.addStreamingListener(listener)
  }

  test("batch creation should not be blocked by batch execution") {
    val stream = new DummyInputDStream(ssc)
    val mappedDStream = stream.transform(rdd => {
      val sum = rdd.sum()
      Thread.sleep(2000)
      rdd.map(x => x + sum)
    })
    mappedDStream.foreachRDD(rdd => {
      rdd.sum()
    })

    ssc.start()

    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    for (_ <- 1 to 5) {
      clock.advance(batchDuration.milliseconds)
    }

    eventually(Timeout(Span(2, ScalaTestSeconds))) {
      assert(submittedBatch.length == 5)
    }

    ssc.stop()
  }
}
