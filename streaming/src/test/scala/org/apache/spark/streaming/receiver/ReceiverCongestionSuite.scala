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

package org.apache.spark.streaming.receiver

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkConf
import org.apache.spark.streaming.FakeBlockGeneratorListener
import org.apache.spark.streaming.TestSuiteBase
import org.apache.spark.streaming.receiver._
import org.apache.spark.util.Utils

class ReceiverCongestionSuite extends TestSuiteBase with Timeouts with Serializable {

 // helps test congestion strategies with a constant rate per Block
  // verifies the rate of messages produced by one of the congestion strategies
  def testBlockGeneratorCongestion(conf: SparkConf,
                                   maxRatePerSecond: Long,
                                   expectedBlocks: Int,
                                   errorBound: Double = 0.05,
                                   testIdentity: Boolean = false // test block data unchanged
                                   ): Unit = {
    val blockGeneratorListener = new FakeBlockGeneratorListener()
    val blockGenerator = new BlockGenerator(blockGeneratorListener, 1, conf)
    blockGenerator.congestionStrategy.onBlockBoundUpdate(maxRatePerSecond)

    val blockIntervalMs = conf.getTimeAsMs("spark.streaming.blockInterval", "200ms")

    val waitTimeinMs = expectedBlocks * blockIntervalMs + 1
    val expectedMessagesPerBlock = maxRatePerSecond * blockIntervalMs / 1000
    val expectedMessages = expectedMessagesPerBlock * expectedBlocks

    val generatedData = new ArrayBuffer[Int]

    // Generate blocks
    val startTime = System.currentTimeMillis()
    blockGenerator.start()
    var count = 0
    while(System.currentTimeMillis - startTime < waitTimeinMs) {
      blockGenerator.addData(count)
      generatedData += count
      count += 1
    }
    blockGenerator.stop()

    val recordedBlocks = blockGeneratorListener.arrayBuffers
    val recordedData = recordedBlocks.flatten
    val rsize = recordedBlocks.size
    assert(rsize > 0, "No blocks received")
    assert(rsize >= expectedBlocks && rsize <= expectedBlocks + 1,
           s"Received ${recordedBlocks.size} blocks instead of the expected $expectedBlocks")
    if(testIdentity) {
          assert(recordedData.toSet === generatedData.toSet, "Received data not same")
    }

    // recordedData size should be close to the expected rate; use an error margin proportional to
    // the value, so that rate changes don't cause a brittle test
    val minExpectedMessages = expectedMessages - errorBound * expectedMessages
    val maxExpectedMessages = expectedMessages + errorBound * expectedMessages
    val numMessages = recordedData.size
    assert(
      numMessages >= minExpectedMessages && numMessages <= maxExpectedMessages,
      s"#records received = $numMessages, not between $minExpectedMessages and $maxExpectedMessages"
    )

    // XXX Checking every block would require an even distribution of messages across blocks,
    // which throttling code does not control. Therefore, test against the average.
    val minExpectedMessagesPerBlock = expectedMessagesPerBlock -
      errorBound * expectedMessagesPerBlock
    val maxExpectedMessagesPerBlock = expectedMessagesPerBlock +
      errorBound * expectedMessagesPerBlock
    val receivedBlockSizes = recordedBlocks.map { _.size }.mkString(",")

    // the first and last block may be incomplete, so we slice them out
    val validBlocks = recordedBlocks.drop(1).dropRight(1)
    val averageBlockSize = validBlocks.map(block => block.size).sum / validBlocks.size

    assert(
      averageBlockSize >= minExpectedMessagesPerBlock &&
        averageBlockSize <= maxExpectedMessagesPerBlock,
      s"# records in received blocks = [$receivedBlockSizes], not between " +
        s"$minExpectedMessagesPerBlock and $maxExpectedMessagesPerBlock, on average"
    )
  }

  ignore("congestion strategy drop") {
    val dropStrategyConf = new SparkConf().
    set("spark.streaming.blockInterval", "100ms").
    set("spark.streaming.backPressure.congestionStrategy", "drop").
    set("spark.streaming.backPressure.enabled", "true")
    testBlockGeneratorCongestion(dropStrategyConf, 1000, 20)
  }

  ignore("congestion strategy sample") {
    val sampleStrategyConf = new SparkConf().
    set("spark.streaming.blockInterval", "100ms").
    set("spark.streaming.backPressure.congestionStrategy", "sample").
    set("spark.streaming.backPressure.enabled", "true")
    testBlockGeneratorCongestion(sampleStrategyConf, 1000, 20)
  }

  ignore("congestion strategy throttle") {
    val throttleStrategyConf = new SparkConf().
    set("spark.streaming.blockInterval", "100ms").
    set("spark.streaming.backPressure.congestionStrategy", "throttle").
    set("spark.streaming.backPressure.enabled", "true")
    testBlockGeneratorCongestion(throttleStrategyConf, 1000, 20)
  }

   ignore("block generator throttling") {
    val throttlingWithoutCongestion = new SparkConf().
      set("spark.streaming.blockInterval", "100ms").
      set("spark.streaming.receiver.maxRate", "100").
      set("spark.streaming.backPressure.enabled", "false")
    testBlockGeneratorCongestion(throttlingWithoutCongestion, 100, 20, 0.05, true)
  }

}
