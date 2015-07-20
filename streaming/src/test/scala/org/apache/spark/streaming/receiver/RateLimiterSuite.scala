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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.SparkFunSuite
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.FakeBlockGeneratorListener

/** Testsuite for testing the network receiver behavior */
class RateLimiterSuite extends SparkFunSuite {

  test("rate limiter initializes even without a maxRate set") {
    val conf = new SparkConf()
    val rateLimiter = new RateLimiter(conf){}
    rateLimiter.updateRate(105)
    assert(rateLimiter.getCurrentLimit == 105)
  }

  test("rate limiter updates when below maxRate") {
    val conf = new SparkConf().set("spark.streaming.receiver.maxRate", "110")
    val rateLimiter = new RateLimiter(conf){}
    rateLimiter.updateRate(105)
    assert(rateLimiter.getCurrentLimit == 105)
  }

  test("rate limiter stays below maxRate despite large updates") {
    val conf = new SparkConf().set("spark.streaming.receiver.maxRate", "100")
    val rateLimiter = new RateLimiter(conf){}
    rateLimiter.updateRate(105)
    assert(rateLimiter.getCurrentLimit === 100)
  }

  def setupGenerator(blockInterval: Int): (BlockGenerator, FakeBlockGeneratorListener) = {
    val blockGeneratorListener = new FakeBlockGeneratorListener
    val conf = new SparkConf().set("spark.streaming.blockInterval", s"${blockInterval}ms")
    val blockGenerator = new BlockGenerator(blockGeneratorListener, 1, conf)
    (blockGenerator, blockGeneratorListener)
  }

  test("throttling block generator") {
    val blockIntervalMs = 100
    val (blockGenerator, blockGeneratorListener) = setupGenerator(blockIntervalMs)
    val maxRate = 1000
    blockGenerator.updateRate(maxRate)
    blockGenerator.start()
    throttlingTest(maxRate, blockGenerator, blockGeneratorListener, blockIntervalMs)
    blockGenerator.stop()
  }

  test("throttling block generator changes rate up") {
    val blockIntervalMs = 100
    val (blockGenerator, blockGeneratorListener) = setupGenerator(blockIntervalMs)
    val maxRate1 = 1000
    blockGenerator.start()
    blockGenerator.updateRate(maxRate1)
    throttlingTest(maxRate1, blockGenerator, blockGeneratorListener, blockIntervalMs)

    blockGeneratorListener.reset()
    val maxRate2 = 5000
    blockGenerator.updateRate(maxRate2)
    throttlingTest(maxRate2, blockGenerator, blockGeneratorListener, blockIntervalMs)
    blockGenerator.stop()
  }

  test("throttling block generator changes rate up and down") {
    val blockIntervalMs = 100
    val (blockGenerator, blockGeneratorListener) = setupGenerator(blockIntervalMs)
    val maxRate1 = 1000
    blockGenerator.updateRate(maxRate1)
    blockGenerator.start()
    throttlingTest(maxRate1, blockGenerator, blockGeneratorListener, blockIntervalMs)

    blockGeneratorListener.reset()
    val maxRate2 = 5000
    blockGenerator.updateRate(maxRate2)
    throttlingTest(maxRate2, blockGenerator, blockGeneratorListener, blockIntervalMs)

    blockGeneratorListener.reset()
    val maxRate3 = 1000
    blockGenerator.updateRate(maxRate3)
    throttlingTest(maxRate3, blockGenerator, blockGeneratorListener, blockIntervalMs)
    blockGenerator.stop()
  }

  def throttlingTest(
      maxRate: Long,
      blockGenerator: BlockGenerator,
      blockGeneratorListener: FakeBlockGeneratorListener,
      blockIntervalMs: Int) {
    val expectedBlocks = 20
    val waitTime = expectedBlocks * blockIntervalMs
    val expectedMessages = maxRate * waitTime / 1000
    val expectedMessagesPerBlock = maxRate * blockIntervalMs / 1000
    val generatedData = new ArrayBuffer[Int]

    // Generate blocks
    val startTime = System.currentTimeMillis()
    var count = 0
    while(System.currentTimeMillis - startTime < waitTime) {
      blockGenerator.addData(count)
      generatedData += count
      count += 1
    }

    val recordedBlocks = blockGeneratorListener.arrayBuffers
    val recordedData = recordedBlocks.flatten
    assert(blockGeneratorListener.arrayBuffers.size > 0, "No blocks received")

    // recordedData size should be close to the expected rate; use an error margin proportional to
    // the value, so that rate changes don't cause a brittle test
    val minExpectedMessages = expectedMessages - 0.05 * expectedMessages
    val maxExpectedMessages = expectedMessages + 0.05 * expectedMessages
    val numMessages = recordedData.size
    assert(
      numMessages >= minExpectedMessages && numMessages <= maxExpectedMessages,
      s"#records received = $numMessages, not between $minExpectedMessages and $maxExpectedMessages"
    )

    // XXX Checking every block would require an even distribution of messages across blocks,
    // which throttling code does not control. Therefore, test against the average.
    val minExpectedMessagesPerBlock = expectedMessagesPerBlock - 0.05 * expectedMessagesPerBlock
    val maxExpectedMessagesPerBlock = expectedMessagesPerBlock + 0.05 * expectedMessagesPerBlock
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
}
