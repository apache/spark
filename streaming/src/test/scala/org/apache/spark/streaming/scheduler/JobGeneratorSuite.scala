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

import java.util.concurrent.CountDownLatch

import scala.concurrent.duration._
import scala.language.postfixOps

import org.scalatest.concurrent.Eventually._

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.util.{ManualClock, Utils}

class JobGeneratorSuite extends TestSuiteBase {

  // SPARK-6222 is a tricky regression bug which causes received block metadata
  // to be deleted before the corresponding batch has completed. This occurs when
  // the following conditions are met.
  // 1. streaming checkpointing is enabled by setting streamingContext.checkpoint(dir)
  // 2. input data is received through a receiver as blocks
  // 3. a batch processing a set of blocks takes a long time, such that a few subsequent
  //    batches have been generated and submitted for processing.
  //
  // The JobGenerator (as of Mar 16, 2015) checkpoints twice per batch, once after generation
  // of a batch, and another time after the completion of a batch. The cleanup of
  // checkpoint data (including block metadata, etc.) from DStream must be done only after the
  // 2nd checkpoint has completed, that is, after the batch has been completely processed.
  // However, the issue is that the checkpoint data and along with it received block data is
  // cleaned even in the case of the 1st checkpoint, causing pre-mature deletion of received block
  // data. For example, if the 3rd batch is still being process, the 7th batch may get generated,
  // and the corresponding "1st checkpoint" will delete received block metadata of batch older
  // than 6th batch. That, is 3rd batch's block metadata gets deleted even before 3rd batch has
  // been completely processed.
  //
  // This test tries to create that scenario by the following.
  // 1. enable checkpointing
  // 2. generate batches with received blocks
  // 3. make the 3rd batch never complete
  // 4. allow subsequent batches to be generated (to allow premature deletion of 3rd batch metadata)
  // 5. verify whether 3rd batch's block metadata still exists
  //
  test("SPARK-6222: Do not clear received block data too soon") {
    import JobGeneratorSuite._
    val checkpointDir = Utils.createTempDir()
    val testConf = conf
    testConf.set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
    testConf.set("spark.streaming.receiver.writeAheadLog.rollingInterval", "1")

    withStreamingContext(new StreamingContext(testConf, batchDuration)) { ssc =>
      val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
      val numBatches = 10
      val longBatchNumber = 3 // 3rd batch will take a long time
      val longBatchTime = longBatchNumber * batchDuration.milliseconds

      val testTimeout = timeout(10 seconds)
      val inputStream = ssc.receiverStream(new TestReceiver)

      inputStream.foreachRDD((rdd: RDD[Int], time: Time) => {
        if (time.milliseconds == longBatchTime) {
          while (waitLatch.getCount() > 0) {
            waitLatch.await()
            println("Await over")
          }
        }
      })

      val batchCounter = new BatchCounter(ssc)
      ssc.checkpoint(checkpointDir.getAbsolutePath)
      ssc.start()

      // Make sure the only 1 batch of information is to be remembered
      assert(inputStream.rememberDuration === batchDuration)
      val receiverTracker = ssc.scheduler.receiverTracker

      // Get the blocks belonging to a batch
      def getBlocksOfBatch(batchTime: Long) = {
        receiverTracker.getBlocksOfBatchAndStream(Time(batchTime), inputStream.id)
      }

      // Wait for new blocks to be received
      def waitForNewReceivedBlocks() {
        eventually(testTimeout) {
          assert(receiverTracker.hasUnallocatedBlocks)
        }
      }

      // Wait for received blocks to be allocated to a batch
      def waitForBlocksToBeAllocatedToBatch(batchTime: Long) {
        eventually(testTimeout) {
          assert(getBlocksOfBatch(batchTime).nonEmpty)
        }
      }

      // Generate a large number of batches with blocks in them
      for (batchNum <- 1 to numBatches) {
        waitForNewReceivedBlocks()
        clock.advance(batchDuration.milliseconds)
        waitForBlocksToBeAllocatedToBatch(clock.getTimeMillis())
      }

      // Wait for 3rd batch to start
      eventually(testTimeout) {
        ssc.scheduler.getPendingTimes().contains(Time(numBatches * batchDuration.milliseconds))
      }

      // Verify that the 3rd batch's block data is still present while the 3rd batch is incomplete
      assert(getBlocksOfBatch(longBatchTime).nonEmpty, "blocks of incomplete batch already deleted")
      assert(batchCounter.getNumCompletedBatches < longBatchNumber)
      waitLatch.countDown()
    }
  }
}

object JobGeneratorSuite {
  val waitLatch = new CountDownLatch(1)
}
