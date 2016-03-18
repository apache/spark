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

import scala.util.Random

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.rdd.BlockRDD
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.rdd.WriteAheadLogBackedBlockRDD
import org.apache.spark.streaming.receiver.{BlockManagerBasedStoreResult, Receiver, WriteAheadLogBasedStoreResult}
import org.apache.spark.streaming.scheduler.ReceivedBlockInfo
import org.apache.spark.streaming.util.{WriteAheadLogRecordHandle, WriteAheadLogUtils}

class ReceiverInputDStreamSuite extends TestSuiteBase with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    try {
      StreamingContext.getActive().map { _.stop() }
    } finally {
      super.afterAll()
    }
  }

  testWithoutWAL("createBlockRDD creates empty BlockRDD when no block info") { receiverStream =>
    val rdd = receiverStream.createBlockRDD(Time(0), Seq.empty)
    assert(rdd.isInstanceOf[BlockRDD[_]])
    assert(!rdd.isInstanceOf[WriteAheadLogBackedBlockRDD[_]])
    assert(rdd.isEmpty())
  }

  testWithoutWAL("createBlockRDD creates correct BlockRDD with block info") { receiverStream =>
    val blockInfos = Seq.fill(5) { createBlockInfo(withWALInfo = false) }
    val blockIds = blockInfos.map(_.blockId)

    // Verify that there are some blocks that are present, and some that are not
    require(blockIds.forall(blockId => SparkEnv.get.blockManager.master.contains(blockId)))

    val rdd = receiverStream.createBlockRDD(Time(0), blockInfos)
    assert(rdd.isInstanceOf[BlockRDD[_]])
    assert(!rdd.isInstanceOf[WriteAheadLogBackedBlockRDD[_]])
    val blockRDD = rdd.asInstanceOf[BlockRDD[_]]
    assert(blockRDD.blockIds.toSeq === blockIds)
  }

  testWithoutWAL("createBlockRDD filters non-existent blocks before creating BlockRDD") {
    receiverStream =>
      val presentBlockInfos = Seq.fill(2)(createBlockInfo(withWALInfo = false, createBlock = true))
      val absentBlockInfos = Seq.fill(3)(createBlockInfo(withWALInfo = false, createBlock = false))
      val blockInfos = presentBlockInfos ++ absentBlockInfos
      val blockIds = blockInfos.map(_.blockId)

      // Verify that there are some blocks that are present, and some that are not
      require(blockIds.exists(blockId => SparkEnv.get.blockManager.master.contains(blockId)))
      require(blockIds.exists(blockId => !SparkEnv.get.blockManager.master.contains(blockId)))

      val rdd = receiverStream.createBlockRDD(Time(0), blockInfos)
      assert(rdd.isInstanceOf[BlockRDD[_]])
      val blockRDD = rdd.asInstanceOf[BlockRDD[_]]
      assert(blockRDD.blockIds.toSeq === presentBlockInfos.map { _.blockId})
  }

  testWithWAL("createBlockRDD creates empty WALBackedBlockRDD when no block info") {
    receiverStream =>
      val rdd = receiverStream.createBlockRDD(Time(0), Seq.empty)
      assert(rdd.isInstanceOf[WriteAheadLogBackedBlockRDD[_]])
      assert(rdd.isEmpty())
  }

  testWithWAL(
    "createBlockRDD creates correct WALBackedBlockRDD with all block info having WAL info") {
    receiverStream =>
      val blockInfos = Seq.fill(5) { createBlockInfo(withWALInfo = true) }
      val blockIds = blockInfos.map(_.blockId)
      val rdd = receiverStream.createBlockRDD(Time(0), blockInfos)
      assert(rdd.isInstanceOf[WriteAheadLogBackedBlockRDD[_]])
      val blockRDD = rdd.asInstanceOf[WriteAheadLogBackedBlockRDD[_]]
      assert(blockRDD.blockIds.toSeq === blockIds)
      assert(blockRDD.walRecordHandles.toSeq === blockInfos.map { _.walRecordHandleOption.get })
  }

  testWithWAL("createBlockRDD creates BlockRDD when some block info don't have WAL info") {
    receiverStream =>
      val blockInfos1 = Seq.fill(2) { createBlockInfo(withWALInfo = true) }
      val blockInfos2 = Seq.fill(3) { createBlockInfo(withWALInfo = false) }
      val blockInfos = blockInfos1 ++ blockInfos2
      val blockIds = blockInfos.map(_.blockId)
      val rdd = receiverStream.createBlockRDD(Time(0), blockInfos)
      assert(rdd.isInstanceOf[BlockRDD[_]])
      val blockRDD = rdd.asInstanceOf[BlockRDD[_]]
      assert(blockRDD.blockIds.toSeq === blockIds)
  }


  private def testWithoutWAL(msg: String)(body: ReceiverInputDStream[_] => Unit): Unit = {
    test(s"Without WAL enabled: $msg") {
      runTest(enableWAL = false, body)
    }
  }

  private def testWithWAL(msg: String)(body: ReceiverInputDStream[_] => Unit): Unit = {
    test(s"With WAL enabled: $msg") {
      runTest(enableWAL = true, body)
    }
  }

  private def runTest(enableWAL: Boolean, body: ReceiverInputDStream[_] => Unit): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[4]").setAppName("ReceiverInputDStreamSuite")
    conf.set(WriteAheadLogUtils.RECEIVER_WAL_ENABLE_CONF_KEY, enableWAL.toString)
    require(WriteAheadLogUtils.enableReceiverLog(conf) === enableWAL)
    val ssc = new StreamingContext(conf, Seconds(1))
    val receiverStream = new ReceiverInputDStream[Int](ssc) {
      override def getReceiver(): Receiver[Int] = null
    }
    withStreamingContext(ssc) { ssc =>
      body(receiverStream)
    }
  }

  /**
   * Create a block info for input to the ReceiverInputDStream.createBlockRDD
   * @param withWALInfo Create block with WAL info in it
   * @param createBlock Actually create the block in the BlockManager
   * @return
   */
  private def createBlockInfo(
      withWALInfo: Boolean,
      createBlock: Boolean = true): ReceivedBlockInfo = {
    val blockId = new StreamBlockId(0, Random.nextLong())
    if (createBlock) {
      SparkEnv.get.blockManager.putSingle(blockId, 1, StorageLevel.MEMORY_ONLY, tellMaster = true)
      require(SparkEnv.get.blockManager.master.contains(blockId))
    }
    val storeResult = if (withWALInfo) {
      new WriteAheadLogBasedStoreResult(blockId, None, new WriteAheadLogRecordHandle { })
    } else {
      new BlockManagerBasedStoreResult(blockId, None)
    }
    new ReceivedBlockInfo(0, None, None, storeResult)
  }
}
