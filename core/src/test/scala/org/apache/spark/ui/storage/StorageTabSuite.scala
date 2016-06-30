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

package org.apache.spark.ui.storage

import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite, Success}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.storage._

/**
 * Test various functionality in the StorageListener that supports the StorageTab.
 */
class StorageTabSuite extends SparkFunSuite with BeforeAndAfter {
  private var bus: LiveListenerBus = _
  private var storageStatusListener: StorageStatusListener = _
  private var storageListener: StorageListener = _
  private val memAndDisk = StorageLevel.MEMORY_AND_DISK
  private val memOnly = StorageLevel.MEMORY_ONLY
  private val none = StorageLevel.NONE
  private val taskInfo = new TaskInfo(0, 0, 0, 0, "big", "dog", TaskLocality.ANY, false)
  private val taskInfo1 = new TaskInfo(1, 1, 1, 1, "big", "cat", TaskLocality.ANY, false)
  private def rddInfo0 = new RDDInfo(0, "freedom", 100, memOnly, Seq(10))
  private def rddInfo1 = new RDDInfo(1, "hostage", 200, memOnly, Seq(10))
  private def rddInfo2 = new RDDInfo(2, "sanity", 300, memAndDisk, Seq(10))
  private def rddInfo3 = new RDDInfo(3, "grace", 400, memAndDisk, Seq(10))
  private val bm1 = BlockManagerId("big", "dog", 1)

  before {
    bus = new LiveListenerBus
    storageStatusListener = new StorageStatusListener(new SparkConf())
    storageListener = new StorageListener(storageStatusListener)
    bus.addListener(storageStatusListener)
    bus.addListener(storageListener)
  }

  test("stage submitted / completed") {
    assert(storageListener._rddInfoMap.isEmpty)
    assert(storageListener.rddInfoList.isEmpty)

    // 2 RDDs are known, but none are cached
    val stageInfo0 = new StageInfo(0, 0, "0", 100, Seq(rddInfo0, rddInfo1), Seq.empty, "details")
    bus.postToAll(SparkListenerStageSubmitted(stageInfo0))
    assert(storageListener._rddInfoMap.size === 2)
    assert(storageListener.rddInfoList.isEmpty)

    // 4 RDDs are known, but only 2 are cached
    val rddInfo2Cached = rddInfo2
    val rddInfo3Cached = rddInfo3
    rddInfo2Cached.numCachedPartitions = 1
    rddInfo3Cached.numCachedPartitions = 1
    val stageInfo1 = new StageInfo(
      1, 0, "0", 100, Seq(rddInfo2Cached, rddInfo3Cached), Seq.empty, "details")
    bus.postToAll(SparkListenerStageSubmitted(stageInfo1))
    assert(storageListener._rddInfoMap.size === 4)
    assert(storageListener.rddInfoList.size === 2)

    // Submitting RDDInfos with duplicate IDs does nothing
    val rddInfo0Cached = new RDDInfo(0, "freedom", 100, StorageLevel.MEMORY_ONLY, Seq(10))
    rddInfo0Cached.numCachedPartitions = 1
    val stageInfo0Cached = new StageInfo(0, 0, "0", 100, Seq(rddInfo0), Seq.empty, "details")
    bus.postToAll(SparkListenerStageSubmitted(stageInfo0Cached))
    assert(storageListener._rddInfoMap.size === 4)
    assert(storageListener.rddInfoList.size === 2)

    // We only keep around the RDDs that are cached
    bus.postToAll(SparkListenerStageCompleted(stageInfo0))
    assert(storageListener._rddInfoMap.size === 2)
    assert(storageListener.rddInfoList.size === 2)
  }

  test("unpersist") {
    val rddInfo0Cached = rddInfo0
    val rddInfo1Cached = rddInfo1
    rddInfo0Cached.numCachedPartitions = 1
    rddInfo1Cached.numCachedPartitions = 1
    val stageInfo0 = new StageInfo(
      0, 0, "0", 100, Seq(rddInfo0Cached, rddInfo1Cached), Seq.empty, "details")
    bus.postToAll(SparkListenerStageSubmitted(stageInfo0))
    assert(storageListener._rddInfoMap.size === 2)
    assert(storageListener.rddInfoList.size === 2)
    bus.postToAll(SparkListenerUnpersistRDD(0))
    assert(storageListener._rddInfoMap.size === 1)
    assert(storageListener.rddInfoList.size === 1)
    bus.postToAll(SparkListenerUnpersistRDD(4)) // doesn't exist
    assert(storageListener._rddInfoMap.size === 1)
    assert(storageListener.rddInfoList.size === 1)
    bus.postToAll(SparkListenerUnpersistRDD(1))
    assert(storageListener._rddInfoMap.size === 0)
    assert(storageListener.rddInfoList.size === 0)
  }

  test("block update") {
    val myRddInfo0 = rddInfo0
    val myRddInfo1 = rddInfo1
    val myRddInfo2 = rddInfo2
    val stageInfo0 = new StageInfo(
      0, 0, "0", 100, Seq(myRddInfo0, myRddInfo1, myRddInfo2), Seq.empty, "details")
    bus.postToAll(SparkListenerBlockManagerAdded(1L, bm1, 1000L))
    bus.postToAll(SparkListenerStageSubmitted(stageInfo0))
    assert(storageListener._rddInfoMap.size === 3)
    assert(storageListener.rddInfoList.size === 0) // not cached
    assert(!storageListener._rddInfoMap(0).isCached)
    assert(!storageListener._rddInfoMap(1).isCached)
    assert(!storageListener._rddInfoMap(2).isCached)

    // Some blocks updated
    val blockUpdateInfos = Seq(
      BlockUpdatedInfo(bm1, RDDBlockId(0, 100), memAndDisk, 400L, 0L),
      BlockUpdatedInfo(bm1, RDDBlockId(0, 101), memAndDisk, 0L, 400L),
      BlockUpdatedInfo(bm1, RDDBlockId(1, 20), memAndDisk, 0L, 240L)
    )
    postUpdateBlocks(bus, blockUpdateInfos)
    assert(storageListener._rddInfoMap(0).memSize === 400L)
    assert(storageListener._rddInfoMap(0).diskSize === 400L)
    assert(storageListener._rddInfoMap(0).numCachedPartitions === 2)
    assert(storageListener._rddInfoMap(0).isCached)
    assert(storageListener._rddInfoMap(1).memSize === 0L)
    assert(storageListener._rddInfoMap(1).diskSize === 240L)
    assert(storageListener._rddInfoMap(1).numCachedPartitions === 1)
    assert(storageListener._rddInfoMap(1).isCached)
    assert(!storageListener._rddInfoMap(2).isCached)
    assert(storageListener._rddInfoMap(2).numCachedPartitions === 0)

    // Drop some blocks
    val blockUpdateInfos2 = Seq(
      BlockUpdatedInfo(bm1, RDDBlockId(0, 100), none, 0L, 0L),
      BlockUpdatedInfo(bm1, RDDBlockId(1, 20), none, 0L, 0L),
      BlockUpdatedInfo(bm1, RDDBlockId(2, 40), none, 0L, 0L), // doesn't actually exist
      BlockUpdatedInfo(bm1, RDDBlockId(4, 80), none, 0L, 0L) // doesn't actually exist
    )
    postUpdateBlocks(bus, blockUpdateInfos2)
    assert(storageListener._rddInfoMap(0).memSize === 0L)
    assert(storageListener._rddInfoMap(0).diskSize === 400L)
    assert(storageListener._rddInfoMap(0).numCachedPartitions === 1)
    assert(storageListener._rddInfoMap(0).isCached)
    assert(!storageListener._rddInfoMap(1).isCached)
    assert(storageListener._rddInfoMap(2).numCachedPartitions === 0)
    assert(!storageListener._rddInfoMap(2).isCached)
    assert(storageListener._rddInfoMap(2).numCachedPartitions === 0)
  }

  test("verify StorageTab contains all cached rdds") {

    val rddInfo0 = new RDDInfo(0, "rdd0", 1, memOnly, Seq(4))
    val rddInfo1 = new RDDInfo(1, "rdd1", 1, memOnly, Seq(4))
    val stageInfo0 = new StageInfo(0, 0, "stage0", 1, Seq(rddInfo0), Seq.empty, "details")
    val stageInfo1 = new StageInfo(1, 0, "stage1", 1, Seq(rddInfo1), Seq.empty, "details")
    val blockUpdateInfos1 = Seq(BlockUpdatedInfo(bm1, RDDBlockId(0, 1), memOnly, 100L, 0L))
    val blockUpdateInfos2 = Seq(BlockUpdatedInfo(bm1, RDDBlockId(1, 1), memOnly, 200L, 0L))
    bus.postToAll(SparkListenerBlockManagerAdded(1L, bm1, 1000L))
    bus.postToAll(SparkListenerStageSubmitted(stageInfo0))
    assert(storageListener.rddInfoList.size === 0)
    postUpdateBlocks(bus, blockUpdateInfos1)
    assert(storageListener.rddInfoList.size === 1)
    bus.postToAll(SparkListenerStageSubmitted(stageInfo1))
    assert(storageListener.rddInfoList.size === 1)
    bus.postToAll(SparkListenerStageCompleted(stageInfo0))
    assert(storageListener.rddInfoList.size === 1)
    postUpdateBlocks(bus, blockUpdateInfos2)
    assert(storageListener.rddInfoList.size === 2)
    bus.postToAll(SparkListenerStageCompleted(stageInfo1))
    assert(storageListener.rddInfoList.size === 2)
  }

  test("verify StorageTab still contains a renamed RDD") {
    val rddInfo = new RDDInfo(0, "original_name", 1, memOnly, Seq(4))
    val stageInfo0 = new StageInfo(0, 0, "stage0", 1, Seq(rddInfo), Seq.empty, "details")
    bus.postToAll(SparkListenerBlockManagerAdded(1L, bm1, 1000L))
    bus.postToAll(SparkListenerStageSubmitted(stageInfo0))
    val blockUpdateInfos1 = Seq(BlockUpdatedInfo(bm1, RDDBlockId(0, 1), memOnly, 100L, 0L))
    postUpdateBlocks(bus, blockUpdateInfos1)
    assert(storageListener.rddInfoList.size == 1)

    val newName = "new_name"
    val rddInfoRenamed = new RDDInfo(0, newName, 1, memOnly, Seq(4))
    val stageInfo1 = new StageInfo(1, 0, "stage1", 1, Seq(rddInfoRenamed), Seq.empty, "details")
    bus.postToAll(SparkListenerStageSubmitted(stageInfo1))
    assert(storageListener.rddInfoList.size == 1)
    assert(storageListener.rddInfoList.head.name == newName)
  }

  private def postUpdateBlocks(
      bus: SparkListenerBus, blockUpdateInfos: Seq[BlockUpdatedInfo]): Unit = {
    blockUpdateInfos.foreach { blockUpdateInfo =>
      bus.postToAll(SparkListenerBlockUpdated(blockUpdateInfo))
    }
  }
}
