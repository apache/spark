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

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.Success
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.storage._

/**
 * Test various functionality in the StorageListener that supports the StorageTab.
 */
class StorageTabSuite extends FunSuite with BeforeAndAfter {
  private var bus: LiveListenerBus = _
  private var storageStatusListener: StorageStatusListener = _
  private var storageListener: StorageListener = _
  private val memAndDisk = StorageLevel.MEMORY_AND_DISK
  private val memOnly = StorageLevel.MEMORY_ONLY
  private val none = StorageLevel.NONE
  private val taskInfo = new TaskInfo(0, 0, 0, 0, "big", "dog", TaskLocality.ANY, false)
  private val taskInfo1 = new TaskInfo(1, 1, 1, 1, "big", "cat", TaskLocality.ANY, false)
  private def rddInfo0 = new RDDInfo(0, "freedom", 100, memOnly)
  private def rddInfo1 = new RDDInfo(1, "hostage", 200, memOnly)
  private def rddInfo2 = new RDDInfo(2, "sanity", 300, memAndDisk)
  private def rddInfo3 = new RDDInfo(3, "grace", 400, memAndDisk)
  private val bm1 = BlockManagerId("big", "dog", 1)

  before {
    bus = new LiveListenerBus
    storageStatusListener = new StorageStatusListener
    storageListener = new StorageListener(storageStatusListener)
    bus.addListener(storageStatusListener)
    bus.addListener(storageListener)
  }

  test("stage submitted / completed") {
    assert(storageListener._rddInfoMap.isEmpty)
    assert(storageListener.rddInfoList.isEmpty)

    // 2 RDDs are known, but none are cached
    val stageInfo0 = new StageInfo(0, 0, "0", 100, Seq(rddInfo0, rddInfo1), "details")
    bus.postToAll(SparkListenerStageSubmitted(stageInfo0))
    assert(storageListener._rddInfoMap.size === 2)
    assert(storageListener.rddInfoList.isEmpty)

    // 4 RDDs are known, but only 2 are cached
    val rddInfo2Cached = rddInfo2
    val rddInfo3Cached = rddInfo3
    rddInfo2Cached.numCachedPartitions = 1
    rddInfo3Cached.numCachedPartitions = 1
    val stageInfo1 = new StageInfo(1, 0, "0", 100, Seq(rddInfo2Cached, rddInfo3Cached), "details")
    bus.postToAll(SparkListenerStageSubmitted(stageInfo1))
    assert(storageListener._rddInfoMap.size === 4)
    assert(storageListener.rddInfoList.size === 2)

    // Submitting RDDInfos with duplicate IDs does nothing
    val rddInfo0Cached = new RDDInfo(0, "freedom", 100, StorageLevel.MEMORY_ONLY)
    rddInfo0Cached.numCachedPartitions = 1
    val stageInfo0Cached = new StageInfo(0, 0, "0", 100, Seq(rddInfo0), "details")
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
    val stageInfo0 = new StageInfo(0, 0, "0", 100, Seq(rddInfo0Cached, rddInfo1Cached), "details")
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

  test("task end") {
    val myRddInfo0 = rddInfo0
    val myRddInfo1 = rddInfo1
    val myRddInfo2 = rddInfo2
    val stageInfo0 = new StageInfo(0, 0, "0", 100, Seq(myRddInfo0, myRddInfo1, myRddInfo2), "details")
    bus.postToAll(SparkListenerBlockManagerAdded(1L, bm1, 1000L))
    bus.postToAll(SparkListenerStageSubmitted(stageInfo0))
    assert(storageListener._rddInfoMap.size === 3)
    assert(storageListener.rddInfoList.size === 0) // not cached
    assert(!storageListener._rddInfoMap(0).isCached)
    assert(!storageListener._rddInfoMap(1).isCached)
    assert(!storageListener._rddInfoMap(2).isCached)

    // Task end with no updated blocks. This should not change anything.
    bus.postToAll(SparkListenerTaskEnd(0, 0, "obliteration", Success, taskInfo, new TaskMetrics))
    assert(storageListener._rddInfoMap.size === 3)
    assert(storageListener.rddInfoList.size === 0)

    // Task end with a few new persisted blocks, some from the same RDD
    val metrics1 = new TaskMetrics
    metrics1.updatedBlocks = Some(Seq(
      (RDDBlockId(0, 100), BlockStatus(memAndDisk, 400L, 0L, 0L)),
      (RDDBlockId(0, 101), BlockStatus(memAndDisk, 0L, 400L, 0L)),
      (RDDBlockId(0, 102), BlockStatus(memAndDisk, 400L, 0L, 200L)),
      (RDDBlockId(1, 20), BlockStatus(memAndDisk, 0L, 240L, 0L))
    ))
    bus.postToAll(SparkListenerTaskEnd(1, 0, "obliteration", Success, taskInfo, metrics1))
    assert(storageListener._rddInfoMap(0).memSize === 800L)
    assert(storageListener._rddInfoMap(0).diskSize === 400L)
    assert(storageListener._rddInfoMap(0).tachyonSize === 200L)
    assert(storageListener._rddInfoMap(0).numCachedPartitions === 3)
    assert(storageListener._rddInfoMap(0).isCached)
    assert(storageListener._rddInfoMap(1).memSize === 0L)
    assert(storageListener._rddInfoMap(1).diskSize === 240L)
    assert(storageListener._rddInfoMap(1).tachyonSize === 0L)
    assert(storageListener._rddInfoMap(1).numCachedPartitions === 1)
    assert(storageListener._rddInfoMap(1).isCached)
    assert(!storageListener._rddInfoMap(2).isCached)
    assert(storageListener._rddInfoMap(2).numCachedPartitions === 0)

    // Task end with a few dropped blocks
    val metrics2 = new TaskMetrics
    metrics2.updatedBlocks = Some(Seq(
      (RDDBlockId(0, 100), BlockStatus(none, 0L, 0L, 0L)),
      (RDDBlockId(1, 20), BlockStatus(none, 0L, 0L, 0L)),
      (RDDBlockId(2, 40), BlockStatus(none, 0L, 0L, 0L)), // doesn't actually exist
      (RDDBlockId(4, 80), BlockStatus(none, 0L, 0L, 0L)) // doesn't actually exist
    ))
    bus.postToAll(SparkListenerTaskEnd(2, 0, "obliteration", Success, taskInfo, metrics2))
    assert(storageListener._rddInfoMap(0).memSize === 400L)
    assert(storageListener._rddInfoMap(0).diskSize === 400L)
    assert(storageListener._rddInfoMap(0).tachyonSize === 200L)
    assert(storageListener._rddInfoMap(0).numCachedPartitions === 2)
    assert(storageListener._rddInfoMap(0).isCached)
    assert(!storageListener._rddInfoMap(1).isCached)
    assert(storageListener._rddInfoMap(2).numCachedPartitions === 0)
    assert(!storageListener._rddInfoMap(2).isCached)
    assert(storageListener._rddInfoMap(2).numCachedPartitions === 0)
  }

  test("verify StorageTab contains all cached rdds") {

    val rddInfo0 = new RDDInfo(0, "rdd0", 1, memOnly)
    val rddInfo1 = new RDDInfo(1, "rdd1", 1 ,memOnly)
    val stageInfo0 = new StageInfo(0, 0, "stage0", 1, Seq(rddInfo0), "details")
    val stageInfo1 = new StageInfo(1, 0, "stage1", 1, Seq(rddInfo1), "details")
    val taskMetrics0 = new TaskMetrics
    val taskMetrics1 = new TaskMetrics
    val block0 = (RDDBlockId(0, 1), BlockStatus(memOnly, 100L, 0L, 0L))
    val block1 = (RDDBlockId(1, 1), BlockStatus(memOnly, 200L, 0L, 0L))
    taskMetrics0.updatedBlocks = Some(Seq(block0))
    taskMetrics1.updatedBlocks = Some(Seq(block1))
    bus.postToAll(SparkListenerBlockManagerAdded(1L, bm1, 1000L))
    bus.postToAll(SparkListenerStageSubmitted(stageInfo0))
    assert(storageListener.rddInfoList.size === 0)
    bus.postToAll(SparkListenerTaskEnd(0, 0, "big", Success, taskInfo, taskMetrics0))
    assert(storageListener.rddInfoList.size === 1)
    bus.postToAll(SparkListenerStageSubmitted(stageInfo1))
    assert(storageListener.rddInfoList.size === 1)
    bus.postToAll(SparkListenerStageCompleted(stageInfo0))
    assert(storageListener.rddInfoList.size === 1)
    bus.postToAll(SparkListenerTaskEnd(1, 0, "small", Success, taskInfo1, taskMetrics1))
    assert(storageListener.rddInfoList.size === 2)
    bus.postToAll(SparkListenerStageCompleted(stageInfo1))
    assert(storageListener.rddInfoList.size === 2)
  }
}
