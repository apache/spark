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

package org.apache.spark.storage

import org.scalatest.FunSuite
import org.apache.spark.Success
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._

/**
 * Test the behavior of StorageStatusListener in response to all relevant events.
 */
class StorageStatusListenerSuite extends FunSuite {
  private val bm1 = BlockManagerId("big", "dog", 1, 1)
  private val bm2 = BlockManagerId("fat", "duck", 2, 2)
  private val taskInfo1 = new TaskInfo(0, 0, 0, 0, "big", "dog", TaskLocality.ANY, false)
  private val taskInfo2 = new TaskInfo(0, 0, 0, 0, "fat", "duck", TaskLocality.ANY, false)

  test("block manager added/removed") {
    val listener = new StorageStatusListener

    // Block manager add
    assert(listener.executorIdToStorageStatus.size === 0)
    listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(bm1, 1000L))
    assert(listener.executorIdToStorageStatus.size === 1)
    assert(listener.executorIdToStorageStatus.get("big").isDefined)
    assert(listener.executorIdToStorageStatus("big").blockManagerId === bm1)
    assert(listener.executorIdToStorageStatus("big").maxMem === 1000L)
    assert(listener.executorIdToStorageStatus("big").blocks.isEmpty)
    listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(bm2, 2000L))
    assert(listener.executorIdToStorageStatus.size === 2)
    assert(listener.executorIdToStorageStatus.get("fat").isDefined)
    assert(listener.executorIdToStorageStatus("fat").blockManagerId === bm2)
    assert(listener.executorIdToStorageStatus("fat").maxMem === 2000L)
    assert(listener.executorIdToStorageStatus("fat").blocks.isEmpty)

    // Block manager remove
    listener.onBlockManagerRemoved(SparkListenerBlockManagerRemoved(bm1))
    assert(listener.executorIdToStorageStatus.size === 1)
    assert(!listener.executorIdToStorageStatus.get("big").isDefined)
    assert(listener.executorIdToStorageStatus.get("fat").isDefined)
    listener.onBlockManagerRemoved(SparkListenerBlockManagerRemoved(bm2))
    assert(listener.executorIdToStorageStatus.size === 0)
    assert(!listener.executorIdToStorageStatus.get("big").isDefined)
    assert(!listener.executorIdToStorageStatus.get("fat").isDefined)
  }

  test("task end without updated blocks") {
    val listener = new StorageStatusListener
    listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(bm1, 1000L))
    listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(bm2, 2000L))
    val taskMetrics = new TaskMetrics

    // Task end with no updated blocks
    assert(listener.executorIdToStorageStatus("big").blocks.isEmpty)
    assert(listener.executorIdToStorageStatus("fat").blocks.isEmpty)
    listener.onTaskEnd(SparkListenerTaskEnd(1, "obliteration", Success, taskInfo1, taskMetrics))
    assert(listener.executorIdToStorageStatus("big").blocks.isEmpty)
    assert(listener.executorIdToStorageStatus("fat").blocks.isEmpty)
    listener.onTaskEnd(SparkListenerTaskEnd(1, "obliteration", Success, taskInfo2, taskMetrics))
    assert(listener.executorIdToStorageStatus("big").blocks.isEmpty)
    assert(listener.executorIdToStorageStatus("fat").blocks.isEmpty)
  }

  test("task end with updated blocks") {
    val listener = new StorageStatusListener
    listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(bm1, 1000L))
    listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(bm2, 2000L))
    val taskMetrics1 = new TaskMetrics
    val taskMetrics2 = new TaskMetrics
    val block1 = (RDDBlockId(1, 1), BlockStatus(StorageLevel.DISK_ONLY, 0L, 100L, 0L))
    val block2 = (RDDBlockId(1, 2), BlockStatus(StorageLevel.DISK_ONLY, 0L, 200L, 0L))
    val block3 = (RDDBlockId(4, 0), BlockStatus(StorageLevel.DISK_ONLY, 0L, 300L, 0L))
    taskMetrics1.updatedBlocks = Some(Seq(block1, block2))
    taskMetrics2.updatedBlocks = Some(Seq(block3))

    // Task end with new blocks
    assert(listener.executorIdToStorageStatus("big").blocks.isEmpty)
    assert(listener.executorIdToStorageStatus("fat").blocks.isEmpty)
    listener.onTaskEnd(SparkListenerTaskEnd(1, "obliteration", Success, taskInfo1, taskMetrics1))
    assert(listener.executorIdToStorageStatus("big").blocks.size === 2)
    assert(listener.executorIdToStorageStatus("fat").blocks.size === 0)
    assert(listener.executorIdToStorageStatus("big").blocks.contains(RDDBlockId(1, 1)))
    assert(listener.executorIdToStorageStatus("big").blocks.contains(RDDBlockId(1, 2)))
    assert(listener.executorIdToStorageStatus("fat").blocks.isEmpty)
    listener.onTaskEnd(SparkListenerTaskEnd(1, "obliteration", Success, taskInfo2, taskMetrics2))
    assert(listener.executorIdToStorageStatus("big").blocks.size === 2)
    assert(listener.executorIdToStorageStatus("fat").blocks.size === 1)
    assert(listener.executorIdToStorageStatus("big").blocks.contains(RDDBlockId(1, 1)))
    assert(listener.executorIdToStorageStatus("big").blocks.contains(RDDBlockId(1, 2)))
    assert(listener.executorIdToStorageStatus("fat").blocks.contains(RDDBlockId(4, 0)))

    // Task end with dropped blocks
    val droppedBlock1 = (RDDBlockId(1, 1), BlockStatus(StorageLevel.NONE, 0L, 0L, 0L))
    val droppedBlock2 = (RDDBlockId(1, 2), BlockStatus(StorageLevel.NONE, 0L, 0L, 0L))
    val droppedBlock3 = (RDDBlockId(4, 0), BlockStatus(StorageLevel.NONE, 0L, 0L, 0L))
    taskMetrics1.updatedBlocks = Some(Seq(droppedBlock1, droppedBlock3))
    taskMetrics2.updatedBlocks = Some(Seq(droppedBlock2, droppedBlock3))
    listener.onTaskEnd(SparkListenerTaskEnd(1, "obliteration", Success, taskInfo1, taskMetrics1))
    assert(listener.executorIdToStorageStatus("big").blocks.size === 1)
    assert(listener.executorIdToStorageStatus("fat").blocks.size === 1)
    assert(!listener.executorIdToStorageStatus("big").blocks.contains(RDDBlockId(1, 1)))
    assert(listener.executorIdToStorageStatus("big").blocks.contains(RDDBlockId(1, 2)))
    assert(listener.executorIdToStorageStatus("fat").blocks.contains(RDDBlockId(4, 0)))
    listener.onTaskEnd(SparkListenerTaskEnd(1, "obliteration", Success, taskInfo2, taskMetrics2))
    assert(listener.executorIdToStorageStatus("big").blocks.size === 1)
    assert(listener.executorIdToStorageStatus("fat").blocks.size === 0)
    assert(!listener.executorIdToStorageStatus("big").blocks.contains(RDDBlockId(1, 1)))
    assert(listener.executorIdToStorageStatus("big").blocks.contains(RDDBlockId(1, 2)))
    assert(listener.executorIdToStorageStatus("fat").blocks.isEmpty)
  }

  test("unpersist RDD") {
    val listener = new StorageStatusListener
    listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(bm1, 1000L))
    val taskMetrics1 = new TaskMetrics
    val taskMetrics2 = new TaskMetrics
    val block1 = (RDDBlockId(1, 1), BlockStatus(StorageLevel.DISK_ONLY, 0L, 100L, 0L))
    val block2 = (RDDBlockId(1, 2), BlockStatus(StorageLevel.DISK_ONLY, 0L, 200L, 0L))
    val block3 = (RDDBlockId(4, 0), BlockStatus(StorageLevel.DISK_ONLY, 0L, 300L, 0L))
    taskMetrics1.updatedBlocks = Some(Seq(block1, block2))
    taskMetrics2.updatedBlocks = Some(Seq(block3))
    listener.onTaskEnd(SparkListenerTaskEnd(1, "obliteration", Success, taskInfo1, taskMetrics1))
    listener.onTaskEnd(SparkListenerTaskEnd(1, "obliteration", Success, taskInfo1, taskMetrics2))
    assert(listener.executorIdToStorageStatus("big").blocks.size === 3)

    // Unpersist RDD
    listener.onUnpersistRDD(SparkListenerUnpersistRDD(9090))
    assert(listener.executorIdToStorageStatus("big").blocks.size === 3)
    listener.onUnpersistRDD(SparkListenerUnpersistRDD(4))
    assert(listener.executorIdToStorageStatus("big").blocks.size === 2)
    assert(listener.executorIdToStorageStatus("big").blocks.contains(RDDBlockId(1, 1)))
    assert(listener.executorIdToStorageStatus("big").blocks.contains(RDDBlockId(1, 2)))
    listener.onUnpersistRDD(SparkListenerUnpersistRDD(1))
    assert(listener.executorIdToStorageStatus("big").blocks.isEmpty)
  }
}
