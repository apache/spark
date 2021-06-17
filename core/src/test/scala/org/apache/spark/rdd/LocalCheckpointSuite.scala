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

package org.apache.spark.rdd

import scala.concurrent.duration._

import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}

import org.apache.spark.{LocalSparkContext, SparkContext, SparkException, SparkFunSuite}
import org.apache.spark.storage.{RDDBlockId, StorageLevel}

/**
 * Fine-grained tests for local checkpointing.
 * For end-to-end tests, see CheckpointSuite.
 */
class LocalCheckpointSuite extends SparkFunSuite with LocalSparkContext {

  override def beforeEach(): Unit = {
    super.beforeEach()
    sc = new SparkContext("local[2]", "test")
  }

  test("transform storage level") {
    val transform = LocalRDDCheckpointData.transformStorageLevel _
    assert(transform(StorageLevel.NONE) === StorageLevel.DISK_ONLY)
    assert(transform(StorageLevel.MEMORY_ONLY) === StorageLevel.MEMORY_AND_DISK)
    assert(transform(StorageLevel.MEMORY_ONLY_SER) === StorageLevel.MEMORY_AND_DISK_SER)
    assert(transform(StorageLevel.MEMORY_ONLY_2) === StorageLevel.MEMORY_AND_DISK_2)
    assert(transform(StorageLevel.MEMORY_ONLY_SER_2) === StorageLevel.MEMORY_AND_DISK_SER_2)
    assert(transform(StorageLevel.DISK_ONLY) === StorageLevel.DISK_ONLY)
    assert(transform(StorageLevel.DISK_ONLY_2) === StorageLevel.DISK_ONLY_2)
    assert(transform(StorageLevel.DISK_ONLY_3) === StorageLevel.DISK_ONLY_3)
    assert(transform(StorageLevel.MEMORY_AND_DISK) === StorageLevel.MEMORY_AND_DISK)
    assert(transform(StorageLevel.MEMORY_AND_DISK_SER) === StorageLevel.MEMORY_AND_DISK_SER)
    assert(transform(StorageLevel.MEMORY_AND_DISK_2) === StorageLevel.MEMORY_AND_DISK_2)
    assert(transform(StorageLevel.MEMORY_AND_DISK_SER_2) === StorageLevel.MEMORY_AND_DISK_SER_2)
  }

  test("basic lineage truncation") {
    val numPartitions = 4
    val parallelRdd = sc.parallelize(1 to 100, numPartitions)
    val mappedRdd = parallelRdd.map { i => i + 1 }
    val filteredRdd = mappedRdd.filter { i => i % 2 == 0 }
    val expectedPartitionIndices = (0 until numPartitions).toArray
    assert(filteredRdd.checkpointData.isEmpty)
    assert(filteredRdd.getStorageLevel === StorageLevel.NONE)
    assert(filteredRdd.partitions.map(_.index) === expectedPartitionIndices)
    assert(filteredRdd.dependencies.size === 1)
    assert(filteredRdd.dependencies.head.rdd === mappedRdd)
    assert(mappedRdd.dependencies.size === 1)
    assert(mappedRdd.dependencies.head.rdd === parallelRdd)
    assert(parallelRdd.dependencies.size === 0)

    // Mark the RDD for local checkpointing
    filteredRdd.localCheckpoint()
    assert(filteredRdd.checkpointData.isDefined)
    assert(!filteredRdd.checkpointData.get.isCheckpointed)
    assert(!filteredRdd.checkpointData.get.checkpointRDD.isDefined)
    assert(filteredRdd.getStorageLevel === LocalRDDCheckpointData.DEFAULT_STORAGE_LEVEL)

    // After an action, the lineage is truncated
    val result = filteredRdd.collect()
    assert(filteredRdd.checkpointData.get.isCheckpointed)
    assert(filteredRdd.checkpointData.get.checkpointRDD.isDefined)
    val checkpointRdd = filteredRdd.checkpointData.flatMap(_.checkpointRDD).get
    assert(filteredRdd.dependencies.size === 1)
    assert(filteredRdd.dependencies.head.rdd === checkpointRdd)
    assert(filteredRdd.partitions.map(_.index) === expectedPartitionIndices)
    assert(checkpointRdd.partitions.map(_.index) === expectedPartitionIndices)

    // Recomputation should yield the same result
    assert(filteredRdd.collect() === result)
    assert(filteredRdd.collect() === result)
  }

  test("basic lineage truncation - caching before checkpointing") {
    testBasicLineageTruncationWithCaching(
      newRdd.persist(StorageLevel.MEMORY_ONLY).localCheckpoint(),
      StorageLevel.MEMORY_AND_DISK)
  }

  test("basic lineage truncation - caching after checkpointing") {
    testBasicLineageTruncationWithCaching(
      newRdd.localCheckpoint().persist(StorageLevel.MEMORY_ONLY),
      StorageLevel.MEMORY_AND_DISK)
  }

  test("indirect lineage truncation") {
    testIndirectLineageTruncation(
      newRdd.localCheckpoint(),
      LocalRDDCheckpointData.DEFAULT_STORAGE_LEVEL)
  }

  test("indirect lineage truncation - caching before checkpointing") {
    testIndirectLineageTruncation(
      newRdd.persist(StorageLevel.MEMORY_ONLY).localCheckpoint(),
      StorageLevel.MEMORY_AND_DISK)
  }

  test("indirect lineage truncation - caching after checkpointing") {
    testIndirectLineageTruncation(
      newRdd.localCheckpoint().persist(StorageLevel.MEMORY_ONLY),
      StorageLevel.MEMORY_AND_DISK)
  }

  test("checkpoint without draining iterator") {
    testWithoutDrainingIterator(
      newSortedRdd.localCheckpoint(),
      LocalRDDCheckpointData.DEFAULT_STORAGE_LEVEL,
      50)
  }

  test("checkpoint without draining iterator - caching before checkpointing") {
    testWithoutDrainingIterator(
      newSortedRdd.persist(StorageLevel.MEMORY_ONLY).localCheckpoint(),
      StorageLevel.MEMORY_AND_DISK,
      50)
  }

  test("checkpoint without draining iterator - caching after checkpointing") {
    testWithoutDrainingIterator(
      newSortedRdd.localCheckpoint().persist(StorageLevel.MEMORY_ONLY),
      StorageLevel.MEMORY_AND_DISK,
      50)
  }

  test("checkpoint blocks exist") {
    testCheckpointBlocksExist(
      newRdd.localCheckpoint(),
      LocalRDDCheckpointData.DEFAULT_STORAGE_LEVEL)
  }

  test("checkpoint blocks exist - caching before checkpointing") {
    testCheckpointBlocksExist(
      newRdd.persist(StorageLevel.MEMORY_ONLY).localCheckpoint(),
      StorageLevel.MEMORY_AND_DISK)
  }

  test("checkpoint blocks exist - caching after checkpointing") {
    testCheckpointBlocksExist(
      newRdd.localCheckpoint().persist(StorageLevel.MEMORY_ONLY),
      StorageLevel.MEMORY_AND_DISK)
  }

  test("missing checkpoint block fails with informative message") {
    val rdd = newRdd.localCheckpoint()
    val numPartitions = rdd.partitions.size
    val partitionIndices = rdd.partitions.map(_.index)
    val bmm = sc.env.blockManager.master

    // After an action, the blocks should be found somewhere in the cache
    rdd.collect()
    partitionIndices.foreach { i =>
      assert(bmm.contains(RDDBlockId(rdd.id, i)))
    }

    // Remove one of the blocks to simulate executor failure
    // Collecting the RDD should now fail with an informative exception
    val blockId = RDDBlockId(rdd.id, numPartitions - 1)
    bmm.removeBlock(blockId)
    // Wait until the block has been removed successfully.
    eventually(timeout(1.second), interval(100.milliseconds)) {
      assert(bmm.getBlockStatus(blockId).isEmpty)
    }
    try {
      rdd.collect()
      fail("Collect should have failed if local checkpoint block is removed...")
    } catch {
      case se: SparkException =>
        assert(se.getMessage.contains(s"Checkpoint block $blockId not found"))
        assert(se.getMessage.contains("rdd.checkpoint()")) // suggest an alternative
        assert(se.getMessage.contains("fault-tolerant")) // justify the alternative
    }
  }

  /**
   * Helper method to create a simple RDD.
   */
  private def newRdd: RDD[Int] = {
    sc.parallelize(1 to 100, 4)
      .map { i => i + 1 }
      .filter { i => i % 2 == 0 }
  }

  /**
   * Helper method to create a simple sorted RDD.
   */
  private def newSortedRdd: RDD[Int] = newRdd.sortBy(identity)

  /**
   * Helper method to test basic lineage truncation with caching.
   *
   * @param rdd an RDD that is both marked for caching and local checkpointing
   */
  private def testBasicLineageTruncationWithCaching[T](
      rdd: RDD[T],
      targetStorageLevel: StorageLevel): Unit = {
    require(targetStorageLevel !== StorageLevel.NONE)
    require(rdd.getStorageLevel !== StorageLevel.NONE)
    require(rdd.isLocallyCheckpointed)
    val result = rdd.collect()
    assert(rdd.getStorageLevel === targetStorageLevel)
    assert(rdd.checkpointData.isDefined)
    assert(rdd.checkpointData.get.isCheckpointed)
    assert(rdd.checkpointData.get.checkpointRDD.isDefined)
    assert(rdd.dependencies.head.rdd === rdd.checkpointData.get.checkpointRDD.get)
    assert(rdd.collect() === result)
    assert(rdd.collect() === result)
  }

  /**
   * Helper method to test indirect lineage truncation.
   *
   * Indirect lineage truncation here means the action is called on one of the
   * checkpointed RDD's descendants, but not on the checkpointed RDD itself.
   *
   * @param rdd a locally checkpointed RDD
   */
  private def testIndirectLineageTruncation[T](
      rdd: RDD[T],
      targetStorageLevel: StorageLevel): Unit = {
    require(targetStorageLevel !== StorageLevel.NONE)
    require(rdd.isLocallyCheckpointed)
    val rdd1 = rdd.map { i => i + "1" }
    val rdd2 = rdd1.map { i => i + "2" }
    val rdd3 = rdd2.map { i => i + "3" }
    val rddDependencies = rdd.dependencies
    val rdd1Dependencies = rdd1.dependencies
    val rdd2Dependencies = rdd2.dependencies
    val rdd3Dependencies = rdd3.dependencies
    assert(rdd1Dependencies.size === 1)
    assert(rdd1Dependencies.head.rdd === rdd)
    assert(rdd2Dependencies.size === 1)
    assert(rdd2Dependencies.head.rdd === rdd1)
    assert(rdd3Dependencies.size === 1)
    assert(rdd3Dependencies.head.rdd === rdd2)

    // Only the locally checkpointed RDD should have special storage level
    assert(rdd.getStorageLevel === targetStorageLevel)
    assert(rdd1.getStorageLevel === StorageLevel.NONE)
    assert(rdd2.getStorageLevel === StorageLevel.NONE)
    assert(rdd3.getStorageLevel === StorageLevel.NONE)

    // After an action, only the dependencies of the checkpointed RDD changes
    val result = rdd3.collect()
    assert(rdd.dependencies !== rddDependencies)
    assert(rdd1.dependencies === rdd1Dependencies)
    assert(rdd2.dependencies === rdd2Dependencies)
    assert(rdd3.dependencies === rdd3Dependencies)
    assert(rdd3.collect() === result)
    assert(rdd3.collect() === result)
  }

  /**
   * Helper method to test checkpointing without fully draining the iterator.
   *
   * Not all RDD actions fully consume the iterator. As a result, a subset of the partitions
   * may not be cached. However, since we want to truncate the lineage safely, we explicitly
   * ensure that *all* partitions are fully cached. This method asserts this behavior.
   *
   * @param rdd a locally checkpointed RDD
   */
  private def testWithoutDrainingIterator[T](
      rdd: RDD[T],
      targetStorageLevel: StorageLevel,
      targetCount: Int): Unit = {
    require(targetCount > 0)
    require(targetStorageLevel !== StorageLevel.NONE)
    require(rdd.isLocallyCheckpointed)

    // This does not drain the iterator, but checkpointing should still work
    val first = rdd.first()
    assert(rdd.count() === targetCount)
    assert(rdd.count() === targetCount)
    assert(rdd.first() === first)
    assert(rdd.first() === first)

    // Test the same thing by calling actions on a descendant instead
    val rdd1 = rdd.repartition(10)
    val rdd2 = rdd1.repartition(100)
    val rdd3 = rdd2.repartition(1000)
    val first2 = rdd3.first()
    assert(rdd3.count() === targetCount)
    assert(rdd3.count() === targetCount)
    assert(rdd3.first() === first2)
    assert(rdd3.first() === first2)
    assert(rdd.getStorageLevel === targetStorageLevel)
    assert(rdd1.getStorageLevel === StorageLevel.NONE)
    assert(rdd2.getStorageLevel === StorageLevel.NONE)
    assert(rdd3.getStorageLevel === StorageLevel.NONE)
  }

  /**
   * Helper method to test whether the checkpoint blocks are found in the cache.
   *
   * @param rdd a locally checkpointed RDD
   */
  private def testCheckpointBlocksExist[T](
      rdd: RDD[T],
      targetStorageLevel: StorageLevel): Unit = {
    val bmm = sc.env.blockManager.master
    val partitionIndices = rdd.partitions.map(_.index)

    // The blocks should not exist before the action
    partitionIndices.foreach { i =>
      assert(!bmm.contains(RDDBlockId(rdd.id, i)))
    }

    // After an action, the blocks should be found in the cache with the expected level
    rdd.collect()
    partitionIndices.foreach { i =>
      val blockId = RDDBlockId(rdd.id, i)
      val status = bmm.getBlockStatus(blockId)
      assert(status.nonEmpty)
      assert(status.values.head.storageLevel === targetStorageLevel)
    }
  }

}
