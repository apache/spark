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

import org.apache.spark.{SparkException, SparkContext, LocalSparkContext, SparkFunSuite}

import org.mockito.Mockito.spy
import org.apache.spark.storage.{RDDBlockId, StorageLevel}

/**
 * Fine-grained tests for local checkpointing.
 * For end-to-end tests, see CheckpointSuite.
 */
class LocalCheckpointSuite extends SparkFunSuite with LocalSparkContext {

  override def beforeEach(): Unit = {
    sc = spy(new SparkContext("local[2]", "test"))
  }

  test("transform storage level") {
    val rdd = sc.parallelize(1 to 100).localCheckpoint()
    assert(rdd.getStorageLevel === StorageLevel.NONE)
    assert(rdd.checkpointData.isDefined)
    val data = rdd.checkpointData match {
      case Some(local: LocalRDDCheckpointData[Int]) => local
      case _ => fail("Checkpoint data was not of expected type!")
    }

    // No storage level -> disk only
    data.transformStorageLevel()
    assert(rdd.getStorageLevel === StorageLevel.DISK_ONLY)

    // Disk only -> disk only
    data.transformStorageLevel()
    assert(rdd.getStorageLevel === StorageLevel.DISK_ONLY)

    // Memory only -> memory and disk
    rdd.setStorageLevel(StorageLevel.MEMORY_ONLY)
    data.transformStorageLevel()
    assert(rdd.getStorageLevel === StorageLevel.MEMORY_AND_DISK)

    // Memory and disk -> memory and disk
    data.transformStorageLevel()
    assert(rdd.getStorageLevel === StorageLevel.MEMORY_AND_DISK)

    // Other properties (i.e. whether to serialize, replication factor) should stay
    rdd.setStorageLevel(StorageLevel.MEMORY_ONLY_SER_2)
    data.transformStorageLevel()
    assert(rdd.getStorageLevel === StorageLevel.MEMORY_AND_DISK_SER_2)

    // Off-heap should fail fast
    rdd.setStorageLevel(StorageLevel.OFF_HEAP)
    intercept[SparkException] { data.transformStorageLevel() }
  }

  test("basic lineage truncation") {
    val numPartitions = 4
    val parallelRdd = sc.parallelize(1 to 100, numPartitions)
    val mappedRdd = parallelRdd.map { i => i + 1 }
    val filteredRdd = mappedRdd.filter { i => i % 2 == 0 }
    val expectedPartitionIndices = (0 until numPartitions).toArray
    assert(filteredRdd.dependencies.size === 1)
    assert(filteredRdd.dependencies.head.rdd === mappedRdd)
    assert(filteredRdd.partitions.map(_.index) === expectedPartitionIndices)
    assert(filteredRdd.checkpointData.isEmpty)
    assert(mappedRdd.dependencies.size === 1)
    assert(parallelRdd.dependencies.size === 0)
    assert(mappedRdd.dependencies.head.rdd === parallelRdd)
    filteredRdd.localCheckpoint()
    assert(filteredRdd.checkpointData.isDefined)
    assert(!filteredRdd.checkpointData.get.isCheckpointed)
    assert(!filteredRdd.checkpointData.get.checkpointRDD.isDefined)

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

  test("indirect lineage truncation") {
    val filteredRdd = sc.parallelize(1 to 100, 4)
      .map { i => i + 1 }
      .filter { i => i % 2 == 0 }
      .localCheckpoint()
    val coalescedRdd = filteredRdd.repartition(10)

    // After an action, only the dependencies of the checkpointed RDD changes
    val filteredDependencies = filteredRdd.dependencies
    val coalescedDependencies = coalescedRdd.dependencies
    val result = coalescedRdd.collect()
    assert(filteredRdd.dependencies !== filteredDependencies)
    assert(coalescedRdd.dependencies === coalescedDependencies)

    // Recomputation should yield the same result
    assert(coalescedRdd.collect() === result)
    assert(coalescedRdd.collect() === result)
  }

  test("checkpoint without draining iterator") {
    val filteredRdd = sc.parallelize(1 to 100, 4)
      .map { i => i + 1 }
      .filter { i => i % 2 == 0 }
      .sortBy(identity) // needed for determinism
      .localCheckpoint()

    // This does not drain the iterator, but checkpointing should still work
    val first = filteredRdd.first()
    assert(filteredRdd.count() === 50)
    assert(filteredRdd.count() === 50)
    assert(filteredRdd.first() === first)
    assert(filteredRdd.first() === first)

    // Test the same thing by calling actions on the child instead
    val coalescedRdd = filteredRdd.repartition(10)
    val first2 = coalescedRdd.first()
    assert(coalescedRdd.count() === 50)
    assert(coalescedRdd.count() === 50)
    assert(coalescedRdd.first() === first2)
    assert(coalescedRdd.first() === first2)
  }

  test("checkpoint files are in disk store") {
    val numPartitions = 4
    val filteredRdd = sc.parallelize(1 to 100, numPartitions)
      .map { i => i + 1 }
      .filter { i => i % 2 == 0 }
      .localCheckpoint()
    val bmm = sc.env.blockManager.master

    // After an action, the blocks should be found somewhere on disk
    assert(bmm.getStorageStatus.forall(_.diskUsed == 0))
    filteredRdd.collect()
    assert(bmm.getStorageStatus.forall(_.diskUsed > 0))
    assert(filteredRdd.checkpointData.isDefined)
    assert(filteredRdd.checkpointData.get.checkpointRDD.isDefined)

    (0 until numPartitions).foreach { i =>
      val blockId = RDDBlockId(filteredRdd.id, i)
      val status = bmm.getBlockStatus(blockId).values.head
      assert(status.storageLevel === StorageLevel.DISK_ONLY)
      assert(status.diskSize > 0)
    }
  }

  test("checkpoint files are in disk store with caching") {
    val numPartitions = 4
    val filteredRdd = sc.parallelize(1 to 100, numPartitions)
      .map { i => i + 1 }
      .filter { i => i % 2 == 0 }
      .persist(StorageLevel.MEMORY_ONLY)
      .localCheckpoint()
    val bmm = sc.env.blockManager.master

    // After an action, the blocks should be found in the
    // block manager with a new storage level that uses disk
    filteredRdd.collect()
    (0 until numPartitions).foreach { i =>
      val blockId = RDDBlockId(filteredRdd.id, i)
      val status = bmm.getBlockStatus(blockId).values.head
      assert(status.storageLevel === StorageLevel.MEMORY_AND_DISK)
      assert(status.memSize > 0)
    }
  }

  test("missing checkpoint file fails with informative message") {
    val numPartitions = 4
    val filteredRdd = sc.parallelize(1 to 100, numPartitions)
      .map { i => i + 1 }
      .filter { i => i % 2 == 0 }
      .localCheckpoint()
    val bmm = sc.env.blockManager.master

    // After an action, the blocks should be found somewhere on disk
    filteredRdd.collect()
    (0 until numPartitions).foreach { i =>
      assert(bmm.contains(RDDBlockId(filteredRdd.id, i)))
    }

    // Remove one of the blocks to simulate executor failure
    // Collecting the RDD should now fail with an informative exception
    val blockId = RDDBlockId(filteredRdd.id, numPartitions - 1)
    bmm.removeBlock(blockId)
    try {
      filteredRdd.collect()
      fail("Collect should have failed if local checkpoint block is removed...")
    } catch {
      case se: SparkException =>
        assert(se.getMessage.contains(s"Checkpoint block $blockId not found"))
        assert(se.getMessage.contains("rdd.checkpoint()")) // suggest an alternative
        assert(se.getMessage.contains("fault-tolerant")) // justify the alternative
    }
  }

}
