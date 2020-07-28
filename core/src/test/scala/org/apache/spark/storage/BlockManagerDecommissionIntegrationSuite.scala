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

import java.util.concurrent.{ConcurrentLinkedQueue, Semaphore}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import org.scalatest.concurrent.Eventually

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.StandaloneSchedulerBackend
import org.apache.spark.util.{ResetSystemProperties, ThreadUtils}

class BlockManagerDecommissionIntegrationSuite extends SparkFunSuite with LocalSparkContext
    with ResetSystemProperties with Eventually {

  val numExecs = 3
  val numParts = 3
  val TaskStarted = "TASK_STARTED"
  val TaskEnded = "TASK_ENDED"
  val JobEnded = "JOB_ENDED"

  test(s"verify that an already running task which is going to cache data succeeds " +
    s"on a decommissioned executor after task start") {
    runDecomTest(true, false, TaskStarted)
  }

  test(s"verify that an already running task which is going to cache data succeeds " +
    s"on a decommissioned executor after one task ends but before job ends") {
    runDecomTest(true, false, TaskEnded)
  }

  test(s"verify that shuffle blocks are migrated") {
    runDecomTest(false, true, JobEnded)
  }

  test(s"verify that both migrations can work at the same time") {
    runDecomTest(true, true, JobEnded)
  }

  private def runDecomTest(
      persist: Boolean,
      shuffle: Boolean,
      whenToDecom: String): Unit = {
    val migrateDuring = whenToDecom != JobEnded
    val master = s"local-cluster[${numExecs}, 1, 1024]"
    val conf = new SparkConf().setAppName("test").setMaster(master)
      .set(config.Worker.WORKER_DECOMMISSION_ENABLED, true)
      .set(config.STORAGE_DECOMMISSION_ENABLED, true)
      .set(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED, persist)
      .set(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED, shuffle)
      // Just replicate blocks as fast as we can during testing, there isn't another
      // workload we need to worry about.
      .set(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL, 1L)

    sc = new SparkContext(master, "test", conf)

    // Wait for the executors to start
    TestUtils.waitUntilExecutorsUp(sc = sc,
      numExecutors = numExecs,
      timeout = 60000) // 60s

    val input = sc.parallelize(1 to numParts, numParts)
    val accum = sc.collectionAccumulator[String]("mapperRunAccumulator")

    // Create a new RDD where we have sleep in each partition, we are also increasing
    // the value of accumulator in each partition
    val baseRdd = input.mapPartitions { x =>
      accum.add(SparkEnv.get.executorId)
      if (migrateDuring) {
        Thread.sleep(1000)
      }
      x.map(y => (y, y))
    }
    val testRdd = shuffle match {
      case true => baseRdd.reduceByKey(_ + _)
      case false => baseRdd
    }

    // Listen for the job & block updates
    val executorRemovedSem = new Semaphore(0)
    val taskEndEvents = new ConcurrentLinkedQueue[SparkListenerTaskEnd]()
    val blocksUpdated = ArrayBuffer.empty[SparkListenerBlockUpdated]

    def getCandidateExecutorToDecom: Option[String] = if (whenToDecom == TaskStarted) {
      accum.value.asScala.headOption
    } else {
      taskEndEvents.asScala.filter(_.taskInfo.successful).map(_.taskInfo.executorId).headOption
    }

    sc.addSparkListener(new SparkListener {
      override def onExecutorRemoved(execRemoved: SparkListenerExecutorRemoved): Unit = {
        executorRemovedSem.release()
      }

      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        taskEndEvents.add(taskEnd)
      }

      override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {
        blocksUpdated.append(blockUpdated)
      }
    })

    // Cache the RDD lazily
    if (persist) {
      testRdd.persist()
    }

    // Start the computation of RDD - this step will also cache the RDD
    val asyncCount = testRdd.countAsync()

    // Make sure the job is either mid run or otherwise has data to migrate.
    if (migrateDuring) {
      // Wait for one of the tasks to succeed and finish writing its blocks.
      // This way we know that this executor had real data to migrate when it is subsequently
      // decommissioned below.
      val intervalMs = if (whenToDecom == TaskStarted) {
        1.milliseconds
      } else {
        10.milliseconds
      }
      eventually(timeout(6.seconds), interval(intervalMs)) {
        assert(getCandidateExecutorToDecom.isDefined)
      }
    } else {
      ThreadUtils.awaitResult(asyncCount, 15.seconds)
    }

    // Decommission one of the executors.
    val sched = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]

    val execToDecommission = getCandidateExecutorToDecom.get
    logInfo(s"Decommissioning executor ${execToDecommission}")
    sched.decommissionExecutor(
      execToDecommission,
      ExecutorDecommissionInfo("", isHostDecommissioned = false))

    // Wait for job to finish.
    val asyncCountResult = ThreadUtils.awaitResult(asyncCount, 15.seconds)
    assert(asyncCountResult === numParts)
    // All tasks finished, so accum should have been increased numParts times.
    assert(accum.value.size() === numParts)

    sc.listenerBus.waitUntilEmpty()
    if (shuffle) {
      //  mappers & reducers which succeeded
      assert(taskEndEvents.asScala.count(_.reason == Success) === 2 * numParts,
        s"Expected ${2 * numParts} tasks got ${taskEndEvents.size} (${taskEndEvents})")
    } else {
      // only mappers which executed successfully
      assert(taskEndEvents.asScala.count(_.reason == Success) === numParts,
        s"Expected ${numParts} tasks got ${taskEndEvents.size} (${taskEndEvents})")
    }

    // Wait for our respective blocks to have migrated
    eventually(timeout(30.seconds), interval(10.milliseconds)) {
      if (persist) {
        // One of our blocks should have moved.
        val rddUpdates = blocksUpdated.filter { update =>
          val blockId = update.blockUpdatedInfo.blockId
          blockId.isRDD}
        val blockLocs = rddUpdates.map { update =>
          (update.blockUpdatedInfo.blockId.name,
            update.blockUpdatedInfo.blockManagerId)}
        val blocksToManagers = blockLocs.groupBy(_._1).mapValues(_.size)
        assert(!blocksToManagers.filter(_._2 > 1).isEmpty,
          s"We should have a block that has been on multiple BMs in rdds:\n ${rddUpdates} from:\n" +
          s"${blocksUpdated}\n but instead we got:\n ${blocksToManagers}")
      }
      // If we're migrating shuffles we look for any shuffle block updates
      // as there is no block update on the initial shuffle block write.
      if (shuffle) {
        val numDataLocs = blocksUpdated.filter { update =>
          val blockId = update.blockUpdatedInfo.blockId
          blockId.isInstanceOf[ShuffleDataBlockId]
        }.size
        val numIndexLocs = blocksUpdated.filter { update =>
          val blockId = update.blockUpdatedInfo.blockId
          blockId.isInstanceOf[ShuffleIndexBlockId]
        }.size
        assert(numDataLocs === 1, s"Expect shuffle data block updates in ${blocksUpdated}")
        assert(numIndexLocs === 1, s"Expect shuffle index block updates in ${blocksUpdated}")
      }
    }

    // Since the RDD is cached or shuffled so further usage of same RDD should use the
    // cached data. Original RDD partitions should not be recomputed i.e. accum
    // should have same value like before
    assert(testRdd.count() === numParts)
    assert(accum.value.size() === numParts)

    val storageStatus = sc.env.blockManager.master.getStorageStatus
    val execIdToBlocksMapping = storageStatus.map(
      status => (status.blockManagerId.executorId, status.blocks)).toMap
    // No cached blocks should be present on executor which was decommissioned
    assert(execIdToBlocksMapping(execToDecommission).keys.filter(_.isRDD).toSeq === Seq(),
      "Cache blocks should be migrated")
    if (persist) {
      // There should still be all the RDD blocks cached
      assert(execIdToBlocksMapping.values.flatMap(_.keys).count(_.isRDD) === numParts)
    }

    // Make the executor we decommissioned exit
    sched.client.killExecutors(List(execToDecommission))

    // Wait for the executor to be removed
    executorRemovedSem.acquire(1)

    // Since the RDD is cached or shuffled so further usage of same RDD should use the
    // cached data. Original RDD partitions should not be recomputed i.e. accum
    // should have same value like before
    assert(testRdd.count() === numParts)
    assert(accum.value.size() === numParts)
  }
}
