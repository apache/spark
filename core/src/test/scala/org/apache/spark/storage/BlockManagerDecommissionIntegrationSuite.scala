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

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicReference

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

  test(s"verify that an already running task which is going to cache data succeeds " +
    s"on a decommissioned executor") {
    runDecomTest(true, false, true)
  }

  test(s"verify that shuffle blocks are migrated") {
    runDecomTest(false, true, false)
  }

  test(s"verify that both migrations can work at the same time.") {
    runDecomTest(true, true, false)
  }

  private def runDecomTest(persist: Boolean, shuffle: Boolean, migrateDuring: Boolean) = {

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
    val accum = sc.longAccumulator("mapperRunAccumulator")
    input.count()

    // Create a new RDD where we have sleep in each partition, we are also increasing
    // the value of accumulator in each partition
    val baseRdd = input.mapPartitions { x =>
      if (migrateDuring) {
        Thread.sleep(1000)
      }
      accum.add(1)
      x.map(y => (y, y))
    }
    val testRdd = shuffle match {
      case true => baseRdd.reduceByKey(_ + _)
      case false => baseRdd
    }

    // Listen for the job & block updates
    val taskStartSem = new Semaphore(0)
    val broadcastSem = new Semaphore(0)
    val executorRemovedSem = new Semaphore(0)
    val taskEndEvents = ArrayBuffer.empty[SparkListenerTaskEnd]
    val blocksUpdated = ArrayBuffer.empty[SparkListenerBlockUpdated]
    val sched = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
    val execToDecommission = new AtomicReference[String](null)

    def maybeDoDecommission(candidateExecToDecom: String): Boolean = {
      if (execToDecommission.compareAndSet(null, candidateExecToDecom)) {
        val decomContext = s"Decommissioning executor ${candidateExecToDecom}"
        logInfo(decomContext)
        sched.decommissionExecutor(candidateExecToDecom,
          ExecutorDecommissionInfo(decomContext, false))
        true
      } else {
        false
      }
    }

    sc.addSparkListener(new SparkListener {
      override def onExecutorRemoved(execRemoved: SparkListenerExecutorRemoved): Unit = {
        executorRemovedSem.release()
      }

      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        taskStartSem.release()
      }

      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        taskEndEvents.append(taskEnd)
        if (migrateDuring && taskEnd.taskInfo.successful) {
          // When migrating during, we decommission an executor _after_ it is done with a task.
          // We don't decommission an executor while it is running, because doing so might cause
          // a task to fail. The task fails because a decommissioned block manager cannot save
          // blocks. When this happens the task will be re-run on a different executor, but by
          // which time we have already decommissioned a "wrong" executor. To avoid this condition,
          // we wait for a task to finish successfully and go and decommission its executor.
          maybeDoDecommission(taskEnd.taskInfo.executorId)
        }
      }

      override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {
        // Once broadcast start landing on the executors we're good to proceed.
        // We don't only use task start as it can occur before the work is on the executor.
        if (blockUpdated.blockUpdatedInfo.blockId.isBroadcast) {
          broadcastSem.release()
        }
        blocksUpdated.append(blockUpdated)
      }
    })


    // Cache the RDD lazily
    if (persist) {
      testRdd.persist()
    }

    // Start the computation of RDD - this step will also cache the RDD
    val asyncCount = testRdd.countAsync()

    // Wait for the job to have started.
    taskStartSem.acquire(1)
    // Wait for each executor + driver to have it's broadcast info delivered.
    broadcastSem.acquire((numExecs + 1))

    // Make sure the job is either mid run or otherwise has data to migrate.
    if (migrateDuring) {
      // Give Spark a tiny bit to start executing after the broadcast blocks land.
      // For me this works at 100, set to 300 for system variance.
      Thread.sleep(300)
    } else {
      ThreadUtils.awaitResult(asyncCount, 15.seconds)
    }

    sc.listenerBus.waitUntilEmpty()

    val asyncCountResult = if (migrateDuring) {
      // Wait for the job to finish
      ThreadUtils.awaitResult(asyncCount, 15.seconds)
    } else {
      // (See comment in listener's onTaskEnd method.)
      // But when migrating after a job is done, we can decommission any executor that had a task
      val executorWithSuccessfulTask = taskEndEvents
        .filter(_.taskInfo.successful)
        .map(_.taskInfo.executorId)
        .head
      val decommissioned = maybeDoDecommission(executorWithSuccessfulTask)
      assert(decommissioned, s"No prior decommissioning should have happened")
      // We had already waited for the job to finish above, so the timeout is zero
      ThreadUtils.awaitResult(asyncCount, 0.seconds)
    }

    assert(asyncCountResult === numParts)
    // All tasks finished, so accum should have been increased numParts times.
    assert(accum.value === numParts)

    if (shuffle) {
      //  mappers & reducers which succeeded
      assert(taskEndEvents.count(_.reason == Success) === 2 * numParts,
        s"Expected ${2 * numParts} tasks got ${taskEndEvents.size} (${taskEndEvents})")
    } else {
      // only mappers which executed successfully
      assert(taskEndEvents.count(_.reason == Success) === numParts,
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
    assert(accum.value === numParts)

    val storageStatus = sc.env.blockManager.master.getStorageStatus
    val execIdToBlocksMapping = storageStatus.map(
      status => (status.blockManagerId.executorId, status.blocks)).toMap
    // No cached blocks should be present on executor which was decommissioned
    assert(execIdToBlocksMapping(execToDecommission.get()).keys.filter(_.isRDD).toSeq === Seq(),
      "Cache blocks should be migrated")
    if (persist) {
      // There should still be all the RDD blocks cached
      assert(execIdToBlocksMapping.values.flatMap(_.keys).count(_.isRDD) === numParts)
    }

    // Make the executor we decommissioned exit
    sched.client.killExecutors(List(execToDecommission.get()))

    // Wait for the executor to be removed
    executorRemovedSem.acquire(1)

    // Since the RDD is cached or shuffled so further usage of same RDD should use the
    // cached data. Original RDD partitions should not be recomputed i.e. accum
    // should have same value like before
    assert(testRdd.count() === numParts)
    assert(accum.value === numParts)

  }
}
