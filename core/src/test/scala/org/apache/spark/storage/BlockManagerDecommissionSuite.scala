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

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import org.scalatest.concurrent.Eventually

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite, Success,
  TestUtils}
import org.apache.spark.internal.config
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.StandaloneSchedulerBackend
import org.apache.spark.util.{ResetSystemProperties, ThreadUtils}

class BlockManagerDecommissionSuite extends SparkFunSuite with LocalSparkContext
    with ResetSystemProperties with Eventually {

  val numExecs = 2

  test(s"verify that an already running task which is going to cache data succeeds " +
    s"on a decommissioned executor") {
    runDecomTest(true, false)
  }

  test(s"verify that shuffle blocks are migrated.") {
    runDecomTest(false, true)
  }

  private def runDecomTest(persist: Boolean, shuffle: Boolean) = {
    val master = s"local-cluster[${numExecs}, 1, 1024]"
    val conf = new SparkConf().setAppName("test").setMaster(master)
      .set(config.Worker.WORKER_DECOMMISSION_ENABLED, true)
      .set(config.STORAGE_DECOMMISSION_ENABLED, true)
      .set(config.STORAGE_RDD_DECOMMISSION_ENABLED, persist)
      .set(config.STORAGE_SHUFFLE_DECOMMISSION_ENABLED, shuffle)
      .set(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL, 1L)

    sc = new SparkContext(master, "test", conf)

    // Create input RDD with 10 partitions
    val input = sc.parallelize(1 to 10, 10)
    val accum = sc.longAccumulator("mapperRunAccumulator")
    // Do a count to wait for the executors to be registered.
    input.count()

    // Create a new RDD where we have sleep in each partition, we are also increasing
    // the value of accumulator in each partition
    val sleepyRdd = input.mapPartitions { x =>
      Thread.sleep(500)
      accum.add(1)
      x.map(y => (y, y))
    }
    val testRdd = shuffle match {
      case true => sleepyRdd.reduceByKey(_ + _)
      case false => sleepyRdd
    }

    // Listen for the job & block updates
    val sem = new Semaphore(0)
    val taskEndEvents = ArrayBuffer.empty[SparkListenerTaskEnd]
    val blocksUpdated = ArrayBuffer.empty[SparkListenerBlockUpdated]
    sc.addSparkListener(new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
       sem.release()
      }

      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        taskEndEvents.append(taskEnd)
      }

      override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {
        blocksUpdated.append(blockUpdated)
      }
    })


    // Cache the RDD lazily
    if (persist) {
      testRdd.persist()
    }

    // Wait for all of the executors to start
    TestUtils.waitUntilExecutorsUp(sc = sc,
      numExecutors = numExecs,
      timeout = 10000) // 10s

    // Start the computation of RDD - this step will also cache the RDD
    val asyncCount = testRdd.countAsync()

    // Wait for the job to have started
    sem.acquire(1)

    // Give Spark a tiny bit to start the tasks after the listener says hello
    Thread.sleep(100)

    // Decommission one of the executor
    val sched = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
    val execs = sched.getExecutorIds()
    assert(execs.size == numExecs, s"Expected ${numExecs} executors but found ${execs.size}")

    val execToDecommission = execs.head
    logDebug(s"Decommissioning executor ${execToDecommission}")
    sched.decommissionExecutor(execToDecommission)

    // Wait for job to finish
    val asyncCountResult = ThreadUtils.awaitResult(asyncCount, 6.seconds)
    assert(asyncCountResult === 10)
    // All 10 tasks finished, so accum should have been increased 10 times
    assert(accum.value === 10)

    // All tasks should be successful, nothing should have failed
    sc.listenerBus.waitUntilEmpty()
    if (shuffle) {
      // 10 mappers & 10 reducers which succeeded
      assert(taskEndEvents.count(_.reason == Success) === 20,
        s"Expected 20 tasks got ${taskEndEvents.size} (${taskEndEvents})")
    } else {
      // 10 mappers which executed successfully
      assert(taskEndEvents.count(_.reason == Success) === 10,
        s"Expected 10 tasks got ${taskEndEvents.size} (${taskEndEvents})")
    }

    // Wait for our respective blocks to have migrated
    eventually(timeout(15.seconds), interval(10.milliseconds)) {
      if (persist) {
        // One of our blocks should have moved.
        val blockLocs = blocksUpdated.map{ update =>
          (update.blockUpdatedInfo.blockId.name,
            update.blockUpdatedInfo.blockManagerId)}
        val blocksToManagers = blockLocs.groupBy(_._1).mapValues(_.toSet.size)
        assert(!blocksToManagers.filter(_._2 > 1).isEmpty,
          s"We should have a block that has been on multiple BMs in ${blocksUpdated}")
      }
      // If we're migrating shuffles we look for any shuffle block updates
      // as there is no block update on the initial shuffle block write.
      if (shuffle) {
        val numLocs = blocksUpdated.filter{ update =>
          val blockId = update.blockUpdatedInfo.blockId
          blockId.isShuffle || blockId.isInternalShuffle
        }.toSet.size
        assert(numLocs > 0, s"No shuffle block updates in ${blocksUpdated}")
      }
    }

    // Since the RDD is cached or shuffled so further usage of same RDD should use the
    // cached data. Original RDD partitions should not be recomputed i.e. accum
    // should have same value like before
    assert(testRdd.count() === 10)
    assert(accum.value === 10)

    val storageStatus = sc.env.blockManager.master.getStorageStatus
    val execIdToBlocksMapping = storageStatus.map(
      status => (status.blockManagerId.executorId, status.blocks)).toMap
    // No cached blocks should be present on executor which was decommissioned
    assert(execIdToBlocksMapping(execToDecommission).keys.filter(_.isRDD).toSeq === Seq(),
      "Cache blocks should be migrated")
    if (persist) {
      // There should still be all 10 RDD blocks cached
      assert(execIdToBlocksMapping.values.flatMap(_.keys).count(_.isRDD) === 10)
    }
  }
}
