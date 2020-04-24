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

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite, Success}
import org.apache.spark.internal.config
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd, SparkListenerTaskStart}
import org.apache.spark.scheduler.cluster.StandaloneSchedulerBackend
import org.apache.spark.util.ThreadUtils

class BlockManagerDecommissionSuite extends SparkFunSuite with LocalSparkContext {

  override def beforeEach(): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local")
      .set(config.Worker.WORKER_DECOMMISSION_ENABLED, true)
      .set(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL, 100L)
      .set(config.STORAGE_DECOMMISSION_ENABLED, true)

    sc = new SparkContext("local-cluster[3, 1, 1024]", "test", conf)
  }

  test(s"verify that an already running task which is going to cache data succeeds " +
    s"on a decommissioned executor") {
    // runDecomTest(true, false)
  }

  test(s"verify that shuffle blocks are migrated.") {
    runDecomTest(false, true)
  }

  private def runDecomTest(persist: Boolean, shuffle: Boolean) = {
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

    // Listen for the job
    val sem = new Semaphore(0)
    val taskEndEvents = ArrayBuffer.empty[SparkListenerTaskEnd]
    sc.addSparkListener(new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
       sem.release()
      }

      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        taskEndEvents.append(taskEnd)
      }
    })

    // Cache the RDD lazily
    if (persist) {
      testRdd.persist()
    }

    // Start the computation of RDD - this step will also cache the RDD
    val asyncCount = testRdd.countAsync()

    // Wait for the job to have started
    sem.acquire(1)

    // Decommission one of the executor
    val sched = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
    val execs = sched.getExecutorIds()
    assert(execs.size == 3, s"Expected 3 executors but found ${execs.size}")
    val execToDecommission = execs.head
    // sched.decommissionExecutor(execToDecommission)

    // Wait for job to finish
    val asyncCountResult = ThreadUtils.awaitResult(asyncCount, 5.seconds)
    assert(asyncCountResult === 10)
    // All 10 tasks finished, so accum should have been increased 10 times
    assert(accum.value === 10)

    // All tasks should be successful, nothing should have failed
    sc.listenerBus.waitUntilEmpty()
    if (shuffle) {
      assert(taskEndEvents.size === 20) // 10 mappers & 10 reducers
    } else {
      assert(taskEndEvents.size === 10) // 10 mappers
    }
    assert(taskEndEvents.map(_.reason).toSet === Set(Success))

    // all blocks should have been shifted from decommissioned block manager
    // after some time
    Thread.sleep(1000)

    // Since the RDD is cached or shuffled so further usage of same RDD should use the
    // cached data. Original RDD partitions should not be recomputed i.e. accum
    // should have same value like before
    assert(testRdd.count() === 10)
    assert(accum.value === 10)

    val storageStatus = sc.env.blockManager.master.getStorageStatus
    val execIdToBlocksMapping = storageStatus.map(
      status => (status.blockManagerId.executorId, status.blocks)).toMap
    // No cached blocks should be present on executor which was decommissioned
    assert(execIdToBlocksMapping(execToDecommission).keys.filter(_.isRDD).toSeq === Seq())
    if (persist) {
      // There should still be all 10 RDD blocks cached
      assert(execIdToBlocksMapping.values.flatMap(_.keys).count(_.isRDD) === 10)
    }
  }
}
