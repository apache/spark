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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, Semaphore, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import org.scalatest.concurrent.Eventually

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.StandaloneSchedulerBackend
import org.apache.spark.util.{ResetSystemProperties, SystemClock, ThreadUtils}

class BlockManagerDecommissionIntegrationSuite extends SparkFunSuite with LocalSparkContext
    with ResetSystemProperties with Eventually {

  val numExecs = 3
  val numParts = 3
  val TaskStarted = "TASK_STARTED"
  val TaskEnded = "TASK_ENDED"
  val JobEnded = "JOB_ENDED"

  Seq(false, true).foreach { isEnabled =>
    test(s"SPARK-32850: BlockManager decommission should respect the configuration " +
      s"(enabled=${isEnabled})") {
      val conf = new SparkConf()
        .setAppName("test-blockmanager-decommissioner")
        .setMaster("local-cluster[2, 1, 1024]")
        .set(config.DECOMMISSION_ENABLED, true)
        .set(config.STORAGE_DECOMMISSION_ENABLED, isEnabled)
        .set(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED, isEnabled)
      sc = new SparkContext(conf)
      TestUtils.waitUntilExecutorsUp(sc, 2, 60000)
      val executors = sc.getExecutorIds().toArray
      val decommissionListener = new SparkListener {
        override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
          // ensure Tasks launched at executors before they're marked as decommissioned by driver
          Thread.sleep(3000)
          sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
            .decommissionExecutors(
              executors.map { id => (id, ExecutorDecommissionInfo("test")) },
              true,
              false)
        }
      }
      sc.addSparkListener(decommissionListener)

      val decommissionStatus: Seq[Boolean] = sc.parallelize(1 to 100, 2).mapPartitions { _ =>
        val startTime = System.currentTimeMillis()
        while (SparkEnv.get.blockManager.decommissioner.isEmpty &&
          // wait at most 6 seconds for BlockManager to start to decommission (if enabled)
          System.currentTimeMillis() - startTime < 6000) {
          Thread.sleep(300)
        }
        val blockManagerDecommissionStatus =
          if (SparkEnv.get.blockManager.decommissioner.isEmpty) false else true
        Iterator.single(blockManagerDecommissionStatus)
      }.collect()
      assert(decommissionStatus.forall(_ == isEnabled))
      sc.removeSparkListener(decommissionListener)
    }
  }

  testRetry("verify that an already running task which is going to cache data succeeds " +
    "on a decommissioned executor after task start") {
    runDecomTest(true, false, TaskStarted)
  }

  test("verify that an already running task which is going to cache data succeeds " +
    "on a decommissioned executor after one task ends but before job ends") {
    runDecomTest(true, false, TaskEnded)
  }

  test("verify that shuffle blocks are migrated") {
    runDecomTest(false, true, JobEnded)
  }

  test("verify that both migrations can work at the same time") {
    runDecomTest(true, true, JobEnded)
  }

  test("SPARK-36782 not deadlock if MapOutput uses broadcast") {
    runDecomTest(false, true, JobEnded, forceMapOutputBroadcast = true)
  }

  private def runDecomTest(
      persist: Boolean,
      shuffle: Boolean,
      whenToDecom: String,
      forceMapOutputBroadcast: Boolean = false): Unit = {
    val migrateDuring = whenToDecom != JobEnded
    val master = s"local-cluster[${numExecs}, 1, 1024]"
    val minBroadcastSize = if (forceMapOutputBroadcast) {
      0
    } else {
      config.SHUFFLE_MAPOUTPUT_MIN_SIZE_FOR_BROADCAST.defaultValue.get
    }
    val conf = new SparkConf().setAppName("test").setMaster(master)
      .set(config.DECOMMISSION_ENABLED, true)
      .set(config.STORAGE_DECOMMISSION_ENABLED, true)
      .set(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED, persist)
      .set(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED, shuffle)
      // Since we use the bus for testing we don't want to drop any messages
      .set(config.LISTENER_BUS_EVENT_QUEUE_CAPACITY, 1000000)
      // Just replicate blocks quickly during testing, there isn't another
      // workload we need to worry about.
      .set(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL, 10L)
      .set(config.SHUFFLE_MAPOUTPUT_MIN_SIZE_FOR_BROADCAST, minBroadcastSize)

    if (whenToDecom == TaskStarted) {
      // We are using accumulators below, make sure those are reported frequently.
      conf.set(config.EXECUTOR_HEARTBEAT_INTERVAL.key, "10ms")
    }
    sc = new SparkContext(master, "test", conf)

    // Wait for the executors to start
    TestUtils.waitUntilExecutorsUp(sc = sc,
      numExecutors = numExecs,
      timeout = 60000) // 60s

    val input = sc.parallelize(1 to numParts, numParts)
    val accum = sc.longAccumulator("mapperRunAccumulator")

    // Create a new RDD where we have sleep in each partition, we are also increasing
    // the value of accumulator in each partition
    val baseRdd = input.mapPartitions { x =>
      accum.add(1)

      val sleepIntervalMs = whenToDecom match {
        // Increase the window of time b/w task started and ended so that we can decom within that.
        case "TASK_STARTED" => 10000
        // Make one task take a really short time so that we can decommission right after it is
        // done but before its peers are done.
        case "TASK_ENDED" =>
          if (TaskContext.getPartitionId() == 0) {
            100
          } else {
            1000
          }
        // No sleep otherwise
        case _ => 0
      }
      if (sleepIntervalMs > 0) {
        Thread.sleep(sleepIntervalMs)
      }
      x.map(y => (y, y))
    }
    val testRdd = if (shuffle) {
      baseRdd.reduceByKey(_ + _)
    } else {
      baseRdd
    }

    // Listen for the job & block updates
    val executorRemovedSem = new Semaphore(0)
    val taskEndEvents = new ConcurrentLinkedQueue[SparkListenerTaskEnd]()
    val executorsActuallyStarted = new ConcurrentHashMap[String, Boolean]()
    val blocksUpdated = ArrayBuffer.empty[SparkListenerBlockUpdated]

    def getCandidateExecutorToDecom: Option[String] = if (whenToDecom == TaskStarted) {
      executorsActuallyStarted.keySet().asScala.headOption
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

      override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = synchronized {
        blocksUpdated.append(blockUpdated)
      }

      override def onExecutorMetricsUpdate(
          executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
        val executorId = executorMetricsUpdate.execId
        if (executorId != SparkContext.DRIVER_IDENTIFIER) {
          val validUpdate = executorMetricsUpdate
            .accumUpdates
            .flatMap(_._4)
            .exists { accumInfo =>
              accumInfo.name == accum.name && accumInfo.update.exists(_.asInstanceOf[Long] >= 1)
            }
          if (validUpdate) {
            executorsActuallyStarted.put(executorId, java.lang.Boolean.TRUE)
          }
        }
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
        3.milliseconds
      } else {
        10.milliseconds
      }
      eventually(timeout(20.seconds), interval(intervalMs)) {
        assert(getCandidateExecutorToDecom.isDefined)
      }
    } else {
      ThreadUtils.awaitResult(asyncCount, 1.minute)
    }

    // Decommission one of the executors.
    val sched = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]

    val execToDecommission = getCandidateExecutorToDecom.get
    logInfo(s"Decommissioning executor ${execToDecommission}")

    // Decommission executor and ensure it is not relaunched by setting adjustTargetNumExecutors
    sched.decommissionExecutor(
      execToDecommission,
      ExecutorDecommissionInfo("", None),
      adjustTargetNumExecutors = true)
    val decomTime = new SystemClock().getTimeMillis()

    // Wait for job to finish.
    val asyncCountResult = ThreadUtils.awaitResult(asyncCount, 1.minute)
    assert(asyncCountResult === numParts)
    // All tasks finished, so accum should have been increased numParts times.
    assert(accum.value === numParts)

    sc.listenerBus.waitUntilEmpty()
    val taskEndEventsCopy = taskEndEvents.asScala
    if (shuffle) {
      //  mappers & reducers which succeeded
      assert(taskEndEventsCopy.count(_.reason == Success) === 2 * numParts,
        s"Expected ${2 * numParts} tasks got ${taskEndEvents.size} (${taskEndEvents})")
    } else {
      // only mappers which executed successfully
      assert(taskEndEventsCopy.count(_.reason == Success) === numParts,
        s"Expected ${numParts} tasks got ${taskEndEvents.size} (${taskEndEvents})")
    }

    val minTaskEndTime = taskEndEventsCopy.map(_.taskInfo.finishTime).min
    val maxTaskEndTime = taskEndEventsCopy.map(_.taskInfo.finishTime).max

    // Verify that the decom time matched our expectations
    val decomAssertMsg = s"$whenToDecom: decomTime: $decomTime, minTaskEnd: $minTaskEndTime," +
      s" maxTaskEnd: $maxTaskEndTime"
    assert(minTaskEndTime <= maxTaskEndTime, decomAssertMsg)
    whenToDecom match {
      case TaskStarted => assert(minTaskEndTime > decomTime, decomAssertMsg)
      case TaskEnded => assert(minTaskEndTime <= decomTime &&
        decomTime < maxTaskEndTime, decomAssertMsg)
      case JobEnded => assert(maxTaskEndTime <= decomTime, decomAssertMsg)
    }

    // Wait for our respective blocks to have migrated
    eventually(timeout(1.minute), interval(10.milliseconds)) {
      if (persist) {
        // One of our blocks should have moved.
        val rddUpdates = blocksUpdated.filter { update =>
          val blockId = update.blockUpdatedInfo.blockId
          blockId.isRDD}
        val blockLocs = rddUpdates.map { update =>
          (update.blockUpdatedInfo.blockId.name,
            update.blockUpdatedInfo.blockManagerId)}
        val blocksToManagers = blockLocs.groupBy(_._1).mapValues(_.size)
        assert(blocksToManagers.exists(_._2 > 1),
          s"We should have a block that has been on multiple BMs in rdds:\n ${rddUpdates} from:\n" +
          s"${blocksUpdated}\n but instead we got:\n ${blocksToManagers}")
      }
      // If we're migrating shuffles we look for any shuffle block updates
      // as there is no block update on the initial shuffle block write.
      if (shuffle) {
        val numDataLocs = blocksUpdated.count { update =>
          val blockId = update.blockUpdatedInfo.blockId
          blockId.isInstanceOf[ShuffleDataBlockId]
        }
        val numIndexLocs = blocksUpdated.count { update =>
          val blockId = update.blockUpdatedInfo.blockId
          blockId.isInstanceOf[ShuffleIndexBlockId]
        }
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
    assert(
      !execIdToBlocksMapping.contains(execToDecommission) ||
      execIdToBlocksMapping(execToDecommission).keys.filter(_.isRDD).toSeq === Seq(),
      "Cache blocks should be migrated")
    if (persist) {
      // There should still be all the RDD blocks cached
      assert(execIdToBlocksMapping.values.flatMap(_.keys).count(_.isRDD) === numParts)
    }

    // Wait for the executor to be removed automatically after migration.
    // This is set to a high value since github actions is sometimes high latency
    // but I've never seen this go for more than a minute.
    assert(executorRemovedSem.tryAcquire(1, 5L, TimeUnit.MINUTES))

    // Since the RDD is cached or shuffled so further usage of same RDD should use the
    // cached data. Original RDD partitions should not be recomputed i.e. accum
    // should have same value like before
    assert(testRdd.count() === numParts)
    assert(accum.value === numParts)
  }
}
