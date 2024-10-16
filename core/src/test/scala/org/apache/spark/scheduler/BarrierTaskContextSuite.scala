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

package org.apache.spark.scheduler

import java.io.File

import scala.collection.mutable
import scala.util.Random

import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.internal.config.LEGACY_LOCALITY_WAIT_RESET
import org.apache.spark.internal.config.Tests.TEST_NO_STAGE_RETRY

class BarrierTaskContextSuite extends SparkFunSuite with LocalSparkContext with Eventually {

  def initLocalClusterSparkContext(numWorker: Int = 4, conf: SparkConf = new SparkConf()): Unit = {
    conf
      // Init local cluster here so each barrier task runs in a separated process, thus `barrier()`
      // call is actually useful.
      .setMaster(s"local-cluster[$numWorker, 1, 1024]")
      .setAppName("test-cluster")
      .set(TEST_NO_STAGE_RETRY, true)
    sc = new SparkContext(conf)
    TestUtils.waitUntilExecutorsUp(sc, numWorker, 60000)
  }

  test("global sync by barrier() call") {
    initLocalClusterSparkContext()
    val rdd = sc.makeRDD(1 to 10, 4)
    val rdd2 = rdd.barrier().mapPartitions { it =>
      val context = BarrierTaskContext.get()
      // Sleep for a random time before global sync.
      Thread.sleep(Random.nextInt(1000))
      context.barrier()
      Seq(System.currentTimeMillis()).iterator
    }

    val times = rdd2.collect()
    // All the tasks shall finish global sync within a short time slot.
    assert(times.max - times.min <= 1000)
  }

  test("share messages with allGather() call") {
    initLocalClusterSparkContext()
    val rdd = sc.makeRDD(1 to 10, 4)
    val rdd2 = rdd.barrier().mapPartitions { it =>
      val context = BarrierTaskContext.get()
      // Sleep for a random time before global sync.
      Thread.sleep(Random.nextInt(1000))
      // Pass partitionId message in
      val message: String = context.partitionId().toString
      val messages: Array[String] = context.allGather(message)
      Iterator.single(messages.toList)
    }
    val messages = rdd2.collect()
    // All the task partitionIds are shared across all tasks
    assert(messages.length === 4)
    assert(messages.forall(_ == List("0", "1", "2", "3")))
  }

  test("throw exception if we attempt to synchronize with different blocking calls") {
    initLocalClusterSparkContext()
    val rdd = sc.makeRDD(1 to 10, 4)
    val rdd2 = rdd.barrier().mapPartitions { it =>
      val context = BarrierTaskContext.get()
      val partitionId = context.partitionId
      if (partitionId == 0) {
        context.barrier()
      } else {
        context.allGather(partitionId.toString)
      }
      Seq(null).iterator
    }
    val error = intercept[SparkException] {
      rdd2.collect()
    }.getMessage
    assert(error.contains("Different barrier sync types found"))
  }

  test("successively sync with allGather and barrier") {
    initLocalClusterSparkContext()
    val rdd = sc.makeRDD(1 to 10, 4)
    val rdd2 = rdd.barrier().mapPartitions { it =>
      val context = BarrierTaskContext.get()
      // Sleep for a random time before global sync.
      Thread.sleep(Random.nextInt(500))
      context.barrier()
      val time1 = System.currentTimeMillis()
      // Sleep for a random time before global sync.
      Thread.sleep(Random.nextInt(1000))
      // Pass partitionId message in
      val message = context.partitionId().toString
      val messages = context.allGather(message)
      val time2 = System.currentTimeMillis()
      Seq((time1, time2)).iterator
    }
    val times = rdd2.collect()
    // All the tasks shall finish the first round of global sync within a short time slot.
    val times1 = times.map(_._1)
    assert(times1.max - times1.min <= 1000)

    // All the tasks shall finish the second round of global sync within a short time slot.
    val times2 = times.map(_._2)
    assert(times2.max - times2.min <= 1000)
  }

  test("support multiple barrier() call within a single task") {
    initLocalClusterSparkContext()
    val rdd = sc.makeRDD(1 to 10, 4)
    val rdd2 = rdd.barrier().mapPartitions { it =>
      val context = BarrierTaskContext.get()
      // Sleep for a random time before global sync.
      Thread.sleep(Random.nextInt(1000))
      context.barrier()
      val time1 = System.currentTimeMillis()
      // Sleep for a random time between two global syncs.
      Thread.sleep(Random.nextInt(1000))
      context.barrier()
      val time2 = System.currentTimeMillis()
      Seq((time1, time2)).iterator
    }

    val times = rdd2.collect()
    // All the tasks shall finish the first round of global sync within a short time slot.
    val times1 = times.map(_._1)
    assert(times1.max - times1.min <= 1000)

    // All the tasks shall finish the second round of global sync within a short time slot.
    val times2 = times.map(_._2)
    assert(times2.max - times2.min <= 1000)
  }

  test("throw exception on barrier() call timeout") {
    initLocalClusterSparkContext()
    sc.conf.set("spark.barrier.sync.timeout", "1")
    val rdd = sc.makeRDD(1 to 10, 4)
    val rdd2 = rdd.barrier().mapPartitions { it =>
      val context = BarrierTaskContext.get()
      // Task 3 shall sleep 2000ms to ensure barrier() call timeout
      if (context.taskAttemptId == 3) {
        Thread.sleep(2000)
      }
      context.barrier()
      it
    }

    val error = intercept[SparkException] {
      rdd2.collect()
    }.getMessage
    assert(error.contains("The coordinator didn't get all barrier sync requests"))
    assert(error.contains("within 1 second(s)"))
  }

  test("throw exception if barrier() call doesn't happen on every task") {
    initLocalClusterSparkContext()
    sc.conf.set("spark.barrier.sync.timeout", "1")
    val rdd = sc.makeRDD(1 to 10, 4)
    val rdd2 = rdd.barrier().mapPartitions { it =>
      val context = BarrierTaskContext.get()
      if (context.taskAttemptId != 0) {
        context.barrier()
      }
      it
    }

    val error = intercept[SparkException] {
      rdd2.collect()
    }.getMessage
    assert(error.contains("The coordinator didn't get all barrier sync requests"))
    assert(error.contains("within 1 second(s)"))
  }

  test("throw exception if the number of barrier() calls are not the same on every task") {
    initLocalClusterSparkContext()
    sc.conf.set("spark.barrier.sync.timeout", "5")
    val rdd = sc.makeRDD(1 to 10, 4)
    val rdd2 = rdd.barrier().mapPartitions { it =>
      val context = BarrierTaskContext.get()
      try {
        if (context.taskAttemptId == 0) {
          // Due to some non-obvious reason, the code can trigger an Exception and skip the
          // following statements within the try ... catch block, including the first barrier()
          // call.
          throw new SparkException("test")
        }
        context.barrier()
      } catch {
        case e: Exception => // Do nothing
      }
      context.barrier()
      it
    }

    val error = intercept[SparkException] {
      rdd2.collect()
    }.getMessage
    assert(error.contains("The coordinator didn't get all barrier sync requests"))
    assert(error.contains("within 5 second(s)"))
  }

  def testBarrierTaskKilled(interruptOnKill: Boolean): Unit = {
    withTempDir { dir =>
      val runningFlagFile = "barrier.task.running"
      val killedFlagFile = "barrier.task.killed"
      val rdd = sc.makeRDD(Seq(0, 1), 2)
      val rdd2 = rdd.barrier().mapPartitions { it =>
        val context = BarrierTaskContext.get()
        if (context.partitionId() == 0) {
          try {
            new File(dir, runningFlagFile).createNewFile()
            context.barrier()
          } catch {
            case _: TaskKilledException =>
              new File(dir, killedFlagFile).createNewFile()
          }
        } else {
          Thread.sleep(5000)
          context.barrier()
        }
        it
      }

      val listener = new SparkListener {
        override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
          val partitionId = taskStart.taskInfo.index
          if (partitionId == 0) {
            new Thread {
              override def run: Unit = {
                eventually(timeout(10.seconds)) {
                  assert(new File(dir, runningFlagFile).exists())
                  sc.killTaskAttempt(taskStart.taskInfo.taskId, interruptThread = interruptOnKill)
                }
              }
            }.start()
          }
        }
      }
      sc.addSparkListener(listener)

      intercept[SparkException] {
        rdd2.collect()
      }

      sc.removeSparkListener(listener)

      assert(new File(dir, killedFlagFile).exists(), "Expect barrier task being killed.")
    }
  }

  test("barrier task killed, no interrupt") {
    initLocalClusterSparkContext()
    testBarrierTaskKilled(interruptOnKill = false)
  }

  test("barrier task killed, interrupt") {
    initLocalClusterSparkContext()
    testBarrierTaskKilled(interruptOnKill = true)
  }

  test("SPARK-24818: disable legacy delay scheduling for barrier stage") {
    val conf = new SparkConf().set(LEGACY_LOCALITY_WAIT_RESET, true)
    initLocalClusterSparkContext(2, conf)
    val taskLocality = new mutable.ArrayBuffer[TaskLocality.TaskLocality]()
    val listener = new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        taskLocality += taskStart.taskInfo.taskLocality
      }
    }

    try {
      sc.addSparkListener(listener)
      val id = sc.getExecutorIds().head
      val rdd0 = sc.parallelize(Seq(0, 1, 2, 3), 2)
      val dep = new OneToOneDependency[Int](rdd0)
      // set up a stage with 2 tasks and both tasks prefer the same executor (only 1 core)
      // for scheduling. So, the first task can always get the best locality (PROCESS_LOCAL),
      // but the second task may not get the best locality depends whether it's a barrier stage
      // or not.
      val rdd = new MyRDD(sc, 2, List(dep), Seq(Seq(s"executor_h_$id"), Seq(s"executor_h_$id"))) {
        override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] = {
          Iterator.single((split.index, split.index + 1))
        }
      }

      // run a barrier stage
      rdd.barrier().mapPartitions { iter =>
        BarrierTaskContext.get().barrier()
        iter
      }.collect()

      // The delay scheduling for barrier TaskSetManager has been disabled. So, the second task
      // would not wait for any time but just launch at ANY locality level.
      assert(taskLocality.sorted === Seq(TaskLocality.PROCESS_LOCAL, TaskLocality.ANY))
      taskLocality.clear()

      // run a common stage
      rdd.mapPartitions { iter =>
        iter
      }.collect()
      // The delay scheduling works for the common stage. So, the second task would be delayed
      // in order to get the better locality.
      assert(taskLocality.sorted === Seq(TaskLocality.PROCESS_LOCAL, TaskLocality.PROCESS_LOCAL))

    } finally {
      taskLocality.clear()
      sc.removeSparkListener(listener)
    }
  }

  test("SPARK-34069: Kill barrier tasks should respect SPARK_JOB_INTERRUPT_ON_CANCEL") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local[2]"))
    var index = 0
    var checkDone = false
    var startTime = 0L
    val listener = new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        if (startTime == 0) {
          startTime = taskStart.taskInfo.launchTime
        }
      }

      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        if (index == 0) {
          assert(taskEnd.reason.isInstanceOf[ExceptionFailure])
          assert(System.currentTimeMillis() - taskEnd.taskInfo.launchTime < 1000)
          index = 1
        } else if (index == 1) {
          assert(taskEnd.reason.isInstanceOf[TaskKilled])
          assert(System.currentTimeMillis() - taskEnd.taskInfo.launchTime < 1000)
          index = 2
          checkDone = true
        }
      }
    }
    sc.addSparkListener(listener)
    sc.setJobGroup("test", "", true)
    sc.parallelize(Seq(1, 2), 2).barrier().mapPartitions { it =>
      if (TaskContext.get().stageAttemptNumber() == 0) {
        if (it.hasNext && it.next() == 1) {
          throw new RuntimeException("failed")
        } else {
          Thread.sleep(5000)
        }
      }
      it
    }.groupBy(x => x).collect()
    sc.listenerBus.waitUntilEmpty()
    assert(checkDone)
    // double check we kill task success
    assert(System.currentTimeMillis() - startTime < 5000)
  }

  test("SPARK-40932, messages of allGather should not been overridden " +
    "by the following barrier APIs") {

    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local[2]"))
    sc.setLogLevel("INFO")
    val rdd = sc.makeRDD(1 to 10, 2)
    val rdd2 = rdd.barrier().mapPartitions { it =>
      val context = BarrierTaskContext.get()
      // Sleep for a random time before global sync.
      Thread.sleep(Random.nextInt(1000))
      // Pass partitionId message in
      val message: String = context.partitionId().toString
      val messages: Array[String] = context.allGather(message)
      context.barrier()
      Iterator.single(messages.toList)
    }
    val messages = rdd2.collect()
    // All the task partitionIds are shared across all tasks
    assert(messages.length === 2)
    assert(messages.forall(_ == List("0", "1")))
  }

}
