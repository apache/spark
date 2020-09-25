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

package org.apache.spark

import java.util.concurrent.{Semaphore, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.must.Matchers

import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Deploy._
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd, SparkListenerTaskStart}
import org.apache.spark.util.ThreadUtils

/**
 * Test suite for cancelling running jobs. We run the cancellation tasks for single job action
 * (e.g. count) as well as multi-job action (e.g. take). We test the local and cluster schedulers
 * in both FIFO and fair scheduling modes.
 */
class JobCancellationSuite extends SparkFunSuite with Matchers with BeforeAndAfter
  with LocalSparkContext {

  override def afterEach(): Unit = {
    try {
      resetSparkContext()
      JobCancellationSuite.taskStartedSemaphore.drainPermits()
      JobCancellationSuite.taskCancelledSemaphore.drainPermits()
      JobCancellationSuite.twoJobsSharingStageSemaphore.drainPermits()
      JobCancellationSuite.executionOfInterruptibleCounter.set(0)
    } finally {
      super.afterEach()
    }
  }

  test("local mode, FIFO scheduler") {
    val conf = new SparkConf().set(SCHEDULER_MODE, "FIFO")
    sc = new SparkContext("local[2]", "test", conf)
    testCount()
    testTake()
    // Make sure we can still launch tasks.
    assert(sc.parallelize(1 to 10, 2).count === 10)
  }

  test("local mode, fair scheduler") {
    val conf = new SparkConf().set(SCHEDULER_MODE, "FAIR")
    val xmlPath = getClass.getClassLoader.getResource("fairscheduler.xml").getFile()
    conf.set(SCHEDULER_ALLOCATION_FILE, xmlPath)
    sc = new SparkContext("local[2]", "test", conf)
    testCount()
    testTake()
    // Make sure we can still launch tasks.
    assert(sc.parallelize(1 to 10, 2).count === 10)
  }

  test("cluster mode, FIFO scheduler") {
    val conf = new SparkConf().set(SCHEDULER_MODE, "FIFO")
    sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)
    testCount()
    testTake()
    // Make sure we can still launch tasks.
    assert(sc.parallelize(1 to 10, 2).count === 10)
  }

  test("cluster mode, fair scheduler") {
    val conf = new SparkConf().set(SCHEDULER_MODE, "FAIR")
    val xmlPath = getClass.getClassLoader.getResource("fairscheduler.xml").getFile()
    conf.set(SCHEDULER_ALLOCATION_FILE, xmlPath)
    sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)
    testCount()
    testTake()
    // Make sure we can still launch tasks.
    assert(sc.parallelize(1 to 10, 2).count === 10)
  }

  test("do not put partially executed partitions into cache") {
    // In this test case, we create a scenario in which a partition is only partially executed,
    // and make sure CacheManager does not put that partially executed partition into the
    // BlockManager.
    import JobCancellationSuite._
    sc = new SparkContext("local", "test")

    // Run from 1 to 10, and then block and wait for the task to be killed.
    val rdd = sc.parallelize(1 to 1000, 2).map { x =>
      if (x > 10) {
        taskStartedSemaphore.release()
        taskCancelledSemaphore.acquire()
      }
      x
    }.cache()

    val rdd1 = rdd.map(x => x)

    Future {
      taskStartedSemaphore.acquire()
      sc.cancelAllJobs()
      taskCancelledSemaphore.release(100000)
    }

    intercept[SparkException] { rdd1.count() }
    // If the partial block is put into cache, rdd.count() would return a number less than 1000.
    assert(rdd.count() === 1000)
  }

  test("job group") {
    sc = new SparkContext("local[2]", "test")

    // Add a listener to release the semaphore once any tasks are launched.
    val sem = new Semaphore(0)
    sc.addSparkListener(new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        sem.release()
      }
    })

    // jobA is the one to be cancelled.
    val jobA = Future {
      sc.setJobGroup("jobA", "this is a job to be cancelled")
      sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.count()
    }

    // Block until both tasks of job A have started and cancel job A.
    sem.acquire(2)

    sc.clearJobGroup()
    val jobB = sc.parallelize(1 to 100, 2).countAsync()
    sc.cancelJobGroup("jobA")
    val e = intercept[SparkException] { ThreadUtils.awaitResult(jobA, Duration.Inf) }.getCause
    assert(e.getMessage contains "cancel")

    // Once A is cancelled, job B should finish fairly quickly.
    assert(jobB.get() === 100)
  }

  test("inherited job group (SPARK-6629)") {
    sc = new SparkContext("local[2]", "test")

    // Add a listener to release the semaphore once any tasks are launched.
    val sem = new Semaphore(0)
    sc.addSparkListener(new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        sem.release()
      }
    })

    sc.setJobGroup("jobA", "this is a job to be cancelled")
    @volatile var exception: Exception = null
    val jobA = new Thread() {
      // The job group should be inherited by this thread
      override def run(): Unit = {
        exception = intercept[SparkException] {
          sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.count()
        }
      }
    }
    jobA.start()

    // Block until both tasks of job A have started and cancel job A.
    sem.acquire(2)
    sc.cancelJobGroup("jobA")
    jobA.join(10000)
    assert(!jobA.isAlive)
    assert(exception.getMessage contains "cancel")

    // Once A is cancelled, job B should finish fairly quickly.
    val jobB = sc.parallelize(1 to 100, 2).countAsync()
    assert(jobB.get() === 100)
  }

  test("job group with interruption") {
    sc = new SparkContext("local[2]", "test")

    // Add a listener to release the semaphore once any tasks are launched.
    val sem = new Semaphore(0)
    sc.addSparkListener(new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        sem.release()
      }
    })

    // jobA is the one to be cancelled.
    val jobA = Future {
      sc.setJobGroup("jobA", "this is a job to be cancelled", interruptOnCancel = true)
      sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(100000); i }.count()
    }

    // Block until both tasks of job A have started and cancel job A.
    sem.acquire(2)

    sc.clearJobGroup()
    val jobB = sc.parallelize(1 to 100, 2).countAsync()
    sc.cancelJobGroup("jobA")
    val e = intercept[SparkException] { ThreadUtils.awaitResult(jobA, 5.seconds) }.getCause
    assert(e.getMessage contains "cancel")

    // Once A is cancelled, job B should finish fairly quickly.
    assert(jobB.get() === 100)
  }

  test("task reaper kills JVM if killed tasks keep running for too long") {
    val conf = new SparkConf()
      .set(TASK_REAPER_ENABLED, true)
      .set(TASK_REAPER_KILL_TIMEOUT.key, "5s")
    sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)

    // Add a listener to release the semaphore once any tasks are launched.
    val sem = new Semaphore(0)
    sc.addSparkListener(new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        sem.release()
      }
    })

    // jobA is the one to be cancelled.
    val jobA = Future {
      sc.setJobGroup("jobA", "this is a job to be cancelled", interruptOnCancel = true)
      sc.parallelize(1 to 10000, 2).map { i =>
        while (true) { }
      }.count()
    }

    // Block until both tasks of job A have started and cancel job A.
    sem.acquire(2)
    // Small delay to ensure tasks actually start executing the task body
    Thread.sleep(1000)

    sc.clearJobGroup()
    val jobB = sc.parallelize(1 to 100, 2).countAsync()
    sc.cancelJobGroup("jobA")
    val e = intercept[SparkException] { ThreadUtils.awaitResult(jobA, 15.seconds) }.getCause
    assert(e.getMessage contains "cancel")

    // Once A is cancelled, job B should finish fairly quickly.
    assert(ThreadUtils.awaitResult(jobB, 1.minute) === 100)
  }

  test("task reaper will not kill JVM if spark.task.killTimeout == -1") {
    val conf = new SparkConf()
      .set(TASK_REAPER_ENABLED, true)
      .set(TASK_REAPER_KILL_TIMEOUT.key, "-1")
      .set(TASK_REAPER_POLLING_INTERVAL.key, "1s")
      .set(MAX_EXECUTOR_RETRIES, 1)
    sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)

    // Add a listener to release the semaphore once any tasks are launched.
    val sem = new Semaphore(0)
    sc.addSparkListener(new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        sem.release()
      }
    })

    // jobA is the one to be cancelled.
    val jobA = Future {
      sc.setJobGroup("jobA", "this is a job to be cancelled", interruptOnCancel = true)
      sc.parallelize(1 to 2, 2).map { i =>
        val startTimeNs = System.nanoTime()
        while (System.nanoTime() < startTimeNs + TimeUnit.SECONDS.toNanos(10)) { }
      }.count()
    }

    // Block until both tasks of job A have started and cancel job A.
    sem.acquire(2)
    // Small delay to ensure tasks actually start executing the task body
    Thread.sleep(1000)

    sc.clearJobGroup()
    val jobB = sc.parallelize(1 to 100, 2).countAsync()
    sc.cancelJobGroup("jobA")
    val e = intercept[SparkException] { ThreadUtils.awaitResult(jobA, 15.seconds) }.getCause
    assert(e.getMessage contains "cancel")

    // Once A is cancelled, job B should finish fairly quickly.
    assert(ThreadUtils.awaitResult(jobB, 1.minute) === 100)
  }

  test("two jobs sharing the same stage") {
    // sem1: make sure cancel is issued after some tasks are launched
    // twoJobsSharingStageSemaphore:
    //   make sure the first stage is not finished until cancel is issued
    val sem1 = new Semaphore(0)

    sc = new SparkContext("local[2]", "test")
    sc.addSparkListener(new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        sem1.release()
      }
    })

    // Create two actions that would share the some stages.
    val rdd = sc.parallelize(1 to 10, 2).map { i =>
      JobCancellationSuite.twoJobsSharingStageSemaphore.acquire()
      (i, i)
    }.reduceByKey(_ + _)
    val f1 = rdd.collectAsync()
    val f2 = rdd.countAsync()

    // Kill one of the action.
    Future {
      sem1.acquire()
      f1.cancel()
      JobCancellationSuite.twoJobsSharingStageSemaphore.release(10)
    }

    // Expect f1 to fail due to cancellation,
    intercept[SparkException] { f1.get() }
    // but f2 should not be affected
    f2.get()
  }

  test("interruptible iterator of shuffle reader") {
    // In this test case, we create a Spark job of two stages. The second stage is cancelled during
    // execution and a counter is used to make sure that the corresponding tasks are indeed
    // cancelled.
    import JobCancellationSuite._
    sc = new SparkContext("local[2]", "test interruptible iterator")

    // Increase the number of elements to be proceeded to avoid this test being flaky.
    val numElements = 10000
    val taskCompletedSem = new Semaphore(0)

    sc.addSparkListener(new SparkListener {
      override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
        // release taskCancelledSemaphore when cancelTasks event has been posted
        if (stageCompleted.stageInfo.stageId == 1) {
          taskCancelledSemaphore.release(numElements)
        }
      }

      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        if (taskEnd.stageId == 1) { // make sure tasks are completed
          taskCompletedSem.release()
        }
      }
    })

    // Explicitly disable interrupt task thread on cancelling tasks, so the task thread can only be
    // interrupted by `InterruptibleIterator`.
    sc.setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false")

    val f = sc.parallelize(1 to numElements).map { i => (i, i) }
      .repartitionAndSortWithinPartitions(new HashPartitioner(1))
      .mapPartitions { iter =>
        taskStartedSemaphore.release()
        iter
      }.foreachAsync { x =>
        // Block this code from being executed, until the job get cancelled. In this case, if the
        // source iterator is interruptible, the max number of increment should be under
        // `numElements`. We sleep a little to make sure that we leave enough time for the
        // "kill" message to be delivered to the executor (10000 * 10ms = 100s allowance for
        // delivery, which should be more than enough).
        Thread.sleep(10)
        taskCancelledSemaphore.acquire()
        executionOfInterruptibleCounter.getAndIncrement()
    }

    taskStartedSemaphore.acquire()
    // Job is cancelled when:
    // 1. task in reduce stage has been started, guaranteed by previous line.
    // 2. task in reduce stage is blocked as taskCancelledSemaphore is not released until
    //    JobCancelled event is posted.
    // After job being cancelled, task in reduce stage will be cancelled asynchronously, thus
    // partial of the inputs should not get processed (It's very unlikely that Spark can process
    // 10000 elements between JobCancelled is posted and task is really killed).
    f.cancel()

    val e = intercept[SparkException](f.get()).getCause
    assert(e.getMessage.contains("cancelled") || e.getMessage.contains("killed"))

    // Make sure tasks are indeed completed.
    taskCompletedSem.acquire()
    assert(executionOfInterruptibleCounter.get() < numElements)
 }

  def testCount(): Unit = {
    // Cancel before launching any tasks
    {
      val f = sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.countAsync()
      Future { f.cancel() }
      val e = intercept[SparkException] { f.get() }.getCause
      assert(e.getMessage.contains("cancelled") || e.getMessage.contains("killed"))
    }

    // Cancel after some tasks have been launched
    {
      // Add a listener to release the semaphore once any tasks are launched.
      val sem = new Semaphore(0)
      sc.addSparkListener(new SparkListener {
        override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
          sem.release()
        }
      })

      val f = sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.countAsync()
      Future {
        // Wait until some tasks were launched before we cancel the job.
        sem.acquire()
        f.cancel()
      }
      val e = intercept[SparkException] { f.get() }.getCause
      assert(e.getMessage.contains("cancelled") || e.getMessage.contains("killed"))
    }
  }

  def testTake(): Unit = {
    // Cancel before launching any tasks
    {
      val f = sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.takeAsync(5000)
      Future { f.cancel() }
      val e = intercept[SparkException] { f.get() }.getCause
      assert(e.getMessage.contains("cancelled") || e.getMessage.contains("killed"))
    }

    // Cancel after some tasks have been launched
    {
      // Add a listener to release the semaphore once any tasks are launched.
      val sem = new Semaphore(0)
      sc.addSparkListener(new SparkListener {
        override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
          sem.release()
        }
      })
      val f = sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.takeAsync(5000)
      Future {
        sem.acquire()
        f.cancel()
      }
      val e = intercept[SparkException] { f.get() }.getCause
      assert(e.getMessage.contains("cancelled") || e.getMessage.contains("killed"))
    }
  }
}


object JobCancellationSuite {
  // To avoid any headaches, reset these global variables in the companion class's afterEach block
  val taskStartedSemaphore = new Semaphore(0)
  val taskCancelledSemaphore = new Semaphore(0)
  val twoJobsSharingStageSemaphore = new Semaphore(0)
  val executionOfInterruptibleCounter = new AtomicInteger(0)
}
