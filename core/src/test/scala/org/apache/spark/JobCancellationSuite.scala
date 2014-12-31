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

import java.util.concurrent.Semaphore

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.future

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.Matchers

import org.apache.spark.SparkContext._
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskStart}

/**
 * Test suite for cancelling running jobs. We run the cancellation tasks for single job action
 * (e.g. count) as well as multi-job action (e.g. take). We test the local and cluster schedulers
 * in both FIFO and fair scheduling modes.
 */
class JobCancellationSuite extends FunSuite with Matchers with BeforeAndAfter
  with LocalSparkContext {

  override def afterEach() {
    super.afterEach()
    resetSparkContext()
  }

  test("local mode, FIFO scheduler") {
    val conf = new SparkConf().set("spark.scheduler.mode", "FIFO")
    sc = new SparkContext("local[2]", "test", conf)
    testCount()
    testTake()
    // Make sure we can still launch tasks.
    assert(sc.parallelize(1 to 10, 2).count === 10)
  }

  test("local mode, fair scheduler") {
    val conf = new SparkConf().set("spark.scheduler.mode", "FAIR")
    val xmlPath = getClass.getClassLoader.getResource("fairscheduler.xml").getFile()
    conf.set("spark.scheduler.allocation.file", xmlPath)
    sc = new SparkContext("local[2]", "test", conf)
    testCount()
    testTake()
    // Make sure we can still launch tasks.
    assert(sc.parallelize(1 to 10, 2).count === 10)
  }

  test("cluster mode, FIFO scheduler") {
    val conf = new SparkConf().set("spark.scheduler.mode", "FIFO")
    sc = new SparkContext("local-cluster[2,1,512]", "test", conf)
    testCount()
    testTake()
    // Make sure we can still launch tasks.
    assert(sc.parallelize(1 to 10, 2).count === 10)
  }

  test("cluster mode, fair scheduler") {
    val conf = new SparkConf().set("spark.scheduler.mode", "FAIR")
    val xmlPath = getClass.getClassLoader.getResource("fairscheduler.xml").getFile()
    conf.set("spark.scheduler.allocation.file", xmlPath)
    sc = new SparkContext("local-cluster[2,1,512]", "test", conf)
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

    future {
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
      override def onTaskStart(taskStart: SparkListenerTaskStart) {
        sem.release()
      }
    })

    // jobA is the one to be cancelled.
    val jobA = future {
      sc.setJobGroup("jobA", "this is a job to be cancelled")
      sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.count()
    }

    // Block until both tasks of job A have started and cancel job A.
    sem.acquire(2)

    sc.clearJobGroup()
    val jobB = sc.parallelize(1 to 100, 2).countAsync()
    sc.cancelJobGroup("jobA")
    val e = intercept[SparkException] { Await.result(jobA, Duration.Inf) }
    assert(e.getMessage contains "cancel")

    // Once A is cancelled, job B should finish fairly quickly.
    assert(jobB.get() === 100)
  }

  test("job group with interruption") {
    sc = new SparkContext("local[2]", "test")

    // Add a listener to release the semaphore once any tasks are launched.
    val sem = new Semaphore(0)
    sc.addSparkListener(new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart) {
        sem.release()
      }
    })

    // jobA is the one to be cancelled.
    val jobA = future {
      sc.setJobGroup("jobA", "this is a job to be cancelled", interruptOnCancel = true)
      sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(100000); i }.count()
    }

    // Block until both tasks of job A have started and cancel job A.
    sem.acquire(2)

    sc.clearJobGroup()
    val jobB = sc.parallelize(1 to 100, 2).countAsync()
    sc.cancelJobGroup("jobA")
    val e = intercept[SparkException] { Await.result(jobA, 5.seconds) }
    assert(e.getMessage contains "cancel")

    // Once A is cancelled, job B should finish fairly quickly.
    assert(jobB.get() === 100)
  }

  ignore("two jobs sharing the same stage") {
    // sem1: make sure cancel is issued after some tasks are launched
    // sem2: make sure the first stage is not finished until cancel is issued
    val sem1 = new Semaphore(0)
    val sem2 = new Semaphore(0)

    sc = new SparkContext("local[2]", "test")
    sc.addSparkListener(new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart) {
        sem1.release()
      }
    })

    // Create two actions that would share the some stages.
    val rdd = sc.parallelize(1 to 10, 2).map { i =>
      sem2.acquire()
      (i, i)
    }.reduceByKey(_+_)
    val f1 = rdd.collectAsync()
    val f2 = rdd.countAsync()

    // Kill one of the action.
    future {
      sem1.acquire()
      f1.cancel()
      sem2.release(10)
    }

    // Expect both to fail now.
    // TODO: update this test when we change Spark so cancelling f1 wouldn't affect f2.
    intercept[SparkException] { f1.get() }
    intercept[SparkException] { f2.get() }
  }

  def testCount() {
    // Cancel before launching any tasks
    {
      val f = sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.countAsync()
      future { f.cancel() }
      val e = intercept[SparkException] { f.get() }
      assert(e.getMessage.contains("cancelled") || e.getMessage.contains("killed"))
    }

    // Cancel after some tasks have been launched
    {
      // Add a listener to release the semaphore once any tasks are launched.
      val sem = new Semaphore(0)
      sc.addSparkListener(new SparkListener {
        override def onTaskStart(taskStart: SparkListenerTaskStart) {
          sem.release()
        }
      })

      val f = sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.countAsync()
      future {
        // Wait until some tasks were launched before we cancel the job.
        sem.acquire()
        f.cancel()
      }
      val e = intercept[SparkException] { f.get() }
      assert(e.getMessage.contains("cancelled") || e.getMessage.contains("killed"))
    }
  }

  def testTake() {
    // Cancel before launching any tasks
    {
      val f = sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.takeAsync(5000)
      future { f.cancel() }
      val e = intercept[SparkException] { f.get() }
      assert(e.getMessage.contains("cancelled") || e.getMessage.contains("killed"))
    }

    // Cancel after some tasks have been launched
    {
      // Add a listener to release the semaphore once any tasks are launched.
      val sem = new Semaphore(0)
      sc.addSparkListener(new SparkListener {
        override def onTaskStart(taskStart: SparkListenerTaskStart) {
          sem.release()
        }
      })
      val f = sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.takeAsync(5000)
      future {
        sem.acquire()
        f.cancel()
      }
      val e = intercept[SparkException] { f.get() }
      assert(e.getMessage.contains("cancelled") || e.getMessage.contains("killed"))
    }
  }
}


object JobCancellationSuite {
  val taskStartedSemaphore = new Semaphore(0)
  val taskCancelledSemaphore = new Semaphore(0)
}
