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

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
// scalastyle:off executioncontextglobal
import scala.concurrent.ExecutionContext.Implicits.global
// scalastyle:on executioncontextglobal
import scala.concurrent.Promise
import scala.concurrent.duration._

import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.must.Matchers

import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Deploy._
import org.apache.spark.scheduler.{JobFailed, SparkListener, SparkListenerExecutorRemoved, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerTaskEnd, SparkListenerTaskStart}
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
    assert(sc.parallelize(1 to 10, 2).count() === 10)
  }

  test("local mode, fair scheduler") {
    val conf = new SparkConf().set(SCHEDULER_MODE, "FAIR")
    val xmlPath = getClass.getClassLoader.getResource("fairscheduler.xml").getFile()
    conf.set(SCHEDULER_ALLOCATION_FILE, xmlPath)
    sc = new SparkContext("local[2]", "test", conf)
    testCount()
    testTake()
    // Make sure we can still launch tasks.
    assert(sc.parallelize(1 to 10, 2).count() === 10)
  }

  test("cluster mode, FIFO scheduler") {
    val conf = new SparkConf().set(SCHEDULER_MODE, "FIFO")
    sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)
    testCount()
    testTake()
    // Make sure we can still launch tasks.
    assert(sc.parallelize(1 to 10, 2).count() === 10)
  }

  test("cluster mode, fair scheduler") {
    val conf = new SparkConf().set(SCHEDULER_MODE, "FAIR")
    val xmlPath = getClass.getClassLoader.getResource("fairscheduler.xml").getFile()
    conf.set(SCHEDULER_ALLOCATION_FILE, xmlPath)
    sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)
    testCount()
    testTake()
    // Make sure we can still launch tasks.
    assert(sc.parallelize(1 to 10, 2).count() === 10)
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

  test("job group with custom reason") {
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

    val reason = "cancelled: test for custom reason string"
    sc.clearJobGroup()
    sc.cancelJobGroup("jobA", reason)

    val e = intercept[SparkException] { ThreadUtils.awaitResult(jobA, Duration.Inf) }.getCause
    assert(e.getMessage contains "cancel")
    assert(e.getMessage contains reason)
  }

  test("if cancel job group and future jobs, skip running jobs in the same job group") {
    sc = new SparkContext("local[2]", "test")

    val sem = new Semaphore(0)
    sc.addSparkListener(new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        sem.release()
      }
    })

    // run a job, cancel the job group and its future jobs
    val jobGroupName = "job-group"
    val job = Future {
      sc.setJobGroup(jobGroupName, "")
      sc.parallelize(1 to 1000).map { i => Thread.sleep (100); i}.count()
    }
    // block until job starts
    sem.acquire(1)
    // cancel the job group and future jobs
    sc.cancelJobGroupAndFutureJobs(jobGroupName)
    ThreadUtils.awaitReady(job, Duration.Inf).failed.foreach { case e: SparkException =>
      checkError(
        exception = e,
        errorClass = "SPARK_JOB_CANCELLED",
        sqlState = "XXKDA",
        parameters = scala.collection.immutable.Map(
          "jobId" -> "0",
          "reason" -> s"part of cancelled job group $jobGroupName")
      )
    }

    // job in the same job group will not run
    checkError(
      exception = intercept[SparkException] {
        sc.setJobGroup(jobGroupName, "")
        sc.parallelize(1 to 100).count()
      },
      errorClass = "SPARK_JOB_CANCELLED",
      sqlState = "XXKDA",
      parameters = scala.collection.immutable.Map(
        "jobId" -> "1",
        "reason" -> s"part of cancelled job group $jobGroupName")
    )

    // job in a different job group should run
    sc.setJobGroup("another-job-group", "")
    assert(sc.parallelize(1 to 100).count() == 100)
  }

  test("cancel job group and future jobs with custom reason") {
    sc = new SparkContext("local[2]", "test")

    val sem = new Semaphore(0)
    sc.addSparkListener(new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        sem.release()
      }
    })

    // run a job, cancel the job group and its future jobs
    val jobGroupName = "job-group"
    val job = Future {
      sc.setJobGroup(jobGroupName, "")
      sc.parallelize(1 to 1000).map { i => Thread.sleep (100); i}.count()
    }
    // block until job starts
    sem.acquire(1)
    // cancel the job group and future jobs
    val reason = "cancelled: test for custom reason string"
    sc.cancelJobGroupAndFutureJobs(jobGroupName, reason)
    ThreadUtils.awaitReady(job, Duration.Inf).failed.foreach { case e: SparkException =>
      checkError(
        exception = e,
        errorClass = "SPARK_JOB_CANCELLED",
        sqlState = "XXKDA",
        parameters = scala.collection.immutable.Map(
          "jobId" -> "0",
          "reason" -> reason)
      )
    }
  }

  test("only keeps limited number of cancelled job groups") {
    val conf = new SparkConf()
      .set(NUM_CANCELLED_JOB_GROUPS_TO_TRACK, 5)
    sc = new SparkContext("local[2]", "test", conf)
    val setSize = sc.getConf.get(NUM_CANCELLED_JOB_GROUPS_TO_TRACK)
    // call cancelJobGroup with cancelFutureJobs = true on (setSize + 1) job groups, the first one
    // should have been evicted from the cancelledJobGroups set
    (0 to setSize).foreach { idx =>
      val sem = new Semaphore(0)
      sc.addSparkListener(new SparkListener {
        override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
          sem.release()
        }
      })
      val job = Future {
        sc.setJobGroup(s"job-group-$idx", "")
        sc.parallelize(1 to 1000).map { i => Thread.sleep (100); i}.count()
      }
      sem.acquire(1)
      sc.cancelJobGroupAndFutureJobs(s"job-group-$idx")
      ThreadUtils.awaitReady(job, Duration.Inf).failed.foreach { case e: SparkException =>
        assert(e.getErrorClass == "SPARK_JOB_CANCELLED")
      }
    }
    // submit a job with the 0 job group that was evicted from cancelledJobGroups set, it should run
    sc.setJobGroup("job-group-0", "")
    assert(sc.parallelize(1 to 100).count() == 100)
  }

  test("job tags") {
    sc = new SparkContext("local[2]", "test")

    // global ExecutionContext has only 2 threads in Apache Spark CI
    // create own thread pool for four Futures used in this test
    val numThreads = 4
    val fpool = ThreadUtils.newForkJoinPool("job-tags-test-thread-pool", numThreads)
    val executionContext = ExecutionContext.fromExecutorService(fpool)

    try {
      // Add a listener to release the semaphore once jobs are launched.
      val sem = new Semaphore(0)
      val jobEnded = new AtomicInteger(0)

      sc.addSparkListener(new SparkListener {
        override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
          sem.release()
        }

        override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
          sem.release()
          jobEnded.incrementAndGet()
        }
      })

      val eSep = intercept[IllegalArgumentException](sc.addJobTag("foo,bar"))
      assert(eSep.getMessage.contains(
        s"Spark job tag cannot contain '${SparkContext.SPARK_JOB_TAGS_SEP}'."))
      val eEmpty = intercept[IllegalArgumentException](sc.addJobTag(""))
      assert(eEmpty.getMessage.contains("Spark job tag cannot be an empty string."))
      val eNull = intercept[IllegalArgumentException](sc.addJobTag(null))
      assert(eNull.getMessage.contains("Spark job tag cannot be null."))

      // Note: since tags are added in the Future threads, they don't need to be cleared in between.
      val jobA = Future {
        assert(sc.getJobTags() == Set())
        sc.addJobTag("two")
        assert(sc.getJobTags() == Set("two"))
        sc.clearJobTags() // check that clearing all tags works
        assert(sc.getJobTags() == Set())
        sc.addJobTag("one")
        assert(sc.getJobTags() == Set("one"))
        try {
          sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(100); i }.count()
        } finally {
          sc.clearJobTags() // clear for the case of thread reuse by another Future
        }
      }(executionContext)
      val jobB = Future {
        assert(sc.getJobTags() == Set())
        sc.addJobTag("one")
        sc.addJobTag("two")
        sc.addJobTag("one")
        sc.addJobTag("two") // duplicates shouldn't matter
        assert(sc.getJobTags() == Set("one", "two"))
        try {
          sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(100); i }.count()
        } finally {
          sc.clearJobTags() // clear for the case of thread reuse by another Future
        }
      }(executionContext)
      val jobC = Future {
        sc.addJobTag("foo")
        sc.removeJobTag("foo")
        assert(sc.getJobTags() == Set()) // check that remove works removing the last tag
        sc.addJobTag("two")
        assert(sc.getJobTags() == Set("two"))
        try {
          sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(100); i }.count()
        } finally {
          sc.clearJobTags() // clear for the case of thread reuse by another Future
        }
      }(executionContext)
      val jobD = Future {
        assert(sc.getJobTags() == Set())
        sc.addJobTag("one")
        sc.addJobTag("two")
        sc.addJobTag("two")
        assert(sc.getJobTags() == Set("one", "two"))
        sc.removeJobTag("two") // check that remove works, despite duplicate add
        assert(sc.getJobTags() == Set("one"))
        try {
          sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(100); i }.count()
        } finally {
          sc.clearJobTags() // clear for the case of thread reuse by another Future
        }
      }(executionContext)

      // Block until four jobs have started.
      val acquired1 = sem.tryAcquire(4, 1, TimeUnit.MINUTES)
      assert(acquired1 == true)

      // Test custom cancellation reason for job tags
      val reason = "job tag cancelled: custom reason test"
      sc.cancelJobsWithTag("two", reason)
      val eB = intercept[SparkException] {
        ThreadUtils.awaitResult(jobB, 1.minute)
      }.getCause
      assert(eB.getMessage contains "cancel")
      assert(eB.getMessage contains reason)

      val eC = intercept[SparkException] {
        ThreadUtils.awaitResult(jobC, 1.minute)
      }.getCause
      assert(eC.getMessage contains "cancel")

      // two jobs cancelled
      val acquired2 = sem.tryAcquire(2, 1, TimeUnit.MINUTES)
      assert(acquired2 == true)
      assert(jobEnded.intValue == 2)

      // this cancels the remaining two jobs
      sc.cancelJobsWithTag("one")
      val eA = intercept[SparkException] {
        ThreadUtils.awaitResult(jobA, 1.minute)
      }.getCause
      assert(eA.getMessage contains "cancel")
      val eD = intercept[SparkException] {
        ThreadUtils.awaitResult(jobD, 1.minute)
      }.getCause
      assert(eD.getMessage contains "cancel")

      // another two jobs cancelled
      val acquired3 = sem.tryAcquire(2, 1, TimeUnit.MINUTES)
      assert(acquired3 == true)
      assert(jobEnded.intValue == 4)
    } finally {
      fpool.shutdownNow()
    }
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

    // Add a listener to release a semaphore once any tasks are launched, and another semaphore
    // once an executor is removed.
    val sem = new Semaphore(0)
    val semExec = new Semaphore(0)
    val execLossReason = new ArrayBuffer[String]()
    sc.addSparkListener(new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        sem.release()
      }

      override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
        execLossReason += executorRemoved.reason
        semExec.release()
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
    semExec.acquire(2)
    val expectedReason = s"Command exited with code ${ExecutorExitCode.KILLED_BY_TASK_REAPER}"
    assert(execLossReason == Seq(expectedReason, expectedReason))

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

  test("cancel FutureAction with custom reason") {

    val cancellationPromise = Promise[Unit]()

    // listener to capture job end events and their reasons
    var failureReason: Option[String] = None

    sc = new SparkContext("local[2]", "test")
    sc.addSparkListener(new SparkListener {
      override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        jobEnd.jobResult match {
          case jobFailed: JobFailed =>
            failureReason = Some(jobFailed.exception.getMessage)
          case _ => // do nothing
        }
      }
    })

    val rdd = sc.parallelize(1 to 100, 2).map(_ * 2)
    val asyncAction = rdd.collectAsync()
    val reason = "custom cancel reason"

    Future {
      asyncAction.cancel(Option(reason))
      cancellationPromise.success(())
    }

    // wait for the cancellation to complete and check the reason
    cancellationPromise.future.map { _ =>
      Thread.sleep(1000)
      assert(failureReason.contains(reason))
    }
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
        // release taskCancelledSemaphore when killAllTaskAttempts event has been posted
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
