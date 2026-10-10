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

import java.io.{IOException, NotSerializableException, ObjectInputStream}
import java.util.Collections
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import org.scalatest.concurrent.Eventually

import org.apache.spark.internal.config.UNSAFE_EXCEPTION_ON_MEMORY_LEAK
import org.apache.spark.memory.{SparkOutOfMemoryError, TestMemoryConsumer}
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{NonSerializable, ThreadUtils, Utils}

// Common state shared by FailureSuite-launched tasks. We use a global object
// for this because any local variables used in the task closures will rightfully
// be copied for each task, so there's no other way for them to share state.
object FailureSuiteState {
  var tasksRun = 0
  var tasksFailed = 0
  @volatile var oomBlockerStarted = new CountDownLatch(1)
  @volatile var secondOom = new CountDownLatch(1)
  @volatile var oomRetryStarted = new CountDownLatch(1)
  @volatile var ordinaryTaskStarted = new CountDownLatch(1)
  @volatile var releaseOomBlocker = new CountDownLatch(1)

  def clear(): Unit = {
    synchronized {
      tasksRun = 0
      tasksFailed = 0
      oomBlockerStarted = new CountDownLatch(1)
      secondOom = new CountDownLatch(1)
      oomRetryStarted = new CountDownLatch(1)
      ordinaryTaskStarted = new CountDownLatch(1)
      releaseOomBlocker = new CountDownLatch(1)
    }
  }
}

class FailureSuite extends SparkFunSuite with LocalSparkContext with Eventually {

  // Run a 3-task map job in which task 1 deterministically fails once, and check
  // whether the job completes successfully and we ran 4 tasks in total.
  test("failure in a single-stage job") {
    sc = new SparkContext("local[1,2]", "test")
    val results = sc.makeRDD(1 to 3, 3).map { x =>
      FailureSuiteState.synchronized {
        FailureSuiteState.tasksRun += 1
        if (x == 1 && FailureSuiteState.tasksFailed == 0) {
          FailureSuiteState.tasksFailed += 1
          throw new Exception("Intentional task failure")
        }
      }
      x * x
    }.collect()
    FailureSuiteState.synchronized {
      assert(FailureSuiteState.tasksRun === 4)
    }
    assert(results.toList === List(1, 4, 9))
    FailureSuiteState.clear()
  }

  // Run a map-reduce job in which a reduce task deterministically fails once.
  test("failure in a two-stage job") {
    sc = new SparkContext("local[1,2]", "test")
    val results = sc.makeRDD(1 to 3).map(x => (x, x)).groupByKey(3).map {
      case (k, v) =>
        FailureSuiteState.synchronized {
          FailureSuiteState.tasksRun += 1
          if (k == 1 && FailureSuiteState.tasksFailed == 0) {
            FailureSuiteState.tasksFailed += 1
            throw new Exception("Intentional task failure")
          }
        }
        (k, v.head * v.head)
      }.collect()
    FailureSuiteState.synchronized {
      assert(FailureSuiteState.tasksRun === 4)
    }
    assert(results.toSet === Set((1, 1), (2, 4), (3, 9)))
    FailureSuiteState.clear()
  }

  // Run a map-reduce job in which the map stage always fails.
  test("failure in a map stage") {
    sc = new SparkContext("local", "test")
    val data = sc.makeRDD(1 to 3).map(x => { throw new Exception; (x, x) }).groupByKey(3)
    intercept[SparkException] {
      data.collect()
    }
    // Make sure that running new jobs with the same map stage also fails
    intercept[SparkException] {
      data.collect()
    }
  }

  test("failure because task results are not serializable") {
    sc = new SparkContext("local[1,1]", "test")
    val results = sc.makeRDD(1 to 3).map(x => new NonSerializable)

    val thrown = intercept[SparkException] {
      results.collect()
    }
    assert(thrown.getClass === classOf[SparkException])
    assert(thrown.getMessage.contains("serializable") ||
      thrown.getCause.getClass === classOf[NotSerializableException],
      "Exception does not contain \"serializable\": " + thrown.getMessage)

    FailureSuiteState.clear()
  }

  test("failure because task closure is not serializable") {
    sc = new SparkContext("local[1,1]", "test")
    val a = new NonSerializable

    // Non-serializable closure in the final result stage
    val thrown = intercept[SparkException] {
      sc.parallelize(1 to 10, 2).map(x => a).count()
    }
    assert(thrown.getClass === classOf[SparkException])
    assert(thrown.getMessage.contains("NotSerializableException") ||
      thrown.getCause.getClass === classOf[NotSerializableException])

    // Non-serializable closure in an earlier stage
    val thrown1 = intercept[SparkException] {
      sc.parallelize(1 to 10, 2).map(x => (x, a)).partitionBy(new HashPartitioner(3)).count()
    }
    assert(thrown1.getClass === classOf[SparkException])
    assert(thrown1.getMessage.contains("NotSerializableException") ||
      thrown1.getCause.getClass === classOf[NotSerializableException])

    // Non-serializable closure in foreach function
    val thrown2 = intercept[SparkException] {
      // scalastyle:off println
      sc.parallelize(1 to 10, 2).foreach(x => println(a))
      // scalastyle:on println
    }
    assert(thrown2.getClass === classOf[SparkException])
    assert(thrown2.getMessage.contains("NotSerializableException") ||
      thrown2.getCause.getClass === classOf[NotSerializableException])

    FailureSuiteState.clear()
  }

  test("managed memory leak error should not mask other failures (SPARK-9266") {
    val conf = new SparkConf().set(UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
    sc = new SparkContext("local[1,1]", "test", conf)

    // If a task leaks memory but fails due to some other cause, then make sure that the original
    // cause is preserved
    val thrownDueToTaskFailure = intercept[SparkException] {
      sc.parallelize(Seq(0)).mapPartitions { iter =>
        val c = new TestMemoryConsumer(TaskContext.get().taskMemoryManager())
        TaskContext.get().taskMemoryManager().allocatePage(128, c)
        throw new Exception("intentional task failure")
        iter
      }.count()
    }
    assert(thrownDueToTaskFailure.getMessage.contains("intentional task failure"))

    // If the task succeeded but memory was leaked, then the task should fail due to that leak
    val thrownDueToMemoryLeak = intercept[SparkException] {
      sc.parallelize(Seq(0)).mapPartitions { iter =>
        val c = new TestMemoryConsumer(TaskContext.get().taskMemoryManager())
        TaskContext.get().taskMemoryManager().allocatePage(128, c)
        iter
      }.count()
    }
    assert(thrownDueToMemoryLeak.getMessage.contains("memory leak"))
  }

  // Run a 3-task map job in which task 1 always fails with a exception message that
  // depends on the failure number, and check that we get the last failure.
  test("last failure cause is sent back to driver") {
    sc = new SparkContext("local[1,2]", "test")
    val data = sc.makeRDD(1 to 3, 3).map { x =>
      FailureSuiteState.synchronized {
        FailureSuiteState.tasksRun += 1
        if (x == 3) {
          FailureSuiteState.tasksFailed += 1
          throw new UserException("oops",
            new IllegalArgumentException("failed=" + FailureSuiteState.tasksFailed))
        }
      }
      x * x
    }
    val thrown = intercept[SparkException] {
      data.collect()
    }
    FailureSuiteState.synchronized {
      assert(FailureSuiteState.tasksRun === 4)
    }
    assert(thrown.getClass === classOf[SparkException])
    assert(thrown.getCause.getClass === classOf[UserException])
    assert(thrown.getCause.getMessage === "oops")
    assert(thrown.getCause.getCause.getClass === classOf[IllegalArgumentException])
    assert(thrown.getCause.getCause.getMessage === "failed=2")
    FailureSuiteState.clear()
  }

  test("failure cause stacktrace is sent back to driver if exception is not serializable") {
    sc = new SparkContext("local", "test")
    val thrown = intercept[SparkException] {
      sc.makeRDD(1 to 3).foreach { _ => throw new NonSerializableUserException }
    }
    assert(thrown.getClass === classOf[SparkException])
    assert(thrown.getCause === null)
    assert(thrown.getMessage.contains("NonSerializableUserException"))
    FailureSuiteState.clear()
  }

  test("failure cause stacktrace is sent back to driver if exception is not deserializable") {
    sc = new SparkContext("local", "test")
    val thrown = intercept[SparkException] {
      sc.makeRDD(1 to 3).foreach { _ => throw new NonDeserializableUserException }
    }
    assert(thrown.getClass === classOf[SparkException])
    assert(thrown.getCause === null)
    assert(thrown.getMessage.contains("NonDeserializableUserException"))
    FailureSuiteState.clear()
  }

  test("OOM retries preserve task CPUs and failure limits through the local backend") {
    sc = new SparkContext(new SparkConf()
      .setMaster("local[2,4]")
      .setAppName("OOM retry isolation")
      .set("spark.scheduler.oomRetry.enabled", "true"))
    val attempts = sc.parallelize(Seq(0), 1).mapPartitions { _ =>
      val context = TaskContext.get()
      if (context.attemptNumber() < 2) {
        // scalastyle:off throwerror
        throw new SparkOutOfMemoryError("_LEGACY_ERROR_USER_RAISED_EXCEPTION",
          Collections.singletonMap("errorMessage", "execution memory"))
        // scalastyle:on throwerror
      }
      Iterator((context.attemptNumber(), context.cpus()))
    }.collect()
    assert(attempts.toSeq == Seq((2, 1)))

    val failure = intercept[SparkException] {
      sc.parallelize(Seq(0), 1).foreach { _ =>
        // scalastyle:off throwerror
        throw new SparkOutOfMemoryError("_LEGACY_ERROR_USER_RAISED_EXCEPTION",
          Collections.singletonMap("errorMessage", "persistent execution memory failure"))
        // scalastyle:on throwerror
      }
    }
    assert(failure.getMessage.contains("failed 4 times"))
  }

  test("OOM isolation timeout wakes the local backend while another task is running") {
    FailureSuiteState.clear()
    sc = new SparkContext(new SparkConf()
      .setMaster("local[2,4]")
      .setAppName("OOM retry isolation timeout")
      .set("spark.scheduler.oomRetry.enabled", "true")
      .set("spark.scheduler.oomRetry.isolationTimeout", "1s"))
    val pool = ThreadUtils.newDaemonSingleThreadExecutor("oom-retry-timeout-test")
    val executionContext = ExecutionContext.fromExecutorService(pool)
    val context = sc
    val job = Future {
      context.parallelize(Seq(0, 1), 2).mapPartitionsWithIndex { (index, _) =>
        val attempt = TaskContext.get().attemptNumber()
        if (index == 0) {
          FailureSuiteState.oomBlockerStarted.countDown()
          require(FailureSuiteState.releaseOomBlocker.await(60, TimeUnit.SECONDS),
            "The retry did not release the blocker")
        } else {
          require(FailureSuiteState.oomBlockerStarted.await(30, TimeUnit.SECONDS),
            "The blocker did not start")
          if (attempt < 2) {
            if (attempt == 1) {
              FailureSuiteState.secondOom.countDown()
            }
            // scalastyle:off throwerror
            throw new SparkOutOfMemoryError("_LEGACY_ERROR_USER_RAISED_EXCEPTION",
              Collections.singletonMap("errorMessage", "execution memory"))
            // scalastyle:on throwerror
          }
          FailureSuiteState.oomRetryStarted.countDown()
          FailureSuiteState.releaseOomBlocker.countDown()
        }
        Iterator((index, attempt))
      }.collect()
    }(executionContext)
    try {
      assert(FailureSuiteState.secondOom.await(30, TimeUnit.SECONDS),
        "The task did not fail twice with OOM")
      // The blocker stays running, so only the deadline can make the retry runnable again.
      assert(FailureSuiteState.oomRetryStarted.await(10, TimeUnit.SECONDS),
        "The isolation deadline passed without waking the local backend")
      assert(ThreadUtils.awaitResult(job, 30.seconds).sorted.toSeq == Seq((0, 0), (1, 2)))
    } finally {
      FailureSuiteState.releaseOomBlocker.countDown()
      try {
        context.cancelAllJobs()
        ThreadUtils.awaitReady(job, 30.seconds)
      } finally {
        pool.shutdownNow()
        assert(pool.awaitTermination(30, TimeUnit.SECONDS))
        FailureSuiteState.clear()
      }
    }
  }

  test("cancelling a pending OOM reservation wakes queued work through the local backend") {
    FailureSuiteState.clear()
    sc = new SparkContext(new SparkConf()
      .setMaster("local[2,4]")
      .setAppName("OOM retry cancellation")
      .set("spark.scheduler.oomRetry.enabled", "true")
      .set("spark.scheduler.oomRetry.isolationTimeout", "60s"))
    val pool = ThreadUtils.newDaemonFixedThreadPool(3, "oom-retry-cancellation-test")
    val executionContext = ExecutionContext.fromExecutorService(pool)
    val context = sc
    val scheduler = context.taskScheduler.asInstanceOf[TaskSchedulerImpl]
    val jobs = ArrayBuffer.empty[Future[_]]
    try {
      val blocker = Future {
        context.setJobGroup("oom-blocker", "occupy one core", interruptOnCancel = true)
        context.parallelize(Seq(0), 1).map { _ =>
          FailureSuiteState.oomBlockerStarted.countDown()
          require(FailureSuiteState.releaseOomBlocker.await(60, TimeUnit.SECONDS),
            "The queued task did not release the blocker")
          0
        }.collect()
      }(executionContext)
      jobs += blocker
      assert(FailureSuiteState.oomBlockerStarted.await(10, TimeUnit.SECONDS),
        "The blocker did not start")

      val oom = Future {
        context.setJobGroup("oom-retry", "reserve the executor", interruptOnCancel = true)
        context.parallelize(Seq(1), 1).map { _ =>
          val attempt = TaskContext.get().attemptNumber()
          if (attempt < 2) {
            if (attempt == 1) {
              FailureSuiteState.secondOom.countDown()
            }
            // scalastyle:off throwerror
            throw new SparkOutOfMemoryError("_LEGACY_ERROR_USER_RAISED_EXCEPTION",
              Collections.singletonMap("errorMessage", "execution memory"))
            // scalastyle:on throwerror
          }
          FailureSuiteState.oomRetryStarted.countDown()
          1
        }.collect()
      }(executionContext)
      jobs += oom
      assert(FailureSuiteState.secondOom.await(10, TimeUnit.SECONDS),
        "The task did not fail twice with OOM")
      eventually(timeout(10.seconds)) {
        scheduler.synchronized {
          val manager = scheduler.rootPool.getSortedTaskSetQueue.find {
            _.taskSet.properties.getProperty("spark.jobGroup.id") == "oom-retry"
          }.get
          assert(manager.taskAttempts.head.size == 2)
          assert(manager.taskAttempts.head.forall(_.failed))
          assert(manager.runningTasks == 0)
        }
      }

      val ordinary = Future {
        context.setJobGroup("ordinary", "queued ordinary task", interruptOnCancel = true)
        context.parallelize(Seq(2), 1).map { _ =>
          FailureSuiteState.ordinaryTaskStarted.countDown()
          FailureSuiteState.releaseOomBlocker.countDown()
          2
        }.collect()
      }(executionContext)
      jobs += ordinary
      eventually(timeout(10.seconds)) {
        assert(scheduler.synchronized {
          scheduler.rootPool.getSortedTaskSetQueue.exists { manager =>
            manager.taskSet.properties.getProperty("spark.jobGroup.id") == "ordinary" &&
              manager.runningTasks == 0
          }
        })
      }
      assert(!FailureSuiteState.ordinaryTaskStarted.await(200, TimeUnit.MILLISECONDS),
        "The reservation did not block ordinary work on the free core")

      // No attempt of this job is running, so cancellation cannot produce a task completion
      // offer. The queued job must start before either the blocker or isolation deadline expires.
      context.cancelJobGroup("oom-retry")
      ThreadUtils.awaitReady(oom, 10.seconds)
      assert(oom.value.get.isFailure)
      assert(FailureSuiteState.ordinaryTaskStarted.await(10, TimeUnit.SECONDS),
        "Cancelling the reservation did not wake the local backend")
      assert(FailureSuiteState.oomRetryStarted.getCount == 1)
      assert(ThreadUtils.awaitResult(ordinary, 10.seconds).toSeq == Seq(2))
      assert(ThreadUtils.awaitResult(blocker, 10.seconds).toSeq == Seq(0))
    } finally {
      FailureSuiteState.releaseOomBlocker.countDown()
      try {
        context.cancelAllJobs()
        jobs.foreach(job => ThreadUtils.awaitReady(job, 10.seconds))
      } finally {
        pool.shutdownNow()
        assert(pool.awaitTermination(10, TimeUnit.SECONDS))
        FailureSuiteState.clear()
      }
    }
  }

  test("ExceptionFailure identifies typed OOM causes without matching exception text") {
    val errors = Seq(
      new OutOfMemoryError("heap"),
      new SparkOutOfMemoryError("_LEGACY_ERROR_USER_RAISED_EXCEPTION",
        Collections.singletonMap("errorMessage", "execution memory")))
    for (error <- errors; preserveCause <- Seq(true, false)) {
      assert(new ExceptionFailure(error, Nil, preserveCause).isOutOfMemoryError)
      val wrapped = new RuntimeException("spill failed", error)
      assert(new ExceptionFailure(wrapped, Nil, preserveCause).isOutOfMemoryError)
      assert(ExceptionFailure(error.getClass.getName, error.getMessage, error.getStackTrace,
        Utils.exceptionString(error), None).isOutOfMemoryError)
    }
    val ordinary = new RuntimeException("java.lang.OutOfMemoryError: OOMKilled")
    assert(!new ExceptionFailure(ordinary, Nil).isOutOfMemoryError)

    val first = new RuntimeException("first")
    val second = new RuntimeException("second", first)
    first.initCause(second)
    assert(!new ExceptionFailure(first, Nil).isOutOfMemoryError)
  }

  test("ExceptionFailure preserves wrapped OOM classification without a serializable cause") {
    val error = new NonSerializableUserException
    error.initCause(new OutOfMemoryError("heap"))
    intercept[NotSerializableException] {
      Utils.serialize(new ExceptionFailure(error, Nil))
    }
    val fallback = new ExceptionFailure(error, Nil, preserveCause = false)
    val restored = Utils.deserialize[ExceptionFailure](Utils.serialize(fallback))
    assert(restored.exception.isEmpty)
    assert(restored.isOutOfMemoryError)
  }

  test("ExceptionFailure preserves wrapped OOM classification if its cause cannot deserialize") {
    val error = new NonDeserializableUserException
    error.initCause(new OutOfMemoryError("heap"))
    val restored = Utils.deserialize[ExceptionFailure](
      Utils.serialize(new ExceptionFailure(error, Nil)))
    assert(restored.exception.isEmpty)
    assert(restored.isOutOfMemoryError)
  }

  // Run a 3-task map stage where one task fails once.
  test("failure in tasks in a submitMapStage") {
    sc = new SparkContext("local[1,2]", "test")
    val rdd = sc.makeRDD(1 to 3, 3).map { x =>
      FailureSuiteState.synchronized {
        FailureSuiteState.tasksRun += 1
        if (x == 1 && FailureSuiteState.tasksFailed == 0) {
          FailureSuiteState.tasksFailed += 1
          throw new Exception("Intentional task failure")
        }
      }
      (x, x)
    }
    val dep = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(2))
    sc.submitMapStage(dep).get()
    FailureSuiteState.synchronized {
      assert(FailureSuiteState.tasksRun === 4)
    }
    FailureSuiteState.clear()
  }

  test("failure because cached RDD partitions are missing from DiskStore (SPARK-15736)") {
    sc = new SparkContext("local[1,2]", "test")
    val rdd = sc.parallelize(1 to 2, 2).persist(StorageLevel.DISK_ONLY)
    rdd.count()
    // Directly delete all files from the disk store, triggering failures when reading cached data:
    SparkEnv.get.blockManager.diskBlockManager.getAllFiles().foreach(_.delete())
    // Each task should fail once due to missing cached data, but then should succeed on its second
    // attempt because the missing cache locations will be purged and the blocks will be recomputed.
    rdd.count()
  }

  test("SPARK-16304: Link error should not crash executor") {
    sc = new SparkContext("local[1,2]", "test")
    intercept[SparkException] {
      sc.parallelize(1 to 2).foreach { i =>
        // scalastyle:off throwerror
        throw new LinkageError()
        // scalastyle:on throwerror
      }
    }
  }

  // TODO: Need to add tests with shuffle fetch failures.
}

class UserException(message: String, cause: Throwable)
  extends RuntimeException(message, cause)

class NonSerializableUserException extends RuntimeException {
  val nonSerializableInstanceVariable = new NonSerializable
}

class NonDeserializableUserException extends RuntimeException {
  private def readObject(in: ObjectInputStream): Unit = {
    throw new IOException("Intentional exception during deserialization.")
  }
}
