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

package org.apache.spark.rdd

import java.util.concurrent.Semaphore

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.util.ThreadUtils

class AsyncRDDActionsSuite extends SparkFunSuite with BeforeAndAfterAll with TimeLimits {

  @transient private var sc: SparkContext = _

  // Necessary to make ScalaTest 3.x interrupt a thread on the JVM like ScalaTest 2.2.x
  implicit val defaultSignaler: Signaler = ThreadSignaler

  override def beforeAll() {
    super.beforeAll()
    sc = new SparkContext("local[2]", "test")
  }

  override def afterAll() {
    try {
      LocalSparkContext.stop(sc)
      sc = null
    } finally {
      super.afterAll()
    }
  }

  lazy val zeroPartRdd = new EmptyRDD[Int](sc)

  test("countAsync") {
    assert(zeroPartRdd.countAsync().get() === 0)
    assert(sc.parallelize(1 to 10000, 5).countAsync().get() === 10000)
  }

  test("collectAsync") {
    assert(zeroPartRdd.collectAsync().get() === Seq.empty)

    val collected = sc.parallelize(1 to 1000, 3).collectAsync().get()
    assert(collected === (1 to 1000))
  }

  test("foreachAsync") {
    zeroPartRdd.foreachAsync(i => Unit).get()

    val accum = sc.longAccumulator
    sc.parallelize(1 to 1000, 3).foreachAsync { i =>
      accum.add(1)
    }.get()
    assert(accum.value === 1000)
  }

  test("foreachPartitionAsync") {
    zeroPartRdd.foreachPartitionAsync(iter => Unit).get()

    val accum = sc.longAccumulator
    sc.parallelize(1 to 1000, 9).foreachPartitionAsync { iter =>
      accum.add(1)
    }.get()
    assert(accum.value === 9)
  }

  test("takeAsync") {
    def testTake(rdd: RDD[Int], input: Seq[Int], num: Int) {
      val expected = input.take(num)
      val saw = rdd.takeAsync(num).get()
      assert(saw == expected, "incorrect result for rdd with %d partitions (expected %s, saw %s)"
        .format(rdd.partitions.size, expected, saw))
    }
    val input = Range(1, 1000)

    var rdd = sc.parallelize(input, 1)
    for (num <- Seq(0, 1, 999, 1000)) {
      testTake(rdd, input, num)
    }

    rdd = sc.parallelize(input, 2)
    for (num <- Seq(0, 1, 3, 500, 501, 999, 1000)) {
      testTake(rdd, input, num)
    }

    rdd = sc.parallelize(input, 100)
    for (num <- Seq(0, 1, 500, 501, 999, 1000)) {
      testTake(rdd, input, num)
    }

    rdd = sc.parallelize(input, 1000)
    for (num <- Seq(0, 1, 3, 999, 1000)) {
      testTake(rdd, input, num)
    }
  }

  /**
   * Make sure onComplete, onSuccess, and onFailure are invoked correctly in the case
   * of a successful job execution.
   */
  test("async success handling") {
    val f = sc.parallelize(1 to 10, 2).countAsync()

    // Use a semaphore to make sure onSuccess and onComplete's success path will be called.
    // If not, the test will hang.
    val sem = new Semaphore(0)

    f.onComplete {
      case scala.util.Success(res) =>
        sem.release()
      case scala.util.Failure(e) =>
        info("Should not have reached this code path (onComplete matching Failure)")
        throw new Exception("Task should succeed")
    }
    f.foreach { a =>
      sem.release()
    }
    f.failed.foreach { t =>
      info("Should not have reached this code path (onFailure)")
      throw new Exception("Task should succeed")
    }
    assert(f.get() === 10)

    failAfter(10.seconds) {
      sem.acquire(2)
    }
  }

  /**
   * Make sure onComplete, onSuccess, and onFailure are invoked correctly in the case
   * of a failed job execution.
   */
  test("async failure handling") {
    val f = sc.parallelize(1 to 10, 2).map { i =>
      throw new Exception("intentional"); i
    }.countAsync()

    // Use a semaphore to make sure onFailure and onComplete's failure path will be called.
    // If not, the test will hang.
    val sem = new Semaphore(0)

    f.onComplete {
      case scala.util.Success(res) =>
        info("Should not have reached this code path (onComplete matching Success)")
        throw new Exception("Task should fail")
      case scala.util.Failure(e) =>
        sem.release()
    }
    f.foreach { a =>
      info("Should not have reached this code path (onSuccess)")
      throw new Exception("Task should fail")
    }
    f.failed.foreach { t =>
      sem.release()
    }
    intercept[SparkException] {
      f.get()
    }

    failAfter(10.seconds) {
      sem.acquire(2)
    }
  }

  /**
   * Awaiting FutureAction results
   */
  test("FutureAction result, infinite wait") {
    val f = sc.parallelize(1 to 100, 4)
              .countAsync()
    assert(ThreadUtils.awaitResult(f, Duration.Inf) === 100)
  }

  test("FutureAction result, finite wait") {
    val f = sc.parallelize(1 to 100, 4)
              .countAsync()
    assert(ThreadUtils.awaitResult(f, Duration(30, "seconds")) === 100)
  }

  test("FutureAction result, timeout") {
    val f = sc.parallelize(1 to 100, 4)
              .mapPartitions(itr => { Thread.sleep(20); itr })
              .countAsync()
    intercept[TimeoutException] {
      ThreadUtils.awaitResult(f, Duration(20, "milliseconds"))
    }
  }

  private def testAsyncAction[R](action: RDD[Int] => FutureAction[R]): Unit = {
    val executionContextInvoked = Promise[Unit]
    val fakeExecutionContext = new ExecutionContext {
      override def execute(runnable: Runnable): Unit = {
        executionContextInvoked.success(())
      }
      override def reportFailure(t: Throwable): Unit = ()
    }
    val starter = Smuggle(new Semaphore(0))
    starter.drainPermits()
    val rdd = sc.parallelize(1 to 100, 4).mapPartitions {itr => starter.acquire(1); itr}
    val f = action(rdd)
    f.onComplete(_ => ())(fakeExecutionContext)
    // Here we verify that registering the callback didn't cause a thread to be consumed.
    assert(!executionContextInvoked.isCompleted)
    // Now allow the executors to proceed with task processing.
    starter.release(rdd.partitions.length)
    // Waiting for the result verifies that the tasks were successfully processed.
    ThreadUtils.awaitResult(executionContextInvoked.future, atMost = 15.seconds)
  }

  test("SimpleFutureAction callback must not consume a thread while waiting") {
    testAsyncAction(_.countAsync())
  }

  test("ComplexFutureAction callback must not consume a thread while waiting") {
    testAsyncAction((_.takeAsync(100)))
  }
}
