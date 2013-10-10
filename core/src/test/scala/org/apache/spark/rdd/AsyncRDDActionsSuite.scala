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
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await
import scala.concurrent.future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkContext, SparkException, LocalSparkContext}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskStart}
import org.apache.spark.scheduler._


class AsyncRDDActionsSuite extends FunSuite with BeforeAndAfterAll {

  @transient private var sc: SparkContext = _

  override def beforeAll() {
    sc = new SparkContext("local-cluster[2,1,512]", "test")
  }

  override def afterAll() {
    LocalSparkContext.stop(sc)
    sc = null
  }

  lazy val zeroPartRdd = new EmptyRDD[Int](sc)

  test("job cancellation before any tasks is launched") {
    val f = sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.countAsync()
    future { f.cancel() }
    val e = intercept[SparkException] { f.get() }
    assert(e.getMessage.contains("cancelled") || e.getMessage.contains("killed"))
  }

  test("job cancellation after some tasks have been launched") {
    // Add a listener to release the semaphore once any tasks are launched.
    val sem = new Semaphore(0)
    sc.dagScheduler.addSparkListener(new SparkListener {
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

  test("cancelling take action") {
    val f = sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.takeAsync(5000)
    future { f.cancel() }
    val e = intercept[SparkException] { f.get() }
    assert(e.getMessage.contains("cancelled") || e.getMessage.contains("killed"))
  }

//
//  test("countAsync") {
//    assert(zeroPartRdd.countAsync().get() === 0)
//    assert(sc.parallelize(1 to 10000, 5).countAsync().get() === 10000)
//  }
//
//  test("collectAsync") {
//    assert(zeroPartRdd.collectAsync().get() === Seq.empty)
//
//    // Note that we sort the collected output because the order is indeterministic.
//    val collected = sc.parallelize(1 to 1000, 3).collectAsync().get().sorted
//    assert(collected === (1 to 1000))
//  }
//
//  test("foreachAsync") {
//    zeroPartRdd.foreachAsync(i => Unit).get()
//
//    val accum = sc.accumulator(0)
//    sc.parallelize(1 to 1000, 3).foreachAsync { i =>
//      accum += 1
//    }.get()
//    assert(accum.value === 1000)
//  }
//
//  test("foreachPartitionAsync") {
//    zeroPartRdd.foreachPartitionAsync(iter => Unit).get()
//
//    val accum = sc.accumulator(0)
//    sc.parallelize(1 to 1000, 9).foreachPartitionAsync { iter =>
//      accum += 1
//    }.get()
//    assert(accum.value === 9)
//  }
//
//  test("takeAsync") {
//    def testTake(rdd: RDD[Int], input: Seq[Int], num: Int) {
//      // Note that we sort the collected output because the order is indeterministic.
//      assert(rdd.takeAsync(num).get().size === input.take(num).size)
//    }
//    val input = Range(1, 1000)
//
//    var nums = sc.parallelize(input, 1)
//    for (num <- Seq(0, 1, 3, 500, 501, 999, 1000)) {
//      testTake(nums, input, num)
//    }
//
//    nums = sc.parallelize(input, 2)
//    for (num <- Seq(0, 1, 3, 500, 501, 999, 1000)) {
//      testTake(nums, input, num)
//    }
//
//    nums = sc.parallelize(input, 100)
//    for (num <- Seq(0, 1, 3, 500, 501, 999, 1000)) {
//      testTake(nums, input, num)
//    }
//
//    nums = sc.parallelize(input, 1000)
//    for (num <- Seq(0, 1, 3, 500, 501, 999, 1000)) {
//      testTake(nums, input, num)
//    }
//  }
//
//  /**
//   * Make sure onComplete, onSuccess, and onFailure are invoked correctly in the case
//   * of a successful job execution.
//   */
//  test("async success handling") {
//    val f = sc.parallelize(1 to 10, 2).countAsync()
//
//    // This semaphore is used to make sure our final assert waits until onComplete / onSuccess
//    // finishes execution.
//    val sem = new Semaphore(0)
//
//    AsyncRDDActionsSuite.asyncSuccessHappened.set(0)
//    f.onComplete {
//      case scala.util.Success(res) =>
//        AsyncRDDActionsSuite.asyncSuccessHappened.incrementAndGet()
//        sem.release()
//      case scala.util.Failure(e) =>
//        throw new Exception("Task should succeed")
//        sem.release()
//    }
//    f.onSuccess { case a: Any =>
//      AsyncRDDActionsSuite.asyncSuccessHappened.incrementAndGet()
//      sem.release()
//    }
//    f.onFailure { case t =>
//      throw new Exception("Task should succeed")
//    }
//    assert(f.get() === 10)
//    sem.acquire(2)
//    assert(AsyncRDDActionsSuite.asyncSuccessHappened.get() === 2)
//  }
//
//  /**
//   * Make sure onComplete, onSuccess, and onFailure are invoked correctly in the case
//   * of a failed job execution.
//   */
//  test("async failure handling") {
//    val f = sc.parallelize(1 to 10, 2).map { i =>
//      throw new Exception("intentional"); i
//    }.countAsync()
//
//    // This semaphore is used to make sure our final assert waits until onComplete / onFailure
//    // finishes execution.
//    val sem = new Semaphore(0)
//
//    AsyncRDDActionsSuite.asyncFailureHappend.set(0)
//    f.onComplete {
//      case scala.util.Success(res) =>
//        throw new Exception("Task should fail")
//        sem.release()
//      case scala.util.Failure(e) =>
//        AsyncRDDActionsSuite.asyncFailureHappend.incrementAndGet()
//        sem.release()
//    }
//    f.onSuccess { case a: Any =>
//      throw new Exception("Task should fail")
//    }
//    f.onFailure { case t =>
//      AsyncRDDActionsSuite.asyncFailureHappend.incrementAndGet()
//      sem.release()
//    }
//    intercept[SparkException] {
//      f.get()
//    }
//    sem.acquire(2)
//    assert(AsyncRDDActionsSuite.asyncFailureHappend.get() === 2)
//  }
}

object AsyncRDDActionsSuite {
  // Some counters used in the test cases above.
  var asyncSuccessHappened = new AtomicInteger

  var asyncFailureHappend = new AtomicInteger
}

