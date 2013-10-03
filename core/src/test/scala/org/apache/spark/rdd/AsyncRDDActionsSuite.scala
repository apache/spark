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

import scala.concurrent.ExecutionContext.Implicits.global

import org.scalatest.FunSuite

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkException, SharedSparkContext}


class AsyncRDDActionsSuite extends FunSuite with SharedSparkContext {

  lazy val zeroPartRdd = new EmptyRDD[Int](sc)

  test("countAsync") {
    assert(sc.parallelize(1 to 10000, 5).countAsync().get() === 10000)
  }

  test("countAsync zero partition") {
    assert(zeroPartRdd.countAsync().get() === 0)
  }

  test("collectAsync") {
    assert(sc.parallelize(1 to 1000, 3).collectAsync().get() === (1 to 1000))
  }

  test("collectAsync zero partition") {
    assert(zeroPartRdd.collectAsync().get() === Seq.empty)
  }

  test("foreachAsync") {
    AsyncRDDActionsSuite.foreachCounter = 0
    sc.parallelize(1 to 1000, 3).foreachAsync { i =>
      AsyncRDDActionsSuite.foreachCounter += 1
    }.get()
    assert(AsyncRDDActionsSuite.foreachCounter === 1000)
  }

  test("foreachAsync zero partition") {
    zeroPartRdd.foreachAsync(i => Unit).get()
  }

  test("foreachPartitionAsync") {
    AsyncRDDActionsSuite.foreachPartitionCounter = 0
    sc.parallelize(1 to 1000, 9).foreachPartitionAsync { iter =>
      AsyncRDDActionsSuite.foreachPartitionCounter += 1
    }.get()
    assert(AsyncRDDActionsSuite.foreachPartitionCounter === 9)
  }

  test("foreachPartitionAsync zero partition") {
    zeroPartRdd.foreachPartitionAsync(iter => Unit).get()
  }

  /**
   * Make sure onComplete, onSuccess, and onFailure are invoked correctly in the case
   * of a successful job execution.
   */
  test("async success handling") {
    val f = sc.parallelize(1 to 10, 2).countAsync()

    // This semaphore is used to make sure our final assert waits until onComplete / onSuccess
    // finishes execution.
    val sem = new Semaphore(0)

    AsyncRDDActionsSuite.asyncSuccessHappened = new AtomicInteger
    f.onComplete {
      case scala.util.Success(res) =>
        AsyncRDDActionsSuite.asyncSuccessHappened.incrementAndGet()
        sem.release()
      case scala.util.Failure(e) =>
        throw new Exception("Task should succeed")
        sem.release()
    }
    f.onSuccess { case a: Any =>
      AsyncRDDActionsSuite.asyncSuccessHappened.incrementAndGet()
      sem.release()
    }
    f.onFailure { case t =>
      throw new Exception("Task should succeed")
    }
    assert(f.get() === 10)
    sem.acquire(2)
    assert(AsyncRDDActionsSuite.asyncSuccessHappened.get() === 2)
  }

  /**
   * Make sure onComplete, onSuccess, and onFailure are invoked correctly in the case
   * of a failed job execution.
   */
  test("async failure handling") {
    val f = sc.parallelize(1 to 10, 2).map { i =>
      throw new Exception("intentional"); i
    }.countAsync()

    // This semaphore is used to make sure our final assert waits until onComplete / onFailure
    // finishes execution.
    val sem = new Semaphore(0)

    AsyncRDDActionsSuite.asyncFailureHappend = new AtomicInteger
    f.onComplete {
      case scala.util.Success(res) =>
        throw new Exception("Task should fail")
        sem.release()
      case scala.util.Failure(e) =>
        AsyncRDDActionsSuite.asyncFailureHappend.incrementAndGet()
        sem.release()
    }
    f.onSuccess { case a: Any =>
      throw new Exception("Task should fail")
    }
    f.onFailure { case t =>
      AsyncRDDActionsSuite.asyncFailureHappend.incrementAndGet()
      sem.release()
    }
    intercept[SparkException] {
      f.get()
    }
    sem.acquire(2)
    assert(AsyncRDDActionsSuite.asyncFailureHappend.get() === 2)
  }
}

object AsyncRDDActionsSuite {
  // Some counters used in the test cases above.
  var foreachCounter = 0

  var foreachPartitionCounter = 0

  var asyncSuccessHappened: AtomicInteger = _

  var asyncFailureHappend: AtomicInteger = _
}

