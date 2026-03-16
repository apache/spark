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
package org.apache.spark.util

import java.io.NotSerializableException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import org.apache.spark.{SerializerTestUtils, SparkFunSuite}

class BestEffortLazyValSuite extends SparkFunSuite with SerializerTestUtils {

  test("BestEffortLazy works") {
    val numInitializerCalls = new AtomicInteger(0)
    // Simulate a race condition where two threads concurrently
    // initialize the lazy value:
    val latch = new CountDownLatch(2)
    val lazyval = new BestEffortLazyVal(() => {
      numInitializerCalls.incrementAndGet()
      latch.countDown()
      latch.await()
      new Object()
    })

    // Ensure no initialization happened before the lazy value was invoked
    assert(numInitializerCalls.get() === 0)

    // Two threads concurrently invoke the lazy value
    implicit val ec: ExecutionContext = ExecutionContext.global
    val future1 = Future { lazyval() }
    val future2 = Future { lazyval() }
    val value1 = ThreadUtils.awaitResult(future1, 10.seconds)
    val value2 = ThreadUtils.awaitResult(future2, 10.seconds)

    // The initializer should have been invoked twice (due to how we set up the
    // race condition via the latch):
    assert(numInitializerCalls.get() === 2)

    // But the value should only have been computed once:
    assert(value1 eq value2)

    // Ensure the subsequent invocation serves the same object
    assert(lazyval() eq value1)
    assert(numInitializerCalls.get() === 2)
  }

  test("BestEffortLazyVal is serializable") {
    val lazyval = new BestEffortLazyVal(() => "test")

    // serialize and deserialize before first invocation
    val lazyval2 = roundtripSerialize(lazyval)
    assert(lazyval2() === "test")

    // first invocation
    assert(lazyval() === "test")

    // serialize and deserialize after first invocation
    val lazyval3 = roundtripSerialize(lazyval)
    assert(lazyval3() === "test")
  }

  test("BestEffortLazyVal is serializable: unserializable value") {
    val lazyval = new BestEffortLazyVal(() => new Object())

    // serialize and deserialize before first invocation
    val lazyval2 = roundtripSerialize(lazyval)
    assert(lazyval2() != null)

    // first invocation
    assert(lazyval() != null)

    // serialize and deserialize after first invocation
    // try to serialize the cached value and cause NotSerializableException
    val e = intercept[NotSerializableException] {
      val lazyval3 = roundtripSerialize(lazyval)
    }
    assert(e.getMessage.contains("java.lang.Object"))
  }

  test("BestEffortLazyVal is serializable: initialization failure") {
    val lazyval = new BestEffortLazyVal[String](() => throw new RuntimeException("test"))

    // serialize and deserialize before first invocation
    val lazyval2 = roundtripSerialize(lazyval)
    val e2 = intercept[RuntimeException] {
      val v = lazyval2()
    }
    assert(e2.getMessage.contains("test"))

    // initialization failure
    val e = intercept[RuntimeException] {
      val v = lazyval()
    }
    assert(e.getMessage.contains("test"))

    // serialize and deserialize after initialization failure
    val lazyval3 = roundtripSerialize(lazyval)
    val e3 = intercept[RuntimeException] {
      val v = lazyval3()
    }
    assert(e3.getMessage.contains("test"))
  }
}
