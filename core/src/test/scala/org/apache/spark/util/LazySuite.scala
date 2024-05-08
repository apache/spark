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

import org.apache.spark.SparkFunSuite

class LazySuite extends SparkFunSuite{
  test("Lazy should initialize only once") {
    var count = 0
    val lazyVal = Lazy {
      count += 1
      count
    }
    assert(count == 0)
    assert(lazyVal.get == 1)
    assert(count == 1)
    assert(lazyVal.get == 1)
    assert(count == 1)
  }

  test("Lazy should re-throw exceptions") {
    val lazyVal = Lazy {
      throw new RuntimeException("test")
    }
    intercept[RuntimeException] {
      lazyVal.get
    }
    intercept[RuntimeException] {
      lazyVal.get
    }
  }

  test("Lazy should re-throw exceptions with current caller stack-trace") {
    val fileName = Thread.currentThread().getStackTrace()(1).getFileName
    val lineNo = Thread.currentThread().getStackTrace()(1).getLineNumber
    val lazyVal = Lazy {
      throw new RuntimeException("test")
    }

    val e1 = intercept[RuntimeException] {
      lazyVal.get // lineNo + 6
    }
    assert(e1.getStackTrace
      .exists(elem => elem.getFileName == fileName && elem.getLineNumber == lineNo + 6))

    val e2 = intercept[RuntimeException] {
      lazyVal.get // lineNo + 12
    }
    assert(e2.getStackTrace
      .exists(elem => elem.getFileName == fileName && elem.getLineNumber == lineNo + 12))
  }

  test("Lazy does not lock containing object") {
    class LazyContainer() {
      @volatile var aSet = 0

      val a: Lazy[Int] = Lazy {
        aSet = 1
        aSet
      }

      val b: Lazy[Int] = Lazy {
        val t = new Thread(new Runnable {
          override def run(): Unit = {
            assert(a.get == 1)
          }
        })
        t.start()
        t.join()
        aSet
      }
    }
    val container = new LazyContainer()
    // Nothing is lazy initialized yet
    assert(container.aSet == 0)
    // This will not deadlock, thread t will initialize a, and update aSet
    assert(container.b.get == 1)
    assert(container.aSet == 1)
  }

  // Scala lazy val tests are added to test for potential changes in the semantics of scala lazy val

  test("Scala lazy val initializing multiple times on error") {
    class LazyValError() {
      var counter = 0
      lazy val a = {
        counter += 1
        throw new RuntimeException("test")
      }
    }
    val lazyValError = new LazyValError()
    intercept[RuntimeException] {
      lazyValError.a
    }
    assert(lazyValError.counter == 1)
    intercept[RuntimeException] {
      lazyValError.a
    }
    assert(lazyValError.counter == 2)
  }

  test("Scala lazy val locking containing object and deadlocking") {
    // Note: this will change in scala 3, with different lazy vals not deadlocking with each other.
    // https://docs.scala-lang.org/scala3/reference/changed-features/lazy-vals-init.html
    class LazyValContainer() {
      @volatile var aSet = 0
      @volatile var t: Thread = _

      lazy val a = {
        aSet = 1
        aSet
      }

      lazy val b = {
        t = new Thread(new Runnable {
          override def run(): Unit = {
            assert(a == 1)
          }
        })
        t.start()
        t.join(1000)
        aSet
      }
    }
    val container = new LazyValContainer()
    // Nothing is lazy initialized yet
    assert(container.aSet == 0)
    // This will deadlock, because b will take monitor on LazyValContainer, and then thread t
    // will wait on that monitor, not able to initialize a.
    // b will therefore see aSet == 0.
    assert(container.b == 0)
    // However, after b finishes initializing, the monitor will be relased, and then thread t
    // will finish initializing a, and set aSet to 1.
    container.t.join()
    assert(container.aSet == 1)
  }
}
