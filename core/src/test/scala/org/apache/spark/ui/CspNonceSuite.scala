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

package org.apache.spark.ui

import java.util.concurrent.{CyclicBarrier, Executors}
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.SparkFunSuite

class CspNonceSuite extends SparkFunSuite {

  override def afterEach(): Unit = {
    try {
      CspNonce.clear()
    } finally {
      super.afterEach()
    }
  }

  test("generate returns a non-empty string and get returns the same value") {
    val nonce = CspNonce.generate()
    assert(nonce != null && nonce.nonEmpty)
    assert(CspNonce.get === nonce)
  }

  test("generate produces different values on each call") {
    val nonce1 = CspNonce.generate()
    val nonce2 = CspNonce.generate()
    assert(nonce1 !== nonce2)
    // get returns the latest value
    assert(CspNonce.get === nonce2)
  }

  test("clear removes the nonce") {
    CspNonce.generate()
    assert(CspNonce.get != null)
    CspNonce.clear()
    assert(CspNonce.get === null)
  }

  test("nonce is thread-local (isolated between threads)") {
    val barrier = new CyclicBarrier(2)
    val nonceFromThread = new AtomicReference[String]()

    CspNonce.generate()
    val mainNonce = CspNonce.get

    val executor = Executors.newSingleThreadExecutor()
    try {
      executor.submit(new Runnable {
        override def run(): Unit = {
          // Before generate, should be null (no nonce set in this thread)
          assert(CspNonce.get === null)
          val threadNonce = CspNonce.generate()
          nonceFromThread.set(threadNonce)
          barrier.await()
        }
      })
      barrier.await()

      // Main thread's nonce should be unchanged
      assert(CspNonce.get === mainNonce)
      // Other thread's nonce should be different
      assert(nonceFromThread.get() !== mainNonce)
    } finally {
      executor.shutdown()
    }
  }
}
