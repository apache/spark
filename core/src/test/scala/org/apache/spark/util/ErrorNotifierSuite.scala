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

import java.util.concurrent.CyclicBarrier

import org.apache.spark.SparkFunSuite

class ErrorNotifierSuite extends SparkFunSuite {

  test("no error is recorded by default") {
    val notifier = new ErrorNotifier
    assert(notifier.getError().isEmpty)
    // throwErrorIfExists is a no-op when no error has been recorded.
    notifier.throwErrorIfExists()
  }

  test("markError records the error and throwErrorIfExists re-throws it") {
    val notifier = new ErrorNotifier
    val err = new RuntimeException("boom")
    notifier.markError(err)

    assert(notifier.getError().contains(err))
    val thrown = intercept[RuntimeException] {
      notifier.throwErrorIfExists()
    }
    assert(thrown eq err)
  }

  test("only the first error is retained; later errors are attached as suppressed") {
    val notifier = new ErrorNotifier
    val first = new RuntimeException("first")
    val second = new IllegalStateException("second")
    notifier.markError(first)
    notifier.markError(second)

    assert(notifier.getError().contains(first))
    assert(first.getSuppressed.toSeq.contains(second))
  }

  test("marking the same error twice does not self-suppress") {
    val notifier = new ErrorNotifier
    val err = new RuntimeException("boom")
    notifier.markError(err)
    notifier.markError(err)

    assert(notifier.getError().contains(err))
    assert(err.getSuppressed.isEmpty)
  }

  test("concurrent markError retains exactly one error and suppresses the rest") {
    val notifier = new ErrorNotifier
    val numThreads = 8
    val errors = (0 until numThreads).map(i => new RuntimeException(s"err-$i"))
    val barrier = new CyclicBarrier(numThreads)
    val threads = errors.map { e =>
      new Thread(() => {
        // Maximize contention on the compareAndSet by releasing all threads at once.
        barrier.await()
        notifier.markError(e)
      })
    }
    threads.foreach(_.start())
    threads.foreach(_.join())

    val winner = notifier.getError()
    assert(winner.isDefined)
    assert(errors.contains(winner.get))
    // Every losing error must be attached as suppressed on the winner, so none is lost.
    assert(winner.get.getSuppressed.toSet == errors.filterNot(_ eq winner.get).toSet)
  }
}
