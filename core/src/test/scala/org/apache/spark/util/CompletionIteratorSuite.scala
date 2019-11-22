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

import java.lang.ref.PhantomReference
import java.lang.ref.ReferenceQueue

import org.apache.spark.SparkFunSuite

class CompletionIteratorSuite extends SparkFunSuite {
  test("basic test") {
    var numTimesCompleted = 0
    val iter = List(1, 2, 3).iterator
    val completionIter = CompletionIterator[Int, Iterator[Int]](iter, { numTimesCompleted += 1 })

    assert(completionIter.hasNext)
    assert(completionIter.next() === 1)
    assert(numTimesCompleted === 0)

    assert(completionIter.hasNext)
    assert(completionIter.next() === 2)
    assert(numTimesCompleted === 0)

    assert(completionIter.hasNext)
    assert(completionIter.next() === 3)
    assert(numTimesCompleted === 0)

    assert(!completionIter.hasNext)
    assert(numTimesCompleted === 1)

    // SPARK-4264: Calling hasNext should not trigger the completion callback again.
    assert(!completionIter.hasNext)
    assert(numTimesCompleted === 1)
  }
  test("reference to sub iterator should not be available after completion") {
    var sub = Iterator(1, 2, 3)

    val refQueue = new ReferenceQueue[Iterator[Int]]
    val ref = new PhantomReference[Iterator[Int]](sub, refQueue)

    val iter = CompletionIterator[Int, Iterator[Int]](sub, {})
    sub = null
    iter.toArray

    for (_ <- 1 to 100 if !ref.isEnqueued) {
      System.gc()
      if (!ref.isEnqueued) {
        Thread.sleep(10)
      }
    }
    assert(ref.isEnqueued)
    assert(refQueue.poll() === ref)
  }
}
