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

package org.apache.spark.storage

import org.mockito.ArgumentMatchers.{eq => meq}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.memory.MemoryMode.ON_HEAP
import org.apache.spark.storage.memory.{MemoryStore, PartiallyUnrolledIterator}

class PartiallyUnrolledIteratorSuite extends SparkFunSuite with MockitoSugar {
  test("join two iterators") {
    val unrollSize = 1000
    val unroll = (0 until unrollSize).iterator
    val restSize = 500
    val rest = (unrollSize until restSize + unrollSize).iterator

    val memoryStore = mock[MemoryStore]
    val joinIterator = new PartiallyUnrolledIterator(memoryStore, ON_HEAP, unrollSize, unroll, rest)

    // Firstly iterate over unrolling memory iterator
    (0 until unrollSize).foreach { value =>
      assert(joinIterator.hasNext)
      assert(joinIterator.hasNext)
      assert(joinIterator.next() == value)
    }

    joinIterator.hasNext
    joinIterator.hasNext
    verify(memoryStore, times(1))
      .releaseUnrollMemoryForThisTask(meq(ON_HEAP), meq(unrollSize.toLong))

    // Secondly, iterate over rest iterator
    (unrollSize until unrollSize + restSize).foreach { value =>
      assert(joinIterator.hasNext)
      assert(joinIterator.hasNext)
      assert(joinIterator.next() == value)
    }

    joinIterator.close()
    // MemoryMode.releaseUnrollMemoryForThisTask is called only once
    verifyNoMoreInteractions(memoryStore)
  }
}
