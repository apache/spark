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

package org.apache.spark.memory

import java.util.concurrent.atomic.AtomicLong

import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.MemoryStore


private[memory] trait MemoryManagerSuite extends SparkFunSuite {

  // TODO: add paragraph long comments to explain why things here need to be this way
  // TODO: make StaticMemoryManager go through this

  //
  private val DEFAULT_ENSURE_FREE_SPACE_CALLED = -1L
  private val ensureFreeSpaceCalled = new AtomicLong(DEFAULT_ENSURE_FREE_SPACE_CALLED)

  /**
   *
   */
  protected def makeMemoryStore(mm: MemoryManager): MemoryStore = {
    val ms = mock(classOf[MemoryStore])
    when(ms.ensureFreeSpace(any(), any(), any())).thenAnswer(new Answer[Boolean] {
      override def answer(invocation: InvocationOnMock): Boolean = {
        val args = invocation.getArguments
        require(args.size === 3 && args(1).isInstanceOf[Long],
          "bad test: invalid invocation arguments for ensureFreeSpace - " + args.mkString(", "))
        mockEnsureFreeSpace(mm, args(1).asInstanceOf[Long])
      }
    })
    when(ms.ensureFreeSpace(any(), any())).thenAnswer(new Answer[Boolean] {
      override def answer(invocation: InvocationOnMock): Boolean = {
        val args = invocation.getArguments
        require(args.size === 2 && args(0).isInstanceOf[Long],
          "bad test: invalid invocation arguments for ensureFreeSpace - " + args.mkString(", "))
        mockEnsureFreeSpace(mm, args(0).asInstanceOf[Long])
      }
    })
    mm.setMemoryStore(ms)
    ms
  }

  private def mockEnsureFreeSpace(mm: MemoryManager, numBytes: Long): Boolean = {
    require(ensureFreeSpaceCalled.get() === DEFAULT_ENSURE_FREE_SPACE_CALLED,
      "bad test: ensure free space variable was not reset")
    // Record the number of bytes we freed this call
    ensureFreeSpaceCalled.compareAndSet(DEFAULT_ENSURE_FREE_SPACE_CALLED, numBytes)
    // Note: here we're simulating the behavior of ensure free space, though we free on the
    // granularity on bytes instead of blocks, which is different from the real behavior.
    if (numBytes <= mm.maxStorageMemory) {
      val before = mm.storageMemoryUsed
      val freeMemory = mm.maxStorageMemory - mm.storageMemoryUsed
      val spaceToRelease = numBytes - freeMemory
      if (spaceToRelease > 0) {
        mm.releaseStorageMemory(spaceToRelease)
      }
      val after = mm.storageMemoryUsed
      after - before >= numBytes
    } else {
      // We attempted to free more bytes than our max allowable memory
      false
    }
  }

  /**
   * Assert that [[MemoryStore.ensureFreeSpace]] is called with the given parameters.
   */
  protected def assertEnsureFreeSpaceCalled(ms: MemoryStore, numBytes: Long): Unit = {
    assert(ensureFreeSpaceCalled.get() === numBytes,
      s"expected ensure free space to be called with $numBytes")
    ensureFreeSpaceCalled.set(DEFAULT_ENSURE_FREE_SPACE_CALLED)
  }

  /**
   * Assert that [[MemoryStore.ensureFreeSpace]] is NOT called.
   */
  protected def assertEnsureFreeSpaceNotCalled[T](ms: MemoryStore): Unit = {
    assert(ensureFreeSpaceCalled.get() === DEFAULT_ENSURE_FREE_SPACE_CALLED,
      "ensure free space should not have been called!")
  }
}
