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

import org.mockito.Matchers.{any, anyLong}
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.MemoryStore


/**
 * Helper trait for sharing code among [[MemoryManager]] tests.
 */
private[memory] trait MemoryManagerSuite extends SparkFunSuite {

  import MemoryManagerSuite.DEFAULT_ENSURE_FREE_SPACE_CALLED

  // Note: Mockito's verify mechanism does not provide a way to reset method call counts
  // without also resetting stubbed methods. Since our test code relies on the latter,
  // we need to use our own variable to track invocations of `ensureFreeSpace`.

  /**
   * The amount of free space requested in the last call to [[MemoryStore.ensureFreeSpace]]
   *
   * This set whenever [[MemoryStore.ensureFreeSpace]] is called, and cleared when the test
   * code makes explicit assertions on this variable through [[assertEnsureFreeSpaceCalled]].
   */
  private val ensureFreeSpaceCalled = new AtomicLong(DEFAULT_ENSURE_FREE_SPACE_CALLED)

  /**
   * Make a mocked [[MemoryStore]] whose [[MemoryStore.ensureFreeSpace]] method is stubbed.
   *
   * This allows our test code to release storage memory when [[MemoryStore.ensureFreeSpace]]
   * is called without relying on [[org.apache.spark.storage.BlockManager]] and all of its
   * dependencies.
   */
  protected def makeMemoryStore(mm: MemoryManager): MemoryStore = {
    val ms = mock(classOf[MemoryStore])
    when(ms.ensureFreeSpace(anyLong(), any())).thenAnswer(ensureFreeSpaceAnswer(mm, 0))
    when(ms.ensureFreeSpace(any(), anyLong(), any())).thenAnswer(ensureFreeSpaceAnswer(mm, 1))
    mm.setMemoryStore(ms)
    ms
  }

  /**
   * Make an [[Answer]] that stubs [[MemoryStore.ensureFreeSpace]] with the right arguments.
   */
  private def ensureFreeSpaceAnswer(mm: MemoryManager, numBytesPos: Int): Answer[Boolean] = {
    new Answer[Boolean] {
      override def answer(invocation: InvocationOnMock): Boolean = {
        val args = invocation.getArguments
        require(args.size > numBytesPos, s"bad test: expected >$numBytesPos arguments " +
          s"in ensureFreeSpace, found ${args.size}")
        require(args(numBytesPos).isInstanceOf[Long], s"bad test: expected ensureFreeSpace " +
          s"argument at index $numBytesPos to be a Long: ${args.mkString(", ")}")
        val numBytes = args(numBytesPos).asInstanceOf[Long]
        mockEnsureFreeSpace(mm, numBytes)
      }
    }
  }

  /**
   * Simulate the part of [[MemoryStore.ensureFreeSpace]] that releases storage memory.
   *
   * This is a significant simplification of the real method, which actually drops existing
   * blocks based on the size of each block. Instead, here we simply release as many bytes
   * as needed to ensure the requested amount of free space. This allows us to set up the
   * test without relying on the [[org.apache.spark.storage.BlockManager]], which brings in
   * many other dependencies.
   *
   * Every call to this method will set a global variable, [[ensureFreeSpaceCalled]], that
   * records the number of bytes this is called with. This variable is expected to be cleared
   * by the test code later through [[assertEnsureFreeSpaceCalled]].
   */
  private def mockEnsureFreeSpace(mm: MemoryManager, numBytes: Long): Boolean = mm.synchronized {
    require(ensureFreeSpaceCalled.get() === DEFAULT_ENSURE_FREE_SPACE_CALLED,
      "bad test: ensure free space variable was not reset")
    // Record the number of bytes we freed this call
    ensureFreeSpaceCalled.set(numBytes)
    if (numBytes <= mm.maxStorageMemory) {
      def freeMemory = mm.maxStorageMemory - mm.storageMemoryUsed
      val spaceToRelease = numBytes - freeMemory
      if (spaceToRelease > 0) {
        mm.releaseStorageMemory(spaceToRelease)
      }
      freeMemory >= numBytes
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

private object MemoryManagerSuite {
  private val DEFAULT_ENSURE_FREE_SPACE_CALLED = -1L
}
