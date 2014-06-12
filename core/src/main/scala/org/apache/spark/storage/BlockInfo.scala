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

import java.util.concurrent.ConcurrentHashMap

private[storage] class BlockInfo(val level: StorageLevel, val tellMaster: Boolean) {
  // To save space, 'pending' and 'failed' are encoded as special sizes:
  @volatile var size: Long = BlockInfo.BLOCK_PENDING
  private def pending: Boolean = size == BlockInfo.BLOCK_PENDING
  private def failed: Boolean = size == BlockInfo.BLOCK_FAILED
  private def initThread: Thread = BlockInfo.blockInfoInitThreads.get(this)

  setInitThread()

  private def setInitThread() {
    /* Set current thread as init thread - waitForReady will not block this thread
     * (in case there is non trivial initialization which ends up calling waitForReady
     * as part of initialization itself) */
    BlockInfo.blockInfoInitThreads.put(this, Thread.currentThread())
  }

  /**
   * Wait for this BlockInfo to be marked as ready (i.e. block is finished writing).
   * Return true if the block is available, false otherwise.
   */
  def waitForReady(): Boolean = {
    if (pending && initThread != Thread.currentThread()) {
      synchronized {
        while (pending) {
          this.wait()
        }
      }
    }
    !failed
  }

  /** Mark this BlockInfo as ready (i.e. block is finished writing) */
  def markReady(sizeInBytes: Long) {
    require(sizeInBytes >= 0, s"sizeInBytes was negative: $sizeInBytes")
    assert(pending)
    size = sizeInBytes
    BlockInfo.blockInfoInitThreads.remove(this)
    synchronized {
      this.notifyAll()
    }
  }

  /** Mark this BlockInfo as ready but failed */
  def markFailure() {
    assert(pending)
    size = BlockInfo.BLOCK_FAILED
    BlockInfo.blockInfoInitThreads.remove(this)
    synchronized {
      this.notifyAll()
    }
  }
}

private object BlockInfo {
  /* initThread is logically a BlockInfo field, but we store it here because
   * it's only needed while this block is in the 'pending' state and we want
   * to minimize BlockInfo's memory footprint. */
  private val blockInfoInitThreads = new ConcurrentHashMap[BlockInfo, Thread]

  private val BLOCK_PENDING: Long = -1L
  private val BLOCK_FAILED: Long = -2L
}
