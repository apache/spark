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

import org.apache.spark.network.buffer.{ManagedBuffer, NettyManagedBuffer}
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * This [[ManagedBuffer]] wraps a [[ChunkedByteBuffer]] retrieved from the [[BlockManager]]
 * so that the corresponding block's read lock can be released once this buffer's references
 * are released.
 *
 * This is effectively a wrapper / bridge to connect the BlockManager's notion of read locks
 * to the network layer's notion of retain / release counts.
 */
private[storage] class BlockManagerManagedBuffer(
    blockInfoManager: BlockInfoManager,
    blockId: BlockId,
    chunkedBuffer: ChunkedByteBuffer) extends NettyManagedBuffer(chunkedBuffer.toNetty) {

  override def retain(): ManagedBuffer = {
    super.retain()
    val locked = blockInfoManager.lockForReading(blockId, blocking = false)
    assert(locked.isDefined)
    this
  }

  override def release(): ManagedBuffer = {
    blockInfoManager.unlock(blockId)
    super.release()
  }
}
