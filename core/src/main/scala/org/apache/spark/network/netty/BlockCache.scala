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

package org.apache.spark.network.netty

import java.util.concurrent._

import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage.BlockId
import org.apache.spark.SparkEnv

object BlockCache extends  Logging{

  val reqBuffer = new ConcurrentHashMap[Seq[BlockId], FutureCacheForBLocks]()

  def releaseAll(blockIds: Array[BlockId]): Unit = {
    reqBuffer.remove(blockIds)
  }

  def addAll(blockIds: Seq[BlockId]): Unit = {
    val data = new FutureCacheForBLocks(blockIds)
    reqBuffer.put(blockIds, data)
  }

  def getAll(blockIds: Seq[BlockId]): LinkedBlockingQueue[ManagedBuffer] = {
    val buffers = reqBuffer.get(blockIds)
    buffers.get()
  }
}

class FutureCacheForBLocks {
  var blockIds: Seq[BlockId] = _
  var future: FutureTask[LinkedBlockingQueue[ManagedBuffer]] = _

  def this (blockIds: Seq[BlockId]) {
    this()
    this.blockIds = blockIds
    future = new FutureTask[LinkedBlockingQueue[ManagedBuffer]](new RealCacheForBlocks(blockIds))

    val executor = Executors.newFixedThreadPool(1)

    executor.submit(future)
  }

  def get(): LinkedBlockingQueue[ManagedBuffer] = {
    future.get()
  }
}

class RealCacheForBlocks extends  Callable[LinkedBlockingQueue[ManagedBuffer]] {
  val blockManager = SparkEnv.get.blockManager
  var blockIds: Seq[BlockId] = _

  def this(blockIds: Seq[BlockId]) {
    this()
    this.blockIds = blockIds
  }

  override def call(): LinkedBlockingQueue[ManagedBuffer] = {
    val resQueue = new LinkedBlockingQueue[ManagedBuffer]()
    val iterator = blockIds.iterator
    while (iterator.hasNext) {
      val blockId = iterator.next()
      if (blockId != null) {
        val data = blockManager.getBlockData(blockId)
        resQueue.add(data)
      }
    }
    resQueue
  }
}
