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

package org.apache.spark.shuffle.sort

import java.io.{DataInputStream, FileInputStream}

import org.apache.spark.shuffle._
import org.apache.spark.{TaskContext, ShuffleDependency}
import org.apache.spark.shuffle.hash.HashShuffleReader
import org.apache.spark.storage.{DiskBlockManager, FileSegment, ShuffleBlockId}

private[spark] class SortShuffleManager extends ShuffleManager {
  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    new BaseShuffleHandle(shuffleId, numMaps, dependency)
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    // We currently use the same block store shuffle fetcher as the hash-based shuffle.
    new HashShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext)
      : ShuffleWriter[K, V] = {
    new SortShuffleWriter(handle.asInstanceOf[BaseShuffleHandle[K, V, _]], mapId, context)
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Unit = {}

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {}

  /** Get the location of a block in a map output file. Uses the index file we create for it. */
  def getBlockLocation(blockId: ShuffleBlockId, diskManager: DiskBlockManager): FileSegment = {
    // The block is actually going to be a range of a single map output file for this map, so
    // figure out the ID of the consolidated file, then the offset within that from our index
    val consolidatedId = blockId.copy(reduceId = 0)
    val indexFile = diskManager.getFile(consolidatedId.name + ".index")
    val in = new DataInputStream(new FileInputStream(indexFile))
    try {
      in.skip(blockId.reduceId * 8)
      val offset = in.readLong()
      val nextOffset = in.readLong()
      new FileSegment(diskManager.getFile(consolidatedId), offset, nextOffset - offset)
    } finally {
      in.close()
    }
  }
}
