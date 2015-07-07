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
package org.apache.spark.shuffle.parquet

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.shuffle._
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.{ShuffleDependency, TaskContext}

class ErrorShuffleManager extends ShuffleManager {

  private def throwError(error: String) = {
    throw new NotImplementedError(
      s"${ParquetShuffleConfig.fallbackShuffleManager} not defined: ${error}")
  }

  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   */
  override def registerShuffle[K, V, C](shuffleId: Int,
                                        numMaps: Int,
                                        dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    throwError(s"Unable to register shuffle for keyClass=${dependency.keyClassName} " +
      s"valueClass=${dependency.valueClassName} combineClass=${dependency.combinerClassName}")
  }

  /**
   * Return a resolver capable of retrieving shuffle block data based on block coordinates.
   */
  override def shuffleBlockResolver: ShuffleBlockResolver = new ShuffleBlockResolver {

    override def stop(): Unit = {} // no-op

    /**
     * Retrieve the data for the specified block. If the data for that block is not available,
     * throws an unspecified exception.
     */
    override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
      throwError(s"Unable to get block data for ${blockId}")
    }
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {} // no-op

  /**
   * Remove a shuffle's metadata from the ShuffleManager.
   * @return true if the metadata removed successfully, otherwise false.
   */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    throwError("Unable to unregister shuffle")
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](handle: ShuffleHandle,
                               mapId: Int,
                               context: TaskContext): ShuffleWriter[K, V] = {
    throwError("Unable to get a writer")
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  override def getReader[K, C](handle: ShuffleHandle,
                               startPartition: Int,
                               endPartition: Int,
                               context: TaskContext): ShuffleReader[K, C] = {
    throwError("Unable to get a reader")
  }
}
