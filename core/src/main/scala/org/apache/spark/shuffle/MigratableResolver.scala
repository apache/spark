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

package org.apache.spark.shuffle

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.BlockId

/**
 * :: Experimental ::
 * An experimental trait to allow Spark to migrate shuffle blocks.
 */
@Experimental
@Since("3.1.0")
trait MigratableResolver {
  /**
   * Get the shuffle ids that are stored locally. Used for block migrations.
   */
  def getStoredShuffles(): Seq[ShuffleBlockInfo]

  /**
   * Write a provided shuffle block as a stream. Used for block migrations.
   */
  def putShuffleBlockAsStream(blockId: BlockId, serializerManager: SerializerManager):
      StreamCallbackWithID

  /**
   * Get the blocks for migration for a particular shuffle and map.
   */
  def getMigrationBlocks(shuffleBlockInfo: ShuffleBlockInfo): List[(BlockId, ManagedBuffer)]
}
