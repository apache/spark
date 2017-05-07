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

package org.apache.spark.ml.recommendation

/**
 * Encoder for storing (blockId, localIndex) into a single integer.
 *
 * We use the leading bits (including the sign bit) to store the block id and the rest to store
 * the local index. This is based on the assumption that users/items are approximately evenly
 * partitioned. With this assumption, we should be able to encode two billion distinct values.
 *
 * @param numBlocks number of blocks
 */
private[recommendation] class LocalIndexEncoder(numBlocks: Int) extends Serializable {

  require(numBlocks > 0, s"numBlocks must be positive but found $numBlocks.")

  private[this] final val numLocalIndexBits =
    math.min(java.lang.Integer.numberOfLeadingZeros(numBlocks - 1), 31)
  private[this] final val localIndexMask = (1 << numLocalIndexBits) - 1

  /** Encodes a (blockId, localIndex) into a single integer. */
  def encode(blockId: Int, localIndex: Int): Int = {
    require(blockId < numBlocks)
    require((localIndex & ~localIndexMask) == 0)
    (blockId << numLocalIndexBits) | localIndex
  }

  /** Gets the block id from an encoded index. */
  @inline
  def blockId(encoded: Int): Int = {
    encoded >>> numLocalIndexBits
  }

  /** Gets the local index from an encoded index. */
  @inline
  def localIndex(encoded: Int): Int = {
    encoded & localIndexMask
  }
}

