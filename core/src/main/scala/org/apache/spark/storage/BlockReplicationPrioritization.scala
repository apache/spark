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

import scala.util.Random

import org.apache.spark.internal.Logging

/* Trait that should be implemented by any class implementing rack aware prioritization */
trait BlockReplicationPrioritization {

  /**
   * Method to prioritize a bunch of candidate peers of a block
 *
   * @param peers A list of peers of a BlockManager
   * @param blockId BlockId of the block being replicated. This can be used as a source of
   *                randomness if needed.
   * @return A prioritized list of peers. Lower the index of a peer, higher its priority
   */
  def prioritize(peers: Seq[BlockManagerId], blockId: BlockId): Seq[BlockManagerId]
}

class DefaultBlockReplicationPrioritization(host: String)
  extends BlockReplicationPrioritization
  with Logging {

  /**
   * Method to prioritize a bunch of candidate peers of a block. This is a basic implementation,
   * that just makes sure we put blocks on different hosts, if possible
   *
   * @param peers A list of peers of a BlockManager
   * @param blockId BlockId of the block being replicated. This can be used as a source of
   *                randomness if needed.
   * @return A prioritized list of peers. Lower the index of a peer, higher its priority
   */
  override def prioritize(peers: Seq[BlockManagerId], blockId: BlockId): Seq[BlockManagerId] = {
    val random = new Random(blockId.hashCode)

    logDebug(s"Input peers : ${peers.mkString(", ")}")
    val ret = random.shuffle(peers)
    logDebug(s"Prioritized peers : ${ret.mkString(", ")}")
    ret
  }
}
