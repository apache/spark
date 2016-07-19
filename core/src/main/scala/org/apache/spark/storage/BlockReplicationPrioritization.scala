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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging

/**
 * ::DeveloperApi::
 * BlockReplicationPrioritization provides logic for prioritizing a sequence of peers for
 * replicating blocks
 */
@DeveloperApi
trait BlockReplicationPrioritization {

  /**
   * Method to prioritize a bunch of candidate peers of a block manager
   *
   * @param blockManagerId Id of the current BlockManager for self identification
   * @param peers A list of peers of a BlockManager
   * @param peersReplicatedTo Set of peers already replicated to
   * @param blockId BlockId of the block being replicated. This can be used as a source of
   *                randomness if needed.
   * @return A prioritized list of peers. Lower the index of a peer, higher its priority
   */
  def prioritize(
    blockManagerId: BlockManagerId,
    peers: Seq[BlockManagerId],
    peersReplicatedTo: Set[BlockManagerId],
    blockId: BlockId): Seq[BlockManagerId]
}

@DeveloperApi
class DefaultBlockReplicationPrioritization
  extends BlockReplicationPrioritization
  with Logging {

  /**
   * Method to prioritize a bunch of candidate peers of a block manager. This is an implementation
   * that just makes sure we put blocks on different hosts, if possible
   *
   * @param blockManagerId Id of the current BlockManager for self identification
   * @param peers A list of peers of a BlockManager
   * @param peersReplicatedTo Set of peers already replicated to
   * @param blockId BlockId of the block being replicated. This can be used as a source of
   *                randomness if needed.
   * @return A prioritized list of peers. Lower the index of a peer, higher its priority
   */
  override def prioritize(
    blockManagerId: BlockManagerId,
    peers: Seq[BlockManagerId],
    peersReplicatedTo: Set[BlockManagerId],
    blockId: BlockId): Seq[BlockManagerId] = {
    val random = new Random(blockId.hashCode)

    logDebug(s"Input peers : ${peers.mkString(", ")}")
    val ret = random.shuffle(peers)
    logDebug(s"Prioritized peers : ${ret.mkString(", ")}")
    ret
  }
}

@DeveloperApi
class PrioritizationWithObjectives
  extends BlockReplicationPrioritization
    with Logging {
  val objectives: Set[BlockReplicationObjective] = Set(
    ReplicateToADifferentHost,
    ReplicateBlockOutsideRack,
    ReplicateBlockWithinRack,
    NoTwoReplicasInSameRack
  )
  /**
   * Method to prioritize a bunch of candidate peers of a block
   *
   * @param blockManagerId    Id of the current BlockManager for self identification
   * @param peers             A list of peers of a BlockManager
   * @param peersReplicatedTo Set of peers already replicated to
   * @param blockId           BlockId of the block being replicated. This can be used as a source of
   *                          randomness if needed.
   * @return A prioritized list of peers. Lower the index of a peer, higher its priority
   */
  override def prioritize(
    blockManagerId: BlockManagerId,
    peers: Seq[BlockManagerId],
    peersReplicatedTo: Set[BlockManagerId],
    blockId: BlockId): Seq[BlockManagerId] = {
    val (optimalPeers, objectivesMet) = BlockReplicationOptimizer.getPeersToMeetObjectives(
      objectives,
      peers.toSet,
      peersReplicatedTo,
      blockId,
      blockManagerId
    )
    logInfo(s"BlockReplication objectives met : ${objectivesMet.mkString(", ")}")
    logInfo(s"Optimal peers : ${optimalPeers.mkString(", ")}")
    // outside of the peers, we don't care about the order of peers, so we randomly shuffle
    val r = new Random(blockId.hashCode)
    val remainingPeers = peers.filter(p => !optimalPeers.contains(p))
    optimalPeers.toSeq ++ r.shuffle(remainingPeers)
  }
}

@DeveloperApi
class BasicBlockReplicationPrioritization
  extends BlockReplicationPrioritization
    with Logging {

  /**
   * Method to prioritize a bunch of candidate peers of a block manager. This implementation
   * replicates the behavior of block replication in HDFS, a peer is chosen within the rack,
   * one outside and that's it. This works best with a total replication factor of 3.
   *
   * @param blockManagerId    Id of the current BlockManager for self identification
   * @param peers             A list of peers of a BlockManager
   * @param peersReplicatedTo Set of peers already replicated to
   * @param blockId           BlockId of the block being replicated. This can be used as a source of
   *                          randomness if needed.
   * @return A prioritized list of peers. Lower the index of a peer, higher its priority
   */
  override def prioritize(
    blockManagerId: BlockManagerId,
    peers: Seq[BlockManagerId],
    peersReplicatedTo: Set[BlockManagerId],
    blockId: BlockId): Seq[BlockManagerId] = {

    logDebug(s"Input peers : $peers")
    logDebug(s"BlockManagerId : $blockManagerId")

    val random = new Random(blockId.hashCode)

    val doneWithinRack = peersReplicatedTo.exists(_.topologyInfo == blockManagerId.topologyInfo)
    val peerWithinRack = if (doneWithinRack) {
      // we are done with in-rack replication, so don't need anymore peers
      Seq.empty[BlockManagerId]
    } else {
      // we choose an in-rack peer at random
      val inRackPeers = peers.filter { p =>
        // we try to get peers within the same rack, but not the current host
        p.topologyInfo == blockManagerId.topologyInfo && p.host != blockManagerId.host
      }

      if(inRackPeers.isEmpty) {
        Seq.empty
      } else {
        Seq(inRackPeers(random.nextInt(inRackPeers.size)))
      }
    }
    val doneOutsideRack = peersReplicatedTo.exists(_.topologyInfo != blockManagerId.topologyInfo)

    val peerOutsideRack = if (doneOutsideRack) {
      Seq.empty[BlockManagerId]
    } else {
      val outOfRackPeers = peers.filter(_.topologyInfo != blockManagerId.topologyInfo)
      if(outOfRackPeers.isEmpty) {
        Seq.empty
      } else {
        Seq(outOfRackPeers(random.nextInt(outOfRackPeers.size)))
      }
    }

    val priorityPeers = peerWithinRack ++ peerOutsideRack

    logInfo(s"Priority peers : $priorityPeers")
    val remainingPeers = random.shuffle((peers.toSet diff priorityPeers.toSet).toSeq)

    priorityPeers ++ remainingPeers
  }
}