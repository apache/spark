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

import scala.annotation.tailrec
import scala.util.Random

trait BlockReplicationObjective {
  def isObjectiveMet(blockManagerId: BlockManagerId, peers: Set[BlockManagerId]): Boolean
  def getPeersToMeetObjective(blockManagerId: BlockManagerId,
    peers: Set[BlockManagerId]): Set[BlockManagerId]
}

object ReplicateToADifferentHost
  extends BlockReplicationObjective {
  override def isObjectiveMet(blockManagerId: BlockManagerId,
    peers: Set[BlockManagerId]): Boolean = {
    peers.exists(_.host != blockManagerId.host)
  }

  override def getPeersToMeetObjective(blockManagerId: BlockManagerId,
    peers: Set[BlockManagerId]): Set[BlockManagerId] = {
    peers.filter(_.host != blockManagerId.host)
  }

  override def toString: String = "ReplicateToADifferentHost"
}

object ReplicateBlockWithinRack
  extends BlockReplicationObjective {

  override def isObjectiveMet(blockManagerId: BlockManagerId,
    peers: Set[BlockManagerId]): Boolean = {
    peers.exists(_.topologyInfo == blockManagerId.topologyInfo)
  }

  override def getPeersToMeetObjective(blockManagerId: BlockManagerId,
    peers: Set[BlockManagerId]): Set[BlockManagerId] = {
    peers.filter(_.topologyInfo == blockManagerId.topologyInfo)
  }

  override def toString: String = "ReplicateBlockWithinRack"
}

object ReplicateBlockOutsideRack
  extends BlockReplicationObjective {
  override def isObjectiveMet(blockManagerId: BlockManagerId,
    peers: Set[BlockManagerId]): Boolean = {
    peers.exists(_.topologyInfo != blockManagerId.topologyInfo)
  }

  override def getPeersToMeetObjective(blockManagerId: BlockManagerId,
    peers: Set[BlockManagerId]): Set[BlockManagerId] = {
    peers.filter(_.topologyInfo != blockManagerId.topologyInfo)
  }

  override def toString: String = "ReplicateBlockOutsideRack"
}

object RandomlyReplicateBlock
  extends BlockReplicationObjective {
  override def isObjectiveMet(blockManagerId: BlockManagerId,
    peers: Set[BlockManagerId]): Boolean = {
    peers.nonEmpty
  }

  override def getPeersToMeetObjective(blockManagerId: BlockManagerId,
    peers: Set[BlockManagerId]): Set[BlockManagerId] = {
    peers
  }

  override def toString: String = "RandomlyReplicateBlock"
}

object BlockReplicationOptimizer {
  def getPeersToMeetObjectives(objectives: Set[BlockReplicationObjective],
    peers: Set[BlockManagerId],
    blockId: BlockId,
    blockManagerId: BlockManagerId): (Set[BlockManagerId], Set[BlockReplicationObjective]) = {
    val peersMap =
      peers.map(p => p -> objectives.filter(_.isObjectiveMet(blockManagerId, Set(p)))).toMap
    val random = new Random(blockId.hashCode)
    getOptimalPeers(peersMap, Set.empty, Set.empty, random)
  }

  /**
   * Greedy solution for set-cover formulation of the problem to get minimal number of peers
   * that meet all the objectives. Each peer satisfies a subset of objectives, and we need to
   * find a set of peers that together satisfy all or most objectives. This is done by
   * 1. greedily picking a peer that satisfies most objectives at a given stage. To make results
   * randomized, we break ties by choosing one or the other randomly.
   * 2. Once chosen, the set of objectives satisfied by the peer are removed from
   * all the remaining peers.
   * 3. Go back to step 1 until we exhaust all peers, or existing peers don't satisfy any more
   * objectives.
   *
   * @param peersMap
   * @param optimalPeers
   * @return
   */
  @tailrec
  private def getOptimalPeers(peersMap: Map[BlockManagerId, Set[BlockReplicationObjective]],
    optimalPeers: Set[BlockManagerId],
    objectivesMetSoFar: Set[BlockReplicationObjective],
    random: Random): (Set[BlockManagerId], Set[BlockReplicationObjective]) = {

    if (peersMap.isEmpty) {
      // we are done here, as no more peers left
      (optimalPeers, objectivesMetSoFar)
    } else {
      val (maxPeer, objectivesMet) =
        peersMap.reduce[(BlockManagerId, Set[BlockReplicationObjective])]
        {case ((pmax, omax), (p, os)) =>
        if (omax.size > os.size) {
          (pmax, omax)
        } else if (omax.size == os.size) {
          // size are the same, we randomly choose between the two with equal probability
          if (random.nextDouble > 0.5) (pmax, omax) else (p, os)
        } else {
          (p, os)
        }
      }
      if(objectivesMet.isEmpty) {
        // we are done here since either no more objectives left, or
        // no more peers left that satisfy any objectives
        (optimalPeers, objectivesMetSoFar)
      } else {
        val updatedPeersMap = (peersMap - maxPeer).map {case (peer, objectives) =>
          peer -> (objectives diff objectivesMet)
        }
        getOptimalPeers(
          updatedPeersMap,
          optimalPeers + maxPeer,
          objectivesMetSoFar ++ objectivesMet,
          random)
      }
    }
  }
}