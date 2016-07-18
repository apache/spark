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

import org.apache.spark.internal.Logging

trait BlockReplicationObjective {
  val weight = 1
  def isObjectiveMet(blockManagerId: BlockManagerId, peers: Set[BlockManagerId]): Boolean

}

case object ReplicateToADifferentHost
  extends BlockReplicationObjective {
  override def isObjectiveMet(blockManagerId: BlockManagerId,
    peers: Set[BlockManagerId]): Boolean = {
    peers.exists(_.host != blockManagerId.host)
  }
}

case object ReplicateBlockWithinRack
  extends BlockReplicationObjective {

  override def isObjectiveMet(blockManagerId: BlockManagerId,
    peers: Set[BlockManagerId]): Boolean = {
    peers.exists(_.topologyInfo == blockManagerId.topologyInfo)
  }
}

case object ReplicateBlockOutsideRack
  extends BlockReplicationObjective {
  override def isObjectiveMet(blockManagerId: BlockManagerId,
    peers: Set[BlockManagerId]): Boolean = {
    peers.exists(_.topologyInfo != blockManagerId.topologyInfo)
  }
}

case object RandomlyReplicateBlock
  extends BlockReplicationObjective {
  override def isObjectiveMet(blockManagerId: BlockManagerId,
    peers: Set[BlockManagerId]): Boolean = {
    peers.nonEmpty
  }
}

case object NoTwoReplicasInSameRack
  extends BlockReplicationObjective {
  override def isObjectiveMet(blockManagerId: BlockManagerId,
    peers: Set[BlockManagerId]): Boolean = {
    val racksReplicatedTo = peers.map(_.topologyInfo).toSet.size
    (peers.size == racksReplicatedTo)
  }
}

object BlockReplicationOptimizer extends Logging {

  /**
   * Get a minimal set of peers that meet the objectives. This is a "best-effort" implementation.
   *
   * @param objectives set of block replication objectives
   * @param peers set of candidate peers
   * @param peersReplicatedTo set of peers we have already replicated to. Empty set if no
   *                          replicas so far
   * @param blockId block Id of the block being replicated, as a source of randomness
   * @param blockManagerId current blockManagerId, so we know where we are
   * @return a tuple of set of optimal peers, and the objectives satisfied by the peers.
   *         Since this is a best-effort implemenation, all objectives might have been met.
   */
  def getPeersToMeetObjectives(objectives: Set[BlockReplicationObjective],
    peers: Set[BlockManagerId],
    peersReplicatedTo: Set[BlockManagerId],
    blockId: BlockId,
    blockManagerId: BlockManagerId): (Set[BlockManagerId], Set[BlockReplicationObjective]) = {

    val random = new Random(blockId.hashCode)
    getOptimalPeers(peers, objectives, Set.empty, peersReplicatedTo, random, blockManagerId)
  }

  /**
   * Greedy solution for set-cover like formulation.
   * 1. We see how many objectives each peer satisfies
   * 2. We choose a peer whose addition to optimal peers set satisfies the most objectives
   * while making sure any previously satisfied objectives are still satisfied.
   * 3. Once chosen, we remove this peer from the set of candidates
   * 4. Repeat till we either run out of peers, or existing peers don't satify any more new
   * objectives
   * @param peers
   * @param objectivesLeft
   * @param objectivesMet
   * @param optimalPeers
   * @param random
   * @param blockManagerId
   * @return
   */
  @tailrec
  private def getOptimalPeers(peers: Set[BlockManagerId],
    objectivesLeft: Set[BlockReplicationObjective],
    objectivesMet: Set[BlockReplicationObjective],
    optimalPeers: Set[BlockManagerId],
    random: Random,
    blockManagerId: BlockManagerId
    ): (Set[BlockManagerId], Set[BlockReplicationObjective]) = {

    logDebug(s"Objectives left : ${objectivesLeft.mkString(", ")}")
    logDebug(s"Objectives met : ${objectivesMet.mkString(", ")}")

    if (peers.isEmpty) {
      // we are done
      (optimalPeers, objectivesMet)
    } else {
      // we see how the addition of this peer to optimalPeers changes objectives left/met
      // ideally, we want a peer whose addition, meets more objectives
      // while making sure we still meet objectives met so far

      val (maxCount, maxPeers) = peers.foldLeft((0, Set.empty[BlockManagerId])) {
        case ((prevMax, maxSet), peer) =>
          val peersSet = optimalPeers + peer
          val allPreviousObjectivesMet =
            objectivesMet.forall(_.isObjectiveMet(blockManagerId, peersSet))
          val score = if (allPreviousObjectivesMet) {
            objectivesLeft.foldLeft(0) { case (c, o) =>
              val weight = if (o.isObjectiveMet(blockManagerId, peersSet)) o.weight else 0
              c + weight
            }
          } else {
            0
          }
          if (score > prevMax) {
            // we found a peer that gets us a higher score!
            (score, Set(peer))
          } else if (score == prevMax) {
            // this peer matches our highest score so far, add this and continue
            (prevMax, maxSet + peer)
          } else {
            // this peer scores lower, we ignore it
            (prevMax, maxSet)
          }
      }

      logDebug(s"Peers ${maxPeers.mkString(", ")} meet $maxCount objective/s")

      if(maxCount > 0) {
        val maxPeer = maxPeers.toSeq(random.nextInt(maxPeers.size))
        val newOptimalPeers = optimalPeers + maxPeer
        val newObjectivesMet =
          objectivesLeft.filter(_.isObjectiveMet(blockManagerId, newOptimalPeers))
        val newObjectivesLeft = objectivesLeft diff newObjectivesMet
        getOptimalPeers(
          peers - maxPeer,
          newObjectivesLeft,
          objectivesMet ++ newObjectivesMet,
          newOptimalPeers,
          random,
          blockManagerId)
      } else {
        // we are done here since either no more objectives left, or
        // no more peers left that satisfy any objectives
        (optimalPeers, objectivesMet)
      }

    }
  }
}
