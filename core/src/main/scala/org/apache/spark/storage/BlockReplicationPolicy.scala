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

import scala.collection.mutable
import scala.util.Random

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging

/**
 * ::DeveloperApi::
 * BlockReplicationPrioritization provides logic for prioritizing a sequence of peers for
 * replicating blocks. BlockManager will replicate to each peer returned in order until the
 * desired replication order is reached. If a replication fails, prioritize() will be called
 * again to get a fresh prioritization.
 */
@DeveloperApi
trait BlockReplicationPolicy {

  /**
   * Method to prioritize a bunch of candidate peers of a block
   *
   * @param blockManagerId Id of the current BlockManager for self identification
   * @param peers A list of peers of a BlockManager
   * @param peersReplicatedTo Set of peers already replicated to
   * @param blockId BlockId of the block being replicated. This can be used as a source of
   *                randomness if needed.
   * @param numReplicas Number of peers we need to replicate to
   * @return A prioritized list of peers. Lower the index of a peer, higher its priority.
   *         This returns a list of size at most `numPeersToReplicateTo`.
   */
  def prioritize(
      blockManagerId: BlockManagerId,
      peers: Seq[BlockManagerId],
      peersReplicatedTo: mutable.HashSet[BlockManagerId],
      blockId: BlockId,
      numReplicas: Int): List[BlockManagerId]
}

object BlockReplicationUtils {
  /**
   * Uses sampling algorithm by Robert Floyd. Finds a random sample in O(n) while
   * minimizing space usage. Please see <a href="https://math.stackexchange.com/q/178690">
   * here</a>.
   *
   * @param n total number of indices
   * @param m number of samples needed
   * @param r random number generator
   * @return list of m random unique indices
   */
  private def getSampleIds(n: Int, m: Int, r: Random): List[Int] = {
    val indices = (n - m + 1 to n).foldLeft(mutable.LinkedHashSet.empty[Int]) {case (set, i) =>
      val t = r.nextInt(i) + 1
      if (set.contains(t)) set + i else set + t
    }
    indices.map(_ - 1).toList
  }

  /**
   * Get a random sample of size m from the elems
   *
   * @param elems
   * @param m number of samples needed
   * @param r random number generator
   * @tparam T
   * @return a random list of size m. If there are fewer than m elements in elems, we just
   *         randomly shuffle elems
   */
  def getRandomSample[T](elems: Seq[T], m: Int, r: Random): List[T] = {
    if (elems.size > m) {
      getSampleIds(elems.size, m, r).map(elems(_))
    } else {
      r.shuffle(elems).toList
    }
  }
}

@DeveloperApi
class RandomBlockReplicationPolicy
  extends BlockReplicationPolicy
  with Logging {

  /**
   * Method to prioritize a bunch of candidate peers of a block. This is a basic implementation,
   * that just makes sure we put blocks on different hosts, if possible
   *
   * @param blockManagerId Id of the current BlockManager for self identification
   * @param peers A list of peers of a BlockManager
   * @param peersReplicatedTo Set of peers already replicated to
   * @param blockId BlockId of the block being replicated. This can be used as a source of
   *                randomness if needed.
   * @param numReplicas Number of peers we need to replicate to
   * @return A prioritized list of peers. Lower the index of a peer, higher its priority
   */
  override def prioritize(
      blockManagerId: BlockManagerId,
      peers: Seq[BlockManagerId],
      peersReplicatedTo: mutable.HashSet[BlockManagerId],
      blockId: BlockId,
      numReplicas: Int): List[BlockManagerId] = {
    val random = new Random(blockId.hashCode)
    logDebug(s"Input peers : ${peers.mkString(", ")}")
    val prioritizedPeers = if (peers.size > numReplicas) {
      BlockReplicationUtils.getRandomSample(peers, numReplicas, random)
    } else {
      if (peers.size < numReplicas) {
        logWarning(s"Expecting ${numReplicas} replicas with only ${peers.size} peer/s.")
      }
      random.shuffle(peers).toList
    }
    logDebug(s"Prioritized peers : ${prioritizedPeers.mkString(", ")}")
    prioritizedPeers
  }
}

@DeveloperApi
class BasicBlockReplicationPolicy
  extends BlockReplicationPolicy
    with Logging {

  /**
   * Method to prioritize a bunch of candidate peers of a block manager. This implementation
   * replicates the behavior of block replication in HDFS. For a given number of replicas needed,
   * we choose a peer within the rack, one outside and remaining blockmanagers are chosen at
   * random, in that order till we meet the number of replicas needed.
   * This works best with a total replication factor of 3, like HDFS.
   *
   * @param blockManagerId    Id of the current BlockManager for self identification
   * @param peers             A list of peers of a BlockManager
   * @param peersReplicatedTo Set of peers already replicated to
   * @param blockId           BlockId of the block being replicated. This can be used as a source of
   *                          randomness if needed.
   * @param numReplicas Number of peers we need to replicate to
   * @return A prioritized list of peers. Lower the index of a peer, higher its priority
   */
  override def prioritize(
      blockManagerId: BlockManagerId,
      peers: Seq[BlockManagerId],
      peersReplicatedTo: mutable.HashSet[BlockManagerId],
      blockId: BlockId,
      numReplicas: Int): List[BlockManagerId] = {

    logDebug(s"Input peers : $peers")
    logDebug(s"BlockManagerId : $blockManagerId")

    val random = new Random(blockId.hashCode)

    // if block doesn't have topology info, we can't do much, so we randomly shuffle
    // if there is, we see what's needed from peersReplicatedTo and based on numReplicas,
    // we choose whats needed
    if (blockManagerId.topologyInfo.isEmpty || numReplicas == 0) {
      // no topology info for the block. The best we can do is randomly choose peers
      BlockReplicationUtils.getRandomSample(peers, numReplicas, random)
    } else {
      // we have topology information, we see what is left to be done from peersReplicatedTo
      val doneWithinRack = peersReplicatedTo.exists(_.topologyInfo == blockManagerId.topologyInfo)
      val doneOutsideRack = peersReplicatedTo.exists { p =>
        p.topologyInfo.isDefined && p.topologyInfo != blockManagerId.topologyInfo
      }

      if (doneOutsideRack && doneWithinRack) {
        // we are done, we just return a random sample
        BlockReplicationUtils.getRandomSample(peers, numReplicas, random)
      } else {
        // we separate peers within and outside rack
        val (inRackPeers, outOfRackPeers) = peers
            .filter(_.host != blockManagerId.host)
            .partition(_.topologyInfo == blockManagerId.topologyInfo)

        val peerWithinRack = if (doneWithinRack) {
          // we are done with in-rack replication, so don't need anymore peers
          Seq.empty
        } else {
          if (inRackPeers.isEmpty) {
            Seq.empty
          } else {
            Seq(inRackPeers(random.nextInt(inRackPeers.size)))
          }
        }

        val peerOutsideRack = if (doneOutsideRack || numReplicas - peerWithinRack.size <= 0) {
          Seq.empty
        } else {
          if (outOfRackPeers.isEmpty) {
            Seq.empty
          } else {
            Seq(outOfRackPeers(random.nextInt(outOfRackPeers.size)))
          }
        }

        val priorityPeers = peerWithinRack ++ peerOutsideRack
        val numRemainingPeers = numReplicas - priorityPeers.size
        val remainingPeers = if (numRemainingPeers > 0) {
          val rPeers = peers.filter(p => !priorityPeers.contains(p))
          BlockReplicationUtils.getRandomSample(rPeers, numRemainingPeers, random)
        } else {
          Seq.empty
        }

        (priorityPeers ++ remainingPeers).toList
      }

    }
  }

}
