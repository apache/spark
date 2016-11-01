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

import org.scalatest.{BeforeAndAfter, Matchers}

import org.apache.spark.{LocalSparkContext, SparkFunSuite}

trait RandomBlockReplicationPolicyBehavior extends SparkFunSuite
    with Matchers
    with BeforeAndAfter
    with LocalSparkContext {

  // Implicitly convert strings to BlockIds for test clarity.
  protected implicit def StringToBlockId(value: String): BlockId = new TestBlockId(value)

  val replicationPolicy: BlockReplicationPolicy

  val blockId = "test-block"
  /**
   * Test if we get the required number of peers when using random sampling from
   * BlockReplicationPolicy
   */
  test("block replication - random block replication policy") {
    val numBlockManagers = 10
    val storeSize = 1000
    val blockManagers = generateBlockManagerIds(numBlockManagers, Seq("/Rack-1"))
    val candidateBlockManager = BlockManagerId("test-store", "localhost", 1000, None)

    (1 to 10).foreach { numReplicas =>
      logDebug(s"Num replicas : $numReplicas")
      val randomPeers = replicationPolicy.prioritize(
        candidateBlockManager,
        blockManagers,
        mutable.HashSet.empty[BlockManagerId],
        blockId,
        numReplicas
      )
      logDebug(s"Random peers : ${randomPeers.mkString(", ")}")
      assert(randomPeers.toSet.size === numReplicas)

      // choosing n peers out of n
      val secondPass = replicationPolicy.prioritize(
        candidateBlockManager,
        randomPeers,
        mutable.HashSet.empty[BlockManagerId],
        blockId,
        numReplicas
      )
      logDebug(s"Random peers : ${secondPass.mkString(", ")}")
      assert(secondPass.toSet.size === numReplicas)
    }
  }

  protected def generateBlockManagerIds(count: Int, racks: Seq[String]): Seq[BlockManagerId] = {
    (1 to count).map{i =>
      BlockManagerId(s"Exec-$i", s"Host-$i", 10000 + i, Some(racks(Random.nextInt(racks.size))))
    }
  }
}

trait TopologyAwareBlockReplicationPolicyBehavior extends RandomBlockReplicationPolicyBehavior {
  test("All peers in the same rack") {
    val racks = Seq("/default-rack")
    val numBlockManager = 10
    (1 to 10).foreach {numReplicas =>
      val peers = generateBlockManagerIds(numBlockManager, racks)
      val blockManager = BlockManagerId("Driver", "Host-driver", 10001, Some(racks.head))

      val prioritizedPeers = replicationPolicy.prioritize(
        blockManager,
        peers,
        mutable.HashSet.empty,
        blockId,
        numReplicas
      )

      assert(prioritizedPeers.toSet.size == numReplicas)
      assert(prioritizedPeers.forall(p => p.host != blockManager.host))
    }
  }

  test("Peers in 2 racks") {
    val racks = Seq("/Rack-1", "/Rack-2")
    (1 to 10).foreach {numReplicas =>
      val peers = generateBlockManagerIds(10, racks)
      val blockManager = BlockManagerId("Driver", "Host-driver", 9001, Some(racks.head))

      val prioritizedPeers = replicationPolicy.prioritize(
        blockManager,
        peers,
        mutable.HashSet.empty,
        blockId,
        numReplicas
      )

      assert(prioritizedPeers.toSet.size == numReplicas)
      val priorityPeers = prioritizedPeers.take(2)
      assert(priorityPeers.forall(p => p.host != blockManager.host))
      if(numReplicas > 1) {
        // both these conditions should be satisfied when numReplicas > 1
        assert(priorityPeers.exists(p => p.topologyInfo == blockManager.topologyInfo))
        assert(priorityPeers.exists(p => p.topologyInfo != blockManager.topologyInfo))
      }
    }
  }
}

class RandomBlockReplicationPolicySuite extends RandomBlockReplicationPolicyBehavior {
  override val replicationPolicy = new RandomBlockReplicationPolicy
}

class BasicBlockReplicationPolicySuite extends TopologyAwareBlockReplicationPolicyBehavior {
  override val replicationPolicy = new BasicBlockReplicationPolicy
}

class ObjectivesBasedReplicationPolicySuite extends TopologyAwareBlockReplicationPolicyBehavior {
  override val replicationPolicy = new ObjectivesBasedReplicationPolicy

  val objectives: Set[BlockReplicationObjective] = Set(
    ReplicateToADifferentHost,
    ReplicateBlockOutsideRack,
    ReplicateBlockWithinRack,
    NoTwoReplicasInSameRack
  )

  test("peers are all in the same rack") {
    val blockManagerIds = generateBlockManagerIds(10, List("Default-rack"))

    val blockId = BlockId("test_block")

    val candidateBMId = generateBlockManagerIds(1, List("Default-rack")).head

    val numReplicas = 2

    val (optimalPeers, objectivesMet) = BlockReplicationOptimizer.getPeersToMeetObjectives(
      objectives,
      blockManagerIds,
      mutable.HashSet.empty,
      blockId,
      candidateBMId,
      numReplicas)

    logDebug(s"Optimal peers : ${optimalPeers}")
    logDebug(s"Objectives met : ${objectivesMet}")
    assert(optimalPeers.size == 1)
    assert(objectivesMet.size == 3)
  }

  test("peers in 3 racks") {
    val racks = List("/Rack1", "/Rack2", "/Rack3")
    val blockManagerIds = generateBlockManagerIds(10, racks)
    val candidateBMId = generateBlockManagerIds(1, racks).head
    val blockId = BlockId("test_block")
    val numReplicas = 2
    val (optimalPeers, objectivesMet) = BlockReplicationOptimizer.getPeersToMeetObjectives(
      objectives,
      blockManagerIds,
      mutable.HashSet.empty,
      blockId,
      candidateBMId,
      numReplicas)

    logDebug(s"Optimal peers : ${optimalPeers}")
    logDebug(s"Objectives met : ${objectivesMet}")
    assert(optimalPeers.size == 2)
    assert(objectivesMet.size == 4)
    assert(objectives.forall(_.isObjectiveMet(candidateBMId, optimalPeers)))
  }

}
