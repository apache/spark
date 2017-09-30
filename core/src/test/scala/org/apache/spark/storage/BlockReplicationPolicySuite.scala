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
import scala.language.implicitConversions
import scala.util.Random

import org.scalatest.{BeforeAndAfter, Matchers}

import org.apache.spark.{LocalSparkContext, SparkFunSuite}

class RandomBlockReplicationPolicyBehavior extends SparkFunSuite
  with Matchers
  with BeforeAndAfter
  with LocalSparkContext {

  // Implicitly convert strings to BlockIds for test clarity.
  protected implicit def StringToBlockId(value: String): BlockId = new TestBlockId(value)

  val replicationPolicy: BlockReplicationPolicy = new RandomBlockReplicationPolicy

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

  /**
   * Returns a sequence of [[BlockManagerId]], whose rack is randomly picked from the given `racks`.
   * Note that, each rack will be picked at least once from `racks`, if `count` is greater or equal
   * to the number of `racks`.
   */
  protected def generateBlockManagerIds(count: Int, racks: Seq[String]): Seq[BlockManagerId] = {
    val randomizedRacks: Seq[String] = Random.shuffle(
      racks ++ racks.length.until(count).map(_ => racks(Random.nextInt(racks.length)))
    )

    (0 until count).map { i =>
      BlockManagerId(s"Exec-$i", s"Host-$i", 10000 + i, Some(randomizedRacks(i)))
    }
  }
}

class TopologyAwareBlockReplicationPolicyBehavior extends RandomBlockReplicationPolicyBehavior {
  override val replicationPolicy = new BasicBlockReplicationPolicy

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
