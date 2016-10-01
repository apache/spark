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

import org.scalatest.{BeforeAndAfter, Matchers}

import org.apache.spark.{LocalSparkContext, SparkFunSuite}

class BlockReplicationPolicySuite extends SparkFunSuite
  with Matchers
  with BeforeAndAfter
  with LocalSparkContext {

  // Implicitly convert strings to BlockIds for test clarity.
  private implicit def StringToBlockId(value: String): BlockId = new TestBlockId(value)

  /**
   * Test if we get the required number of peers when using random sampling from
   * RandomBlockReplicationPolicy
   */
  test(s"block replication - random block replication policy") {
    val numBlockManagers = 10
    val storeSize = 1000
    val blockManagers = (1 to numBlockManagers).map { i =>
      BlockManagerId(s"store-$i", "localhost", 1000 + i, None)
    }
    val candidateBlockManager = BlockManagerId("test-store", "localhost", 1000, None)
    val replicationPolicy = new RandomBlockReplicationPolicy
    val blockId = "test-block"

    (1 to 10).foreach {numReplicas =>
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

}
