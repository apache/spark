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

import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite

class BlockReplicationStrategySuite extends SparkFunSuite with Matchers {
  val prioritizer = new BasicBlockReplicationPrioritization
  val blockId = BlockId("test_block")

  test("All peers in the same rack") {
    val racks = Seq("/default-rack")

    val peers = generateBlockManagerIds(10, racks)
    val blockManager = BlockManagerId("Driver", "Host-driver", 10001, Some(racks.head))

    val prioritizedPeers = prioritizer.prioritize(
      blockManager,
      peers.toSeq,
      Set.empty,
      blockId
    )

    assert(prioritizedPeers.size == peers.size)
    val priorityPeer = prioritizedPeers.head
    assert(priorityPeer.host != blockManager.host)
  }

  test("Peers in 2 racks") {
    val racks = Seq("/Rack-1", "/Rack-2")

    val peers = generateBlockManagerIds(10, racks)
    val blockManager = BlockManagerId("Exec-1", "Host-1", 9001, Some(racks.head))

    val prioritizedPeers = prioritizer.prioritize(
      blockManager,
      peers.toSeq,
      Set.empty,
      blockId
    )

    assert(prioritizedPeers.size == peers.size)
    val priorityPeers = prioritizedPeers.take(2)
    assert(priorityPeers.forall(p => p.host != blockManager.host))
    assert(priorityPeers.exists(p => p.topologyInfo == blockManager.topologyInfo))
    assert(priorityPeers.exists(p => p.topologyInfo != blockManager.topologyInfo))
  }


  private def generateBlockManagerIds(count: Int, racks: Seq[String]): Set[BlockManagerId] = {
    (1 to count).map{i =>
      BlockManagerId(s"Exec-$i", s"Host-$i", 10000 + i, Some(racks(Random.nextInt(racks.size))))
    }.toSet
  }

}
