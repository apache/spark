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

class BlockReplicationObjectives extends SparkFunSuite with Matchers {
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

    val (optimalPeers, objectivesMet) = BlockReplicationOptimizer.getPeersToMeetObjectives(
      objectives,
      blockManagerIds,
      blockId,
      candidateBMId)

    logInfo(s"Optimal peers : ${optimalPeers}")
    logInfo(s"Objectives met : ${objectivesMet}")
    assert(optimalPeers.size == 1)
  }

  test("peers in 3 racks") {
    val racks = List("/Rack1", "/Rack2", "/Rack3")
    val blockManagerIds = generateBlockManagerIds(10, racks)
    val candidateBMId = generateBlockManagerIds(1, racks).head
    val blockId = BlockId("test_block")
    val (optimalPeers, objectivesMet) = BlockReplicationOptimizer.getPeersToMeetObjectives(
      objectives,
      blockManagerIds,
      blockId,
      candidateBMId)

    logInfo(s"Optimal peers : ${optimalPeers}")
    logInfo(s"Objectives met : ${objectivesMet}")
    assert(optimalPeers.size == 2)
    assert(objectivesMet.size == 4)
    assert(objectives.forall(_.isObjectiveMet(candidateBMId, optimalPeers)))
  }

  private def generateBlockManagerIds(count: Int, racks: Seq[String]): Set[BlockManagerId] = {
    (1 to count).map{i =>
      BlockManagerId(s"Exec-$i", s"Host-$i", 10000 + i, Some(racks(Random.nextInt(racks.size))))
    }.toSet
  }
}
