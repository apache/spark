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

package org.apache.spark.sql.execution.local

import org.mockito.Mockito.{mock, when}

import org.apache.spark.broadcast.TorrentBroadcast
import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{InterpretedMutableProjection, UnsafeProjection, Expression}
import org.apache.spark.sql.execution.joins.{HashedRelation, BuildLeft, BuildRight, BuildSide}

class HashJoinNodeSuite extends LocalNodeTest {

  // Test all combinations of the two dimensions: with/out unsafe and build sides
  private val buildSides = Seq(BuildLeft, BuildRight)
  buildSides.foreach { buildSide =>
    testJoin(buildSide)
  }

  /**
   * Builds a [[HashedRelation]] based on a resolved `buildKeys`
   * and a resolved `buildNode`.
   */
  private def buildHashedRelation(
      conf: SQLConf,
      buildKeys: Seq[Expression],
      buildNode: LocalNode): HashedRelation = {

    val buildSideKeyGenerator = UnsafeProjection.create(buildKeys, buildNode.output)
    buildNode.prepare()
    buildNode.open()
    val hashedRelation = HashedRelation(buildNode, buildSideKeyGenerator)
    buildNode.close()

    hashedRelation
  }

  /**
   * Test inner hash join with varying degrees of matches.
   */
  private def testJoin(buildSide: BuildSide): Unit = {
    val testNamePrefix = buildSide
    val someData = (1 to 100).map { i => (i, "burger" + i) }.toArray
    val conf = new SQLConf

    // Actual test body
    def runTest(leftInput: Array[(Int, String)], rightInput: Array[(Int, String)]): Unit = {
      val rightInputMap = rightInput.toMap
      val leftNode = new DummyNode(joinNameAttributes, leftInput)
      val rightNode = new DummyNode(joinNicknameAttributes, rightInput)
      val makeBinaryHashJoinNode = (node1: LocalNode, node2: LocalNode) => {
        val binaryHashJoinNode =
          BinaryHashJoinNode(conf, Seq('id1), Seq('id2), buildSide, node1, node2)
        resolveExpressions(binaryHashJoinNode)
      }
      val makeBroadcastJoinNode = (node1: LocalNode, node2: LocalNode) => {
        val leftKeys = Seq('id1.attr)
        val rightKeys = Seq('id2.attr)
        // Figure out the build side and stream side.
        val (buildNode, buildKeys, streamedNode, streamedKeys) = buildSide match {
          case BuildLeft => (node1, leftKeys, node2, rightKeys)
          case BuildRight => (node2, rightKeys, node1, leftKeys)
        }
        // Resolve the expressions of the build side and then create a HashedRelation.
        val resolvedBuildNode = resolveExpressions(buildNode)
        val resolvedBuildKeys = resolveExpressions(buildKeys, resolvedBuildNode)
        val hashedRelation = buildHashedRelation(conf, resolvedBuildKeys, resolvedBuildNode)
        val broadcastHashedRelation = mock(classOf[TorrentBroadcast[HashedRelation]])
        when(broadcastHashedRelation.value).thenReturn(hashedRelation)

        val hashJoinNode =
          BroadcastHashJoinNode(
            conf,
            streamedKeys,
            streamedNode,
            buildSide,
            resolvedBuildNode.output,
            broadcastHashedRelation)
        resolveExpressions(hashJoinNode)
      }

      val expectedOutput = leftInput
        .filter { case (k, _) => rightInputMap.contains(k) }
        .map { case (k, v) => (k, v, k, rightInputMap(k)) }

      Seq(makeBinaryHashJoinNode, makeBroadcastJoinNode).foreach { makeNode =>
        val makeUnsafeNode = wrapForUnsafe(makeNode)
        val hashJoinNode = makeUnsafeNode(leftNode, rightNode)

        val actualOutput = hashJoinNode.collect().map { row =>
          // (id, name, id, nickname)
          (row.getInt(0), row.getString(1), row.getInt(2), row.getString(3))
        }
        assert(actualOutput === expectedOutput)
      }
    }

    test(s"$testNamePrefix: empty") {
      runTest(Array.empty, Array.empty)
      runTest(someData, Array.empty)
      runTest(Array.empty, someData)
    }

    test(s"$testNamePrefix: no matches") {
      val someIrrelevantData = (10000 to 100100).map { i => (i, "piper" + i) }.toArray
      runTest(someData, Array.empty)
      runTest(Array.empty, someData)
      runTest(someData, someIrrelevantData)
      runTest(someIrrelevantData, someData)
    }

    test(s"$testNamePrefix: partial matches") {
      val someOtherData = (50 to 150).map { i => (i, "finnegan" + i) }.toArray
      runTest(someData, someOtherData)
      runTest(someOtherData, someData)
    }

    test(s"$testNamePrefix: full matches") {
      val someSuperRelevantData = someData.map { case (k, v) => (k, "cooper" + v) }.toArray
      runTest(someData, someSuperRelevantData)
      runTest(someSuperRelevantData, someData)
    }
  }

}
