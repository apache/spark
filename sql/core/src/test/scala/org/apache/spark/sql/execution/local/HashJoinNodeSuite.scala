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

import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}


class HashJoinNodeSuite extends LocalNodeTest {

  // Test all combinations of the two dimensions: with/out unsafe and build sides
  private val maybeUnsafeAndCodegen = Seq(false, true)
  private val buildSides = Seq(BuildLeft, BuildRight)
  maybeUnsafeAndCodegen.foreach { unsafeAndCodegen =>
    buildSides.foreach { buildSide =>
      testJoin(unsafeAndCodegen, buildSide)
    }
  }

  /**
   * Test inner hash join with varying degrees of matches.
   */
  private def testJoin(
      unsafeAndCodegen: Boolean,
      buildSide: BuildSide): Unit = {
    val simpleOrUnsafe = if (!unsafeAndCodegen) "simple" else "unsafe"
    val testNamePrefix = s"$simpleOrUnsafe / $buildSide"
    val someData = (1 to 100).map { i => (i, "burger" + i) }.toArray
    val conf = new SQLConf
    conf.setConf(SQLConf.UNSAFE_ENABLED, unsafeAndCodegen)
    conf.setConf(SQLConf.CODEGEN_ENABLED, unsafeAndCodegen)

    // Actual test body
    def runTest(leftInput: Array[(Int, String)], rightInput: Array[(Int, String)]): Unit = {
      val rightInputMap = rightInput.toMap
      val leftNode = new DummyNode(joinNameAttributes, leftInput)
      val rightNode = new DummyNode(joinNicknameAttributes, rightInput)
      val makeNode = (node1: LocalNode, node2: LocalNode) => {
        resolveExpressions(new HashJoinNode(
          conf, Seq('id1), Seq('id2), buildSide, node1, node2))
      }
      val makeUnsafeNode = if (unsafeAndCodegen) wrapForUnsafe(makeNode) else makeNode
      val hashJoinNode = makeUnsafeNode(leftNode, rightNode)
      val expectedOutput = leftInput
        .filter { case (k, _) => rightInputMap.contains(k) }
        .map { case (k, v) => (k, v, k, rightInputMap(k)) }
      val actualOutput = hashJoinNode.collect().map { row =>
        // (id, name, id, nickname)
        (row.getInt(0), row.getString(1), row.getInt(2), row.getString(3))
      }
      assert(actualOutput === expectedOutput)
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
