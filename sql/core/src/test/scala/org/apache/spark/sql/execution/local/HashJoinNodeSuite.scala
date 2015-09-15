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
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{IntegerType, StringType}


class HashJoinNodeSuite extends LocalNodeTest {
  private val names = Seq(
    AttributeReference("id", IntegerType)(),
    AttributeReference("name", StringType)())
  private val orders = Seq(
    AttributeReference("id", IntegerType)(),
    AttributeReference("orders", IntegerType)())

  /**
   * Test inner hash join with varying degrees of matches.
   */
  private def testJoin(
      testNamePrefix: String,
      buildSide: BuildSide,
      unsafeAndCodegen: Boolean): Unit = {
    val leftData = (1 to 100).map { i => (i, "burger" + i) }.toArray
    val conf = new SQLConf
    conf.setConf(SQLConf.UNSAFE_ENABLED, unsafeAndCodegen)
    conf.setConf(SQLConf.CODEGEN_ENABLED, unsafeAndCodegen)

    def runTest(
        testName: String,
        leftData: Array[(Int, String)],
        rightData: Array[(Int, Int)]): Unit = {
      test(testName) {
        val rightDataMap = rightData.toMap
        val leftNode = new DummyNode(names, leftData)
        val rightNode = new DummyNode(orders, rightData)
        val makeNode = (node1: LocalNode, node2: LocalNode) => {
          new HashJoinNode(
            conf, Seq(node1.output(0)), Seq(node2.output(0)), buildSide, node1, node2)
        }
        val makeUnsafeNode = if (unsafeAndCodegen) wrapForUnsafe(makeNode) else makeNode
        val hashJoinNode = makeUnsafeNode(leftNode, rightNode)
        val expectedOutput = leftData
          .filter { case (k, _) => rightDataMap.contains(k) }
          .map { case (k, v) => (k, v, k, rightDataMap(k)) }
        val actualOutput = hashJoinNode.collect().map { row =>
          // (id, name, id, order)
          (row.getInt(0), row.getString(1), row.getInt(2), row.getInt(3))
        }
        assert(actualOutput === expectedOutput)
      }
    }

    runTest(
      s"$testNamePrefix: empty",
      leftData,
      Array.empty[(Int, Int)])

    runTest(
      s"$testNamePrefix: no matches",
      leftData,
      (10000 to 100100).map { i => (i, i) }.toArray)

    runTest(
      s"$testNamePrefix: one match per row",
      leftData,
      (50 to 100).map { i => (i, i * 1000) }.toArray)

    runTest(
      s"$testNamePrefix: multiple matches per row",
      leftData,
      (1 to 100)
        .flatMap { i => Seq(i, i / 2, i / 3, i / 5, i / 8) }
        .distinct
        .map { i => (i, i) }
        .toArray)
  }

  // Test all combinations of build sides and whether unsafe is enabled
  Seq(false, true).foreach { unsafeAndCodegen =>
    val simpleOrUnsafe = if (unsafeAndCodegen) "unsafe" else "simple"
    Seq(BuildLeft, BuildRight).foreach { buildSide =>
      val leftOrRight = buildSide match {
        case BuildLeft => "left"
        case BuildRight => "right"
      }
      testJoin(s"$simpleOrUnsafe (build $leftOrRight)", buildSide, unsafeAndCodegen)
    }
  }

}
