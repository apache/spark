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
import org.apache.spark.sql.catalyst.plans.{FullOuter, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}


class NestedLoopJoinNodeSuite extends LocalNodeTest {

  // Test all combinations of the three dimensions: with/out unsafe, build sides, and join types
  private val maybeUnsafeAndCodegen = Seq(false, true)
  private val buildSides = Seq(BuildLeft, BuildRight)
  private val joinTypes = Seq(LeftOuter, RightOuter, FullOuter)
  maybeUnsafeAndCodegen.foreach { unsafeAndCodegen =>
    buildSides.foreach { buildSide =>
      joinTypes.foreach { joinType =>
        testJoin(unsafeAndCodegen, buildSide, joinType)
      }
    }
  }

  /**
   * Test outer nested loop joins with varying degrees of matches.
   */
  private def testJoin(
      unsafeAndCodegen: Boolean,
      buildSide: BuildSide,
      joinType: JoinType): Unit = {
    val simpleOrUnsafe = if (!unsafeAndCodegen) "simple" else "unsafe"
    val testNamePrefix = s"$simpleOrUnsafe / $buildSide / $joinType"
    val someData = (1 to 100).map { i => (i, "burger" + i) }.toArray
    val conf = new SQLConf
    conf.setConf(SQLConf.UNSAFE_ENABLED, unsafeAndCodegen)
    conf.setConf(SQLConf.CODEGEN_ENABLED, unsafeAndCodegen)

    // Actual test body
    def runTest(
        joinType: JoinType,
        leftInput: Array[(Int, String)],
        rightInput: Array[(Int, String)]): Unit = {
      val leftNode = new DummyNode(joinNameAttributes, leftInput)
      val rightNode = new DummyNode(joinNicknameAttributes, rightInput)
      val cond = 'id1 === 'id2
      val makeNode = (node1: LocalNode, node2: LocalNode) => {
        resolveExpressions(
          new NestedLoopJoinNode(conf, node1, node2, buildSide, joinType, Some(cond)))
      }
      val makeUnsafeNode = if (unsafeAndCodegen) wrapForUnsafe(makeNode) else makeNode
      val hashJoinNode = makeUnsafeNode(leftNode, rightNode)
      val expectedOutput = generateExpectedOutput(leftInput, rightInput, joinType)
      val actualOutput = hashJoinNode.collect().map { row =>
        // (id, name, id, nickname)
        (row.getInt(0), row.getString(1), row.getInt(2), row.getString(3))
      }
      assert(actualOutput.toSet === expectedOutput.toSet)
    }

    test(s"$testNamePrefix: empty") {
      runTest(joinType, Array.empty, Array.empty)
    }

    test(s"$testNamePrefix: no matches") {
      val someIrrelevantData = (10000 to 10100).map { i => (i, "piper" + i) }.toArray
      runTest(joinType, someData, Array.empty)
      runTest(joinType, Array.empty, someData)
      runTest(joinType, someData, someIrrelevantData)
      runTest(joinType, someIrrelevantData, someData)
    }

    test(s"$testNamePrefix: partial matches") {
      val someOtherData = (50 to 150).map { i => (i, "finnegan" + i) }.toArray
      runTest(joinType, someData, someOtherData)
      runTest(joinType, someOtherData, someData)
    }

    test(s"$testNamePrefix: full matches") {
      val someSuperRelevantData = someData.map { case (k, v) => (k, "cooper" + v) }
      runTest(joinType, someData, someSuperRelevantData)
      runTest(joinType, someSuperRelevantData, someData)
    }
  }

  /**
   * Helper method to generate the expected output of a test based on the join type.
   */
  private def generateExpectedOutput(
      leftInput: Array[(Int, String)],
      rightInput: Array[(Int, String)],
      joinType: JoinType): Array[(Int, String, Int, String)] = {
    joinType match {
      case LeftOuter =>
        val rightInputMap = rightInput.toMap
        leftInput.map { case (k, v) =>
          val rightKey = rightInputMap.get(k).map { _ => k }.getOrElse(0)
          val rightValue = rightInputMap.getOrElse(k, null)
          (k, v, rightKey, rightValue)
        }

      case RightOuter =>
        val leftInputMap = leftInput.toMap
        rightInput.map { case (k, v) =>
          val leftKey = leftInputMap.get(k).map { _ => k }.getOrElse(0)
          val leftValue = leftInputMap.getOrElse(k, null)
          (leftKey, leftValue, k, v)
        }

      case FullOuter =>
        val leftInputMap = leftInput.toMap
        val rightInputMap = rightInput.toMap
        val leftOutput = leftInput.map { case (k, v) =>
          val rightKey = rightInputMap.get(k).map { _ => k }.getOrElse(0)
          val rightValue = rightInputMap.getOrElse(k, null)
          (k, v, rightKey, rightValue)
        }
        val rightOutput = rightInput.map { case (k, v) =>
          val leftKey = leftInputMap.get(k).map { _ => k }.getOrElse(0)
          val leftValue = leftInputMap.getOrElse(k, null)
          (leftKey, leftValue, k, v)
        }
        (leftOutput ++ rightOutput).distinct

      case other =>
        throw new IllegalArgumentException(s"Join type $other is not applicable")
    }
  }

}
