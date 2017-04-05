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

package org.apache.spark.ml.tree

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.tree.impurity.GiniCalculator
import org.apache.spark.mllib.tree.model.ImpurityStats
import org.apache.spark.mllib.util.MLlibTestSparkContext

/**
 * Test suite for [[LearningNode]].
 */
class LearningNodeSuite extends SparkFunSuite with MLlibTestSparkContext {
  import LearningNodeSuite._

  test("SPARK-3159: Check for reducible DecisionTree") {
    val classA = TreeUtils.makeImpurityStats(2, 0)
    val classB = TreeUtils.makeImpurityStats(2, 1)

    val root = TreeUtils.fillBinaryTree(classA)(3)
    NodeUtils.setAsLeaf(root, Array(5))
    NodeUtils.setPredictValue(root, Array(6, 7, 12, 13, 14), classB)
    /**
     * use TreeUtils.toDebugTreeString(root).
     * input:
                8(0.0)
             4--|
                9(0.0)
          2--|
             5(0.0)
       1--|
                12(1.0)
             6--|
                13(1.0)
          3--|
                14(1.0)
             7--|
                15(0.0)
      */
    assert(true === hasPairsOfSameChildren(root))

    val mergeCounts = LearningNode.mergeChildrenWithSamePrediction(root)
    /**
     * use TreeUtils.toDebugTreeString(root).
     * result:
          2(0.0)
       1--|
             6(1.0)
          3--|
                14(1.0)
             7--|
                15(0.0)
      */
    assert(false === hasPairsOfSameChildren(root))
    assert(mergeCounts === 3)
  }
}

object LearningNodeSuite {

  /** check if there exists pairs of leaf nodes with same prediction of the same parent. */
  def hasPairsOfSameChildren(node: LearningNode): Boolean =
    if (node.isLeaf) {
      false

    } else {
      val left = node.leftChild.get
      val right = node.rightChild.get

      if (left.isLeaf && right.isLeaf) {
        val leftPredict = left.stats.impurityCalculator.predict
        val rightPredict = right.stats.impurityCalculator.predict

        leftPredict == rightPredict

      } else {
        // shortcut if find.
        hasPairsOfSameChildren(left) || hasPairsOfSameChildren(right)
      }
    }

  /** helper methods for constructing tree. */
  object TreeUtils {
    import LearningNode._

    /** construct a full binary tree with same impurityStats. */
    def fillBinaryTree(impurityStats: ImpurityStats)(maxHeight: Int): LearningNode = {
      def create(id: Int, height: Int): LearningNode = {
        if (height == 0) {
          new LearningNode(id, None, None, None, true, impurityStats)

        } else {
          val leftNode = create(leftChildIndex(id), height - 1)
          val rightNode = create(rightChildIndex(id), height - 1)
          // use id to help locate node when debug.
          val split = new ContinuousSplit(id, id)

          new LearningNode(
            id, Some(leftNode), Some(rightNode),
            Some(split), false, impurityStats)
        }
      }

      create(1, maxHeight)
    }

    /** create an ImpurityStats for classification. */
    def makeImpurityStats(numClass: Int, predictClassId: Int): ImpurityStats = {
      val stat = Array.fill(numClass)(0.0)
      stat(predictClassId) = 1.0

      val calculator = new GiniCalculator(stat)

      new ImpurityStats(0.0, 0.0, calculator, null, null)
    }

    /** Full description of model */
    def toDebugTreeString(node: LearningNode, indent: Int = 0): String = {
      val prefix: String = "   " * indent
      val id = node.id

      if (node.isLeaf) {
        val predict = node.stats.impurityCalculator.predict

        prefix + id + "(" + predict + ")" + "\n"

      } else {
        val left = toDebugTreeString(node.leftChild.get, indent + 1)
        val right = toDebugTreeString(node.rightChild.get, indent + 1)

        left +
        prefix + id + "--|\n" +
        right
      }
    }
  }

  /** helper methods used to operate nodes. */
  object NodeUtils {
    import LearningNode._

    /** assign the ImpurityStats to all nodes required in nodeIds. */
    def setPredictValue(root: LearningNode,
                        nodeIds: Array[Int],
                        impurityStats: ImpurityStats): Unit =
      nodeIds.foreach { id =>
        val node = getNode(id, root)

        node.stats = impurityStats
      }

    /** set internal nodes as leaf. */
    def setAsLeaf(root: LearningNode, nodeIds: Array[Int]): Unit =
      nodeIds.foreach { id =>
        val node = getNode(id, root)

        if (! node.isLeaf) {
          removeChildren(node)
          node.isLeaf = true
        }
      }
  }
}
