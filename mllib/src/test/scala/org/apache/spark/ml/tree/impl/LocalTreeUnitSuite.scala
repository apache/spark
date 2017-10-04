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

package org.apache.spark.ml.tree.impl

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.tree._
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext

/** Unit tests for helper classes/methods specific to local tree training */
class LocalTreeUnitSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("Fit a single decision tree regressor on constant features") {
    // Generate constant, continuous data
    val data = sc.parallelize(Range(0, 8).map(_ => LabeledPoint(1, Vectors.dense(1))))
    val df = spark.sqlContext.createDataFrame(data)
    // Initialize estimator
    val dt = new LocalDecisionTreeRegressor()
      .setFeaturesCol("features") // indexedFeatures
      .setLabelCol("label")
      .setMaxDepth(3)
    // Fit model
    val model = dt.fit(df)
    assert(model.rootNode.isInstanceOf[LeafNode])
    val root = model.rootNode.asInstanceOf[LeafNode]
    assert(root.prediction == 1)
  }

  test("Fit a single decision tree regressor on some continuous features") {
    // Generate continuous data
    val data = sc.parallelize(Range(0, 8).map(x => LabeledPoint(x, Vectors.dense(x))))
    val df = spark.createDataFrame(data)
    // Initialize estimator
    val dt = new LocalDecisionTreeRegressor()
      .setFeaturesCol("features") // indexedFeatures
      .setLabelCol("label")
      .setMaxDepth(3)
    // Fit model
    val model = dt.fit(df)

    // Check that model is of depth 3 (the specified max depth) and that leaf/internal nodes have
    // the correct class.
    // Validate root
    assert(model.rootNode.isInstanceOf[InternalNode])
    // Validate first level of tree (nodes with depth = 1)
    val root = model.rootNode.asInstanceOf[InternalNode]
    assert(root.leftChild.isInstanceOf[InternalNode] && root.rightChild.isInstanceOf[InternalNode])
    // Validate second and third levels of tree (nodes with depth = 2 or 3)
    val left = root.leftChild.asInstanceOf[InternalNode]
    val right = root.rightChild.asInstanceOf[InternalNode]
    val grandkids = Array(left.leftChild, left.rightChild, right.leftChild, right.rightChild)
    grandkids.foreach { grandkid =>
      assert(grandkid.isInstanceOf[InternalNode])
      val grandkidNode = grandkid.asInstanceOf[InternalNode]
      assert(grandkidNode.leftChild.isInstanceOf[LeafNode])
      assert(grandkidNode.rightChild.isInstanceOf[LeafNode])
    }
  }

  test("Fit deep local trees") {

    /** Helper method: get depth of subtree rooted at passed-in node. */
    def getTreeDepth(node: Node): Int = {
      node match {
        case internal: InternalNode =>
          1 + math.max(getTreeDepth(internal.leftChild), getTreeDepth(internal.rightChild))
        case _: LeafNode =>
          0
        case other => throw new UnsupportedOperationException("Decision tree node must be " +
          s"LeafNode or InternalNode, got ${other.getClass}")
      }
    }

    /**
     * Deep tree test. Tries to fit tree on synthetic data designed to force tree
     * to split to specified depth.
     */
    def deepTreeTest(depth: Int): Unit = {
      val data = sc.parallelize(Range(0, depth + 1).map { idx =>
        val features = Array.fill[Double](depth)(1)
        if (idx == depth) {
          LabeledPoint(1.0, Vectors.dense(features))
        } else {
          features(idx) = 0.0
          LabeledPoint(0.0, Vectors.dense(features))
        }
      })
      val df = spark.createDataFrame(data)
      // Construct estimators; single-tree random forest & decision tree regressor.
      val localTree = new LocalDecisionTreeRegressor()
        .setFeaturesCol("features") // indexedFeatures
        .setLabelCol("label")
        .setMaxDepth(depth)
        .setMinInfoGain(0.0)

      // Fit model, check depth...
      val localModel = localTree.fit(df)
      assert(getTreeDepth(localModel.rootNode) == depth)
    }

    // Test small depth tree
    deepTreeTest(10)
    // Test medium depth tree
    deepTreeTest(40)
    // Test high depth tree
    deepTreeTest(200)
  }

}
