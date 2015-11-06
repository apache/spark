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
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.impl.TreeTests
import org.apache.spark.ml.tree.{ContinuousSplit, DecisionTreeModel, LeafNode, Node}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.tree.impurity.GiniCalculator
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.util.collection.OpenHashMap

/**
 * Test suite for [[CodeGenerationDecisionTreeModel]].
 */
class CodeGenerationDecisionTreeModelSuite extends SparkFunSuite with MLlibTestSparkContext {
  // Codegened trees of just a leaf should always return that score
  test("leaf node conversion") {
    val scores = List(-1.0, -0.1, 0.0, 0.1, 1.0, 100.0)
    // impurity ignored by codegen model, so use a junk value
    val imp = new GiniCalculator(Array(1.0, 5.0, 1.0))
    val nodes = scores.map(new LeafNode(_, imp.calculate, imp))
    val predictors = nodes.map(CodeGenerationDecisionTreeModel.getScorer(_))
    val input = Vectors.dense(0)
    val results = predictors.map(_(input))
    (scores zip results) foreach {
      case (e, v) =>
        assert(e ~== v absTol 1E-5)
    }
  }

}
