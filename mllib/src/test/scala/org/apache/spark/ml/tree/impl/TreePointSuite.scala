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

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.tree.{ContinuousSplit, Split}
import org.apache.spark.mllib.tree.configuration.{Algo, Strategy}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.SparkFunSuite

/**
 * Test suite for [[TreePoint]].
 */
class TreePointSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("LabeledPoint RDD: dense presentation when numFeatures < 10000") {
    val arr = Array(
      LabeledPoint(0, Vectors.dense(Array(-0.5, 0.0, 1.5, 2.0, 3.5))),
      LabeledPoint(1, Vectors.dense(Array(0.0, 0, 0, 0, 0))))
    val rdd = sc.parallelize(arr)
    // all share the same splits: [0.0, 1.0, 2.0, 3.0]
    val splits = Array.fill(5)(Range(0, 4).map(i => new ContinuousSplit(i, i)).toArray[Split])
    val metadata = DecisionTreeMetadata.buildMetadata(
      rdd,
      Strategy.defaultStrategy(Algo.Classification))

    val results = TreePoint.convertToTreeRDD(rdd, splits, metadata)
      .collect()
      .map(_.asInstanceOf[TreeDensePoint])

    assert(results(0).label === 0)
    assert(results(0)._binnedFeatures === Array(0, 0, 2, 2, 4))
    assert(results(1).label === 1)
    assert(results(1)._binnedFeatures === Array(0, 0, 0, 0, 0))
  }

  test("LabeledPoint RDD: sparse presentation when numFeatures >= 10000") {
    val arr = Array(
      LabeledPoint(0, Vectors.sparse(20000, Range(0, 5).toArray, Array(-0.5, 0.0, 1.5, 2.0, 3.5))),
      LabeledPoint(1, Vectors.sparse(20000, Array.emptyIntArray, Array.emptyDoubleArray)))
    val rdd = sc.parallelize(arr)
    // all share the same splits: [0.0, 1.0, 2.0, 3.0]
    val splits = Array.fill(20000)(Range(0, 4).map(i => new ContinuousSplit(i, i)).toArray[Split])
    val metadata = DecisionTreeMetadata.buildMetadata(
      rdd,
      Strategy.defaultStrategy(Algo.Classification))

    val results = TreePoint.convertToTreeRDD(rdd, splits, metadata)
      .collect()
      .map(_.asInstanceOf[TreeSparsePoint])

    assert(results(0).label === 0)
    assert(results(0)._binnedFeatures ===
      Vectors.sparse(20000, Array((2, 2.0), (3, 2.0), (4, 4.0))).asBreeze)
    assert(results(1).label === 1)
    assert(results(1)._binnedFeatures ===
      Vectors.sparse(20000, Array.empty[(Int, Double)]).asBreeze)
  }
}
