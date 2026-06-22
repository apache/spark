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

package org.apache.spark.ml

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.{MinMaxScaler, StringIndexer, Word2Vec}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext

class ModelSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("SPARK-57521: estimatedSize should not include parent's reachable object graph") {
    val df = spark.createDataFrame(Seq(
      Tuple1("a"), Tuple1("b"), Tuple1("c")
    )).toDF("label")

    val model = new StringIndexer()
      .setInputCol("label").setOutputCol("idx")
      .fit(df)

    assert(model.hasParent, "model should have parent after fit()")
    val size = model.estimatedSize

    // Model data is 3 string labels + overhead, well under 50KB.
    // Without the fix, SizeEstimator traverses model.parent -> estimator ->
    // SparkSession, counting hundreds of KB (local) to hundreds of MB
    // (production cluster) of shared session state per model.
    assert(size < 50 * 1024,
      s"estimatedSize ($size bytes) should reflect model data only, " +
      s"not the parent estimator's reachable object graph (SparkSession)")

    assert(model.hasParent, "parent should be preserved after estimatedSize call")
  }

  test("SPARK-57521: estimatedSize excludes parent for multiple estimator types") {
    // The issue affects all estimators that execute DataFrame operations during fit().
    // Test with a second estimator type to confirm the fix is not estimator-specific.
    val df = spark.createDataFrame(Seq(
      Tuple1(Vectors.dense(1.0, 2.0)),
      Tuple1(Vectors.dense(3.0, 4.0))
    )).toDF("features")

    val model = new MinMaxScaler()
      .setInputCol("features").setOutputCol("scaled")
      .fit(df)

    assert(model.hasParent)
    val size = model.estimatedSize

    assert(size < 50 * 1024,
      s"MinMaxScalerModel estimatedSize ($size bytes) should not include " +
      s"parent's reachable object graph")
    assert(model.hasParent, "parent should be preserved after estimatedSize call")
  }

  test("SPARK-57521: Word2Vec estimatedSize grows with vocabulary size") {
    // Word2VecModel stores learned vectors in a @transient field. The fix must not
    // exclude transient fields wholesale — only the parent estimator's reference to shared
    // infrastructure should be excluded. This test ensures Word2Vec's learned state
    // is properly reflected in estimatedSize.
    val smallData = spark.createDataFrame(Seq(
      Tuple1(Array("a", "b")),
      Tuple1(Array("b", "c"))
    )).toDF("text")

    val largeData = spark.createDataFrame(Seq(
      Tuple1(Array("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")),
      Tuple1(Array("k", "l", "m", "n", "o", "p", "q", "r", "s", "t"))
    )).toDF("text")

    val w2v = new Word2Vec()
      .setInputCol("text").setOutputCol("result")
      .setVectorSize(10).setMinCount(0)

    val smallModel = w2v.fit(smallData)
    val largeModel = w2v.fit(largeData)

    val smallSize = smallModel.estimatedSize
    val largeSize = largeModel.estimatedSize

    // Larger vocabulary should produce a measurably larger model
    assert(largeSize > smallSize,
      s"Word2Vec estimatedSize should grow with vocabulary: " +
      s"small=$smallSize, large=$largeSize")

    // Both should still be reasonable (not inflated by SparkSession)
    assert(smallSize < 100 * 1024,
      s"Small Word2Vec model ($smallSize bytes) should not include SparkSession state")
  }
}
