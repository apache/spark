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

package org.apache.spark.ml.stat

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.Instance
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class SummarizersSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("getRegressionSummarizers") {
    val instances = sc.parallelize(Seq(
      Instance(1.0, 1.0, Vectors.dense(-2.0, 0.0).toSparse),
      Instance(3.0, 1.0, Vectors.dense(0.0, 4.0))))
    val (featureSummarizer, labelSummarizer) =
      Summarizers.getRegressionSummarizers(instances)

    // verify feature summarizer
    assert(featureSummarizer.mean ~== Vectors.dense(-1.0, 2.0) absTol 1E-5)
    assert(featureSummarizer.min ~== Vectors.dense(-2.0, 0.0) absTol 1E-5)
    assert(featureSummarizer.max ~== Vectors.dense(0.0, 4.0) absTol 1E-5)
    assert(featureSummarizer.numNonzeros ~== Vectors.dense(1, 1) absTol 1E-5)
    assert(featureSummarizer.variance ~== Vectors.dense(2.0, 8.0) absTol 1E-5)
    assert(featureSummarizer.count === 2)

    // verify regression label summarizer
    assert(labelSummarizer.mean ~== Vectors.dense(2.0) absTol 1E-5)
    assert(labelSummarizer.min ~== Vectors.dense(1.0) absTol 1E-5)
    assert(labelSummarizer.max ~== Vectors.dense(3.0) absTol 1E-5)
    assert(labelSummarizer.numNonzeros ~== Vectors.dense(2.0) absTol 1E-5)
    assert(labelSummarizer.variance ~== Vectors.dense(2.0) absTol 1E-5)
    assert(labelSummarizer.count === 2)
  }

  test("getClassificationSummarizers") {
    val instances = sc.parallelize(Seq(
      Instance(0.0, 1.0, Vectors.dense(-2.0, 0.0).toSparse),
      Instance(1.0, 9.0, Vectors.dense(0.0, 4.0))))
    val (featureSummarizer, labelSummarizer) =
      Summarizers.getClassificationSummarizers(instances)

    // verify feature summarizer
    assert(featureSummarizer.mean ~== Vectors.dense(-0.2, 3.6) absTol 1E-5)
    assert(featureSummarizer.min ~== Vectors.dense(-2.0, 0.0) absTol 1E-5)
    assert(featureSummarizer.max ~== Vectors.dense(0.0, 4.0) absTol 1E-5)
    assert(featureSummarizer.numNonzeros ~== Vectors.dense(1, 1) absTol 1E-5)
    assert(featureSummarizer.count === 2)

    // verify classification label summarizer
    assert(labelSummarizer.histogram === Array[Double](1, 9))
    assert(labelSummarizer.countInvalid === 0)
    assert(labelSummarizer.numClasses === 2)
  }
}
