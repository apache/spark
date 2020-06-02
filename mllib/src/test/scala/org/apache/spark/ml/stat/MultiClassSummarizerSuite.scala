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
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.util.TestingUtils._

class MultiClassSummarizerSuite extends SparkFunSuite {

  test("MultiClassSummarizer") {
    val summarizer1 = (new MultiClassSummarizer)
      .add(0.0).add(3.0).add(4.0).add(3.0).add(6.0)
    assert(summarizer1.histogram === Array[Double](1, 0, 0, 2, 1, 0, 1))
    assert(summarizer1.countInvalid === 0)
    assert(summarizer1.numClasses === 7)

    val summarizer2 = (new MultiClassSummarizer)
      .add(1.0).add(5.0).add(3.0).add(0.0).add(4.0).add(1.0)
    assert(summarizer2.histogram === Array[Double](1, 2, 0, 1, 1, 1))
    assert(summarizer2.countInvalid === 0)
    assert(summarizer2.numClasses === 6)

    val summarizer3 = (new MultiClassSummarizer)
      .add(0.0).add(1.3).add(5.2).add(2.5).add(2.0).add(4.0).add(4.0).add(4.0).add(1.0)
    assert(summarizer3.histogram === Array[Double](1, 1, 1, 0, 3))
    assert(summarizer3.countInvalid === 3)
    assert(summarizer3.numClasses === 5)

    val summarizer4 = (new MultiClassSummarizer)
      .add(3.1).add(4.3).add(2.0).add(1.0).add(3.0)
    assert(summarizer4.histogram === Array[Double](0, 1, 1, 1))
    assert(summarizer4.countInvalid === 2)
    assert(summarizer4.numClasses === 4)

    val summarizer5 = new MultiClassSummarizer
    assert(summarizer5.histogram.isEmpty)
    assert(summarizer5.numClasses === 0)

    // small map merges large one
    val summarizerA = summarizer1.merge(summarizer2)
    assert(summarizerA.hashCode() === summarizer2.hashCode())
    assert(summarizerA.histogram === Array[Double](2, 2, 0, 3, 2, 1, 1))
    assert(summarizerA.countInvalid === 0)
    assert(summarizerA.numClasses === 7)

    // large map merges small one
    val summarizerB = summarizer3.merge(summarizer4)
    assert(summarizerB.hashCode() === summarizer3.hashCode())
    assert(summarizerB.histogram === Array[Double](1, 2, 2, 1, 3))
    assert(summarizerB.countInvalid === 5)
    assert(summarizerB.numClasses === 5)
  }

  test("MultiClassSummarizer with weighted samples") {
    val summarizer1 = (new MultiClassSummarizer)
      .add(label = 0.0, weight = 0.2).add(3.0, 0.8).add(4.0, 3.2).add(3.0, 1.3).add(6.0, 3.1)
    assert(Vectors.dense(summarizer1.histogram) ~==
      Vectors.dense(Array(0.2, 0, 0, 2.1, 3.2, 0, 3.1)) absTol 1E-10)
    assert(summarizer1.countInvalid === 0)
    assert(summarizer1.numClasses === 7)

    val summarizer2 = (new MultiClassSummarizer)
      .add(1.0, 1.1).add(5.0, 2.3).add(3.0).add(0.0).add(4.0).add(1.0).add(2, 0.0)
    assert(Vectors.dense(summarizer2.histogram) ~==
      Vectors.dense(Array[Double](1.0, 2.1, 0.0, 1, 1, 2.3)) absTol 1E-10)
    assert(summarizer2.countInvalid === 0)
    assert(summarizer2.numClasses === 6)

    val summarizer = summarizer1.merge(summarizer2)
    assert(Vectors.dense(summarizer.histogram) ~==
      Vectors.dense(Array(1.2, 2.1, 0.0, 3.1, 4.2, 2.3, 3.1)) absTol 1E-10)
    assert(summarizer.countInvalid === 0)
    assert(summarizer.numClasses === 7)
  }
}
