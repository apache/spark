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

package org.apache.spark.mllib.stat

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.TestingUtils._

class MultivariateOnlineSummarizerSuite extends FunSuite {

  test("basic error handing") {
    val summarizer = new MultivariateOnlineSummarizer

    assert(summarizer.count === 0, "should be zero since nothing is added.")

    withClue("Getting numNonzeros from empty summarizer should throw exception.") {
      intercept[IllegalArgumentException] {
        summarizer.numNonzeros
      }
    }

    withClue("Getting variance from empty summarizer should throw exception.") {
      intercept[IllegalArgumentException] {
        summarizer.variance
      }
    }

    withClue("Getting mean from empty summarizer should throw exception.") {
      intercept[IllegalArgumentException] {
        summarizer.mean
      }
    }

    withClue("Getting max from empty summarizer should throw exception.") {
      intercept[IllegalArgumentException] {
        summarizer.max
      }
    }

    withClue("Getting min from empty summarizer should throw exception.") {
      intercept[IllegalArgumentException] {
        summarizer.min
      }
    }

    summarizer.add(Vectors.dense(-1.0, 2.0, 6.0)).add(Vectors.sparse(3, Seq((0, -2.0), (1, 6.0))))

    withClue("Adding a new dense sample with different array size should throw exception.") {
      intercept[IllegalArgumentException] {
        summarizer.add(Vectors.dense(3.0, 1.0))
      }
    }

    withClue("Adding a new sparse sample with different array size should throw exception.") {
      intercept[IllegalArgumentException] {
        summarizer.add(Vectors.sparse(5, Seq((0, -2.0), (1, 6.0))))
      }
    }

    val summarizer2 = (new MultivariateOnlineSummarizer).add(Vectors.dense(1.0, -2.0, 0.0, 4.0))
    withClue("Merging a new summarizer with different dimensions should throw exception.") {
      intercept[IllegalArgumentException] {
        summarizer.merge(summarizer2)
      }
    }
  }

  test("dense vector input") {
    // For column 2, the maximum will be 0.0, and it's not explicitly added since we ignore all
    // the zeros; it's a case we need to test. For column 3, the minimum will be 0.0 which we
    // need to test as well.
    val summarizer = (new MultivariateOnlineSummarizer)
      .add(Vectors.dense(-1.0, 0.0, 6.0))
      .add(Vectors.dense(3.0, -3.0, 0.0))

    assert(summarizer.mean ~== Vectors.dense(1.0, -1.5, 3.0) absTol 1E-5, "mean mismatch")

    assert(summarizer.min ~== Vectors.dense(-1.0, -3, 0.0) absTol 1E-5, "min mismatch")

    assert(summarizer.max ~== Vectors.dense(3.0, 0.0, 6.0) absTol 1E-5, "max mismatch")

    assert(summarizer.numNonzeros ~== Vectors.dense(2, 1, 1) absTol 1E-5, "numNonzeros mismatch")

    assert(summarizer.variance ~== Vectors.dense(8.0, 4.5, 18.0) absTol 1E-5, "variance mismatch")

    assert(summarizer.count === 2)
  }

  test("sparse vector input") {
    val summarizer = (new MultivariateOnlineSummarizer)
      .add(Vectors.sparse(3, Seq((0, -1.0), (2, 6.0))))
      .add(Vectors.sparse(3, Seq((0, 3.0), (1, -3.0))))

    assert(summarizer.mean ~== Vectors.dense(1.0, -1.5, 3.0) absTol 1E-5, "mean mismatch")

    assert(summarizer.min ~== Vectors.dense(-1.0, -3, 0.0) absTol 1E-5, "min mismatch")

    assert(summarizer.max ~== Vectors.dense(3.0, 0.0, 6.0) absTol 1E-5, "max mismatch")

    assert(summarizer.numNonzeros ~== Vectors.dense(2, 1, 1) absTol 1E-5, "numNonzeros mismatch")

    assert(summarizer.variance ~== Vectors.dense(8.0, 4.5, 18.0) absTol 1E-5, "variance mismatch")

    assert(summarizer.count === 2)
  }

  test("mixing dense and sparse vector input") {
    val summarizer = (new MultivariateOnlineSummarizer)
      .add(Vectors.sparse(3, Seq((0, -2.0), (1, 2.3))))
      .add(Vectors.dense(0.0, -1.0, -3.0))
      .add(Vectors.sparse(3, Seq((1, -5.1))))
      .add(Vectors.dense(3.8, 0.0, 1.9))
      .add(Vectors.dense(1.7, -0.6, 0.0))
      .add(Vectors.sparse(3, Seq((1, 1.9), (2, 0.0))))

    assert(summarizer.mean ~==
      Vectors.dense(0.583333333333, -0.416666666666, -0.183333333333) absTol 1E-5, "mean mismatch")

    assert(summarizer.min ~== Vectors.dense(-2.0, -5.1, -3) absTol 1E-5, "min mismatch")

    assert(summarizer.max ~== Vectors.dense(3.8, 2.3, 1.9) absTol 1E-5, "max mismatch")

    assert(summarizer.numNonzeros ~== Vectors.dense(3, 5, 2) absTol 1E-5, "numNonzeros mismatch")

    assert(summarizer.variance ~==
      Vectors.dense(3.857666666666, 7.0456666666666, 2.48166666666666) absTol 1E-5,
      "variance mismatch")

    assert(summarizer.count === 6)
  }

  test("merging two summarizers") {
    val summarizer1 = (new MultivariateOnlineSummarizer)
      .add(Vectors.sparse(3, Seq((0, -2.0), (1, 2.3))))
      .add(Vectors.dense(0.0, -1.0, -3.0))

    val summarizer2 = (new MultivariateOnlineSummarizer)
      .add(Vectors.sparse(3, Seq((1, -5.1))))
      .add(Vectors.dense(3.8, 0.0, 1.9))
      .add(Vectors.dense(1.7, -0.6, 0.0))
      .add(Vectors.sparse(3, Seq((1, 1.9), (2, 0.0))))

    val summarizer = summarizer1.merge(summarizer2)

    assert(summarizer.mean ~==
      Vectors.dense(0.583333333333, -0.416666666666, -0.183333333333) absTol 1E-5, "mean mismatch")

    assert(summarizer.min ~== Vectors.dense(-2.0, -5.1, -3) absTol 1E-5, "min mismatch")

    assert(summarizer.max ~== Vectors.dense(3.8, 2.3, 1.9) absTol 1E-5, "max mismatch")

    assert(summarizer.numNonzeros ~== Vectors.dense(3, 5, 2) absTol 1E-5, "numNonzeros mismatch")

    assert(summarizer.variance ~==
      Vectors.dense(3.857666666666, 7.0456666666666, 2.48166666666666) absTol 1E-5,
      "variance mismatch")

    assert(summarizer.count === 6)
  }

  test("merging summarizer with empty summarizer") {
    // If one of two is non-empty, this should return the non-empty summarizer.
    // If both of them are empty, then just return the empty summarizer.
    val summarizer1 = (new MultivariateOnlineSummarizer)
      .add(Vectors.dense(0.0, -1.0, -3.0)).merge(new MultivariateOnlineSummarizer)
    assert(summarizer1.count === 1)

    val summarizer2 = (new MultivariateOnlineSummarizer)
      .merge((new MultivariateOnlineSummarizer).add(Vectors.dense(0.0, -1.0, -3.0)))
    assert(summarizer2.count === 1)

    val summarizer3 = (new MultivariateOnlineSummarizer).merge(new MultivariateOnlineSummarizer)
    assert(summarizer3.count === 0)

    assert(summarizer1.mean ~== Vectors.dense(0.0, -1.0, -3.0) absTol 1E-5, "mean mismatch")

    assert(summarizer2.mean ~== Vectors.dense(0.0, -1.0, -3.0) absTol 1E-5, "mean mismatch")

    assert(summarizer1.min ~== Vectors.dense(0.0, -1.0, -3.0) absTol 1E-5, "min mismatch")

    assert(summarizer2.min ~== Vectors.dense(0.0, -1.0, -3.0) absTol 1E-5, "min mismatch")

    assert(summarizer1.max ~== Vectors.dense(0.0, -1.0, -3.0) absTol 1E-5, "max mismatch")

    assert(summarizer2.max ~== Vectors.dense(0.0, -1.0, -3.0) absTol 1E-5, "max mismatch")

    assert(summarizer1.numNonzeros ~== Vectors.dense(0, 1, 1) absTol 1E-5, "numNonzeros mismatch")

    assert(summarizer2.numNonzeros ~== Vectors.dense(0, 1, 1) absTol 1E-5, "numNonzeros mismatch")

    assert(summarizer1.variance ~== Vectors.dense(0, 0, 0) absTol 1E-5, "variance mismatch")

    assert(summarizer2.variance ~== Vectors.dense(0, 0, 0) absTol 1E-5, "variance mismatch")
  }
}
