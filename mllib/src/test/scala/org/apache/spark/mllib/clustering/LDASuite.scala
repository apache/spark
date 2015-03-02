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

package org.apache.spark.mllib.clustering

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.{DenseMatrix, Matrix, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class LDASuite extends FunSuite with MLlibTestSparkContext {

  import LDASuite._

  test("LocalLDAModel") {
    val model = new LocalLDAModel(tinyTopics)

    // Check: basic parameters
    assert(model.k === tinyK)
    assert(model.vocabSize === tinyVocabSize)
    assert(model.topicsMatrix === tinyTopics)

    // Check: describeTopics() with all terms
    val fullTopicSummary = model.describeTopics()
    assert(fullTopicSummary.size === tinyK)
    fullTopicSummary.zip(tinyTopicDescription).foreach {
      case ((algTerms, algTermWeights), (terms, termWeights)) =>
        assert(algTerms === terms)
        assert(algTermWeights === termWeights)
    }

    // Check: describeTopics() with some terms
    val smallNumTerms = 3
    val smallTopicSummary = model.describeTopics(maxTermsPerTopic = smallNumTerms)
    smallTopicSummary.zip(tinyTopicDescription).foreach {
      case ((algTerms, algTermWeights), (terms, termWeights)) =>
        assert(algTerms === terms.slice(0, smallNumTerms))
        assert(algTermWeights === termWeights.slice(0, smallNumTerms))
    }
  }

  test("running and DistributedLDAModel") {
    val k = 3
    val topicSmoothing = 1.2
    val termSmoothing = 1.2

    // Train a model
    val lda = new LDA()
    lda.setK(k)
      .setDocConcentration(topicSmoothing)
      .setTopicConcentration(termSmoothing)
      .setMaxIterations(5)
      .setSeed(12345)
    val corpus = sc.parallelize(tinyCorpus, 2)

    val model: DistributedLDAModel = lda.run(corpus)

    // Check: basic parameters
    val localModel = model.toLocal
    assert(model.k === k)
    assert(localModel.k === k)
    assert(model.vocabSize === tinyVocabSize)
    assert(localModel.vocabSize === tinyVocabSize)
    assert(model.topicsMatrix === localModel.topicsMatrix)

    // Check: topic summaries
    //  The odd decimal formatting and sorting is a hack to do a robust comparison.
    val roundedTopicSummary = model.describeTopics().map { case (terms, termWeights) =>
      // cut values to 3 digits after the decimal place
      terms.zip(termWeights).map { case (term, weight) =>
        ("%.3f".format(weight).toDouble, term.toInt)
      }
    }.sortBy(_.mkString(""))
    val roundedLocalTopicSummary = localModel.describeTopics().map { case (terms, termWeights) =>
      // cut values to 3 digits after the decimal place
      terms.zip(termWeights).map { case (term, weight) =>
        ("%.3f".format(weight).toDouble, term.toInt)
      }
    }.sortBy(_.mkString(""))
    roundedTopicSummary.zip(roundedLocalTopicSummary).foreach { case (t1, t2) =>
      assert(t1 === t2)
    }

    // Check: per-doc topic distributions
    val topicDistributions = model.topicDistributions.collect()
    //  Ensure all documents are covered.
    assert(topicDistributions.size === tinyCorpus.size)
    assert(tinyCorpus.map(_._1).toSet === topicDistributions.map(_._1).toSet)
    //  Ensure we have proper distributions
    topicDistributions.foreach { case (docId, topicDistribution) =>
      assert(topicDistribution.size === tinyK)
      assert(topicDistribution.toArray.sum ~== 1.0 absTol 1e-5)
    }

    // Check: log probabilities
    assert(model.logLikelihood < 0.0)
    assert(model.logPrior < 0.0)
  }

  test("vertex indexing") {
    // Check vertex ID indexing and conversions.
    val docIds = Array(0, 1, 2)
    val docVertexIds = docIds
    val termIds = Array(0, 1, 2)
    val termVertexIds = Array(-1, -2, -3)
    assert(docVertexIds.forall(i => !LDA.isTermVertex((i.toLong, 0))))
    assert(termIds.map(LDA.term2index) === termVertexIds)
    assert(termVertexIds.map(i => LDA.index2term(i.toLong)) === termIds)
    assert(termVertexIds.forall(i => LDA.isTermVertex((i.toLong, 0))))
  }
}

private[clustering] object LDASuite {

  def tinyK: Int = 3
  def tinyVocabSize: Int = 5
  def tinyTopicsAsArray: Array[Array[Double]] = Array(
    Array[Double](0.1, 0.2, 0.3, 0.4, 0.0), // topic 0
    Array[Double](0.5, 0.05, 0.05, 0.1, 0.3), // topic 1
    Array[Double](0.2, 0.2, 0.05, 0.05, 0.5) // topic 2
  )
  def tinyTopics: Matrix = new DenseMatrix(numRows = tinyVocabSize, numCols = tinyK,
    values = tinyTopicsAsArray.fold(Array.empty[Double])(_ ++ _))
  def tinyTopicDescription: Array[(Array[Int], Array[Double])] = tinyTopicsAsArray.map { topic =>
    val (termWeights, terms) = topic.zipWithIndex.sortBy(-_._1).unzip
    (terms.toArray, termWeights.toArray)
  }

  def tinyCorpus = Array(
    Vectors.dense(1, 3, 0, 2, 8),
    Vectors.dense(0, 2, 1, 0, 4),
    Vectors.dense(2, 3, 12, 3, 1),
    Vectors.dense(0, 3, 1, 9, 8),
    Vectors.dense(1, 1, 4, 2, 6)
  ).zipWithIndex.map { case (wordCounts, docId) => (docId.toLong, wordCounts) }
  assert(tinyCorpus.forall(_._2.size == tinyVocabSize)) // sanity check for test data

}
