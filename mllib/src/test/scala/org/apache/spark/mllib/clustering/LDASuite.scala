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

import breeze.linalg.{DenseMatrix => BDM}

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.{Vector, DenseMatrix, Matrix, Vectors}
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
    assert(fullTopicSummary.length === tinyK)
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

  test("running and DistributedLDAModel with default Optimizer (EM)") {
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

    val model: DistributedLDAModel = lda.run(corpus).asInstanceOf[DistributedLDAModel]

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
    assert(topicDistributions.length === tinyCorpus.length)
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

  test("setter alias") {
    val lda = new LDA().setAlpha(2.0).setBeta(3.0)
    assert(lda.getAlpha === 2.0)
    assert(lda.getDocConcentration === 2.0)
    assert(lda.getBeta === 3.0)
    assert(lda.getTopicConcentration === 3.0)
  }

  test("OnlineLDAOptimizer initialization") {
    val lda = new LDA().setK(2)
    val corpus = sc.parallelize(tinyCorpus, 2)
    val op = new OnlineLDAOptimizer().initialize(corpus, lda)
    op.setKappa(0.9876).setMiniBatchFraction(0.123).setTau_0(567)
    assert(op.getAlpha == 0.5) // default 1.0 / k
    assert(op.getEta == 0.5)   // default 1.0 / k
    assert(op.getKappa == 0.9876)
    assert(op.getMiniBatchFraction == 0.123)
    assert(op.getTau_0 == 567)
  }

  test("OnlineLDAOptimizer one iteration") {
    // run OnlineLDAOptimizer for 1 iteration to verify it's consistency with Blei-lab,
    // [[https://github.com/Blei-Lab/onlineldavb]]
    val k = 2
    val vocabSize = 6

    def docs: Array[(Long, Vector)] = Array(
      Vectors.sparse(vocabSize, Array(0, 1, 2), Array(1, 1, 1)), // apple, orange, banana
      Vectors.sparse(vocabSize, Array(3, 4, 5), Array(1, 1, 1)) // tiger, cat, dog
    ).zipWithIndex.map { case (wordCounts, docId) => (docId.toLong, wordCounts) }
    val corpus = sc.parallelize(docs, 2)

    // Set GammaShape large to avoid the stochastic impact.
    val op = new OnlineLDAOptimizer().setTau_0(1024).setKappa(0.51).setGammaShape(1e40)
      .setMiniBatchFraction(1)
    val lda = new LDA().setK(k).setMaxIterations(1).setOptimizer(op).setSeed(12345)

    val state = op.initialize(corpus, lda)
    // override lambda to simulate an intermediate state
    //    [[ 1.1  1.2  1.3  0.9  0.8  0.7]
    //     [ 0.9  0.8  0.7  1.1  1.2  1.3]]
    op.setLambda(new BDM[Double](k, vocabSize,
      Array(1.1, 0.9, 1.2, 0.8, 1.3, 0.7, 0.9, 1.1, 0.8, 1.2, 0.7, 1.3)))

    // run for one iteration
    state.submitMiniBatch(corpus)

    // verify the result, Note this generate the identical result as
    // [[https://github.com/Blei-Lab/onlineldavb]]
    val topic1 = op.getLambda(0, ::).inner.toArray.map("%.4f".format(_)).mkString(", ")
    val topic2 = op.getLambda(1, ::).inner.toArray.map("%.4f".format(_)).mkString(", ")
    assert("1.1101, 1.2076, 1.3050, 0.8899, 0.7924, 0.6950" == topic1)
    assert("0.8899, 0.7924, 0.6950, 1.1101, 1.2076, 1.3050" == topic2)
  }

  test("OnlineLDAOptimizer with toy data") {
    def toydata: Array[(Long, Vector)] = Array(
      Vectors.sparse(6, Array(0, 1), Array(1, 1)),
      Vectors.sparse(6, Array(1, 2), Array(1, 1)),
      Vectors.sparse(6, Array(0, 2), Array(1, 1)),
      Vectors.sparse(6, Array(3, 4), Array(1, 1)),
      Vectors.sparse(6, Array(3, 5), Array(1, 1)),
      Vectors.sparse(6, Array(4, 5), Array(1, 1))
    ).zipWithIndex.map { case (wordCounts, docId) => (docId.toLong, wordCounts) }

    val docs = sc.parallelize(toydata)
    val op = new OnlineLDAOptimizer().setMiniBatchFraction(1).setTau_0(1024).setKappa(0.51)
      .setGammaShape(1e10)
    val lda = new LDA().setK(2)
      .setDocConcentration(0.01)
      .setTopicConcentration(0.01)
      .setMaxIterations(100)
      .setOptimizer(op)
      .setSeed(12345)

    val ldaModel = lda.run(docs)
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights)
    }

    // check distribution for each topic, typical distribution is (0.3, 0.3, 0.3, 0.02, 0.02, 0.02)
    topics.foreach { topic =>
      val smalls = topic.filter(t => t._2 < 0.1).map(_._2)
      assert(smalls.length == 3 && smalls.sum < 0.2)
    }
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

  def tinyCorpus: Array[(Long, Vector)] = Array(
    Vectors.dense(1, 3, 0, 2, 8),
    Vectors.dense(0, 2, 1, 0, 4),
    Vectors.dense(2, 3, 12, 3, 1),
    Vectors.dense(0, 3, 1, 9, 8),
    Vectors.dense(1, 1, 4, 2, 6)
  ).zipWithIndex.map { case (wordCounts, docId) => (docId.toLong, wordCounts) }
  assert(tinyCorpus.forall(_._2.size == tinyVocabSize)) // sanity check for test data

}
