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

import java.util.{ArrayList => JArrayList}

import breeze.linalg.{argmax, argtopk, max, DenseMatrix => BDM}

import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx.Edge
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrix, Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.util.Utils

class LDASuite extends SparkFunSuite with MLlibTestSparkContext {

  import LDASuite._

  test("LocalLDAModel") {
    val model = new LocalLDAModel(tinyTopics,
      Vectors.dense(Array.fill(tinyTopics.numRows)(1.0 / tinyTopics.numRows)), 1D, 100D)

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
      .setOptimizer(new EMLDAOptimizer)
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
    val topicSummary = model.describeTopics().map { case (terms, termWeights) =>
      Vectors.sparse(tinyVocabSize, terms, termWeights)
    }.sortBy(_.toString)
    val localTopicSummary = localModel.describeTopics().map { case (terms, termWeights) =>
      Vectors.sparse(tinyVocabSize, terms, termWeights)
    }.sortBy(_.toString)
    topicSummary.zip(localTopicSummary).foreach { case (topics, topicsLocal) =>
      assert(topics ~== topicsLocal absTol 0.01)
    }

    // Check: per-doc topic distributions
    val topicDistributions = model.topicDistributions.collect()

    //  Ensure all documents are covered.
    // SPARK-5562. since the topicDistribution returns the distribution of the non empty docs
    // over topics. Compare it against nonEmptyTinyCorpus instead of tinyCorpus
    val nonEmptyTinyCorpus = getNonEmptyDoc(tinyCorpus)
    assert(topicDistributions.length === nonEmptyTinyCorpus.length)
    assert(nonEmptyTinyCorpus.map(_._1).toSet === topicDistributions.map(_._1).toSet)
    //  Ensure we have proper distributions
    topicDistributions.foreach { case (docId, topicDistribution) =>
      assert(topicDistribution.size === tinyK)
      assert(topicDistribution.toArray.sum ~== 1.0 absTol 1e-5)
    }

    val top2TopicsPerDoc = model.topTopicsPerDocument(2).map(t => (t._1, (t._2, t._3)))
    model.topicDistributions.join(top2TopicsPerDoc).collect().foreach {
      case (docId, (topicDistribution, (indices, weights))) =>
        assert(indices.length == 2)
        assert(weights.length == 2)
        val bdvTopicDist = topicDistribution.asBreeze
        val top2Indices = argtopk(bdvTopicDist, 2)
        assert(top2Indices.toSet === indices.toSet)
        assert(bdvTopicDist(top2Indices).toArray.toSet === weights.toSet)
    }

    // Check: log probabilities
    assert(model.logLikelihood < 0.0)
    assert(model.logPrior < 0.0)

    // Check: topDocumentsPerTopic
    // Compare it with top documents per topic derived from topicDistributions
    val topDocsByTopicDistributions = { n: Int =>
      Range(0, k).map { topic =>
        val (doc, docWeights) = topicDistributions.sortBy(-_._2(topic)).take(n).unzip
        (doc.toArray, docWeights.map(_(topic)).toArray)
      }.toArray
    }

    // Top 3 documents per topic
    model.topDocumentsPerTopic(3).zip(topDocsByTopicDistributions(3)).foreach { case (t1, t2) =>
      assert(t1._1 === t2._1)
      assert(t1._2 === t2._2)
    }

    // All documents per topic
    val q = tinyCorpus.length
    model.topDocumentsPerTopic(q).zip(topDocsByTopicDistributions(q)).foreach { case (t1, t2) =>
      assert(t1._1 === t2._1)
      assert(t1._2 === t2._2)
    }

    // Check: topTopicAssignments
    // Make sure it assigns a topic to each term appearing in each doc.
    val topTopicAssignments: Map[Long, (Array[Int], Array[Int])] =
      model.topicAssignments.collect().map(x => x._1 -> ((x._2, x._3))).toMap
    assert(topTopicAssignments.keys.max < tinyCorpus.length)
    tinyCorpus.foreach { case (docID: Long, doc: Vector) =>
      if (topTopicAssignments.contains(docID)) {
        val (inds, vals) = topTopicAssignments(docID)
        assert(inds.length === doc.numNonzeros)
        // For "term" in actual doc,
        // check that it has a topic assigned.
        doc.foreachActive((term, wcnt) => assert(wcnt === 0 || inds.contains(term)))
      } else {
        assert(doc.numNonzeros === 0)
      }
    }
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
    assert(lda.getAsymmetricAlpha.toArray.forall(_ === 2.0))
    assert(lda.getAsymmetricDocConcentration.toArray.forall(_ === 2.0))
    assert(lda.getBeta === 3.0)
    assert(lda.getTopicConcentration === 3.0)
  }

  test("initializing with alpha length != k or 1 fails") {
    intercept[IllegalArgumentException] {
      val lda = new LDA().setK(2).setAlpha(Vectors.dense(1, 2, 3, 4))
      val corpus = sc.parallelize(tinyCorpus, 2)
      lda.run(corpus)
    }
  }

  test("initializing with elements in alpha < 0 fails") {
    intercept[IllegalArgumentException] {
      val lda = new LDA().setK(4).setAlpha(Vectors.dense(-1, 2, 3, 4))
      val corpus = sc.parallelize(tinyCorpus, 2)
      lda.run(corpus)
    }
  }

  test("OnlineLDAOptimizer initialization") {
    val lda = new LDA().setK(2)
    val corpus = sc.parallelize(tinyCorpus, 2)
    val op = new OnlineLDAOptimizer().initialize(corpus, lda)
    op.setKappa(0.9876).setMiniBatchFraction(0.123).setTau0(567)
    assert(op.getAlpha.toArray.forall(_ === 0.5)) // default 1.0 / k
    assert(op.getEta === 0.5)   // default 1.0 / k
    assert(op.getKappa === 0.9876)
    assert(op.getMiniBatchFraction === 0.123)
    assert(op.getTau0 === 567)
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
    val op = new OnlineLDAOptimizer().setTau0(1024).setKappa(0.51).setGammaShape(1e40)
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
    val topic1: Vector = Vectors.fromBreeze(op.getLambda(0, ::).t)
    val topic2: Vector = Vectors.fromBreeze(op.getLambda(1, ::).t)
    val expectedTopic1 = Vectors.dense(1.1101, 1.2076, 1.3050, 0.8899, 0.7924, 0.6950)
    val expectedTopic2 = Vectors.dense(0.8899, 0.7924, 0.6950, 1.1101, 1.2076, 1.3050)
    assert(topic1 ~== expectedTopic1 absTol 0.01)
    assert(topic2 ~== expectedTopic2 absTol 0.01)
  }

  test("OnlineLDAOptimizer with toy data") {
    val docs = sc.parallelize(toyData)
    val op = new OnlineLDAOptimizer().setMiniBatchFraction(1).setTau0(1024).setKappa(0.51)
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

  test("LocalLDAModel logLikelihood") {
    val ldaModel: LocalLDAModel = toyModel

    val docsSingleWord = sc.parallelize(Seq(Vectors.sparse(6, Array(0), Array(1)))
      .zipWithIndex
      .map { case (wordCounts, docId) => (docId.toLong, wordCounts) })
    val docsRepeatedWord = sc.parallelize(Seq(Vectors.sparse(6, Array(0), Array(5)))
      .zipWithIndex
      .map { case (wordCounts, docId) => (docId.toLong, wordCounts) })

    /* Verify results using gensim:
       import numpy as np
       from gensim import models
       corpus = [
          [(0, 1.0), (1, 1.0)],
          [(1, 1.0), (2, 1.0)],
          [(0, 1.0), (2, 1.0)],
          [(3, 1.0), (4, 1.0)],
          [(3, 1.0), (5, 1.0)],
          [(4, 1.0), (5, 1.0)]]
       np.random.seed(2345)
       lda = models.ldamodel.LdaModel(
          corpus=corpus, alpha=0.01, eta=0.01, num_topics=2, update_every=0, passes=100,
          decay=0.51, offset=1024)
       docsSingleWord = [[(0, 1.0)]]
       docsRepeatedWord = [[(0, 5.0)]]
       print(lda.bound(docsSingleWord))
       > -25.9706969833
       print(lda.bound(docsRepeatedWord))
       > -31.4413908227
     */

    assert(ldaModel.logLikelihood(docsSingleWord) ~== -25.971 relTol 1E-3D)
    assert(ldaModel.logLikelihood(docsRepeatedWord) ~== -31.441  relTol 1E-3D)
  }

  test("LocalLDAModel logPerplexity") {
    val docs = sc.parallelize(toyData)
    val ldaModel: LocalLDAModel = toyModel

    /* Verify results using gensim:
       import numpy as np
       from gensim import models
       corpus = [
          [(0, 1.0), (1, 1.0)],
          [(1, 1.0), (2, 1.0)],
          [(0, 1.0), (2, 1.0)],
          [(3, 1.0), (4, 1.0)],
          [(3, 1.0), (5, 1.0)],
          [(4, 1.0), (5, 1.0)]]
       np.random.seed(2345)
       lda = models.ldamodel.LdaModel(
          corpus=corpus, alpha=0.01, eta=0.01, num_topics=2, update_every=0, passes=100,
          decay=0.51, offset=1024)
       print(lda.log_perplexity(corpus))
       > -3.69051285096
     */

    // Gensim's definition of perplexity is negative our (and Stanford NLP's) definition
    assert(ldaModel.logPerplexity(docs) ~== 3.690D relTol 1E-3D)
  }

  test("LocalLDAModel predict") {
    val docs = sc.parallelize(toyData)
    val ldaModel: LocalLDAModel = toyModel

    /* Verify results using gensim:
       import numpy as np
       from gensim import models
       corpus = [
          [(0, 1.0), (1, 1.0)],
          [(1, 1.0), (2, 1.0)],
          [(0, 1.0), (2, 1.0)],
          [(3, 1.0), (4, 1.0)],
          [(3, 1.0), (5, 1.0)],
          [(4, 1.0), (5, 1.0)]]
       np.random.seed(2345)
       lda = models.ldamodel.LdaModel(
          corpus=corpus, alpha=0.01, eta=0.01, num_topics=2, update_every=0, passes=100,
          decay=0.51, offset=1024)
       print(list(lda.get_document_topics(corpus)))
       > [[(0, 0.99504950495049516)], [(0, 0.99504950495049516)],
       > [(0, 0.99504950495049516)], [(1, 0.99504950495049516)],
       > [(1, 0.99504950495049516)], [(1, 0.99504950495049516)]]
     */

    val expectedPredictions = List(
      (0, 0.99504), (0, 0.99504),
      (0, 0.99504), (1, 0.99504),
      (1, 0.99504), (1, 0.99504))

    val actualPredictions = ldaModel.topicDistributions(docs).cache()
    val topTopics = actualPredictions.map { case (id, topics) =>
        // convert results to expectedPredictions format, which only has highest probability topic
        val topicsBz = topics.asBreeze.toDenseVector
        (id, (argmax(topicsBz), max(topicsBz)))
      }.sortByKey()
      .values
      .collect()

    expectedPredictions.zip(topTopics).foreach { case (expected, actual) =>
      assert(expected._1 === actual._1 && (expected._2 ~== actual._2 relTol 1E-3D))
    }

    docs.collect()
      .map(doc => ldaModel.topicDistribution(doc._2))
      .zip(actualPredictions.map(_._2).collect())
      .foreach { case (single, batch) =>
        assert(single ~== batch relTol 1E-3D)
      }
    actualPredictions.unpersist()
  }

  test("OnlineLDAOptimizer with asymmetric prior") {
    val docs = sc.parallelize(toyData)
    val op = new OnlineLDAOptimizer().setMiniBatchFraction(1).setTau0(1024).setKappa(0.51)
      .setGammaShape(1e10)
    val lda = new LDA().setK(2)
      .setDocConcentration(Vectors.dense(0.00001, 0.1))
      .setTopicConcentration(0.01)
      .setMaxIterations(100)
      .setOptimizer(op)
      .setSeed(12345)

    val ldaModel = lda.run(docs)
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights)
    }

    /* Verify results with Python:

       import numpy as np
       from gensim import models
       corpus = [
           [(0, 1.0), (1, 1.0)],
           [(1, 1.0), (2, 1.0)],
           [(0, 1.0), (2, 1.0)],
           [(3, 1.0), (4, 1.0)],
           [(3, 1.0), (5, 1.0)],
           [(4, 1.0), (5, 1.0)]]
       np.random.seed(10)
       lda = models.ldamodel.LdaModel(
           corpus=corpus, alpha=np.array([0.00001, 0.1]), num_topics=2, update_every=0, passes=100)
       lda.print_topics()

       > ['0.167*0 + 0.167*1 + 0.167*2 + 0.167*3 + 0.167*4 + 0.167*5',
          '0.167*0 + 0.167*1 + 0.167*2 + 0.167*4 + 0.167*3 + 0.167*5']
     */
    topics.foreach { topic =>
      assert(topic.forall { case (_, p) => p ~= 0.167 absTol 0.05 })
    }
  }

  test("OnlineLDAOptimizer alpha hyperparameter optimization") {
    val k = 2
    val docs = sc.parallelize(toyData)
    val op = new OnlineLDAOptimizer().setMiniBatchFraction(1).setTau0(1024).setKappa(0.51)
      .setGammaShape(100).setOptimizeDocConcentration(true).setSampleWithReplacement(false)
    val lda = new LDA().setK(k)
      .setDocConcentration(1D / k)
      .setTopicConcentration(0.01)
      .setMaxIterations(100)
      .setOptimizer(op)
      .setSeed(12345)
    val ldaModel: LocalLDAModel = lda.run(docs).asInstanceOf[LocalLDAModel]

    /* Verify the results with gensim:
      import numpy as np
      from gensim import models
      corpus = [
       [(0, 1.0), (1, 1.0)],
       [(1, 1.0), (2, 1.0)],
       [(0, 1.0), (2, 1.0)],
       [(3, 1.0), (4, 1.0)],
       [(3, 1.0), (5, 1.0)],
       [(4, 1.0), (5, 1.0)]]
      np.random.seed(2345)
      lda = models.ldamodel.LdaModel(
         corpus=corpus, alpha='auto', eta=0.01, num_topics=2, update_every=0, passes=100,
         decay=0.51, offset=1024)
      print(lda.alpha)
      > [ 0.42582646  0.43511073]
     */

    assert(ldaModel.docConcentration ~== Vectors.dense(0.42582646, 0.43511073) absTol 0.05)
  }

  test("model save/load") {
    // Test for LocalLDAModel.
    val localModel = new LocalLDAModel(tinyTopics,
      Vectors.dense(Array.fill(tinyTopics.numRows)(0.01)), 0.5D, 10D)
    val tempDir1 = Utils.createTempDir()
    val path1 = tempDir1.toURI.toString

    // Test for DistributedLDAModel.
    val k = 3
    val docConcentration = 1.2
    val topicConcentration = 1.5
    val lda = new LDA()
    lda.setK(k)
      .setDocConcentration(docConcentration)
      .setTopicConcentration(topicConcentration)
      .setMaxIterations(5)
      .setSeed(12345)
    val corpus = sc.parallelize(tinyCorpus, 2)
    val distributedModel: DistributedLDAModel = lda.run(corpus).asInstanceOf[DistributedLDAModel]
    val tempDir2 = Utils.createTempDir()
    val path2 = tempDir2.toURI.toString

    try {
      localModel.save(sc, path1)
      distributedModel.save(sc, path2)
      val samelocalModel = LocalLDAModel.load(sc, path1)
      assert(samelocalModel.topicsMatrix === localModel.topicsMatrix)
      assert(samelocalModel.k === localModel.k)
      assert(samelocalModel.vocabSize === localModel.vocabSize)
      assert(samelocalModel.docConcentration === localModel.docConcentration)
      assert(samelocalModel.topicConcentration === localModel.topicConcentration)
      assert(samelocalModel.gammaShape === localModel.gammaShape)

      val sameDistributedModel = DistributedLDAModel.load(sc, path2)
      assert(distributedModel.topicsMatrix === sameDistributedModel.topicsMatrix)
      assert(distributedModel.k === sameDistributedModel.k)
      assert(distributedModel.vocabSize === sameDistributedModel.vocabSize)
      assert(distributedModel.iterationTimes === sameDistributedModel.iterationTimes)
      assert(distributedModel.docConcentration === sameDistributedModel.docConcentration)
      assert(distributedModel.topicConcentration === sameDistributedModel.topicConcentration)
      assert(distributedModel.gammaShape === sameDistributedModel.gammaShape)
      assert(distributedModel.globalTopicTotals === sameDistributedModel.globalTopicTotals)
      assert(distributedModel.logLikelihood ~== sameDistributedModel.logLikelihood absTol 1e-6)
      assert(distributedModel.logPrior ~== sameDistributedModel.logPrior absTol 1e-6)

      val graph = distributedModel.graph
      val sameGraph = sameDistributedModel.graph
      assert(graph.vertices.sortByKey().collect() === sameGraph.vertices.sortByKey().collect())
      val edge = graph.edges.map {
        case Edge(sid: Long, did: Long, nos: Double) => (sid, did, nos)
      }.sortBy(x => (x._1, x._2)).collect()
      val sameEdge = sameGraph.edges.map {
        case Edge(sid: Long, did: Long, nos: Double) => (sid, did, nos)
      }.sortBy(x => (x._1, x._2)).collect()
      assert(edge === sameEdge)
    } finally {
      Utils.deleteRecursively(tempDir1)
      Utils.deleteRecursively(tempDir2)
    }
  }

  test("EMLDAOptimizer with empty docs") {
    val vocabSize = 6
    val emptyDocsArray = Array.fill(6)(Vectors.sparse(vocabSize, Array.empty, Array.empty))
    val emptyDocs = emptyDocsArray
      .zipWithIndex.map { case (wordCounts, docId) =>
        (docId.toLong, wordCounts)
    }
    val distributedEmptyDocs = sc.parallelize(emptyDocs, 2)

    val op = new EMLDAOptimizer()
    val lda = new LDA()
      .setK(3)
      .setMaxIterations(5)
      .setSeed(12345)
      .setOptimizer(op)

    val model = lda.run(distributedEmptyDocs)
    assert(model.vocabSize === vocabSize)
  }

  test("OnlineLDAOptimizer with empty docs") {
    val vocabSize = 6
    val emptyDocsArray = Array.fill(6)(Vectors.sparse(vocabSize, Array.empty, Array.empty))
    val emptyDocs = emptyDocsArray
      .zipWithIndex.map { case (wordCounts, docId) =>
        (docId.toLong, wordCounts)
    }
    val distributedEmptyDocs = sc.parallelize(emptyDocs, 2)

    val op = new OnlineLDAOptimizer()
    val lda = new LDA()
      .setK(3)
      .setMaxIterations(5)
      .setSeed(12345)
      .setOptimizer(op)

    val model = lda.run(distributedEmptyDocs)
    assert(model.vocabSize === vocabSize)
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
    Vectors.dense(0, 0, 0, 0, 0), // empty doc
    Vectors.dense(1, 3, 0, 2, 8),
    Vectors.dense(0, 2, 1, 0, 4),
    Vectors.dense(2, 3, 12, 3, 1),
    Vectors.dense(0, 0, 0, 0, 0), // empty doc
    Vectors.dense(0, 3, 1, 9, 8),
    Vectors.dense(1, 1, 4, 2, 6)
  ).zipWithIndex.map { case (wordCounts, docId) => (docId.toLong, wordCounts) }
  assert(tinyCorpus.forall(_._2.size == tinyVocabSize)) // sanity check for test data

  def getNonEmptyDoc(corpus: Array[(Long, Vector)]): Array[(Long, Vector)] = corpus.filter {
    case (_, wc: Vector) => Vectors.norm(wc, p = 1.0) != 0.0
  }

  def toyData: Array[(Long, Vector)] = Array(
    Vectors.sparse(6, Array(0, 1), Array(1, 1)),
    Vectors.sparse(6, Array(1, 2), Array(1, 1)),
    Vectors.sparse(6, Array(0, 2), Array(1, 1)),
    Vectors.sparse(6, Array(3, 4), Array(1, 1)),
    Vectors.sparse(6, Array(3, 5), Array(1, 1)),
    Vectors.sparse(6, Array(4, 5), Array(1, 1))
  ).zipWithIndex.map { case (wordCounts, docId) => (docId.toLong, wordCounts) }

  /** Used in the Java Test Suite */
  def javaToyData: JArrayList[(java.lang.Long, Vector)] = {
    val javaData = new JArrayList[(java.lang.Long, Vector)]
    var i = 0
    while (i < toyData.length) {
      javaData.add((toyData(i)._1, toyData(i)._2))
      i += 1
    }
    javaData
  }

  def toyModel: LocalLDAModel = {
    val k = 2
    val vocabSize = 6
    val alpha = 0.01
    val eta = 0.01
    val gammaShape = 100
    val topics = new DenseMatrix(numRows = vocabSize, numCols = k, values = Array(
      1.86738052, 1.94056535, 1.89981687, 0.0833265, 0.07405918, 0.07940597,
      0.15081551, 0.08637973, 0.12428538, 1.9474897, 1.94615165, 1.95204124))
    val ldaModel: LocalLDAModel = new LocalLDAModel(
      topics, Vectors.dense(Array.fill(k)(alpha)), eta, gammaShape)
    ldaModel
  }
}
