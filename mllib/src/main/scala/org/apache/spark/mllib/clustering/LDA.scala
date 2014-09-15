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

import java.util.Random
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}

import org.apache.spark.Logging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{DenseVector => SDV, SparseVector => SSV, Vector => SV}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

case class Document(docId: Int, content: Array[Int]) {
  private[spark] var topics: Array[Int] = null
  private[spark] var topicsDist: BV[Double] = null
}

class TopicModel private[spark](
  private[spark] val topicCounts_ : BDV[Double],
  private[spark] val topicTermCounts_ : Array[BSV[Double]],
  private[spark] val alpha: Double,
  private[spark] val beta: Double
) extends Serializable {

  def this(topicCounts_ : SDV, topicTermCounts_ : Array[SSV], alpha: Double, beta: Double) {
    this(new BDV[Double](topicCounts_.toArray), topicTermCounts_.map(t =>
      new BSV(t.indices, t.values, t.size)), alpha, beta)
  }

  def topicCounts = Vectors.dense(topicCounts_.toArray)

  def topicTermCounts = topicTermCounts_.map(t => Vectors.sparse(t.size, t.activeIterator.toSeq))

  def update(term: Int, topic: Int, inc: Int) = {
    topicCounts_(topic) += inc
    topicTermCounts_(topic)(term) += inc
    this
  }

  def merge(other: TopicModel) = {
    topicCounts_ += other.topicCounts_
    var i = 0
    while (i < topicTermCounts_.length) {
      topicTermCounts_(i) += other.topicTermCounts_(i)
      i += 1
    }
    this
  }

  def infer(
    doc: Document, totalIter: Int = 10,
    burnInIter: Int = 5, rand: Random = new Random()
  ): SV = {
    require(totalIter > burnInIter, "totalIter is less than burnInIter")
    require(totalIter > 0, "totalIter is less than 0")
    require(burnInIter > 0, "burnInIter is less than 0")
    val numTopics = topicCounts_.size
    val topicThisTerm = BDV.zeros[Double](numTopics)
    val topicThisDoc = BSV.zeros[Double](numTopics)

    for (i <- 1 to totalIter) {
      generateTopicDistForDocument(doc, topicThisTerm, rand)
      if (i > burnInIter) {
        topicThisDoc :+= doc.topicsDist
      }
    }
    topicThisDoc :/= (totalIter - burnInIter).toDouble

    new SDV(topicThisDoc.toArray)
  }

  /**
   * This function used for computing the new distribution after drop one from current document,
   * which is a really essential part of Gibbs sampling for LDA, you can refer to the paper:
   * <I>Parameter estimation for text analysis<I>
   */
  private[spark] def dropOneDistSampler(
    docTopicCount: BV[Double],
    topicThisTerm: BDV[Double],
    rand: Random, term: Int,
    currentTopic: Int
  ): Int = {
    val ttm = generateTopicDistForTerm(docTopicCount, topicThisTerm, term,
      currentTopic, isTrainModel = true)
    CGS.multinomialDistSampler(rand, ttm)
  }

  private[spark] def generateTopicDistForTerm(
    docTopicCount: BV[Double],
    topicThisTerm: BDV[Double],
    term: Int,
    currentTopic: Int = -1,
    isTrainModel: Boolean = false
  ): BDV[Double] = {
    val numTopics = topicCounts_.size
    val numTerms = topicTermCounts_.head.size
    var i = 0
    while (i < numTopics) {
      val adjustment = if (isTrainModel && i == currentTopic) -1 else 0
      topicThisTerm(i) = (topicTermCounts_(i)(term) + adjustment + beta) /
        (topicCounts_(i) + adjustment + (numTerms * beta)) *
        (docTopicCount(i) + adjustment + alpha)
      i += 1
    }
    topicThisTerm
  }

  private[spark] def generateTopicDistForDocument(doc: Document,
    topicThisTerm: BDV[Double], rand: Random): Unit = {
    val content = doc.content
    val numTopics = topicCounts_.size
    var currentTopicDist = doc.topicsDist
    var currentTopics = doc.topics
    if (currentTopicDist != null) {
      for (i <- 0 until content.length) {
        val term = content(i)
        val topic = currentTopics(i)
        val dist = generateTopicDistForTerm(currentTopicDist, topicThisTerm, term)
        val lastTopic = CGS.multinomialDistSampler(rand, dist)
        if (lastTopic != topic) {
          currentTopics(i) = lastTopic
          currentTopicDist(topic) += -1
          currentTopicDist(lastTopic) += 1
        }
      }
    }
    else {
      currentTopicDist = BSV.zeros[Double](numTopics)
      currentTopics = content.map { term =>
        val lastTopic = CGS.uniformDistSampler(rand, numTopics)
        currentTopicDist(lastTopic) += 1
        lastTopic
      }
      doc.topicsDist = currentTopicDist
      doc.topics = currentTopics
    }
  }

  private[spark] def phi(topic: Int, term: Int): Double = {
    val numTerms = topicTermCounts_.head.size
    (topicTermCounts_(topic)(term) + beta) / (topicCounts_(topic) + numTerms * beta)
  }
}

object TopicModel {
  def apply(numTopics: Int, numTerms: Int, alpha: Double = 0.1, beta: Double = 0.01) = {
    new TopicModel(
      BDV.zeros[Double](numTopics),
      Array(0 until numTopics: _*).map(_ => BSV.zeros[Double](numTerms)),
      alpha, beta)
  }
}

class LDA private(
  var numTerms: Int,
  var numTopics: Int,
  totalIter: Int,
  burnInIter: Int,
  var alpha: Double,
  var beta: Double
) extends Serializable with Logging {
  def run(input: RDD[Document]): (TopicModel, RDD[Document]) = {
    val initModel = TopicModel(numTopics, numTerms, alpha, beta)
    CGS.runGibbsSampling(input, initModel, totalIter, burnInIter)
  }
}

object LDA extends Logging {
  def train(
    data: RDD[Document],
    numTopics: Int,
    totalIter: Int = 150,
    burnInIter: Int = 5,
    alpha: Double = 0.1,
    beta: Double = 0.01): (TopicModel, RDD[Document]) = {
    val numTerms = data.flatMap(_.content).max() + 1
    val lda = new LDA(numTerms, numTopics, totalIter, burnInIter, alpha, beta)
    lda.run(data)
  }

  /**
   * Perplexity is a kind of evaluation method of LDA. Usually it is used on unseen data. But here
   * we use it for current documents, which is also OK. If using it on unseen data, you must do an
   * iteration of Gibbs sampling before calling this. Small perplexity means good result.
   */
  def perplexity(data: RDD[Document], computedModel: TopicModel): Double = {
    val broadcastModel = data.context.broadcast(computedModel)
    val (termProb, totalNum) = data.mapPartitions { docs =>
      val model = broadcastModel.value
      val numTopics = model.topicCounts_.size
      val numTerms = model.topicTermCounts_.head.size
      val rand = new Random
      val alpha = model.alpha
      val currentTheta = BDV.zeros[Double](numTerms)
      val topicThisTerm = BDV.zeros[Double](numTopics)
      docs.flatMap { doc =>
        val content = doc.content
        if (doc.topicsDist != null) {
          model.generateTopicDistForDocument(doc, topicThisTerm, rand)
        }
        content.foreach(term => currentTheta(term) = 0)
        content.foreach { term =>
          (0 until numTopics).foreach { topic =>
            currentTheta(term) += model.phi(topic, term) * ((doc.topicsDist(topic) + alpha) /
              (content.size + alpha * numTopics))
          }
        }
        content.map(x => (math.log(currentTheta(x)), 1))
      }
    }.reduce { (lhs, rhs) =>
      (lhs._1 + rhs._1, lhs._2 + rhs._2)
    }
    math.exp(-1 * termProb / totalNum)
  }

}

/**
 * Collapsed Gibbs sampling from for Latent Dirichlet Allocation.
 */
private[spark] object CGS extends Logging {

  /**
   * Main function of running a Gibbs sampling method. It contains two phases of total Gibbs
   * sampling: first is initialization, second is real sampling.
   */
  def runGibbsSampling(
    data: RDD[Document], initModel: TopicModel,
    totalIter: Int, burnInIter: Int): (TopicModel, RDD[Document]) = {
    require(totalIter > burnInIter, "totalIter is less than burnInIter")
    require(totalIter > 0, "totalIter is less than 0")
    require(burnInIter > 0, "burnInIter is less than 0")

    val (numTopics, numTerms, alpha, beta) = (initModel.topicCounts_.size,
      initModel.topicTermCounts_.head.size,
      initModel.alpha, initModel.beta)
    val probModel = TopicModel(numTopics, numTerms, alpha, beta)

    logInfo("Start initialization")
    var (topicModel, corpus) = sampleTermAssignment(data, initModel)

    for (iter <- 1 to totalIter) {
      logInfo("Start Gibbs sampling (Iteration %d/%d)".format(iter, totalIter))
      val broadcastModel = data.context.broadcast(topicModel)
      val previousCorpus = corpus
      corpus = sampleTopicCounters(corpus, broadcastModel, numTerms,
        numTopics).setName(s"LDA-$iter").persist(StorageLevel.MEMORY_AND_DISK)
      if (iter % 5 == 0 && data.context.getCheckpointDir.isDefined) {
        corpus.checkpoint()
      }
      topicModel = collectTopicCounters(corpus, numTerms, numTopics, alpha, beta)
      if (iter > burnInIter) {
        probModel.merge(topicModel)
      }
      previousCorpus.unpersist()
      broadcastModel.unpersist()
    }
    val burnIn = (totalIter - burnInIter).toDouble
    probModel.topicCounts_ :/= burnIn
    probModel.topicTermCounts_.foreach(_ :/= burnIn)
    (probModel, corpus)
  }

  private def sampleTopicCounters(corpus: RDD[Document], broadcast: Broadcast[TopicModel],
    numTerms: Int, numTopics: Int): RDD[Document] = {
    corpus.mapPartitions { docs =>
      val rand = new Random
      val currentModel = broadcast.value
      val topicThisTerm = BDV.zeros[Double](numTopics)
      docs.map { doc =>
        val content = doc.content
        val topics = doc.topics
        val topicsDist = doc.topicsDist
        for (i <- 0 until content.length) {
          val term = content(i)
          val topic = topics(i)
          val chosenTopic = currentModel.dropOneDistSampler(topicsDist, topicThisTerm,
            rand, term, topic)
          if (topic != chosenTopic) {
            topics(i) = chosenTopic
            topicsDist(topic) += -1
            topicsDist(chosenTopic) += 1
            currentModel.update(term, topic, -1)
            currentModel.update(term, chosenTopic, 1)
          }
        }
        doc
      }
    }
  }

  private def collectTopicCounters(docTopics: RDD[Document], numTerms: Int,
    numTopics: Int, alpha: Double, beta: Double): TopicModel = {
    docTopics.mapPartitions { iter =>
      val topicCounters = TopicModel(numTopics, numTerms, alpha, beta)
      iter.foreach { doc =>
        val content = doc.content
        val topics = doc.topics
        for (i <- 0 until doc.content.length) {
          topicCounters.update(content(i), topics(i), 1)
        }
      }
      Iterator(topicCounters)
    }.fold(TopicModel(numTopics, numTerms, alpha, beta)) { (thatOne, otherOne) =>
      thatOne.merge(otherOne)
    }
  }

  /**
   * Initial step of Gibbs sampling, which supports incremental LDA.
   */
  private def sampleTermAssignment(data: RDD[Document], topicModel: TopicModel):
  (TopicModel, RDD[Document]) = {
    val (numTopics, numTerms, alpha, beta) = (topicModel.topicCounts_.size,
      topicModel.topicTermCounts_.head.size,
      topicModel.alpha, topicModel.beta)
    val broadcast = data.context.broadcast(topicModel)
    val initialDocs = if (topicModel.topicCounts_.norm(2) == 0) {
      uniformDistAssignment(data, numTopics, numTerms)
    } else {
      multinomialDistAssignment(data, broadcast, numTopics, numTopics)
    }
    initialDocs.persist(StorageLevel.MEMORY_AND_DISK)
    val initialModel = collectTopicCounters(initialDocs, numTerms, numTopics, alpha, beta)
    broadcast.unpersist()
    (initialModel, initialDocs)
  }

  private def multinomialDistAssignment(data: RDD[Document], broadcast: Broadcast[TopicModel],
    numTopics: Int, numTerms: Int): RDD[Document] = {
    data.mapPartitions { docs =>
      val rand = new Random
      val currentParams = broadcast.value
      val topicThisTerm = BDV.zeros[Double](numTopics)
      docs.map { doc =>
        val content = doc.content
        val topicDist = doc.topicsDist
        val lastDocTopicCount = BDV.zeros[Double](numTopics)
        val lastTopics = content.map { term =>
          val dist = currentParams.generateTopicDistForTerm(topicDist, topicThisTerm, term)
          val lastTopic = multinomialDistSampler(rand, dist)
          lastDocTopicCount(lastTopic) += 1
          lastTopic
        }
        doc.topicsDist = lastDocTopicCount
        doc.topics = lastTopics
        doc
      }
    }
  }

  private def uniformDistAssignment(data: RDD[Document],
    numTopics: Int, numTerms: Int): RDD[Document] = {
    data.mapPartitions { docs =>
      val rand = new Random
      docs.map { doc =>
        val content = doc.content
        val lastDocTopicCount = BDV.zeros[Double](numTopics)
        val lastTopics = content.map { term =>
          val topic = uniformDistSampler(rand, numTopics)
          lastDocTopicCount(topic) += 1
          topic
        }
        doc.topicsDist = lastDocTopicCount
        doc.topics = lastTopics
        doc
      }
    }
  }

  /**
   * A uniform distribution sampler, which is only used for initialization.
   */
  def uniformDistSampler(rand: Random, dimension: Int): Int = {
    rand.nextInt(dimension)
  }

  /**
   * A multinomial distribution sampler, using roulette method to sample an Int back.
   */
  def multinomialDistSampler(rand: Random, dist: BDV[Double]): Int = {
    val distSum = rand.nextDouble() * breeze.linalg.sum[BDV[Double], Double](dist)

    def loop(index: Int, accum: Double): Int = {
      if (index == dist.length) return dist.length - 1
      val sum = accum - dist(index)
      if (sum <= 0) index else loop(index + 1, sum)
    }

    loop(0, distSum)
  }
}
