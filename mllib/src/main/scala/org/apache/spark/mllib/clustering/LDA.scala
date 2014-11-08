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

import scala.collection.mutable

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, sum => brzSum}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._
import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix}
import org.apache.spark.mllib.linalg.{DenseVector => SDV, SparseVector => SSV, Vector => SV}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext._

import LDA._

class LDA private[mllib](
  @transient var corpus: Graph[VD, ED],
  val numTopics: Int,
  val numTerms: Int,
  val alpha: Double,
  val beta: Double,
  val alphaAS: Double,
  @transient val storageLevel: StorageLevel)
  extends Serializable with Logging {

  def this(docs: RDD[(DocId, SSV)],
    numTopics: Int,
    alpha: Double,
    beta: Double,
    alphaAS: Double,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK,
    computedModel: Broadcast[LDAModel] = null) {
    this(initializeCorpus(docs, numTopics, storageLevel, computedModel),
      numTopics, docs.first()._2.size, alpha, beta, alphaAS, storageLevel)
  }

  /**
   * The number of documents in the corpus
   */
  val numDocs = docVertices.count()

  /**
   * The number of terms in the corpus
   */
  private val sumTerms = corpus.edges.map(e => e.attr.size.toDouble).sum().toLong

  @transient private val sc = corpus.vertices.context
  @transient private val seed = new Random().nextInt()
  @transient private var innerIter = 1
  @transient private var globalParameter: GlobalParameter = collectGlobalParameter(corpus)

  private def termVertices = corpus.vertices.filter(t => t._1 >= 0)

  private def docVertices = corpus.vertices.filter(t => t._1 < 0)

  private def checkpoint(): Unit = {
    if (innerIter % 10 == 0 && sc.getCheckpointDir.isDefined) {
      val edges = corpus.edges.map(t => t)
      edges.checkpoint()
      val newCorpus: Graph[VD, ED] = Graph.fromEdges(edges, null,
        storageLevel, storageLevel)
      corpus = updateCounter(newCorpus, numTopics).persist(storageLevel)
    }
  }

  private def collectGlobalParameter(graph: Graph[VD, ED]): GlobalParameter = {
    val globalTopicCounter = collectGlobalCounter(graph, numTopics)
    assert(brzSum(globalTopicCounter) == sumTerms)
    val (denominator, denominator1) = denominatorBDV(globalTopicCounter,
      sumTerms, numTerms, numTopics, alpha, beta, alphaAS)
    val (t, t1) = LDA.t(globalTopicCounter, denominator, denominator1,
      sumTerms, numTopics, alpha, beta, alphaAS)
    GlobalParameter(globalTopicCounter, t, t1, denominator, denominator1)
  }

  private def gibbsSampling(): Unit = {
    val broadcast = sc.broadcast(globalParameter)
    val sampleCorpus = sampleTopics(corpus, broadcast,
      innerIter + seed, sumTerms, numTopics, alpha, beta, alphaAS)
    sampleCorpus.persist(storageLevel)

    val counterCorpus = updateCounter(sampleCorpus, numTopics)
    counterCorpus.persist(storageLevel)
    // counterCorpus.vertices.count()
    counterCorpus.edges.count()
    globalParameter = collectGlobalParameter(counterCorpus)

    corpus.edges.unpersist(false)
    corpus.vertices.unpersist(false)
    sampleCorpus.edges.unpersist(false)
    sampleCorpus.vertices.unpersist(false)
    corpus = counterCorpus
    broadcast.unpersist(false)

    checkpoint()
    innerIter += 1
  }

  def saveModel(burnInIter: Int): LDAModel = {
    var termTopicCounter: RDD[(Int, BSV[Double])] = null
    for (iter <- 1 to burnInIter) {
      logInfo(s"Save TopicModel (Iteration $iter/$burnInIter)")
      var previousTermTopicCounter = termTopicCounter
      gibbsSampling()
      val newTermTopicCounter = updateModel(termVertices)
      termTopicCounter = Option(termTopicCounter).map(_.join(newTermTopicCounter).map {
        case (term, (a, b)) =>
          val c = a + b
          c.compact()
          (term, c)
      }).getOrElse(newTermTopicCounter)

      termTopicCounter.cache().count()
      Option(previousTermTopicCounter).foreach(_.unpersist())
      previousTermTopicCounter = termTopicCounter
    }
    val model = LDAModel(numTopics, numTerms, alpha, beta)
    termTopicCounter.collect().foreach { case (term, counter) =>
      model.merge(term, counter)
    }
    model.gtc :/= burnInIter.toDouble
    model.ttc.foreach { ttc =>
      ttc :/= burnInIter.toDouble
      ttc.compact()
    }
    model
  }

  def runGibbsSampling(iterations: Int): Unit = {
    for (iter <- 1 to iterations) {
      logInfo(s"Start Gibbs sampling (Iteration $iter/$iterations)")
      gibbsSampling()
    }
  }

  def mergeDuplicateTopic(threshold: Double = 0.95D): Map[Int, Int] = {
    val rows = termVertices.map(t => t._2).map { bsv =>
      val length = bsv.length
      val used = bsv.used
      val index = bsv.index.slice(0, used)
      val data = bsv.data.slice(0, used).map(_.toDouble)
      new SSV(length, index, data).asInstanceOf[SV]
    }
    val simMatrix = new RowMatrix(rows).columnSimilarities()
    val minMap = simMatrix.entries.filter { case MatrixEntry(row, column, sim) =>
      sim > threshold && row != column
    }.map { case MatrixEntry(row, column, sim) =>
      (column.toInt, row.toInt)
    }.groupByKey().map { case (topic, simTopics) =>
      (topic, simTopics.min)
    }.collect().toMap
    if (minMap.size > 0) {
      corpus = corpus.mapEdges(edges => {
        edges.attr.map { topic =>
          minMap.get(topic).getOrElse(topic)
        }
      })
      corpus = updateCounter(corpus, numTopics)
    }
    minMap
  }

  def perplexity(): Double = {
    val totalTopicCounter = this.globalParameter.totalTopicCounter
    val numTopics = this.numTopics
    val numTerms = this.numTerms
    val alpha = this.alpha
    val beta = this.beta
    val totalSize = brzSum(totalTopicCounter)
    var totalProb = 0D

    totalTopicCounter.activeIterator.foreach { case (topic, cn) =>
      totalProb += alpha * beta / (cn + numTerms * beta)
    }

    val termProb = corpus.mapVertices { (vid, counter) =>
      val probDist = BSV.zeros[Double](numTopics)
      if (vid >= 0) {
        val termTopicCounter = counter
        termTopicCounter.activeIterator.foreach { case (topic, cn) =>
          probDist(topic) = cn * alpha /
            (totalTopicCounter(topic) + numTerms * beta)
        }
      } else {
        val docTopicCounter = counter
        docTopicCounter.activeIterator.foreach { case (topic, cn) =>
          probDist(topic) = cn * beta /
            (totalTopicCounter(topic) + numTerms * beta)
        }
      }
      probDist.compact()
      (counter, probDist)
    }.mapTriplets { triplet =>
      val (termTopicCounter, termProb) = triplet.srcAttr
      val (docTopicCounter, docProb) = triplet.dstAttr
      val docSize = brzSum(docTopicCounter)
      val docTermSize = triplet.attr.length

      var prob = 0D

      docTopicCounter.activeIterator.foreach { case (topic, cn) =>
        prob += cn * termTopicCounter(topic) /
          (totalTopicCounter(topic) + numTerms * beta)
      }
      prob += brzSum(docProb) + brzSum(termProb) + totalProb
      prob += prob / (docSize + numTopics * alpha)

      docTermSize * Math.log(prob)
    }.edges.map(t => t.attr).sum()

    math.exp(-1 * termProb / totalSize)
  }
}

object LDA {

  import LDAUtils._

  private[mllib] type DocId = VertexId
  private[mllib] type WordId = VertexId
  private[mllib] type Count = Int
  private[mllib] type ED = Array[Count]
  private[mllib] type VD = BSV[Count]

  private[mllib] case class GlobalParameter(totalTopicCounter: BDV[Count],
    t: BDV[Double], t1: BDV[Double], denominator: BDV[Double], denominator1: BDV[Double])

  private[mllib] case class Parameter(counter: BSV[Count], dist: BSV[Double], dist1: BSV[Double])

  def train(docs: RDD[(DocId, SSV)],
    numTopics: Int = 2048,
    totalIter: Int = 150,
    burnIn: Int = 5,
    alpha: Double = 0.1,
    beta: Double = 0.01,
    alphaAS: Double = 0.1): LDAModel = {
    require(totalIter > burnIn, "totalIter is less than burnIn")
    require(totalIter > 0, "totalIter is less than 0")
    require(burnIn > 0, "burnIn is less than 0")
    val topicModeling = new LDA(docs, numTopics, alpha, beta, alphaAS)
    topicModeling.runGibbsSampling(totalIter - burnIn)
    topicModeling.saveModel(burnIn)
  }

  def incrementalTrain(docs: RDD[(DocId, SSV)],
    computedModel: LDAModel,
    alphaAS: Double = 1,
    totalIter: Int = 150,
    burnIn: Int = 5): LDAModel = {
    require(totalIter > burnIn, "totalIter is less than burnIn")
    require(totalIter > 0, "totalIter is less than 0")
    require(burnIn > 0, "burnIn is less than 0")
    val numTopics = computedModel.ttc.size
    val alpha = computedModel.alpha
    val beta = computedModel.beta

    val broadcastModel = docs.context.broadcast(computedModel)
    val topicModeling = new LDA(docs, numTopics, alpha, beta, alphaAS,
      computedModel = broadcastModel)
    broadcastModel.unpersist()
    topicModeling.runGibbsSampling(totalIter - burnIn)
    topicModeling.saveModel(burnIn)
  }

  // scalastyle:off
  /**
   * A multinomial distribution sampler, using roulette method to sample an Int back.
   * Asymmetric Dirichlet Priors you can refer to the paper:
   * "Rethinking LDA: Why Priors Matter", available at
   * [[http://people.ee.duke.edu/~lcarin/Eric3.5.2010.pdf]]
   *
   * if you want to know more about the above codes, you can refer to the following formula:
   * First), the original Gibbis sampling formula is :<img src="http://www.forkosh.com/mathtex.cgi? P(z^{(d)}_{n}|W, Z_{\backslash d,n}, \alpha u, \beta u)\propto P(w^{(d)}_{n}|z^{(d)}_{n},W_{\backslash d,n}, Z_{\backslash d,n}, \beta u) P(z^{(d)}_{n}|Z_{\backslash d,n}, \alpha u)"> (1)
   * Second), using the Asymmetric Dirichlet Priors, the second term of formula (1) can be written as following:
   * <img src="http://www.forkosh.com/mathtex.cgi? P(z^{(d)}_{N_{d+1}}=t|Z, \alpha, \alpha^{'}u)=\int dm P(z^{(d)}_{N_{d+1}}=t|Z, \alpha m)P(m|Z, \alpha^{'}u)=\frac{N_{t|d}+\alpha \frac{\widehat{N}_{T}+\frac{\alpha^{'}}{T}}{\Sigma_{t}\widehat{N}_{t}+ \alpha ^{'}}}{N_{d}+\alpha}"> (2)
   * Third), in this code, we set the <img src="http://www.forkosh.com/mathtex.cgi? \alpha=\alpha^{'}">, you can set different value for them. Additionally, in our code the parameter "alpha" is equal to <img src="http://www.forkosh.com/mathtex.cgi?\alpha * T">;
   * "adjustment" denote that if this is the current topic, you need to reduce number one from the corresponding term;
   * <img src="http://www.forkosh.com/mathtex.cgi? ratio=\frac{\widehat{N}_{t}+\frac{\alpha^{'}}{T}}{\Sigma _{t}\widehat{N}_{t}+\alpha^{'}} \qquad asPrior = ratio * (alpha * numTopics)">;
   * Finally), we put them into formula (1) to get the final Asymmetric Dirichlet Priors Gibbs sampling formula.
   *
   */
  // scalastyle:on
  private[mllib] def sampleTopics(
    graph: Graph[VD, ED],
    broadcast: Broadcast[GlobalParameter],
    innerIter: Long,
    sumTerms: Long,
    numTopics: Int,
    alpha: Double,
    beta: Double,
    alphaAS: Double): Graph[VD, ED] = {
    val parts = graph.edges.partitions.size

    val sampleTopics = (gen: Random,
      wMap: mutable.Map[VertexId, Parameter],
      totalTopicCounter: BDV[Count],
      t: BDV[Double],
      t1: BDV[Double],
      denominator: BDV[Double],
      denominator1: BDV[Double],
      d: BDV[Double],
      d1: BDV[Double],
      triplet: EdgeTriplet[VD, ED]) => {
      val term = triplet.srcId
      val termTopicCounter = triplet.srcAttr
      val docTopicCounter = triplet.dstAttr
      val topics = triplet.attr
      val parameter = wMap.getOrElseUpdate(term, w(totalTopicCounter, denominator, denominator1,
        termTopicCounter, sumTerms, numTopics, alpha, beta, alphaAS))
      this.d(denominator, denominator1, termTopicCounter, docTopicCounter,
        d, d1, sumTerms, numTopics, beta, alphaAS)

      var i = 0
      while (i < topics.length) {
        val currentTopic = topics(i)
        val adjustment = d1(currentTopic) + parameter.dist1(currentTopic) + t1(currentTopic)
        val newTopic = multinomialDistSampler(gen, docTopicCounter, d, parameter.dist, t,
          adjustment, currentTopic)
        assert(newTopic < numTopics)
        topics(i) = newTopic
        i += 1
      }
      topics
    }

    graph.mapTriplets {
      (pid, iter) =>
        val gen = new Random(parts * innerIter + pid)
        val d = BDV.zeros[Double](numTopics)
        val d1 = BDV.zeros[Double](numTopics)
        val wMap = mutable.Map[VertexId, Parameter]()
        val GlobalParameter(totalTopicCounter, t, t1, denominator, denominator1) = broadcast.value
        iter.map {
          token =>
            sampleTopics(gen, wMap, totalTopicCounter, t, t1,
              denominator, denominator1, d, d1, token)
        }
    }
  }

  private def updateCounter(graph: Graph[VD, ED], numTopics: Int): Graph[VD, ED] = {
    val newCounter = graph.mapReduceTriplets[BSV[Count]](e => {
      val docId = e.dstId
      val wordId = e.srcId
      val topics = e.attr
      val vector = BSV.zeros[Count](numTopics)
      for (topic <- topics) {
        vector(topic) += 1
      }
      vector.compact()
      Iterator((docId, vector), (wordId, vector))
    }, _ :+ _).mapValues(t => {
      t.compact()
      t
    })
    graph.outerJoinVertices(newCounter)((_, _, n) => n.get)
  }

  private def collectGlobalCounter(graph: Graph[VD, ED], numTopics: Int): BDV[Count] = {
    graph.vertices.filter(t => t._1 >= 0).map(_._2).
      aggregate(BDV.zeros[Count](numTopics))(_ :+= _, _ :+= _)
  }

  private def updateModel(termVertices: VertexRDD[VD]): RDD[(Int, BSV[Double])] = {
    termVertices.map(vertex => {
      val termTopicCounter = vertex._2
      val index = termTopicCounter.index.slice(0, termTopicCounter.used)
      val data = termTopicCounter.data.slice(0, termTopicCounter.used).map(_.toDouble)
      val used = termTopicCounter.used
      val length = termTopicCounter.length
      (vertex._1.toInt, new BSV[Double](index, data, used, length))
    })
  }

  private def initializeCorpus(
    docs: RDD[(LDA.DocId, SSV)],
    numTopics: Int,
    storageLevel: StorageLevel,
    computedModel: Broadcast[LDAModel] = null): Graph[VD, ED] = {
    val edges = docs.mapPartitionsWithIndex((pid, iter) => {
      val gen = new Random(pid)
      var model: LDAModel = null
      if (computedModel != null) model = computedModel.value
      iter.flatMap {
        case (docId, doc) =>
          initializeEdges(gen, new BSV[Int](doc.indices, doc.values.map(_.toInt), doc.size),
            docId, numTopics, model)
      }
    })
    var corpus: Graph[VD, ED] = Graph.fromEdges(edges, null, storageLevel, storageLevel)
    // corpus.partitionBy(PartitionStrategy.EdgePartition1D)
    corpus = updateCounter(corpus, numTopics).cache()
    corpus.vertices.count()
    corpus
  }

  private def initializeEdges(gen: Random, doc: BSV[Int], docId: DocId, numTopics: Int,
    computedModel: LDAModel = null): Array[Edge[ED]] = {
    assert(docId >= 0)
    val newDocId: DocId = -(docId + 1L)
    if (computedModel == null) {
      doc.activeIterator.map { case (term, counter) =>
        val topic = (0 until counter).map { i =>
          uniformDistSampler(gen, numTopics)
        }.toArray
        Edge(term, newDocId, topic)
      }.toArray
    }
    else {
      var docTopicCounter = computedModel.uniformDistSampler(doc, gen)
      (0 to 10).foreach(t => {
        docTopicCounter = computedModel.generateTopicDistForDocument(docTopicCounter, doc, gen)
      })
      doc.activeIterator.map { case (term, counter) =>
        val topic = (0 until counter).map { i =>
          computedModel.termMultinomialDistSampler(docTopicCounter, term, gen)
        }.toArray
        Edge(term, newDocId, topic)
      }.toArray
    }
  }

  /**
   * A multinomial distribution sampler, using roulette method to sample an Int back.
   */
  @inline private def multinomialDistSampler[V](rand: Random, docTopicCounter: BSV[Count],
    d: BDV[Double], w: BSV[Double], t: BDV[Double],
    adjustment: Double, currentTopic: Int): Int = {
    val numTopics = d.length
    val lastSum = t(numTopics - 1) + w.data(w.used - 1) + d(numTopics - 1) + adjustment
    val distSum = rand.nextDouble() * lastSum
    if (distSum >= lastSum) {
      return numTopics - 1
    }
    val fun = index(docTopicCounter, d, w, t, adjustment, currentTopic) _
    minMaxValueSearch(fun, distSum, numTopics)
  }

  @inline private def index(docTopicCounter: BSV[Count],
    d: BDV[Double], w: BSV[Double], t: BDV[Double],
    adjustment: Double, currentTopic: Int)(i: Int) = {
    val lastDS = maxMinD(i, docTopicCounter, d)
    val lastWS = maxMinW(i, w)
    val lastTS = maxMinT(i, t)
    if (i >= currentTopic) {
      lastDS + lastWS + lastTS + adjustment
    } else {
      lastDS + lastWS + lastTS
    }
  }

  @inline private def d(
    denominator: BDV[Double],
    denominator1: BDV[Double],
    termTopicCounter: BSV[Count],
    docTopicCounter: BSV[Count],
    d: BDV[Double],
    d1: BDV[Double],
    sumTerms: Long,
    numTopics: Int,
    beta: Double,
    alphaAS: Double): Unit = {
    val used = docTopicCounter.used
    val index = docTopicCounter.index
    val data = docTopicCounter.data

    val termSum = sumTerms - 1D + alphaAS * numTopics

    var i = 0
    var lastDsum = 0D

    while (i < used) {
      val topic = index(i)
      val count = data(i)
      val lastD = count * termSum * (termTopicCounter(topic) + beta) /
        denominator(topic)

      val lastD1 = count * termSum * (termTopicCounter(topic) - 1D + beta) /
        denominator1(topic)

      lastDsum += lastD
      d(topic) = lastDsum
      d1(topic) = lastD1 - lastD

      i += 1
    }
    d(numTopics - 1) = lastDsum
  }

  @inline private def w(
    totalTopicCounter: BDV[Count],
    denominator: BDV[Double],
    denominator1: BDV[Double],
    termTopicCounter: VD,
    sumTerms: Long,
    numTopics: Int,
    alpha: Double,
    beta: Double,
    alphaAS: Double): Parameter = {
    val alphaSum = alpha * numTopics
    val termSum = sumTerms - 1D + alphaAS * numTopics
    val length = termTopicCounter.length
    val used = termTopicCounter.used
    val index = termTopicCounter.index
    val data = termTopicCounter.data
    val w = new Array[Double](used)
    val w1 = new Array[Double](used)

    var lastWsum = 0D
    var i = 0

    while (i < used) {
      val topic = index(i)
      val count = data(i)
      val lastW = count * alphaSum * (totalTopicCounter(topic) + alphaAS) /
        denominator(topic)

      val lastW1 = count * (alphaSum * (totalTopicCounter(topic) - 1D + alphaAS) - termSum) /
        denominator1(topic)

      lastWsum += lastW
      w(i) = lastWsum
      w1(i) = lastW1 - lastW
      i += 1
    }
    Parameter(termTopicCounter, new BSV[Double](index, w, used, length),
      new BSV[Double](index, w1, used, length))
  }

  private def t(
    totalTopicCounter: BDV[Count],
    denominator: BDV[Double],
    denominator1: BDV[Double],
    sumTerms: Long,
    numTopics: Int,
    alpha: Double,
    beta: Double,
    alphaAS: Double): (BDV[Double], BDV[Double]) = {
    val t = BDV.zeros[Double](numTopics)
    val t1 = BDV.zeros[Double](numTopics)

    val alphaSum = alpha * numTopics
    val termSum = sumTerms - 1D + alphaAS * numTopics

    var lastTsum = 0D
    for (topic <- 0 until numTopics) {
      val lastT = beta * alphaSum * (totalTopicCounter(topic) + alphaAS) /
        denominator(topic)

      val lastT1 = (-1D + beta) * (alphaSum * (totalTopicCounter(topic) +
        (-1D + alphaAS)) - termSum) / denominator1(topic)

      lastTsum += lastT
      t(topic) = lastTsum
      t1(topic) = lastT1 - lastT
    }
    (t, t1)
  }

  private def denominatorBDV(
    totalTopicCounter: BDV[Count],
    sumTerms: Long,
    numTerms: Int,
    numTopics: Int,
    alpha: Double,
    beta: Double,
    alphaAS: Double): (BDV[Double], BDV[Double]) = {
    val termSum = sumTerms - 1D + alphaAS * numTopics
    val betaSum = numTerms * beta
    val denominator = BDV.zeros[Double](numTopics)
    val denominator1 = BDV.zeros[Double](numTopics)
    for (topic <- 0 until numTopics) {
      denominator(topic) = (totalTopicCounter(topic) + betaSum) * termSum
      denominator1(topic) = (totalTopicCounter(topic) - 1D + betaSum) * termSum
    }
    (denominator, denominator1)
  }
}

object LDAUtils {

  /**
   * A uniform distribution sampler, which is only used for initialization.
   */
  @inline private[mllib] def uniformDistSampler(rand: Random, dimension: Int): Int = {
    rand.nextInt(dimension)
  }

  @inline private[mllib] def minMaxIndexSearch[V](v: BSV[V], i: Int,
    lastReturnedPos: Int): Int = {
    val array = v.array
    val index = array.index
    if (array.activeSize == 0) return -1
    if (index(0) > i) return -1
    if (lastReturnedPos >= array.activeSize - 1) return array.activeSize - 1
    var begin = lastReturnedPos
    var end = array.activeSize - 1
    var found = false
    if (end > i) end = i
    if (begin < 0) begin = 0

    var mid = (end + begin) >> 1

    while (!found && begin <= end) {
      if (index(mid) < i) {
        begin = mid + 1
        mid = (end + begin) >> 1
      }
      else if (index(mid) > i) {
        end = mid - 1
        mid = (end + begin) >> 1
      }
      else {
        found = true
      }
    }

    val minMax = if (found || index(mid) < i || mid == 0) {
      mid
    }
    else {
      mid - 1
    }
    assert(index(minMax) <= i)
    if (minMax < array.activeSize - 1) assert(index(minMax + 1) > i)
    minMax
  }

  @inline private[mllib] def minMaxValueSearch(index: (Int) => Double, distSum: Double,
    numTopics: Int): Int = {
    var begin = 0
    var end = numTopics - 1
    var found = false
    var mid = (end + begin) >> 1
    var sum = 0D
    var isLeft = false
    while (!found && begin <= end) {
      sum = index(mid)
      if (sum < distSum) {
        isLeft = false
        begin = mid + 1
        mid = (end + begin) >> 1
      }
      else if (sum > distSum) {
        isLeft = true
        end = mid - 1
        mid = (end + begin) >> 1
      }
      else {
        found = true
      }
    }

    val topic = if (found) {
      mid
    }
    else if (sum < distSum || isLeft) {
      mid + 1
    } else {
      mid - 1
    }

    assert(index(topic) >= distSum)
    if (topic > 0) assert(index(topic - 1) <= distSum)
    topic
  }

  @inline private[mllib] def maxMinD[V](i: Int, docTopicCounter: BSV[V], d: BDV[Double]) = {
    val lastReturnedPos = minMaxIndexSearch(docTopicCounter, i, -1)
    if (lastReturnedPos > -1) {
      d(docTopicCounter.index(lastReturnedPos))
    }
    else {
      0D
    }
  }

  @inline private[mllib] def maxMinW(i: Int, w: BSV[Double]) = {
    val lastReturnedPos = minMaxIndexSearch(w, i, -1)
    if (lastReturnedPos > -1) {
      w.data(lastReturnedPos)
    }
    else {
      0D
    }
  }

  @inline private[mllib] def maxMinT(i: Int, t: BDV[Double]) = {
    t(i)
  }
}

class LDAKryoRegistrator extends KryoRegistrator {
  def registerClasses(kryo: com.esotericsoftware.kryo.Kryo) {
    val gkr = new GraphKryoRegistrator
    gkr.registerClasses(kryo)

    kryo.register(classOf[BSV[LDA.Count]])
    kryo.register(classOf[BSV[Double]])

    kryo.register(classOf[BDV[LDA.Count]])
    kryo.register(classOf[BDV[Double]])

    kryo.register(classOf[SV])
    kryo.register(classOf[SSV])
    kryo.register(classOf[SDV])

    kryo.register(classOf[LDA.ED])
    kryo.register(classOf[LDA.VD])
    kryo.register(classOf[LDA.Parameter])
    kryo.register(classOf[LDA.GlobalParameter])

    kryo.register(classOf[Random])
    kryo.register(classOf[LDA])
    kryo.register(classOf[LDAModel])
  }
}
