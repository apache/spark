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

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, sum => brzSum, norm => brzNorm}
import org.apache.spark.mllib.linalg.{Vectors, DenseVector => SDV, SparseVector => SSV}

class LDAModel private[mllib](
  private[mllib] val gtc: BDV[Double],
  private[mllib] val ttc: Array[BSV[Double]],
  val alpha: Double,
  val beta: Double) extends Serializable {

  def this(topicCounts: SDV, topicTermCounts: Array[SSV], alpha: Double, beta: Double) {
    this(new BDV[Double](topicCounts.toArray), topicTermCounts.map(t =>
      new BSV(t.indices, t.values, t.size)), alpha, beta)
  }

  val (numTopics, numTerms) = (gtc.size, ttc.size)

  def globalTopicCounter = Vectors.fromBreeze(gtc)

  def topicTermCounter = ttc.map(t => Vectors.fromBreeze(t))

  def inference(doc: SSV, totalIter: Int = 10, burnIn: Int = 5, rand: Random = new Random): SSV = {
    require(totalIter > burnIn, "totalIter is less than burnInIter")
    require(totalIter > 0, "totalIter is less than 0")
    require(burnIn > 0, "burnInIter is less than 0")
    val topicDist = BSV.zeros[Double](numTopics)
    val bDoc = new BSV[Int](doc.indices, doc.values.map(_.toInt), doc.size)
    var docTopicCounter = uniformDistSampler(bDoc, rand)

    for (i <- 0 until totalIter) {
      docTopicCounter = generateTopicDistForDocument(docTopicCounter, bDoc, rand)
      if (i + burnIn >= totalIter) topicDist :+= docTopicCounter
    }

    topicDist :/= brzNorm(topicDist, 1)
    Vectors.fromBreeze(topicDist).asInstanceOf[SSV]
  }

  private[mllib] def generateTopicDistForDocument(
    docTopicCounter: BSV[Double],
    doc: BSV[Int],
    rand: Random): BSV[Double] = {
    val newDocTopicCounter = BSV.zeros[Double](numTopics)
    doc.activeIterator.filter(t => ttc(t._1).used > 0).foreach { kv =>
      val term = kv._1
      val cn = kv._2
      var i = 0
      while (i < cn) {
        val newTopic = termMultinomialDistSampler(docTopicCounter, term, rand)
        newDocTopicCounter(newTopic) += 1
        i += 1
      }
    }
    newDocTopicCounter
  }

  private[mllib] def termMultinomialDistSampler(
    docTopicCounter: BSV[Double],
    term: Int,
    rand: Random): Int = {
    val d = this.d(docTopicCounter, term)
    multinomialDistSampler(rand, t, w(term), docTopicCounter, d)
  }

  private[mllib] def uniformDistSampler(doc: BSV[Int], rand: Random): BSV[Double] = {
    val docTopicCounter = BSV.zeros[Double](numTopics)
    (0 until brzSum(doc)).foreach { i =>
      val topic = LDAUtils.uniformDistSampler(rand, numTopics)
      docTopicCounter(topic) += 1
    }
    docTopicCounter
  }

  /**
   * A multinomial distribution sampler, using roulette method to sample an Int back.
   */
  @inline private def multinomialDistSampler(rand: Random, t: BDV[Double],
    w: BSV[Double], docTopicCounter: BSV[Double], d: BDV[Double]): Int = {
    val distSum = rand.nextDouble * (t(numTopics - 1) + w.data(w.used - 1) + d(numTopics - 1))
    val fun = index(t, w, docTopicCounter, d) _
    LDAUtils.minMaxValueSearch(fun, distSum, numTopics)
  }

  @inline private def index(t: BDV[Double],
    w: BSV[Double], docTopicCounter: BSV[Double], d: BDV[Double])(i: Int) = {
    val lastDS = LDAUtils.maxMinD(i, docTopicCounter, d)
    val lastWS = LDAUtils.maxMinW(i, w)
    val lastTS = LDAUtils.maxMinT(i, t)
    lastDS + lastWS + lastTS
  }

  @transient private lazy val localD = new ThreadLocal[BDV[Double]] {
    override def initialValue: BDV[Double] = {
      BDV.zeros[Double](numTopics)
    }
  }

  @inline private def d(docTopicCounter: BSV[Double], term: Int): BDV[Double] = {
    val d = localD.get()
    var i = 0
    var lastSum = 0D
    docTopicCounter.activeIterator.foreach { t =>
      val topic = t._1
      val cn = t._2
      val lastD = cn * (ttc(term)(topic) + beta) / (gtc(topic) + (numTerms * beta))
      lastSum += lastD
      d(topic) = lastSum
      i += 1
    }
    d(numTopics - 1) = lastSum
    d
  }

  @transient private lazy val t = {
    val t = BDV.zeros[Double](numTopics)
    var lastSum = 0D
    for (i <- 0 until numTopics) {
      t(i) = alpha * beta / (gtc(i) + (numTerms * beta))
      lastSum = t(i) + lastSum
      t(i) = lastSum
    }
    t
  }

  @transient private lazy val w = {
    val w = new Array[BSV[Double]](numTerms)
    for (term <- 0 until numTerms) {
      w(term) = BSV.zeros[Double](numTopics)
      var lastSum = 0D
      ttc(term).activeIterator.foreach { case (topic, cn) =>
        w(term)(topic) = alpha * cn / (gtc(topic) + (numTerms * beta))
        lastSum = w(term)(topic) + lastSum
        w(term)(topic) = lastSum
      }
    }
    w
  }

  private[mllib] def merge(term: Int, topic: Int, inc: Int) = {
    gtc(topic) += inc
    ttc(term)(topic) += inc
    this
  }

  private[mllib] def merge(term: Int, counter: BSV[Double]) = {
    ttc(term) :+= counter
    gtc :+= counter
    this
  }

  private[mllib] def merge(other: LDAModel) = {
    gtc :+= other.gtc
    for (i <- 0 until ttc.length) {
      ttc(i) :+= other.ttc(i)
    }
    this
  }

}

object LDAModel {
  def apply(numTopics: Int, numTerms: Int, alpha: Double = 0.1, beta: Double = 0.01) = {
    new LDAModel(
      BDV.zeros[Double](numTopics),
      (0 until numTerms).map(_ => BSV.zeros[Double](numTopics)).toArray, alpha, beta)
  }
}
