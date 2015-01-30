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

import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.scalatest.FunSuite

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV}
import breeze.stats.distributions.Poisson
import org.apache.spark.mllib.linalg.{Vectors, SparseVector => SSV}

class LDASuite extends FunSuite with MLlibTestSparkContext {

  import LDASuite._

  test("LDA || Gibbs sampling") {
    val model = generateRandomLDAModel(numTopics, numTerms)
    val corpus = sampleCorpus(model, numDocs, numTerms, numTopics)

    val data = sc.parallelize(corpus, 2)
    val pps = new Array[Double](incrementalLearning)
    val lda = new LDA(data, numTopics, alpha, beta, alphaAS)
    var i = 0
    val startedAt = System.currentTimeMillis()
    while (i < incrementalLearning) {
      lda.runGibbsSampling(totalIterations)
      pps(i) = lda.perplexity
      i += 1
    }

    // println((System.currentTimeMillis() - startedAt) / 1e3)
    // pps.foreach(println)

    val ppsDiff = pps.init.zip(pps.tail).map { case (lhs, rhs) => lhs - rhs}
    assert(ppsDiff.count(_ > 0).toDouble / ppsDiff.size > 0.6)
    assert(pps.head - pps.last > 0)
  }

}

object LDASuite {
  val numTopics = 5
  val numTerms = 1000
  val numDocs = 100
  val expectedDocLength = 300
  val alpha = 0.01
  val alphaAS = 1D
  val beta = 0.01
  val totalIterations = 2
  val burnInIterations = 1
  val incrementalLearning = 10

  /**
   * Generate a random LDA model, i.e. the topic-term matrix.
   */
  def generateRandomLDAModel(numTopics: Int, numTerms: Int): Array[BDV[Double]] = {
    val model = new Array[BDV[Double]](numTopics)
    val width = numTerms * 1.0 / numTopics
    var topic = 0
    var i = 0
    while (topic < numTopics) {
      val topicCentroid = width * (topic + 1)
      model(topic) = BDV.zeros[Double](numTerms)
      i = 0
      while (i < numTerms) {
        // treat the term list as a circle, so the distance between the first one and the last one
        // is 1, not n-1.
        val distance = Math.abs(topicCentroid - i) % (numTerms / 2)
        // Possibility is decay along with distance
        model(topic)(i) = 1.0 / (1 + Math.abs(distance))
        i += 1
      }
      topic += 1
    }
    model
  }

  /**
   * Sample one document given the topic-term matrix.
   */
  def ldaSampler(
    model: Array[BDV[Double]],
    topicDist: BDV[Double],
    numTermsPerDoc: Int): Array[Int] = {
    val samples = new Array[Int](numTermsPerDoc)
    val rand = new Random()
    (0 until numTermsPerDoc).foreach { i =>
      samples(i) = multinomialDistSampler(
        rand,
        model(multinomialDistSampler(rand, topicDist))
      )
    }
    samples
  }

  /**
   * Sample corpus (many documents) from a given topic-term matrix.
   */
  def sampleCorpus(
    model: Array[BDV[Double]],
    numDocs: Int,
    numTerms: Int,
    numTopics: Int): Array[(Long, SSV)] = {
    (0 until numDocs).map { i =>
      val rand = new Random()
      val numTermsPerDoc = Poisson.distribution(expectedDocLength).sample()
      val numTopicsPerDoc = rand.nextInt(numTopics / 2) + 1
      val topicDist = BDV.zeros[Double](numTopics)
      (0 until numTopicsPerDoc).foreach { _ =>
        topicDist(rand.nextInt(numTopics)) += 1
      }
      val sv = BSV.zeros[Double](numTerms)
      ldaSampler(model, topicDist, numTermsPerDoc).foreach { term => sv(term) += 1}
      (i.toLong, Vectors.fromBreeze(sv).asInstanceOf[SSV])
    }.toArray
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
