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

package org.apache.spark.mllib.expectation

import java.util.Random

import breeze.linalg.{DenseVector => BDV, sum}

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.{Document, LDAParams}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
 * Gibbs sampling from a given dataset and org.apache.spark.mllib.model.
 * @param data Dataset, such as corpus.
 * @param numOuterIterations Number of outer iteration.
 * @param numInnerIterations Number of inner iteration, used in each partition.
 * @param docTopicSmoothing Document-topic smoothing.
 * @param topicTermSmoothing Topic-term smoothing.
 */
class GibbsSampling(
    data: RDD[Document],
    numOuterIterations: Int,
    numInnerIterations: Int,
    docTopicSmoothing: Double,
    topicTermSmoothing: Double)
  extends Logging with Serializable {

  import GibbsSampling._

  /**
   * Main function of running a Gibbs sampling method. It contains two phases of total Gibbs
   * sampling: first is initialization, second is real sampling.
   */
  def runGibbsSampling(
      initParams: LDAParams,
      data: RDD[Document] = data,
      numOuterIterations: Int = numOuterIterations,
      numInnerIterations: Int = numInnerIterations,
      docTopicSmoothing: Double = docTopicSmoothing,
      topicTermSmoothing: Double = topicTermSmoothing): LDAParams = {

    val numTerms = initParams.topicTermCounts.head.size
    val numDocs = initParams.docCounts.size
    val numTopics = initParams.topicCounts.size

    // Construct topic assignment RDD
    logInfo("Start initialization")

    val cpInterval = System.getProperty("spark.gibbsSampling.checkPointInterval", "10").toInt
    val sc = data.context
    val (initialParams, initialChosenTopics) = sampleTermAssignment(initParams, data)

    // Gibbs sampling
    val (params, _, _) = Iterator.iterate((sc.accumulable(initialParams), initialChosenTopics, 0)) {
      case (lastParams, lastChosenTopics, i) =>
        logInfo("Start Gibbs sampling")

        val rand = new Random(42 + i * i)
        val params = sc.accumulable(LDAParams(numDocs, numTopics, numTerms))
        val chosenTopics = data.zip(lastChosenTopics).map {
          case (Document(docId, content), topics) =>
            content.zip(topics).map { case (term, topic) =>
              lastParams += (docId, term, topic, -1)

              val chosenTopic = lastParams.localValue.dropOneDistSampler(
                docTopicSmoothing, topicTermSmoothing, term, docId, rand)

              lastParams += (docId, term, chosenTopic, 1)
              params += (docId, term, chosenTopic, 1)

              chosenTopic
            }
        }.cache()

        if (i + 1 % cpInterval == 0) {
          chosenTopics.checkpoint()
        }

        // Trigger a job to collect accumulable LDA parameters.
        chosenTopics.count()
        lastChosenTopics.unpersist()

        (params, chosenTopics, i + 1)
    }.drop(1 + numOuterIterations).next()

    params.value
  }

  /**
   * Model matrix Phi and Theta are inferred via LDAParams.
   */
  def solvePhiAndTheta(
      params: LDAParams,
      docTopicSmoothing: Double = docTopicSmoothing,
      topicTermSmoothing: Double = topicTermSmoothing): (Array[Vector], Array[Vector]) = {
    val numTopics = params.topicCounts.size
    val numTerms = params.topicTermCounts.head.size

    val docCount = params.docCounts.toBreeze :+ (docTopicSmoothing * numTopics)
    val topicCount = params.topicCounts.toBreeze :+ (topicTermSmoothing * numTerms)
    val docTopicCount = params.docTopicCounts.map(vec => vec.toBreeze :+ docTopicSmoothing)
    val topicTermCount = params.topicTermCounts.map(vec => vec.toBreeze :+ topicTermSmoothing)

    var i = 0
    while (i < numTopics) {
      topicTermCount(i) :/= topicCount(i)
      i += 1
    }

    i = 0
    while (i < docCount.length) {
      docTopicCount(i) :/= docCount(i)
      i += 1
    }

    (topicTermCount.map(vec => Vectors.fromBreeze(vec)),
      docTopicCount.map(vec => Vectors.fromBreeze(vec)))
  }
}

object GibbsSampling extends Logging {

  /**
   * Initial step of Gibbs sampling, which supports incremental LDA.
   */
  private def sampleTermAssignment(
      params: LDAParams,
      data: RDD[Document]): (LDAParams, RDD[Iterable[Int]]) = {

    val sc = data.context
    val initialParams = sc.accumulable(params)
    val rand = new Random(42)
    val initialChosenTopics = data.map { case Document(docId, content) =>
      val docTopics = params.docTopicCounts(docId)
      if (docTopics.toBreeze.norm(2) == 0) {
        content.map { term =>
          val topic = uniformDistSampler(rand, params.topicCounts.size)
          initialParams += (docId, term, topic, 1)
          topic
        }
      } else {
        content.map { term =>
          val topicTerms = Vectors.dense(params.topicTermCounts.map(_(term))).toBreeze
          val dist = docTopics.toBreeze :* topicTerms
          multinomialDistSampler(rand, dist.asInstanceOf[BDV[Double]])
        }
      }
    }.cache()

    // Trigger a job to collect accumulable LDA parameters.
    initialChosenTopics.count()

    (initialParams.value, initialChosenTopics)
  }

  /**
   * A uniform distribution sampler, which is only used for initialization.
   */
  private def uniformDistSampler(rand: Random, dimension: Int): Int = rand.nextInt(dimension)

  /**
   * A multinomial distribution sampler, using roulette method to sample an Int back.
   */
  def multinomialDistSampler(rand: Random, dist: BDV[Double]): Int = {
    val roulette = rand.nextDouble()

    dist :/= sum[BDV[Double], Double](dist)

    def loop(index: Int, accum: Double): Int = {
      if(index == dist.length) return dist.length - 1
      val sum = accum + dist(index)
      if (sum >= roulette) index else loop(index + 1, sum)
    }

    loop(0, 0.0)
  }

  /**
   * Perplexity is a kind of evaluation method of LDA. Usually it is used on unseen data. But here
   * we use it for current documents, which is also OK. If using it on unseen data, you must do an
   * iteration of Gibbs sampling before calling this. Small perplexity means good result.
   */
  def perplexity(data: RDD[Document], phi: Array[Vector], theta: Array[Vector]): Double = {
    val (termProb, totalNum) = data.flatMap { case Document(docId, content) =>
      val currentTheta = BDV.zeros[Double](phi.head.size)
      var col = 0
      var row = 0
      while (col < phi.head.size) {
        row = 0
        while (row < phi.length) {
          currentTheta(col) += phi(row)(col) * theta(docId)(row)
          row += 1
        }
        col += 1
      }
      content.map(x => (math.log(currentTheta(x)), 1))
    }.reduce { (lhs, rhs) =>
      (lhs._1 + rhs._1, lhs._2 + rhs._2)
    }
    math.exp(-1 * termProb / totalNum)
  }
}
