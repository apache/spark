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

package org.apache.spark.mllib.clustering.topicmodeling

import java.util.Random

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.clustering.topicmodeling.regulaizers.{DocumentOverTopicDistributionRegularizer, TopicsRegularizer, UniformDocumentOverTopicRegularizer, UniformTopicRegularizer}
import org.apache.spark.mllib.feature.Document
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}


/**
 * distributed topic modeling via RobustPLSA (Hofmann (1999), Vorontsov, Potapenko (2014) )
 *
 * @param sc  spark context
 * @param numberOfTopics number of topics
 * @param numberOfIterations number of iterations
 * @param random java.util.Random need for initialisation
 * @param documentOverTopicDistributionRegularizer
 * @param topicRegularizer
 * @param computePpx boolean. If true, model computes perplexity and prints it puts in the log at
 *                   INFO level. it takes some time and memory
 * @param gamma weight of background
 * @param eps   weight of noise
 */
class RobustPLSA(@transient protected val sc: SparkContext,
                 protected val numberOfTopics: Int,
                 protected val numberOfIterations: Int,
                 protected val random: Random,
                 private val documentOverTopicDistributionRegularizer:
                  DocumentOverTopicDistributionRegularizer =
                          new UniformDocumentOverTopicRegularizer,
                 @transient protected val topicRegularizer: TopicsRegularizer =
                                                        new UniformTopicRegularizer,
                 private val computePpx: Boolean = true,
                 private val gamma: Float = 0.3f,
                 private val eps: Float = 0.01f)
  extends AbstractPLSA[RobustDocumentParameters, RobustGlobalParameters, RobustGlobalCounters]
  with Logging
  with Serializable {


  /**
   *
   * @param documents  -- document collection
   * @return a pair of rdd of document parameters global parameters
   */
  override def infer(documents: RDD[Document])
      : (RDD[RobustDocumentParameters], RobustGlobalParameters) = {
    val alphabetSize = getAlphabetSize(documents)
    EM(documents, getInitialTopics(alphabetSize), alphabetSize, foldingIn = false)
  }


  /**
   *
   * @param documents  docs to be folded in
   * @param globalParams global parameters that were produced by infer method (stores topics)
   * @return
   */
  override def foldIn(documents: RDD[Document],
                      globalParams: RobustGlobalParameters): RDD[RobustDocumentParameters] = {
    EM(documents, sc.broadcast(globalParams.phi), globalParams.alphabetSize, foldingIn = true)._1
  }

  private def EM(documents: RDD[Document],
                 topicBC: Broadcast[Array[Array[Float]]],
                 alphabetSize : Int,
                 foldingIn : Boolean) :(RDD[RobustDocumentParameters], RobustGlobalParameters) = {
    val collectionLength = getCollectionLength(documents)

    val parameters = documents.map(doc => RobustDocumentParameters(doc,
                                                  numberOfTopics,
                                                  gamma,
                                                  eps,
                                                  documentOverTopicDistributionRegularizer))

    val initBackground = sc.broadcast(Array.fill(alphabetSize)(1f / alphabetSize))

    val (result, topics, newBackground) = newIteration(parameters,
                                          topicBC,
                                          initBackground,
                                          alphabetSize,
                                          collectionLength,
                                          0,
                                          foldingIn)

    (result, new RobustGlobalParameters(topics.value, alphabetSize, newBackground.value))
  }


  private def newIteration(parameters: RDD[RobustDocumentParameters],
                           topicsBC: Broadcast[Array[Array[Float]]],
                           backgroundBC: Broadcast[Array[Float]],
                           alphabetSize: Int,
                           collectionLength: Int,
                           numberOfIteration: Int,
                           foldingIn : Boolean):
                  (RDD[RobustDocumentParameters],
                    Broadcast[Array[Array[Float]]],
                    Broadcast[Array[Float]]) = {

    if (computePpx) {
      logInfo("Iteration number " + numberOfIteration)
      logInfo("Perplexity=" + perplexity(topicsBC, parameters, backgroundBC, collectionLength))
    }
    if (numberOfIteration == numberOfIterations) {
      (parameters, topicsBC, backgroundBC)
    } else {
      val newParameters = parameters.map(parameter =>
        parameter.getNewTheta(topicsBC.value, backgroundBC.value, eps, gamma)).cache()
      val globalCounters = getGlobalCounters(parameters, topicsBC, backgroundBC, alphabetSize)
      val newTopics = getTopics(newParameters,
          alphabetSize,
          topicsBC,
          globalCounters,
          foldingIn)

      val newBackground = sc.broadcast(getNewBackground(globalCounters))

      parameters.unpersist()
      topicsBC.unpersist()
      backgroundBC.unpersist()

      newIteration(newParameters,
        newTopics,
        newBackground,
        alphabetSize,
        collectionLength,
        numberOfIteration + 1,
        foldingIn)
    }
  }

  private def getGlobalCounters(parameters: RDD[RobustDocumentParameters],
                                  topics: Broadcast[Array[Array[Float]]],
                                  background: Broadcast[Array[Float]],
                                  alphabetSize: Int) = {
    parameters.aggregate[RobustGlobalCounters](RobustGlobalCounters(numberOfTopics,
      alphabetSize))(
        (thatOne, otherOne) =>
          thatOne.add(otherOne, topics.value, background.value, eps, gamma,alphabetSize),
        (thatOne, otherOne) =>
          thatOne + otherOne)
  }

  private def getNewBackground(globalCounters: RobustGlobalCounters) = {
    val sum = globalCounters.backgroundWords.sum
    if (sum > 0 && gamma != 0) {
      globalCounters.backgroundWords.map(i => i / sum)
    } else {
      Array.fill[Float](globalCounters.alphabetSize)(0f)
    }
  }


  private def perplexity(topicsBC: Broadcast[Array[Array[Float]]],
                         parameters: RDD[RobustDocumentParameters],
                         backgroundBC: Broadcast[Array[Float]],
                         collectionLength: Int) =
    generalizedPerplexity(topicsBC,
      parameters,
      collectionLength,
      par => (word,num) =>
        num * math.log((probabilityOfWordGivenTopic(word, par, topicsBC) +
          gamma * backgroundBC.value(word) +
          eps * par.noise(word)) / (1 + eps + gamma)).toFloat)

}

