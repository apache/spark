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
 *
 * distributed topic modeling via PLSA (Hofmann (1999), Vorontsov, Potapenko (2014) )
 * @param sc  spark context
 * @param numberOfTopics number of topics
 * @param numberOfIterations number of iterations
 * @param random java.util.Random need for initialisation
 * @param documentOverTopicDistributionRegularizer
 * @param topicRegularizer
 * @param computePpx boolean. If true, model computes perplexity and prints it puts in the log at
 *                   INFO level. it takes some time and memory
 */
class PLSA(@transient protected val sc: SparkContext,
           protected val numberOfTopics: Int,
           private val numberOfIterations: Int,
           protected val random: Random,
           private val documentOverTopicDistributionRegularizer:
              DocumentOverTopicDistributionRegularizer = new UniformDocumentOverTopicRegularizer,
           @transient protected val topicRegularizer: TopicsRegularizer =
                  new UniformTopicRegularizer,
           private val computePpx: Boolean = true)
  extends AbstractPLSA[DocumentParameters, GlobalParameters, GlobalCounters]
  with Logging
  with Serializable {


  /**
   *
   * @param documents  -- document collection
   * @return a pair of rdd of document parameters global parameters
   */
  override def infer(documents: RDD[Document]): (RDD[DocumentParameters], GlobalParameters) = {
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
                      globalParams: GlobalParameters): RDD[DocumentParameters] = {
    EM(documents, sc.broadcast(globalParams.phi), globalParams.alphabetSize, foldingIn = true)._1
  }

  private def EM(documents: RDD[Document],
                 topicBC: Broadcast[Array[Array[Float]]],
                 alphabetSize : Int,
                 foldingIn : Boolean): (RDD[DocumentParameters], GlobalParameters) = {
    val alphabetSize = getAlphabetSize(documents)

    val collectionLength = getCollectionLength(documents)

    val parameters = documents.map(doc => DocumentParameters(doc,
                                                    numberOfTopics,
                                                    documentOverTopicDistributionRegularizer))

    val (result, topics) = newIteration(parameters,
        topicBC,
        alphabetSize,
        collectionLength,
        0,
        foldingIn)

    (result, new GlobalParameters(topics.value,alphabetSize))
  }

  private def newIteration(parameters: RDD[DocumentParameters],
       topicsBC: Broadcast[Array[Array[Float]]],
       alphabetSize: Int,
       collectionLength: Int,
       numberOfIteration: Int,
       foldingIn : Boolean): (RDD[DocumentParameters], Broadcast[Array[Array[Float]]]) = {
    if (computePpx) {
      logInfo("Iteration number " + numberOfIteration)
      logInfo("Perplexity=" + perplexity(topicsBC, parameters, collectionLength))
    }
    if (numberOfIteration == numberOfIterations) {
      (parameters, topicsBC)
    } else {
      val newParameters = parameters.map(param => param.getNewTheta(topicsBC.value)).cache()
      val globalCounters = getGlobalCounters(parameters, topicsBC, alphabetSize)
      val newTopics = getTopics(newParameters,
          alphabetSize,
          topicsBC,
          globalCounters,
          foldingIn)

      parameters.unpersist()
      topicsBC.unpersist()

      newIteration(newParameters,
        newTopics,
        alphabetSize,
        collectionLength,
        numberOfIteration + 1,
        foldingIn)
    }
  }

  private def getGlobalCounters(parameters: RDD[DocumentParameters],
                                  topics: Broadcast[Array[Array[Float]]],
                                  alphabetSize: Int) = {
    parameters.aggregate[GlobalCounters](GlobalCounters(numberOfTopics,
      alphabetSize))((thatOne, otherOne) => thatOne.add(otherOne, topics.value, alphabetSize),
        (thatOne, otherOne) => thatOne + otherOne)
  }

  private def perplexity(topicsBC: Broadcast[Array[Array[Float]]],
                         parameters: RDD[DocumentParameters], collectionLength: Int) = {
    generalizedPerplexity(topicsBC,
      parameters,
      collectionLength,
      par => (word,num) => num * math.log(probabilityOfWordGivenTopic(word, par, topicsBC)).toFloat)
  }
}
