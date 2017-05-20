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

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.clustering.topicmodeling.regulaizers.{TopicsRegularizer, MatrixInPlaceModification}
import org.apache.spark.mllib.feature.Document
import org.apache.spark.rdd.RDD

import breeze.linalg._

private[topicmodeling] trait AbstractPLSA[DocumentParameterType <: DocumentParameters,
                  GlobalParameterType <: GlobalParameters,
                  GlobalCounterType <: GlobalCounters]
    extends TopicModel[DocumentParameterType, GlobalParameterType] with MatrixInPlaceModification {
  protected val numberOfTopics: Int
  protected val random: Random
  protected val topicRegularizer: TopicsRegularizer
  protected val sc: SparkContext

  protected def generalizedPerplexity(topicsBC: Broadcast[Array[Array[Float]]],
      parameters: RDD[DocumentParameterType],
      collectionLength: Int,
      wordGivenModel: DocumentParameterType => (Int, Int) => Float) = {
    math.exp(-(parameters.aggregate(0f)((thatOne, otherOne) =>
      thatOne + singleDocumentLikelihood(otherOne, topicsBC, wordGivenModel(otherOne)),
      (thatOne, otherOne) => thatOne + otherOne) + topicRegularizer(topicsBC.value)) /
      collectionLength)
  }

  protected def getAlphabetSize(documents: RDD[Document]) = documents.first().alphabetSize

  protected def getCollectionLength(documents: RDD[Document]) =
    documents.map(doc => sum(doc.tokens)).reduce(_ + _)

  protected def singleDocumentLikelihood(parameter: DocumentParameters,
      topicsBC: Broadcast[Array[Array[Float]]],
      wordGivenModel: ((Int, Int) => Float)) = {
    sum(parameter.document.tokens.mapActivePairs(wordGivenModel)) +
      parameter.priorThetaLogProbability
  }

  protected def probabilityOfWordGivenTopic(word: Int,
      parameter: DocumentParameters,
      topicsBC: Broadcast[Array[Array[Float]]]) = {
    var underLog = 0f
    for (topic <- 0 until numberOfTopics) {
      underLog += parameter.theta(topic) * topicsBC.value(topic)(word)
    }
    underLog
  }

  protected def getInitialTopics(alphabetSize: Int) = {
    val topics = Array.fill[Float](numberOfTopics, alphabetSize)(random.nextFloat)
    normalize(topics)
    sc.broadcast(topics)
  }

  protected def getTopics(parameters: RDD[DocumentParameterType],
      alphabetSize: Int,
      oldTopics: Broadcast[Array[Array[Float]]],
      globalCounters: GlobalCounterType,
      foldingIn : Boolean) = {
    if (foldingIn) oldTopics
    else {
      val newTopicCnt: Array[Array[Float]] = globalCounters.wordsFromTopics

      topicRegularizer.regularize(newTopicCnt, oldTopics.value)
      normalize(newTopicCnt)

      sc.broadcast(newTopicCnt)
    }
  }

  private def normalize(matrix: Array[Array[Float]]) = {
    matrix.foreach(array => {
      val sum = array.sum
      shift(array, (arr, i) => arr(i) /= sum)
    })
  }
}
