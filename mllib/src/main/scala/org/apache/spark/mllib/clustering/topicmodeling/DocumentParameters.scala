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

import breeze.linalg.SparseVector
import org.apache.spark.mllib.clustering.topicmodeling.regulaizers.DocumentOverTopicDistributionRegularizer
import org.apache.spark.mllib.feature.Document

/**
 * the class contains document parameter in PLSA model
 * @param document
 * @param theta the distribution over topics
 * @param regularizer
 */
class DocumentParameters(val document: Document,
                         val theta: Array[Float],
                         private val regularizer: DocumentOverTopicDistributionRegularizer)
    extends Serializable {
  private def getZ(topics: Array[Array[Float]]) = {
    val numberOfTopics = topics.size

    document.tokens.mapActivePairs { case (word, n) =>
      (0 until numberOfTopics).foldLeft(0f)((sum, topic) =>
        sum + topics(topic)(word) * theta(topic))
    }
  }

  private[topicmodeling] def wordsFromTopics(topics: Array[Array[Float]]):
      Array[SparseVector[Float]] = {
    val Z = getZ(topics)

    wordsToTopicCnt(topics, Z)
  }

  private[topicmodeling] def wordsToTopicCnt(topics: Array[Array[Float]],
      Z: SparseVector[Float]): Array[SparseVector[Float]] = {
    val array = Array.ofDim[SparseVector[Float]](theta.size)
    forWithIndex(theta)((topicWeight, topicNum) =>
      array(topicNum) = document.tokens.mapActivePairs { case (word,
      num) => num * topics(topicNum)(word) * topicWeight / Z(word)
      }
    )
    array
  }

  private def forWithIndex(array: Array[Float])(operation: (Float, Int) => Unit) {
    var i = 0
    val size = array.size
    while (i < size) {
      operation(array(i), i)
      i += 1
    }
  }

  private[topicmodeling] def assignNewTheta(topics: Array[Array[Float]],
      Z: SparseVector[Float]) {
    val newTheta: Array[Float] = {
      val array = Array.ofDim[Float](theta.size)
      forWithIndex(theta)((weight, topicNum) => array(topicNum) = weight * document.tokens
        .activeIterator.foldLeft(0f) { case (sum, (word, wordNum)) => 
          sum + wordNum * topics(topicNum)(word) / Z(word)
      })
      array
    }
    regularizer.regularize(newTheta, theta)

    val newThetaSum = newTheta.sum

    forWithIndex(newTheta)((wordsNum, topicNum) => theta(topicNum) = wordsNum / newThetaSum)

  }

  private[topicmodeling] def getNewTheta(topics: Array[Array[Float]]) = {
    val Z = getZ(topics)
    assignNewTheta(topics, Z)

    this
  }

  private[topicmodeling]  def priorThetaLogProbability = regularizer(theta)

}


private[topicmodeling] object DocumentParameters extends SparseVectorFasterSum {

  def apply(document: Document,
            numberOfTopics: Int,
            regularizer: DocumentOverTopicDistributionRegularizer) = {
    val theta = getTheta(numberOfTopics)
    new DocumentParameters(document, theta, regularizer)
  }

  private def getTheta(numberOfTopics: Int) = {
    Array.fill[Float](numberOfTopics)(1f / numberOfTopics)
  }
}
