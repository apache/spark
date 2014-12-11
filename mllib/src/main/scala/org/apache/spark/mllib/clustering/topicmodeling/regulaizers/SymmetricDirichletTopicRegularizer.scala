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



package org.apache.spark.mllib.clustering.topicmodeling.regulaizers


/** Defines Dirichlet symmetric prior on every topic
 *
 * @param alpha - parmeter of Dirichlet distribution
 */
class SymmetricDirichletTopicRegularizer(private[mllib] val alpha: Float) extends TopicsRegularizer
    with MatrixInPlaceModification with SymmetricDirichletHelper {
  private[mllib] override def apply(topics: Array[Array[Float]]): Float =
    topics.foldLeft(0f)((sum,x) => sum + dirichletLogLikelihood(x))

  private[mllib] override def regularize(topics: Array[Array[Float]],
                                         oldTopics: Array[Array[Float]]): Unit = {
    shift(topics, (matrix, i, j) => matrix(i)(j) += (alpha - 1))
    super.regularize(topics, oldTopics: Array[Array[Float]])
  }
}
