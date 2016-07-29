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
package org.apache.spark.mllib.api.python

import scala.collection.JavaConverters._

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.LDAModel
import org.apache.spark.mllib.linalg.{Matrix, Vector}
import org.apache.spark.rdd.RDD

/**
 * Wrapper around LDAModel to provide helper methods in Python
 */
private[python] class LDAModelWrapper(model: LDAModel) {

  def topicsMatrix(): Matrix = model.topicsMatrix

  def vocabSize(): Int = model.vocabSize

  def describeTopics(): Array[Byte] = describeTopics(this.model.vocabSize)

  def describeTopics(maxTermsPerTopic: Int): Array[Byte] = {
    val topics = model.describeTopics(maxTermsPerTopic).map { case (terms, termWeights) =>
      val jTerms = seqAsJavaListConverter(terms).asJava
      val jTermWeights = seqAsJavaListConverter(termWeights).asJava
      Array[Any](jTerms, jTermWeights)
    }
    SerDe.dumps(seqAsJavaListConverter(topics).asJava)
  }

  def topicDistributions(
    data: JavaRDD[java.util.List[Any]]): JavaRDD[Array[Any]] = {

    val documents = data.rdd.map(_.asScala.toArray).map { r =>
      r(0) match {
        case i: java.lang.Integer => (i.toLong, r(1).asInstanceOf[Vector])
        case i: java.lang.Long => (i.toLong, r(1).asInstanceOf[Vector])
        case _ => throw new IllegalArgumentException("input values contains invalid type value.")
      }
    }

    val distributions = model.topicDistributions(documents)

    SerDe.fromTuple2RDD( distributions.map {
      case (id, vector) => ( id.toLong, vector.asInstanceOf[ Vector ] )
    }.asInstanceOf[ RDD[(Any, Any)] ])

  }

  def save(sc: SparkContext, path: String): Unit = model.save(sc, path)
}
