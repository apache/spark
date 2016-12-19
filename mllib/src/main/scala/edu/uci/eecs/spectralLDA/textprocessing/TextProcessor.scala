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


package edu.uci.eecs.spectralLDA.textprocessing

import breeze.linalg.{DenseVector, SparseVector}

import org.apache.spark.rdd.RDD

object TextProcessor {

  /** Returns the Inverse Document Frequency
   * @param docs the documents RDD
   * @return     the IDF vector
   */
  def inverseDocumentFrequency(docs: RDD[(Long, SparseVector[Double])], approxCount: Boolean = true)
      : DenseVector[Double] = {
    val numDocs: Double = if (approxCount) {
      docs.countApprox(30000L).getFinalValue.mean
    }
    else {
      docs.count.toDouble
    }

    val vocabSize: Int = docs.map(_._2.length).take(1)(0)
    val documentFrequency: DenseVector[Double] = DenseVector.zeros[Double](vocabSize)

    docs
      .flatMap[(Int, Double)] {
        case (_, w: SparseVector[Double]) =>
          for ((token, count) <- w.activeIterator)
            yield (token, if (count > 0.0) 1.0 else 0.0)
      }
      .reduceByKey(_ + _)
      .collect
      .foreach {
        case (token, df) => documentFrequency(token) = df
      }

    numDocs / documentFrequency
  }

  /** Filter out terms whose IDF is lower than the given bound
   *
   * @param docs           the documents RDD
   * @param idfLowerBound  lower bound for the IDF
   */
  def filterIDF(docs: RDD[(Long, SparseVector[Double])], idfLowerBound: Double)
      : RDD[(Long, SparseVector[Double])] = {
    if (idfLowerBound > 1.0 + 1e-12) {
      val idf = inverseDocumentFrequency(docs)
      val invalidTermIndices = (idf :< idfLowerBound).toVector

      docs.map {
        case (id: Long, w: SparseVector[Double]) =>
          w(invalidTermIndices) := 0.0
          (id, w)
      }
    }
    else {
      docs
    }
  }
}
