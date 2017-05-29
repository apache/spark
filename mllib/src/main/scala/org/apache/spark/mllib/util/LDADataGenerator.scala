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
package org.apache.spark.mllib.util

import breeze.linalg.{SparseVector => BSV}
import breeze.stats.distributions.{Dirichlet, Multinomial}

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
 * :: DeveloperApi ::
 * Generate test data for LDA.
 */
@DeveloperApi
object LDADataGenerator {

  /**
   * Generate an RDD containing test data for LDA.
   *
   * @param sc SparkContext to use for creating the RDD
   * @param k Number of topics
   * @param docNum Number of documents
   * @param docLength Number of words per document
   * @param vocabSize size of the vocabulary
   * @param alpha docConcentration
   * @param beta topicConcentration
   * @param numPartitions Number of partitions of the generated RDD
   */
  def generateLDARDD(
      sc: SparkContext,
      k: Int,
      docNum: Int,
      docLength: Int,
      vocabSize: Int,
      alpha: Array[Double],
      beta: Array[Double],
      numPartitions: Int) : RDD[Vector] = {
    // generate multinomial distribution over words for each topic
    val prior = Dirichlet(beta)
    val phi = (0 until k).map(i => prior.draw())

    // generate words for each document
    sc.parallelize(0 until docNum, numPartitions).map { idx =>
      val topicDiri = Dirichlet(alpha)
      val theta = topicDiri.draw()
      val doc = BSV.zeros[Double](vocabSize)
      for(j <- 0 until docLength){
        val z = Multinomial(theta).draw() // sample topic
        val w = Multinomial(phi(z)).draw() // sample word
        doc(w) = doc(w) + 1.0
      }
      Vectors.fromBreeze(doc)
    }
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      // scalastyle:off println
      println("Usage: LDADataGenerator " +
        "<master> <output_dir> [k] [docNum] [docLength] [vocabSize] [alpha] [beta] [parts]")
      // scalastyle:on println
      System.exit(1)
    }

    val sparkMaster = args(0)
    val outputPath = args(1)
    val k = if (args.length > 2) args(2).toInt else 10
    val docNum = if (args.length > 3) args(3).toInt else 100
    val docLength = if (args.length > 4) args(4).toInt else 200
    val vocabSize = if (args.length > 5) args(5).toInt else 1000
    val alpha = if (args.length > 6) args(6).toDouble else 0.9
    val beta = if (args.length > 7) args(7).toDouble else 0.01
    val parts = if (args.length > 8) args(8).toInt else 2

    val alphaArr = Array.fill(k)(alpha)
    val betaArr = Array.fill(vocabSize)(beta)

    val sc = new SparkContext(sparkMaster, "LDADataGenerator")
    val data = generateLDARDD(sc, k, docNum, docLength, vocabSize, alphaArr, betaArr, parts)
    data.saveAsTextFile(outputPath)

    System.exit(0)
  }
}
