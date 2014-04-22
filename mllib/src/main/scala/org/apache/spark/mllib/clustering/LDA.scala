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

import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.{AccumulableParam, Logging, SparkContext}
import org.apache.spark.mllib.expectation.GibbsSampling
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

case class Document(docId: Int, content: Iterable[Int])

case class LDAParams (
    docCounts: Vector,
    topicCounts: Vector,
    docTopicCounts: Array[Vector],
    topicTermCounts: Array[Vector])
  extends Serializable {

  def update(docId: Int, term: Int, topic: Int, inc: Int) = {
    docCounts.toBreeze(docId) += inc
    topicCounts.toBreeze(topic) += inc
    docTopicCounts(docId).toBreeze(topic) += inc
    topicTermCounts(topic).toBreeze(term) += inc
    this
  }

  def merge(other: LDAParams) = {
    docCounts.toBreeze += other.docCounts.toBreeze
    topicCounts.toBreeze += other.topicCounts.toBreeze

    var i = 0
    while (i < docTopicCounts.length) {
      docTopicCounts(i).toBreeze += other.docTopicCounts(i).toBreeze
      i += 1
    }

    i = 0
    while (i < topicTermCounts.length) {
      topicTermCounts(i).toBreeze += other.topicTermCounts(i).toBreeze
      i += 1
    }
    this
  }

  /**
   * This function used for computing the new distribution after drop one from current document,
   * which is a really essential part of Gibbs sampling for LDA, you can refer to the paper:
   * <I>Parameter estimation for text analysis</I>
   */
  def dropOneDistSampler(
      docTopicSmoothing: Double,
      topicTermSmoothing: Double,
      termId: Int,
      docId: Int,
      rand: Random): Int = {
    val (numTopics, numTerms) = (topicCounts.size, topicTermCounts.head.size)
    val topicThisTerm = BDV.zeros[Double](numTopics)
    var i = 0
    while (i < numTopics) {
      topicThisTerm(i) =
        ((topicTermCounts(i)(termId) + topicTermSmoothing)
          / (topicCounts(i) + (numTerms * topicTermSmoothing))
        ) + (docTopicCounts(docId)(i) + docTopicSmoothing)
      i += 1
    }
    GibbsSampling.multinomialDistSampler(rand, topicThisTerm)
  }
}

object LDAParams {
  implicit val ldaParamsAP = new LDAParamsAccumulableParam

  def apply(numDocs: Int, numTopics: Int, numTerms: Int) = new LDAParams(
    Vectors.fromBreeze(BDV.zeros[Double](numDocs)),
    Vectors.fromBreeze(BDV.zeros[Double](numTopics)),
    Array(0 until numDocs: _*).map(_ => Vectors.fromBreeze(BDV.zeros[Double](numTopics))),
    Array(0 until numTopics: _*).map(_ => Vectors.fromBreeze(BDV.zeros[Double](numTerms))))
}

class LDAParamsAccumulableParam extends AccumulableParam[LDAParams, (Int, Int, Int, Int)] {
  def addAccumulator(r: LDAParams, t: (Int, Int, Int, Int)) = {
    val (docId, term, topic, inc) = t
    r.update(docId, term, topic, inc)
  }

  def addInPlace(r1: LDAParams, r2: LDAParams): LDAParams = r1.merge(r2)

  def zero(initialValue: LDAParams): LDAParams = initialValue
}

class LDA private (
    var numTopics: Int,
    var docTopicSmoothing: Double,
    var topicTermSmoothing: Double,
    var numIteration: Int,
    var numDocs: Int,
    var numTerms: Int)
  extends Serializable with Logging {
  def run(input: RDD[Document]): (GibbsSampling, LDAParams) = {
    val trainer = new GibbsSampling(
      input,
      numIteration,
      1,
      docTopicSmoothing,
      topicTermSmoothing)
    (trainer, trainer.runGibbsSampling(LDAParams(numDocs, numTopics, numTerms)))
  }
}

object LDA extends Logging {

  def train(
      data: RDD[Document],
      numTopics: Int,
      docTopicSmoothing: Double,
      topicTermSmoothing: Double,
      numIterations: Int,
      numDocs: Int,
      numTerms: Int): (Array[Vector], Array[Vector]) = {
    val lda = new LDA(numTopics,
      docTopicSmoothing,
      topicTermSmoothing,
      numIterations,
      numDocs,
      numTerms)
    val (trainer, model) = lda.run(data)
    trainer.solvePhiAndTheta(model)
  }

  def main(args: Array[String]) {
    if (args.length != 5) {
      println("Usage: LDA <master> <input_dir> <k> <max_iterations> <mini-split>")
      System.exit(1)
    }

    val (master, inputDir, k, iters, minSplit) =
      (args(0), args(1), args(2).toInt, args(3).toInt, args(4).toInt)
    val checkPointDir = System.getProperty("spark.gibbsSampling.checkPointDir", "/tmp/lda-cp")
    val sc = new SparkContext(master, "LDA")
    sc.setCheckpointDir(checkPointDir)
    val (data, wordMap, docMap) = MLUtils.loadCorpus(sc, inputDir, minSplit)
    val numDocs = docMap.size
    val numTerms = wordMap.size

    val (phi, theta) = LDA.train(data, k, 0.01, 0.01, iters, numDocs, numTerms)
    val pp = GibbsSampling.perplexity(data, phi, theta)
    println(s"final mode perplexity is $pp")
  }
}
