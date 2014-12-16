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
package org.apache.spark.mllib.topicmodel

import org.apache.spark.rdd.RDD
import java.util.Random
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.BoundedPriorityQueue
import org.apache.hadoop.fs.shell.Count




import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, sum => brzSum, normalize}
import org.apache.spark.mllib.linalg.{DenseVector => SDV, SparseVector => SSV, Vector => SV}

import org.apache.spark.graphx._


/**
 *
 *
 */
class LatentDirichletAllocation(var numTopics: Int,
                                var maxIterations: Int,
                                var topicSmoothing: Double,
                                var wordSmoothing: Double,
                                randomSeed: Long) {
  def this(numTopics: Int, maxIterations: Int) = {
    this(numTopics, maxIterations, 0.1, 0.1, System.currentTimeMillis())
  }

  def this(numTopics: Int) = this(numTopics, 100)


  def setNumTopics(k: Int):this.type = {numTopics = k; this}

  def setTopicSmoothing(alpha: Double):this.type = {topicSmoothing = alpha; this}

  def setWordSmoothing(beta: Double):this.type = {wordSmoothing = beta; this}

  import LatentDirichletAllocation._

  def iterations(docs: RDD[LatentDirichletAllocation.Document]):Iterator[State] = {
    val state = initialState(docs, numTopics, wordSmoothing, topicSmoothing, randomSeed)
    Iterator.iterate(state)(_.next()).drop(2).take(maxIterations)
  }

  def run(docs: RDD[LatentDirichletAllocation.Document]):State = {
    import breeze.util.Implicits._
    iterations(docs).last
  }
}

object LatentDirichletAllocation {
  case class Document(counts: SSV, id: VertexId)

  private type TopicCounts = BDV[Double]
  // Strictly should be an integer, but the algorithm works with Doubles
  private type WordCount = Double

  trait State {
    def logLikelihood: Double

    def topWords(k: Int):Array[Array[(Double, Int)]]
  }

  /**
   *
   * Has all the information needed to run EM.
   *
   * The Graph has two kinds of nodes: words and documents. The attr for a word
   * is p(w|z). The attr for a document is p(z|doc)
   *
   * @param graph
   * @param numTopics
   * @param numWords
   * @param topicSmoothing
   * @param wordSmoothing
   * @param numEStepIters
   */
  private case class LearningState(graph: Graph[TopicCounts, Double],
                                   numTopics: Int,
                                   numWords: Int,
                                   topicSmoothing: Double,
                                   wordSmoothing: Double,
                                   numEStepIters: Int = 10) extends State {

    def next() = copy(graph = mStep(eStep(graph)))

    // update p(z|doc) for each doc
    private def eStep(graph: Graph[TopicCounts, Double]): Graph[TopicCounts, Double] = {
      (0 until numEStepIters).foldLeft(graph) { (graph, _) =>
        // TODO: we should be able to detect which documents have converged and
        // filter them so we don't bother with them for the rest of the estep
        val docTopicTotals = updateExpectedCounts(graph, _.srcId)
        val alpha = topicSmoothing
        val newTotals = docTopicTotals.mapValues(total => normalize(total += alpha, 1))
        graph.outerJoinVertices(newTotals){ (vid, old, newOpt) => newOpt.getOrElse(old)}
      }
    }

    // update p(w|z) for each word
    private def mStep(graph: Graph[TopicCounts, Double]): Graph[TopicCounts, Double] = {
      val wordTotals = updateExpectedCounts(graph, _.dstId)
      val beta: Double = wordSmoothing
      val topicTotals = wordTotals.map(_._2).fold(BDV.zeros[Double](numTopics))(_ + _)
      // smooth the totals
      topicTotals += (beta * numWords)

      graph.outerJoinVertices(wordTotals)( (vid, old, newOpt) =>
        newOpt
          .map ( counts => (counts += beta) :/= topicTotals) // smooth individual counts; normalize
          .getOrElse(old) // keep old p(z|doc) vectors
      )
    }

    lazy val logLikelihood = {
      graph.triplets.aggregate(0.0)({ (acc, triple) =>
        val scores = triple.srcAttr :* triple.dstAttr
        val logScores = breeze.numerics.log(scores)
        scores /= brzSum(scores)
        brzSum(scores :*= logScores) * triple.attr
      }, _ + _)
    }

    // cribbed from jegonzal's implementation
    def topWords(k: Int): Array[Array[(Double, Int)]] = {
      val nt = numTopics
      val nw = numWords
      graph.vertices.filter {
        case (vid, c) => vid < nw
      }.mapPartitions { items =>
        val queues = Array.fill(nt)(new BoundedPriorityQueue[(Double, Int)](k))
        for ((wordId, factor) <- items) {
          var t = 0
          while (t < nt) {
            queues(t) += (factor(t)  -> wordId.toInt)
            t += 1
          }
        }
        Iterator(queues)
      }.reduce { (q1, q2) =>
        q1.zip(q2).foreach { case (a,b) => a ++= b }
        q1
      }.map ( q => q.toArray )
    }


  }


  private def updateExpectedCounts(wordCountGraph: Graph[TopicCounts, Double],
                                   sendToWhere: (EdgeTriplet[_, _]) => VertexId) = {
    wordCountGraph.mapReduceTriplets[TopicCounts]({
      trip => Iterator(sendToWhere(trip) -> computePTopic(trip))
    }, _ += _)
  }

  /**
   * Compute bipartite term/doc graph. doc ids are shifted by numWords to maintain uniqueness
   * @param docs
   * @param numTopics
   * @param randomSeed
   * @return
   */
  private def initialState(docs: RDD[LatentDirichletAllocation.Document],
                           numTopics: Int,
                           topicSmoothing: Double,
                           wordSmoothing: Double,
                           randomSeed: Long): LearningState = {
    val edges:RDD[Edge[WordCount]] = for {
      d <- docs
      (word, count) <- d.counts.toBreeze.activeIterator
      if count != 0.0
    } yield {
      Edge(d.id, word, count)
    }

    val numWords = docs.take(1).head.counts.size

    val initialDocTopics = docs.map { doc =>
      val random: Random = new Random(doc.id + randomSeed)
      (numWords + doc.id) -> BDV.fill(numTopics)(random.nextDouble())
    }
    val initialWordCounts = docs.sparkContext.parallelize(0 until numWords).map { wid =>
      val random: Random = new Random(randomSeed + wid)
      wid.toLong -> BDV.fill(numTopics)(random.nextDouble())
    }

    // partition such that edges are grouped by document
    val graph = (
      Graph(initialDocTopics ++ initialWordCounts, edges)
        .partitionBy(PartitionStrategy.EdgePartition1D)
      )

    LearningState(graph, numTopics, numWords, topicSmoothing, wordSmoothing)


  }

  private def computePTopic(edge: EdgeTriplet[TopicCounts, WordCount]):TopicCounts = {
    // \propto p(w|z) * p(z|d)
    val scores = (edge.srcAttr :* edge.dstAttr)
    // normalize and scale by number of times word occurs
    scores *= (edge.attr / brzSum(scores))
  }
}

