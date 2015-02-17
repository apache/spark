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

import scala.collection.mutable.Map
import scala.collection.mutable.Set

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.annotation.Experimental
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 *
 * Model produced by [[AffinityPropagation]].
 *
 * @param clusters the vertexIDs of each cluster.
 * @param exemplars the vertexIDs of all exemplars.
 */
@Experimental
class AffinityPropagationModel(
    val clusters: Seq[Set[Long]],
    val exemplars: Seq[Long]) extends Serializable {

  /**
   * Set the number of clusters
   */
  def getK(): Int = clusters.size

  /**
   * Find the cluster the given vertex belongs
   */
  def findCluster(vertexID: Long): Set[Long] = {
    clusters.filter(_.contains(vertexID))(0)
  } 
 
  /**
   * Find the cluster id the given vertex belongs
   */
  def findClusterID(vertexID: Long): Option[Int] = {
    var i = 0
    clusters.foreach(cluster => {
      if (cluster.contains(vertexID)) {
        return Some(i)
      }
      i += i
    })
    None 
  } 
}

/**
 * :: Experimental ::
 *
 * AffinityPropagation (AP), a graph clustering algorithm based on the concept of "message passing"
 * between data points. Unlike clustering algorithms such as k-means or k-medoids, AP does not
 * require the number of clusters to be determined or estimated before running it. AP is developed
 * by [[http://www.psi.toronto.edu/affinitypropagation/FreyDueckScience07.pdf Frey and Dueck]].
 *
 * @param maxIterations Maximum number of iterations of the AP algorithm.
 *
 * @see [[http://en.wikipedia.org/wiki/Affinity_propagation (Wikipedia)]]
 */
@Experimental
class AffinityPropagation private[clustering] (
    private var maxIterations: Int,
    private var lambda: Double,
    private var normalization: Boolean) extends Serializable {

  import org.apache.spark.mllib.clustering.AffinityPropagation._

  /** Constructs a AP instance with default parameters: {maxIterations: 100, lambda: 0.5,
   *    normalization: false}.
   */
  def this() = this(maxIterations = 100, lambda = 0.5, normalization = false)

  /**
   * Set maximum number of iterations of the messaging iteration loop
   */
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }
 
  /**
   * Get maximum number of iterations of the messaging iteration loop
   */
  def getMaxIterations(): Int = {
    this.maxIterations
  }
 
  /**
   * Set lambda of the messaging iteration loop
   */
  def setLambda(lambda: Double): this.type = {
    this.lambda = lambda
    this
  }
 
  /**
   * Get lambda of the messaging iteration loop
   */
  def getLambda(): Double = {
    this.lambda
  }
 
  /**
   * Set whether to do normalization or not
   */
  def setNormalization(normalization: Boolean): this.type = {
    this.normalization = normalization
    this
  }
 
  /**
   * Get whether to do normalization or not
   */
  def getNormalization(): Boolean = {
    this.normalization
  }
 
  /**
   * Run the AP algorithm.
   *
   * @param similarities an RDD of (i, j, s,,ij,,) tuples representing the similarity matrix, which
   *                     is the matrix S in the AP paper. The similarity s,,ij,, is set to
   *                     real-valued similarities. This is not required to be a symmetric matrix 
   *                     and hence s,,ij,, can be not equal to s,,ji,,. Tuples with i = j are
   *                     referred to as "preferences" in the AP paper. The data points with larger
   *                     values of s,,ii,, are more likely to be chosen as exemplars.
   *
   * @param symmetric the given similarity matrix is symmetric or not. Default value: true
   * @return a [[AffinityPropagationModel]] that contains the clustering result
   */
  def run(similarities: RDD[(Long, Long, Double)], symmetric: Boolean = true)
    : AffinityPropagationModel = {
    val s = constructGraph(similarities, normalization, symmetric)
    ap(s)
  }

  /**
   * Runs the AP algorithm.
   *
   * @param s The (normalized) similarity matrix, which is the matrix S in the AP paper with vertex
   *          similarities and the initial availabilities and responsibilities as its edge
   *          properties.
   */
  private def ap(s: Graph[Seq[Double], Seq[Double]]): AffinityPropagationModel = {
    val g = apIter(s, maxIterations, lambda)
    chooseExemplars(g)
  }
}

private[clustering] object AffinityPropagation extends Logging {
  /**
   * Construct the similarity matrix (S) and do normalization if needed.
   * Returns the (normalized) similarity matrix (S).
   */
  def constructGraph(similarities: RDD[(Long, Long, Double)], normalize: Boolean,
    symmetric: Boolean):
    Graph[Seq[Double], Seq[Double]] = {
    val edges = similarities.flatMap { case (i, j, s) =>
      if (symmetric && i != j) {
        Seq(Edge(i, j, Seq(s, 0.0, 0.0)), Edge(j, i, Seq(s, 0.0, 0.0)))
      } else {
        Seq(Edge(i, j, Seq(s, 0.0, 0.0)))
      }
    }

    if (normalize) {
      val gA = Graph.fromEdges(edges, Seq(0.0))
      val vD = gA.aggregateMessages[Seq[Double]](
        sendMsg = ctx => {
          ctx.sendToSrc(Seq(ctx.attr(0)))
        },
        mergeMsg = (s1, s2) => Seq(s1(0) + s2(0)),
        TripletFields.EdgeOnly)
      val normalized = GraphImpl.fromExistingRDDs(vD, gA.edges)
        .mapTriplets(
          e => {
            val s = if (e.srcAttr(0) == 0.0) { e.attr(0) } else { e.attr(0) / e.srcAttr(0) }
            Seq(s, 0.0, 0.0)
          }, TripletFields.Src)
      Graph.fromEdges(normalized.edges, Seq(0.0, 0.0))
    } else {
      Graph.fromEdges(edges, Seq(0.0, 0.0))
    }
  }

  /**
   * Runs AP's iteration.
   * @param g input graph with edges representing the (normalized) similarity matrix (S) and
   *          the initial availabilities and responsibilities.
   * @param maxIterations maximum number of iterations.
   * @return a [[Graph]] representing the final graph.
   */
  def apIter(
      g: Graph[Seq[Double], Seq[Double]],
      maxIterations: Int, lambda: Double): Graph[Seq[Double], Seq[Double]] = {
    val tol = math.max(1e-5 / g.vertices.count(), 1e-8)
    var prevDelta = (Double.MaxValue, Double.MaxValue)
    var diffDelta = (Double.MaxValue, Double.MaxValue)
    var curG = g
    for (iter <- 0 until maxIterations
      if math.abs(diffDelta._1) > tol || math.abs(diffDelta._2) > tol) {
      val msgPrefix = s"Iteration $iter"

      // update responsibilities
      val vD_r = curG.aggregateMessages[Seq[Double]](
        sendMsg = ctx => ctx.sendToSrc(Seq(ctx.attr(0) + ctx.attr(1))),
        mergeMsg = _ ++ _,
        TripletFields.EdgeOnly)
    
      val updated_r = GraphImpl.fromExistingRDDs(vD_r, curG.edges)
        .mapTriplets(
          (e) => {
            val filtered = e.srcAttr.filter(_ != (e.attr(0) + e.attr(1)))
            val pool = if (filtered.size < e.srcAttr.size - 1) {
              filtered.:+(e.attr(0) + e.attr(1))
            } else {
              filtered
            }
            val maxValue = if (pool.isEmpty) { 0.0 } else { pool.max }
            Seq(e.attr(0), e.attr(1), lambda * (e.attr(0) - maxValue) + (1.0 - lambda) * e.attr(2))
          }, TripletFields.Src)

      var iterG = Graph.fromEdges(updated_r.edges, Seq(0.0))

      // update availabilities
      val vD_a = iterG.aggregateMessages[Seq[Double]](
        sendMsg = ctx => {
          if (ctx.srcId != ctx.dstId) {
            ctx.sendToDst(Seq(math.max(ctx.attr(2), 0.0)))
          } else {
            ctx.sendToDst(Seq(ctx.attr(2)))
          }
        }, mergeMsg = (s1, s2) => Seq(s1(0) + s2(0)),
        TripletFields.EdgeOnly)

      val updated_a = GraphImpl.fromExistingRDDs(vD_a, iterG.edges)
        .mapTriplets(
          (e) => {
            if (e.srcId != e.dstId) {
              val newA = lambda * math.min(0.0, e.dstAttr(0) - math.max(e.attr(2), 0.0)) +
                         (1.0 - lambda) * e.attr(1)
              Seq(e.attr(0), newA, e.attr(2))
            } else {
              val newA = lambda * (e.dstAttr(0) - e.attr(2)) + (1.0 - lambda) * e.attr(1)
              Seq(e.attr(0), newA, e.attr(2))
            }
          }, TripletFields.Dst)

      iterG = Graph.fromEdges(updated_a.edges, Seq(0.0))

      // compare difference
      if (iter % 10 == 0) {
        val vaD = iterG.aggregateMessages[Seq[Double]](
          sendMsg = ctx => ctx.sendToSrc(Seq(ctx.attr(1), ctx.attr(2))),
          mergeMsg = (s1, s2) => Seq(s1(0) + s2(0), s1(1) + s2(1)),
          TripletFields.EdgeOnly)

        val prev_vaD = curG.aggregateMessages[Seq[Double]](
          sendMsg = ctx => ctx.sendToSrc(Seq(ctx.attr(1), ctx.attr(2))),
          mergeMsg = (s1, s2) => Seq(s1(0) + s2(0), s1(1) + s2(1)),
          TripletFields.EdgeOnly)

        val delta = vaD.join(prev_vaD).values.collect().map { x =>
          (x._1(0) - x._2(0), x._1(1) - x._2(1))
        }.foldLeft((0.0, 0.0)) {(s, t) => (s._1 + t._1, s._2 + t._2)}

        logInfo(s"$msgPrefix: availability delta = ${delta._1}.")
        logInfo(s"$msgPrefix: responsibility delta = ${delta._2}.")

        diffDelta = (math.abs(delta._1 - prevDelta._1), math.abs(delta._2 - prevDelta._2))
        
        logInfo(s"$msgPrefix: diff(delta) = $diffDelta.")

        prevDelta = delta
      }
      curG = iterG
    }
    curG
  }
 
  /**
   * Choose exemplars for nodes in graph.
   * @param g input graph with edges representing the final availabilities and responsibilities.
   * @return a [[AffinityPropagationModel]] representing the clustering results.
   */
  def chooseExemplars(
      g: Graph[Seq[Double], Seq[Double]]): AffinityPropagationModel = {
    val accum = g.edges.map(a => (a.srcId, (a.dstId, a.attr(1) + a.attr(2))))
    val clusterMembers = accum.reduceByKey((ar1, ar2) => {
      if (ar1._2 > ar2._2) {
        (ar1._1, ar1._2)
      } else {
        (ar2._1, ar2._2)
      }
    }).map(kv => (kv._2._1, kv._1)).aggregateByKey(Set[Long]())(
      seqOp = (s, d) => s ++ Set(d),
      combOp = (s1, s2) => s1 ++ s2
    ).cache()
    
    val neighbors = clusterMembers.map(kv => kv._2 ++ Set(kv._1)).collect()
    val exemplars = clusterMembers.map(kv => kv._1).collect()

    var clusters = Seq[Set[Long]]()

    var i = 0
    var nz = neighbors.size
    while (i < nz) {    
      var curCluster = neighbors(i)
      var j = i + 1
      while (j < nz) {
        if (!curCluster.intersect(neighbors(j)).isEmpty) {
          curCluster ++= neighbors(j)
        }
        j += 1
      }
      val overlap = clusters.filter(!_.intersect(curCluster).isEmpty)
      if (overlap.isEmpty) {
        clusters = clusters.:+(curCluster)
      } else {
        overlap(0) ++= curCluster
      }
      i += 1
    }
    new AffinityPropagationModel(clusters, exemplars)
  }
}
