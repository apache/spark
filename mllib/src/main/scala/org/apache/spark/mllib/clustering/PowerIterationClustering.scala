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

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.XORShiftRandom

/**
 * Model produced by [[PowerIterationClustering]].
 *
 * @param k number of clusters
 * @param assignments an RDD of (vertexID, clusterID) pairs
 */
class PowerIterationClusteringModel(
    val k: Int,
    val assignments: RDD[(Long, Int)]) extends Serializable

/**
 * Power Iteration Clustering (PIC), a scalable graph clustering algorithm developed by Lin and
 * Cohen (see http://www.icml2010.org/papers/387.pdf). From the abstract: PIC finds a very
 * low-dimensional embedding of a dataset using truncated power iteration on a normalized pair-wise
 * similarity matrix of the data.
 *
 * @param k Number of clusters.
 * @param maxIterations Maximum number of iterations of the PIC algorithm.
 */
class PowerIterationClustering private[clustering] (
    private var k: Int,
    private var maxIterations: Int) extends Serializable {

  import org.apache.spark.mllib.clustering.PowerIterationClustering._

  /** Constructs a PIC instance with default parameters: {k: 2, maxIterations: 100}. */
  def this() = this(k = 2, maxIterations = 100)

  /**
   * Set the number of clusters.
   */
  def setK(k: Int): this.type = {
    this.k = k
    this
  }

  /**
   * Set maximum number of iterations of the power iteration loop
   */
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  /**
   * Run the PIC algorithm.
   *
   * @param similarities an RDD of (i, j, s_ij_) tuples representing the affinity matrix, which is
   *                     the matrix A in the PIC paper. The similarity s_ij_ must be nonnegative.
   *                     This is a symmetric matrix and hence s_ij_ = s_ji_. For any (i, j) with
   *                     nonzero similarity, there should be either (i, j, s_ij_) or (j, i, s_ji_)
   *                     in the input. Tuples with i = j are ignored, because we assume s_ij_ = 0.0.
   *
   * @return a [[PowerIterationClusteringModel]] that contains the clustering result
   */
  def run(similarities: RDD[(Long, Long, Double)]): PowerIterationClusteringModel = {
    val w = normalize(similarities)
    val w0 = randomInit(w)
    pic(w0)
  }

  /**
   * Runs the PIC algorithm.
   *
   * @param w The normalized affinity matrix, which is the matrix W in the PIC paper with
   *          w_ij_ = a_ij_ / d_ii_ as its edge properties and the initial vector of the power
   *          iteration as its vertex properties.
   */
  private def pic(w: Graph[Double, Double]): PowerIterationClusteringModel = {
    val v = powerIter(w, maxIterations)
    val assignments = kMeans(v, k)
    new PowerIterationClusteringModel(k, assignments)
  }
}

private[clustering] object PowerIterationClustering extends Logging {
  /**
   * Normalizes the affinity matrix (A) by row sums and returns the normalized affinity matrix (W).
   */
  def normalize(similarities: RDD[(Long, Long, Double)]): Graph[Double, Double] = {
    val edges = similarities.flatMap { case (i, j, s) =>
      if (s < 0.0) {
        throw new SparkException("Similarity must be nonnegative but found s($i, $j) = $s.")
      }
      if (i != j) {
        Seq(Edge(i, j, s), Edge(j, i, s))
      } else {
        None
      }
    }
    val gA = Graph.fromEdges(edges, 0.0)
    val vD = gA.aggregateMessages[Double](
      sendMsg = ctx => {
        ctx.sendToSrc(ctx.attr)
      },
      mergeMsg = _ + _,
      TripletFields.EdgeOnly)
    GraphImpl.fromExistingRDDs(vD, gA.edges)
      .mapTriplets(
        e => e.attr / math.max(e.srcAttr, MLUtils.EPSILON),
        TripletFields.Src)
  }

  /**
   * Generates random vertex properties (v0) to start power iteration.
   * 
   * @param g a graph representing the normalized affinity matrix (W)
   * @return a graph with edges representing W and vertices representing a random vector
   *         with unit 1-norm
   */
  def randomInit(g: Graph[Double, Double]): Graph[Double, Double] = {
    val r = g.vertices.mapPartitionsWithIndex(
      (part, iter) => {
        val random = new XORShiftRandom(part)
        iter.map { case (id, _) =>
          (id, random.nextGaussian())
        }
      }, preservesPartitioning = true).cache()
    val sum = r.values.map(math.abs).sum()
    val v0 = r.mapValues(x => x / sum)
    GraphImpl.fromExistingRDDs(VertexRDD(v0), g.edges)
  }

  /**
   * Runs power iteration.
   * @param g input graph with edges representing the normalized affinity matrix (W) and vertices
   *          representing the initial vector of the power iterations.
   * @param maxIterations maximum number of iterations
   * @return a [[VertexRDD]] representing the pseudo-eigenvector
   */
  def powerIter(
      g: Graph[Double, Double],
      maxIterations: Int): VertexRDD[Double] = {
    // the default tolerance used in the PIC paper, with a lower bound 1e-8
    val tol = math.max(1e-5 / g.vertices.count(), 1e-8)
    var prevDelta = Double.MaxValue
    var diffDelta = Double.MaxValue
    var curG = g
    for (iter <- 0 until maxIterations if math.abs(diffDelta) > tol) {
      val msgPrefix = s"Iteration $iter"
      // multiply W by vt
      val v = curG.aggregateMessages[Double](
        sendMsg = ctx => ctx.sendToSrc(ctx.attr * ctx.dstAttr),
        mergeMsg = _ + _,
        TripletFields.Dst).cache()
      // normalize v
      val norm = v.values.map(math.abs).sum()
      logInfo(s"$msgPrefix: norm(v) = $norm.")
      val v1 = v.mapValues(x => x / norm)
      // compare difference
      val delta = curG.joinVertices(v1) { case (_, x, y) =>
        math.abs(x - y)
      }.vertices.values.sum()
      logInfo(s"$msgPrefix: delta = $delta.")
      diffDelta = math.abs(delta - prevDelta)
      logInfo(s"$msgPrefix: diff(delta) = $diffDelta.")
      // update v
      curG = GraphImpl.fromExistingRDDs(VertexRDD(v1), g.edges)
      prevDelta = delta
    }
    curG.vertices
  }

  /**
   * Runs k-means clustering.
   * @param v a [[VertexRDD]] representing the pseudo-eigenvector
   * @param k number of clusters
   * @return a [[VertexRDD]] representing the clustering assignments
   */
  def kMeans(v: VertexRDD[Double], k: Int): VertexRDD[Int] = {
    val points = v.mapValues(x => Vectors.dense(x)).cache()
    val model = new KMeans()
      .setK(k)
      .setRuns(5)
      .setSeed(0L)
      .run(points.values)
    points.mapValues(p => model.predict(p)).cache()
  }
}
