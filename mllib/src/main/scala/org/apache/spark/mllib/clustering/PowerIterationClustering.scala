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

import scala.language.existentials

import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
 * Implements the scalable graph clustering algorithm Power Iteration Clustering (see
 * www.icml2010.org/papers/387.pdf).  From the abstract:
 *
 * The input data is normalized pair-wise similarity matrix of the data.
 * Power iteration is used to find a dimensionality-reduced
 * representation.  The resulting pseudo-eigenvector provides effective clustering - as
 * performed by Parallel KMeans.
 *
 * @param k  Number of clusters to create
 * @param maxIterations Number of iterations of the PIC algorithm
 *                      that calculates primary PseudoEigenvector and Eigenvalue
 * @return Tuple of (Seq[(Cluster Id,Cluster Center)],
 *         Seq[(VertexId, ClusterID Membership)]
 */
class PowerIterationClustering(
                                        private var k: Int,
                                        private var maxIterations: Int)
  extends Serializable with Logging {

  import PowerIterationClustering._

  /**
   * Set the number of clusters
   */
  def setK(k: Int) = this.k = k

  /**
   * Set maximum number of iterations of the power iteration loop
   */
  def setMaxIterations(maxIterations: Int) = this.maxIterations = maxIterations

  /**
   *
   * Run the Power Iteration Clustering algorithm
   *
   * @param affinityRdd  Set of Pairwise affinities of input points in following format:
   *                     (SrcPointId, DestPointId, Affinity Value)
   *                     SrcPointId : Long = Label of Source Point of Pair
   *                     DestPointId : Long = Label of Destination Point Pair
   *                     Note: this is a directed graph so if
   *                     AffinityValue: Double = Scalar measure of
   *                     inter-point affinity
   *                     Note: this is a Directed Graph
   *                     (a) In general: (a,b, affinity(a,b)) != (b,a,affinity(b,a))
   *                     (b) If the input graph were symmetric, it is still necessary to
   *                     provide both (a,b, affinityAB) and (b,a, affinityAB)
   *                     where affinityAB is the identical bidirectional affinity between (a,b)
   *
   * @return Tuple of (Seq[(Cluster Id,Cluster Center)],
   *         RDD[(VertexId, ClusterID Membership)]
   */
  def run(affinityRdd: RDD[(Long, Long, Double)])
  : (Seq[(Int, Double)], RDD[((Long, Double), Int)]) = {
    val edgesRdd = affinityRdd.map { case (v1, v2, vect) =>
      Edge(v1, v2, vect)
    }
    val g: Graph[Double, Double] = Graph.fromEdges(edgesRdd, -1L)
    run(g)
  }

  /**
   *
   * Run the Power Iteration Clustering algorithm
   *
   * @param g  The affinity matrix in a Sparse Graph structure
   *           The Graph Vertices are the VertexId's associated with each input Data point
   *             (Long type)
   *           The Graph Edges are the Normalized Affinities between pairs of points
   *           (srcId = from Point, dstId = toPoint, attr = affinity (Double type))
   *           E.g. (3232L, 1122L, 0.0233442)
   * @return Tuple of (Seq[(Cluster Id,Cluster Center)],
   *         RDD[(VertexId, ClusterID Membership)]
   */
  def run(g: Graph[Double, Double]) = {

    val (gUpdated, lambda, vt) = getPrincipalEigen(g, maxIterations)
    val sc = g.vertices.sparkContext
    val vectRdd = vt.map(v => (v._1, Vectors.dense(v._2)))
    vectRdd.cache()
    val nRuns = defaultKMeansRuns
    val model = KMeans.train(vectRdd.map {
      _._2
    }, k, nRuns)
    vectRdd.unpersist()

    var localVt: Seq[(Long, Double)] = null // Only used when in trace logging mode
    if (isTraceEnabled) {
      localVt = vt.collect.sortBy(_._1)
      logDebug(s"Eigenvalue = $lambda EigenVector: ${
        localVt.mkString(",")
      }")
    }
    val estimates = vectRdd.zip(model.predict(vectRdd.map(_._2)))
      .map { case ((vid, vect), clustId) =>
      ((vid, vect.toArray.apply(0)), clustId)
    }
    if (isTraceEnabled) {
      logDebug(s"lambda=$lambda  eigen=${
        localVt.mkString(",")
      }")
    }
    val ccs = (0 until model.clusterCenters.length).zip(model.clusterCenters)
      .map { case (vid, center) =>
      (vid, center.toArray.apply(0))
    }
    if (isTraceEnabled) {
      logDebug(s"Kmeans model cluster centers: ${
        ccs.mkString(",")
      }")
    }
    (ccs, estimates)
  }

}

object PowerIterationClustering extends Logging {
  type LabeledPoint = (VertexId, BDV[Double])
  type Points = Seq[LabeledPoint]
  type DGraph = Graph[Double, Double]
  type IndexedVector[Double] = (Long, BDV[Double])

  // Terminate iteration when norm changes by less than this value
  val defaultMinNormChange: Double = 1e-11

  // Default number of iterations for PIC loop
  val defaultIterations: Int = 20

  // Do not allow divide by zero: change to this value instead
  val defaultDivideByZeroVal: Double = 1e-15

  // Default number of runs by the KMeans.run() method
  private[mllib] val defaultKMeansRuns = 1


  /**
   * Create a Graph given an initial Vt0 and a set of Edges that
   * represent the Normalized Affinity Matrix (W)
   */
  private[mllib] def createGraphFromEdges(edgesRdd: RDD[Edge[Double]],
                                          optInitialVt: Option[Seq[(VertexId, Double)]] = None) = {

    val g = Graph.fromEdges(edgesRdd, -1.0)
    val outG: Graph[Double, Double] = if (optInitialVt.isDefined) {
      g.outerJoinVertices(
        g.vertices.sparkContext.parallelize(optInitialVt.get)) {
        case (vid, graphAttrib, optVtValue ) =>
          optVtValue.getOrElse(graphAttrib)
      }
    } else {
      g
    }
    outG
  }

  /**
   * Creates an initial Vt(0) used within the first iteration of the PIC
   */
  private[mllib] def createInitialVector(sc: SparkContext,
                                         labels: Seq[VertexId],
                                         rowSums: Seq[Double]) = {
    val volume = rowSums.fold(0.0) {
      _ + _
    }
    val initialVt = labels.zip(rowSums.map(_ / volume))
    initialVt
  }

  /**
   * Calculate the dominant Eigenvalue and Eigenvector for a given sparse graph
   * using the PIC method
   * @param g  Input Graph representing the Normalized Affinity Matrix (W)
   * @param maxIterations Number of iterations of the PIC algorithm
   * @param optMinNormChange Minimum norm acceleration for detecting convergence
   *                         - indicated as "epsilon" in the PIC paper
   * @return
   */
  private[mllib] def getPrincipalEigen(g: DGraph,
                                       maxIterations: Int = defaultIterations,
                                       optMinNormChange: Option[Double] = None
                                        ): (DGraph, Double, VertexRDD[Double]) = {

    var priorNorm = Double.MaxValue
    var norm = Double.MaxValue
    var priorNormVelocity = Double.MaxValue
    var normVelocity = Double.MaxValue
    var normAccel = Double.MaxValue
    val DummyVertexId = -99L
    var vnorm: Double = -1.0
    var outG: DGraph = null
    var prevG: DGraph = g
    // The epsilon calculation is provided by the original paper www.icml2010.org/papers/387.pdf
    //   as epsilon = 1e-5/(#points)
    // However that seems quite small for large#points
    // Instead we use  epsilonPrime = Max(epsilon, 1e-10)

    val epsilon = optMinNormChange
      .getOrElse(math.max(1e-5 / g.vertices.count(), 1e-10))
    for (iter <- 0 until maxIterations
         if math.abs(normAccel) > epsilon) {

      val tmpEigen = prevG.aggregateMessages[Double](ctx => {
        ctx.sendToSrc(ctx.attr * ctx.srcAttr);
        ctx.sendToDst(ctx.attr * ctx.dstAttr)
      },
        _ + _)
      if (isTraceEnabled()) {
        logTrace(s"tmpEigen[$iter]: ${
          tmpEigen.collect.mkString(",")
        }\n")
      }
      val vnorm =
        prevG.vertices.map {
          _._2
        }.fold(0.0) {
          case (sum, dval) =>
            sum + math.abs(dval)
        }
      if (isTraceEnabled()) {
        logTrace(s"vnorm[$iter]=$vnorm")
      }
      outG = prevG.outerJoinVertices(tmpEigen) {
        case (vid, wval, optTmpEigJ) =>
          val normedEig = optTmpEigJ.getOrElse {
            -1.0
          } / vnorm
          if (isTraceEnabled()) {
            logTrace(s"Updating vertex[$vid] from $wval to $normedEig")
          }
          normedEig
      }
      prevG = outG

      if (isTraceEnabled()) {
        val localVertices = outG.vertices.collect
        val graphSize = localVertices.size
        print(s"Vertices[$iter]: ${
          localVertices.mkString(",")
        }\n")
      }
      normVelocity = vnorm - priorNorm
      normAccel = normVelocity - priorNormVelocity
      if (isTraceEnabled()) {
        logTrace(s"normAccel[$iter]= $normAccel")
      }
      priorNorm = vnorm
      priorNormVelocity = vnorm - priorNorm
    }
    (outG, vnorm, outG.vertices)
  }

}
