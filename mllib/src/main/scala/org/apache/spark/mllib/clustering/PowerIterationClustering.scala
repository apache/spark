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

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.language.existentials

/**
 * Implements the scalable graph clustering algorithm Power Iteration Clustering (see
 * www.icml2010.org/papers/387.pdf).  From the abstract:
 *
 * The input data is first transformed to a normalized Affinity Matrix via Gaussian pairwise
 * distance calculations. Power iteration is then used to find a dimensionality-reduced
 * representation.  The resulting pseudo-eigenvector provides effective clustering - as
 * performed by Parallel KMeans.
 */
object PowerIterationClustering {

  private val logger = Logger.getLogger(getClass.getName())

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
  val defaultKMeansRuns = 10

  /**
   *
   * Run a Power Iteration Clustering
   *
   * @param sc  Spark Context
   * @param G   Affinity Matrix in a Sparse Graph structure
   * @param nClusters  Number of clusters to create
   * @param nIterations Number of iterations of the PIC algorithm
   *                    that calculates primary PseudoEigenvector and Eigenvalue
   * @param nRuns  Number of runs for the KMeans clustering
   * @return Tuple of (Seq[(Cluster Id,Cluster Center)],
   *         Seq[(VertexId, ClusterID Membership)]
   */
  def run(sc: SparkContext,
          G: Graph[Double, Double],
          nClusters: Int,
          nIterations: Int = defaultIterations,
          nRuns: Int = defaultKMeansRuns)
  : (Seq[(Int, Vector)], Seq[((VertexId, Vector), Int)]) = {
    val (gUpdated, lambda, vt) = getPrincipalEigen(sc, G, nIterations)
    // TODO: avoid local collect and then sc.parallelize.
    val localVt = vt.collect.sortBy(_._1)
    val vectRdd = sc.parallelize(localVt.map(v => (v._1, Vectors.dense(v._2))))
    vectRdd.cache()
    val model = KMeans.train(vectRdd.map {
      _._2
    }, nClusters, nRuns)
    vectRdd.unpersist()
    if (logger.isDebugEnabled) {
      logger.debug(s"Eigenvalue = $lambda EigenVector: ${localVt.mkString(",")}")
    }
    val estimates = vectRdd.zip(model.predict(vectRdd.map(_._2)))
    if (logger.isDebugEnabled) {
      logger.debug(s"lambda=$lambda  eigen=${localVt.mkString(",")}")
    }
    val ccs = (0 until model.clusterCenters.length).zip(model.clusterCenters)
    if (logger.isDebugEnabled) {
      logger.debug(s"Kmeans model cluster centers: ${ccs.mkString(",")}")
    }
    val estCollected = estimates.collect.sortBy(_._1._1)
    if (logger.isDebugEnabled) {
      val clusters = estCollected.map(_._2)
      val counts = estCollected.groupBy(_._2).mapValues {
        _.length
      }
      logger.debug(s"Cluster counts: Counts: ${counts.mkString(",")}"
        + s"\nCluster Estimates: ${estCollected.mkString(",")}")
    }
    (ccs, estCollected)
  }


  /**
   * Create a Graph given an initial Vt0 and a set of Edges that
   * represent the Normalized Affinity Matrix (W)
   */
  def createGraphFromEdges(sc: SparkContext,
                           edgesRdd: RDD[Edge[Double]],
                           nPoints: Int,
                           optInitialVt: Option[Seq[(VertexId, Double)]] = None) = {

    assert(nPoints > 0, "Must provide number of points from the original dataset")
    val G = if (optInitialVt.isDefined) {
      val initialVt = optInitialVt.get
      val vertsRdd = sc.parallelize(initialVt)
      Graph(vertsRdd, edgesRdd)
    } else {
      Graph.fromEdges(edgesRdd, -1.0)
    }
    G

  }

  /**
   * Calculate the dominant Eigenvalue and Eigenvector for a given sparse graph
   * using the PIC method
   * @param sc
   * @param G  Input Graph representing the Normalized Affinity Matrix (W)
   * @param nIterations Number of iterations of the PIC algorithm
   * @param optMinNormChange Minimum norm acceleration for detecting convergence
   *                         - indicated as "epsilon" in the PIC paper
   * @return
   */
  def getPrincipalEigen(sc: SparkContext,
                        G: DGraph,
                        nIterations: Int = defaultIterations,
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
    var prevG: DGraph = G
    val epsilon = optMinNormChange
      .getOrElse(1e-5 / G.vertices.count())
    for (iter <- 0 until nIterations
         if Math.abs(normAccel) > epsilon) {

      val tmpEigen = prevG.aggregateMessages[Double](ctx => {
        ctx.sendToSrc(ctx.attr * ctx.srcAttr);
        ctx.sendToDst(ctx.attr * ctx.dstAttr)
      },
        _ + _)
      if (logger.isDebugEnabled) {
        logger.debug(s"tmpEigen[$iter]: ${tmpEigen.collect.mkString(",")}\n")
      }
      val vnorm =
        prevG.vertices.map {
          _._2
        }.fold(0.0) { case (sum, dval) =>
          sum + Math.abs(dval)
        }
      if (logger.isDebugEnabled) {
        logger.debug(s"vnorm[$iter]=$vnorm")
      }
      outG = prevG.outerJoinVertices(tmpEigen) { case (vid, wval, optTmpEigJ) =>
        val normedEig = optTmpEigJ.getOrElse {
          -1.0
        } / vnorm
        if (logger.isDebugEnabled) {
          logger.debug(s"Updating vertex[$vid] from $wval to $normedEig")
        }
        normedEig
      }
      prevG = outG

      if (logger.isDebugEnabled) {
        val localVertices = outG.vertices.collect
        val graphSize = localVertices.size
        print(s"Vertices[$iter]: ${localVertices.mkString(",")}\n")
      }
      normVelocity = vnorm - priorNorm
      normAccel = normVelocity - priorNormVelocity
      if (logger.isDebugEnabled) {
        logger.debug(s"normAccel[$iter]= $normAccel")
      }
      priorNorm = vnorm
      priorNormVelocity = vnorm - priorNorm
    }
    (outG, vnorm, outG.vertices)
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


}
