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
  private[mllib] val defaultMinNormChange: Double = 1e-11

    // Default sigma for Gaussian Distance calculations
  private[mllib] val defaultSigma = 1.0

  // Default number of iterations for PIC loop
  private[mllib] val defaultIterations: Int = 20

  // Default minimum affinity between points - lower than this it is considered
  // zero and no edge will be created
  private[mllib] val defaultMinAffinity = 1e-11

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
   * @param sigma   Sigma for Gaussian distribution calculation according to
   *                [1/2 *sqrt(pi*sigma)] exp (- (x-y)**2 / 2sigma**2
   * @param minAffinity  Minimum Affinity between two Points in the input dataset: below
   *                     this threshold the affinity will be considered "close to" zero and
   *                     no Edge will be created between those Points in the sparse matrix
   * @param nRuns  Number of runs for the KMeans clustering
   * @return Tuple of (Seq[(Cluster Id,Cluster Center)],
   *         Seq[(VertexId, ClusterID Membership)]
   */
  def run(sc: SparkContext,
          G: Graph[Double, Double],
          nClusters: Int,
          nIterations: Int = defaultIterations,
          sigma: Double = defaultSigma,
          minAffinity: Double = defaultMinAffinity,
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
   *
   * Create an affinity matrix
   *
   * @param sc  Spark Context
   * @param points  Input Points in format of [(VertexId,(x,y)]
   *                where VertexId is a Long
   * @param sigma   Sigma for Gaussian distribution calculation according to
   *                [1/2 *sqrt(pi*sigma)] exp (- (x-y)**2 / 2sigma**2
   * @param minAffinity  Minimum Affinity between two Points in the input dataset: below
   *                     this threshold the affinity will be considered "close to" zero and
   *                     no Edge will be created between those Points in the sparse matrix
   * @return Tuple of (Seq[(Cluster Id,Cluster Center)],
   *         Seq[(VertexId, ClusterID Membership)]
   */
  def createGaussianAffinityMatrix(sc: SparkContext,
                                   points: Points,
                                   sigma: Double = defaultSigma,
                                   minAffinity: Double = defaultMinAffinity)
  : Graph[Double, Double] = {
    val vidsRdd = sc.parallelize(points.map(_._1).sorted)
    val nVertices = points.length

    val (wRdd, rowSums) = createNormalizedAffinityMatrix(sc, points, sigma)
    val initialVt = createInitialVector(sc, points.map(_._1), rowSums)
    if (logger.isDebugEnabled) {
      logger.debug(s"Vt(0)=${
        printVector(new BDV(initialVt.map {
          _._2
        }.toArray))
      }")
    }
    val edgesRdd = createSparseEdgesRdd(sc, wRdd, minAffinity)
    val G = createGraphFromEdges(sc, edgesRdd, points.size, Some(initialVt))
    if (logger.isDebugEnabled) {
      logger.debug(printMatrixFromEdges(G.edges))
    }
    G
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

  /**
   * Calculate the Gaussian distance between two Vectors according to:
   *
   * exp( -(X1-X2)^2/2*sigma^2))
   *
   * where X1 and X2 are Vectors
   *
   * @param vect1 Input Vector1
   * @param vect2 Input Vector2
   * @param sigma Gaussian parameter sigma
   * @return
   */
  private[mllib] def gaussianDist(vect1: BDV[Double], vect2: BDV[Double], sigma: Double) = {
    val c1c2 = vect1.toArray.zip(vect2.toArray)
    val dist = Math.exp((-0.5 / Math.pow(sigma, 2.0)) * c1c2.foldLeft(0.0) {
      case (dist: Double, (c1: Double, c2: Double)) =>
        dist + Math.pow(c1 - c2, 2)
    })
    dist
  }

  /**
   * Create a sparse EdgeRDD from an array of densevectors. The elements that
   * are "close to" zero - as configured by the minAffinity value - do not
   * result in an Edge being created.
   *
   * @param sc
   * @param wRdd
   * @param minAffinity
   * @return
   */
  private[mllib] def createSparseEdgesRdd(sc: SparkContext, wRdd: RDD[IndexedVector[Double]],
                                          minAffinity: Double = defaultMinAffinity) = {
    val labels = wRdd.map { case (vid, vect) => vid}.collect
    val edgesRdd = wRdd.flatMap { case (vid, vect) =>
      for ((dval, ix) <- vect.toArray.zipWithIndex
           if Math.abs(dval) >= minAffinity)
      yield Edge(vid, labels(ix), dval)
    }
    edgesRdd
  }

  /**
   * Create the normalized affinity matrix "W" given a set of Points
   *
   * @param sc SparkContext
   * @param points  Input Points in format of [(VertexId,(x,y)]
   *                where VertexId is a Long
   * @param sigma Gaussian parameter sigma
   * @return
   */
  private[mllib] def createNormalizedAffinityMatrix(sc: SparkContext,
                                                    points: Points, sigma: Double) = {
    val nVertices = points.length
    val affinityRddNotNorm = sc.parallelize({
      val ivect = new Array[IndexedVector[Double]](nVertices)
      for (i <- 0 until points.size) {
        ivect(i) = new IndexedVector(points(i)._1, new BDV(Array.fill(nVertices)(100.0)))
        for (j <- 0 until points.size) {
          val dist = if (i != j) {
            gaussianDist(points(i)._2, points(j)._2, sigma)
          } else {
            0.0
          }
          ivect(i)._2(j) = dist
        }
      }
      ivect.zipWithIndex.map { case (vect, ix) =>
        (ix, vect)
      }
    }, nVertices)
    if (logger.isDebugEnabled) {
      logger.debug(s"Affinity:\n${
        printMatrix(affinityRddNotNorm.map(_._2), nVertices, nVertices)
      }")
    }
    val rowSums = affinityRddNotNorm.map { case (ix, (vid, vect)) =>
      vect.foldLeft(0.0) {
        _ + _
      }
    }
    val materializedRowSums = rowSums.collect
    val similarityRdd = affinityRddNotNorm.map { case (rowx, (vid, vect)) =>
      (vid, vect.map {
        _ / materializedRowSums(rowx)
      })
    }
    if (logger.isDebugEnabled) {
      logger.debug(s"W:\n${printMatrix(similarityRdd, nVertices, nVertices)}")
    }
    (similarityRdd, materializedRowSums)
  }

  private[mllib] def printMatrix(denseVectorRDD: RDD[LabeledPoint], i: Int, i1: Int) = {
    denseVectorRDD.collect.map {
      case (vid, dvect) => dvect.toArray
    }.flatten
  }

  private[mllib] def printMatrixFromEdges(edgesRdd: EdgeRDD[_]) = {
    val edgec = edgesRdd.collect
    val sorted = edgec.sortWith { case (e1, e2) =>
      e1.srcId < e2.srcId || (e1.srcId == e2.srcId && e1.dstId <= e2.dstId)
    }

  }

  private[mllib] def makeNonZero(dval: Double, tol: Double = defaultDivideByZeroVal) = {
    if (Math.abs(dval) < tol) {
      Math.signum(dval) * tol
    } else {
      dval
    }
  }

  private[mllib] def printMatrix(mat: BDM[Double]): String
  = printMatrix(mat, mat.rows, mat.cols)

  private[mllib] def printMatrix(mat: BDM[Double], numRows: Int, numCols: Int): String
  = printMatrix(mat.toArray, numRows, numCols)

  private[mllib] def printMatrix(vectors: Array[BDV[Double]]): String = {
    printMatrix(vectors.map {
      _.toArray
    }.flatten, vectors.length, vectors.length)
  }

  private[mllib] def printMatrix(vect: Array[Double], numRows: Int, numCols: Int): String = {
    val darr = vect
    val stride = darr.length / numCols
    val sb = new StringBuilder
    def leftJust(s: String, len: Int) = {
      "         ".substring(0, len - Math.min(len, s.length)) + s
    }

    assert(darr.length == numRows * numCols,
      s"Input array is not correct length (${darr.length}) given #rows/cols=$numRows/$numCols")
    for (r <- 0 until numRows) {
      for (c <- 0 until numCols) {
        sb.append(leftJust(f"${darr(r * stride + c)}%.6f", 9) + " ")
      }
      sb.append("\n")
    }
    sb.toString
  }

  private[mllib] def printVector(dvect: BDV[Double]) = {
    dvect.toArray.mkString(",")
  }

}
