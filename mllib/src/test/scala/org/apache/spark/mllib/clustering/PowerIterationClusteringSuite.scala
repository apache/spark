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
import org.apache.spark.graphx.{EdgeRDD, Edge, Graph}
import org.apache.spark.mllib.clustering.PowerIterationClustering.{LabeledPoint, Points, IndexedVector}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

import scala.util.Random

class PowerIterationClusteringSuite extends FunSuite with MLlibTestSparkContext {

  val logger = Logger.getLogger(getClass.getName)

  import org.apache.spark.mllib.clustering.PowerIterationClusteringSuite._

  test("concentricCirclesTest") {
    concentricCirclesTest()
  }

  def concentricCirclesTest() = {
    val sigma = 1.0
    val nIterations = 10

    val circleSpecs = Seq(
      // Best results for 30 points
      CircleSpec(Point(0.0, 0.0), 0.03, 0.1, 3),
      CircleSpec(Point(0.0, 0.0), 0.3, 0.03, 12),
      CircleSpec(Point(0.0, 0.0), 1.0, 0.01, 15)
      // Add following to get 100 points
      , CircleSpec(Point(0.0, 0.0), 1.5, 0.005, 30),
      CircleSpec(Point(0.0, 0.0), 2.0, 0.002, 40)
    )

    val nClusters = circleSpecs.size
    val cdata = createConcentricCirclesData(circleSpecs)
    val vertices = new Random().shuffle(cdata.map { p =>
      (p.label, new BDV(Array(p.x, p.y)))
    })

    val nVertices = vertices.length
    val G = createGaussianAffinityMatrix(sc, vertices)
    val (ccenters, estCollected) = PIC.run(sc, G, nClusters, nIterations)
    logger.info(s"Cluster centers: ${ccenters.mkString(",")} " +
      s"\nEstimates: ${estCollected.mkString("[", ",", "]")}")
    assert(ccenters.size == circleSpecs.length, "Did not get correct number of centers")

  }

}

object PowerIterationClusteringSuite {
  val logger = Logger.getLogger(getClass.getName)
  val A = Array
  val PIC = PowerIterationClustering

  // Default sigma for Gaussian Distance calculations
  val defaultSigma = 1.0

  // Default minimum affinity between points - lower than this it is considered
  // zero and no edge will be created
  val defaultMinAffinity = 1e-11

  def pdoub(d: Double) = f"$d%1.6f"

  case class Point(label: Long, x: Double, y: Double) {
    def this(x: Double, y: Double) = this(-1L, x, y)

    override def toString() = s"($label, (${pdoub(x)},${pdoub(y)}))"
  }

  object Point {
    def apply(x: Double, y: Double) = new Point(-1L, x, y)
  }

  case class CircleSpec(center: Point, radius: Double, noiseToRadiusRatio: Double,
                        nPoints: Int, uniformDistOnCircle: Boolean = true)

  def createConcentricCirclesData(circleSpecs: Seq[CircleSpec]) = {
    import org.apache.spark.mllib.random.StandardNormalGenerator
    val normalGen = new StandardNormalGenerator
    var idStart = 0
    val circles = for (csp <- circleSpecs) yield {
      idStart += 1000
      val circlePoints = for (thetax <- 0 until csp.nPoints) yield {
        val theta = thetax * 2 * Math.PI / csp.nPoints
        val (x, y) = (csp.radius * Math.cos(theta)
          * (1 + normalGen.nextValue * csp.noiseToRadiusRatio),
          csp.radius * Math.sin(theta) * (1 + normalGen.nextValue * csp.noiseToRadiusRatio))
        (Point(idStart + thetax, x, y))
      }
      circlePoints
    }
    val points = circles.flatten.sortBy(_.label)
    logger.info(printPoints(points))
    points
  }

  def printPoints(points: Seq[Point]) = {
    points.mkString("[", " , ", "]")
  }

  private[mllib] def printVector(dvect: BDV[Double]) = {
    dvect.toArray.mkString(",")
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
    val initialVt = PIC.createInitialVector(sc, points.map(_._1), rowSums)
    if (logger.isDebugEnabled) {
      logger.debug(s"Vt(0)=${
        printVector(new BDV(initialVt.map {
          _._2
        }.toArray))
      }")
    }
    val edgesRdd = createSparseEdgesRdd(sc, wRdd, minAffinity)
    val G = PIC.createGraphFromEdges(sc, edgesRdd, points.size, Some(initialVt))
    if (logger.isDebugEnabled) {
      logger.debug(printMatrixFromEdges(G.edges))
    }
    G
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


  private[mllib] def printMatrixFromEdges(edgesRdd: EdgeRDD[_]) = {
    val edgec = edgesRdd.collect
    val sorted = edgec.sortWith { case (e1, e2) =>
      e1.srcId < e2.srcId || (e1.srcId == e2.srcId && e1.dstId <= e2.dstId)
    }

  }

  private[mllib] def makeNonZero(dval: Double, tol: Double = PIC.defaultDivideByZeroVal) = {
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

}
