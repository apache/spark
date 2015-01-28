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

import org.apache.log4j.Logger
import org.apache.spark.graphx.{EdgeRDD, Edge, VertexId}
import org.apache.spark.mllib.clustering.PIClustering.IndexedVector
import org.apache.spark.mllib.clustering.{PICLinalg => LA}
import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.mllib.clustering.PICLinalg._
import scala.util.Random
import scala.language.existentials

/**
 * Linear Algebra helper routines for RDD's associated with the
 * PIClustering implementation
 *
 */
object RDDLinalg {

  private val logger = Logger.getLogger(getClass.getName)

  val DefaultMinNormAccel: Double = 1e-11

  val DefaultIterations: Int = 20
  val calcEigenDiffs = true

  /**
   * Run the Power Iteration Clustering algorithm to calculate the
   * principal eigenvector of a Matrix. This version performs the
   * calculation using vanilla RDD via MapPartitions.
   *
   */
  def getPrincipalEigen(sc: SparkContext,
                        vectRdd: RDD[IndexedVector[Double]],
                        rddSize: Option[Int] = None,
                        nIterations: Int = DefaultIterations,
                        minNormAccel: Double = DefaultMinNormAccel
                         ): (Double, BDV[Double]) = {

    vectRdd.cache()
    val rowMajorRdd = vectRdd.map(identity)
    val numVects = rddSize.getOrElse(vectRdd.count().toInt)
    var eigenRdd: RDD[(Long, Double)] = null
    val rand = new Random()
    var eigenRddCollected: Seq[(Long, Double)] = for (ix <- 0 until numVects)
    yield (ix.toLong, rand.nextDouble)
    val enorm = LA.norm(new BDV(eigenRddCollected.map(_._2).toArray))
    eigenRddCollected = eigenRddCollected.map { case (ix, d) =>
      (ix, d / enorm)
    }
    var eigenRddCollectedPrior = Array.fill(numVects)(1.0 / Math.sqrt(numVects))
    var priorNorm = LA.norm(new BDV(eigenRddCollectedPrior))
    var cnorm = 0.0
    var normDiffVelocity = Double.MaxValue
    var priorNormDiffVelocity = 0.0
    var normDiffAccel = Double.MaxValue
    for (iter <- 0 until nIterations
         if Math.abs(normDiffAccel) >= minNormAccel) {
      val bcEigenRdd = sc.broadcast(eigenRddCollected)

      eigenRdd = rowMajorRdd.mapPartitions { piter =>
        val localEigenRdd = bcEigenRdd.value
        piter.map { case (ix, dvect) =>
          val d = LA.dot(dvect, new BDV(localEigenRdd.map(_._2).toArray))
          println(s"localEigenRdd($ix)=$d (from ${dvect.mkString(",")} "
            + s" and ${localEigenRdd.map(_._2).mkString(",")})")
          (ix, d)
        }
      }
      eigenRddCollected = eigenRdd.collect()
      println(s"eigenRddCollected=\n${eigenRddCollected.map(_._2).mkString(",")}")
      cnorm = LA.norm(new BDV(eigenRddCollected.map(_._2).toArray))
      eigenRddCollected = eigenRddCollected.map { case (ix, dval) =>
        (ix, dval / LA.makeNonZero(cnorm))
      }
      normDiffVelocity = cnorm - priorNorm
      normDiffAccel = normDiffVelocity - priorNormDiffVelocity
      priorNorm = cnorm
      println(s"norm is $cnorm")

    }
    vectRdd.unpersist()

    val eigenVect = eigenRddCollected.map(_._2).toArray
    val pdiff = eigenRddCollectedPrior.zip(eigenVect).foldLeft(0.0) { case (sum, (e1v, e2v)) =>
      sum + Math.abs(e1v - e2v)
    }
    assert(LA.withinTol(pdiff),
      s"Why is the prior eigenValue not nearly equal to present one:  diff=$pdiff")
    val lambda = LA.dot(vectRdd.take(1)(0)._2, new BDV(eigenVect)) / eigenVect(0)
    (lambda, new BDV(eigenVect))
  }

  /**
   * Calculate K - as defined by number of clusters -  eigenvectors/eigenvalues of
   * a matrix by iteratively:
   *  - calculate the Principal Eigenvalue/Eigenvector
   *  - subtract the contribution along that eigenvector from the original matrix
   *    (via the ShurComplement method)
   *  - calculate the Principal Eigenvalue/Eigenvector of the resulting reduced matrix
   *
   * @param sc
   * @param matrixRdd
   * @param nClusters
   * @param nPowerIterations
   * @return
   */
  def eigens(sc: SparkContext, matrixRdd: RDD[IndexedVector[Double]], nClusters: Int,
             nPowerIterations: Int) = {
    val lambdas = new Array[Double](nClusters)
    val eigens = new Array[RDD[Array[Double]]](nClusters)
    var deflatedRdd = matrixRdd.map(identity) // Clone the original matrix
    val nVertices = deflatedRdd.count.toInt
      if (logger.isDebugEnabled) {
      println(s"Degrees Matrix:\n${
        printMatrix(matrixRdd, nVertices, nVertices)
      }")
    }
    for (ex <- 0 until nClusters) {
      val (lambda, eigen) = getPrincipalEigen(sc, deflatedRdd, Some(nVertices),
        nPowerIterations)

      deflatedRdd = subtractProjection(sc, deflatedRdd, eigen)
      if (logger.isDebugEnabled) {
        println(s"EigensRemovedRDDCollected=\n${
          printMatrix(deflatedRdd, nVertices, nVertices)
        }")
      }
      val arrarr = new Array[Array[Double]](1)
      arrarr(0) = new Array[Double](nVertices)
      System.arraycopy(eigen, 0, arrarr(0), 0, nVertices)
      lambdas(ex) = lambda
      eigens(ex) = sc.parallelize(arrarr, 1)
      println(s"Lambda=$lambda Eigen=${LA.printMatrix(eigen.toArray, 1, nVertices)}")
    }
    val combinedEigens = eigens.reduceLeft(_.union(_))
    (lambdas, combinedEigens)
  }

  /**
   * Calculate the projection of a matrix onto a Basis and return the
   * result of subtracting that result from the original matrix
   *
   * @param sc
   * @param vectorsRdd
   * @param vect
   * @return
   */
  def subtractProjection(sc: SparkContext, vectorsRdd: RDD[IndexedVector[Double]], vect: BDV[Double]):
  RDD[IndexedVector[Double]] = {
    val bcVect = sc.broadcast(vect)
    val subVectRdd = vectorsRdd.mapPartitions { iter =>
      val localVect = bcVect.value
      iter.map { case (ix, row) =>
        val subproj = LA.subtractProjection(row, localVect)
        (ix, subproj)
      }
    }
    subVectRdd
  }

  def printVertices(vertices: Array[(VertexId, Double)]) = {
    vertices.map { case (vid, dval) => s"($vid,$dval)"}.mkString(" , ")
  }

  def printMatrix(denseVectorRDD: RDD[LabeledVector], i: Int, i1: Int) = {
    denseVectorRDD.collect.map {
      case (vid, dvect) => dvect.toArray
    }.flatten
  }

  def printMatrixFromEdges(edgesRdd: EdgeRDD[_]) = {
    val edgec = edgesRdd.collect
    val sorted = edgec.sortWith { case (e1, e2) =>
      e1.srcId < e2.srcId || (e1.srcId == e2.srcId && e1.dstId <= e2.dstId)
    }

  }
}
