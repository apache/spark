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

import org.apache.spark.graphx.{EdgeRDD, Edge, VertexId}
import org.apache.spark.mllib.clustering.{PICLinalg => LA}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.mllib.clustering.PICLinalg._
import scala.util.Random

object RDDLinalg {
  val DefaultMinNormAccel: Double = 1e-11

  val DefaultIterations: Int = 20
    val calcEigenDiffs = true

  def getPrincipalEigen(sc: SparkContext,
                        vectRdd: RDD[IndexedVector],
                        rddSize: Option[Int] = None,
                        nIterations: Int = DefaultIterations,
                        minNormAccel: Double = DefaultMinNormAccel
                         ): (Double, DVector) = {

    vectRdd.cache()
    val rowMajorRdd = vectRdd.map(identity) // Linalg.transpose(vectRdd)
    val numVects = rddSize.getOrElse(vectRdd.count().toInt)
    var eigenRdd: RDD[(Long, Double)] = null
    val rand = new Random()
    var eigenRddCollected: Seq[(Long, Double)] = for (ix <- 0 until numVects)
    yield (ix.toLong, rand.nextDouble)
    val enorm = LA.norm(eigenRddCollected.map(_._2).toArray)
    eigenRddCollected = eigenRddCollected.map { case (ix, d) =>
      (ix, d / enorm)
    }

    //      var eigenRddCollectedPrior =  eigenRddCollected.map(_._2).toArray
    var eigenRddCollectedPrior = Array.fill(numVects)(1.0 / Math.sqrt(numVects))
    var priorNorm = LA.norm(eigenRddCollectedPrior)
    var cnorm = 0.0
    var normDiffVelocity = Double.MaxValue
    var priorNormDiffVelocity = 0.0
    var normDiffAccel = Double.MaxValue
    for (iter <- 0 until nIterations) {
      //           if Math.abs(normDiffAccel) >= minNormAccel
      //             || iter < nIterations / 2) {
      val bcEigenRdd = sc.broadcast(eigenRddCollected)

      eigenRdd = rowMajorRdd.mapPartitions { piter =>
        val localEigenRdd = bcEigenRdd.value
        piter.map { case (ix, dvect) =>
          val d = LA.dot(dvect, localEigenRdd.map(_._2).toArray)
          println(s"localEigenRdd($ix)=$d (from ${dvect.mkString(",")} "
            + s" and ${localEigenRdd.map(_._2).mkString(",")})")
          (ix, d)
        }
      }
      eigenRddCollected = eigenRdd.collect()
      println(s"eigenRddCollected=\n${eigenRddCollected.map(_._2).mkString(",")}")
      cnorm = LA.norm(eigenRddCollected.map(_._2).toArray)
      eigenRddCollected = eigenRddCollected.map { case (ix, dval) =>
        (ix, dval / LA.makeNonZero(cnorm))
      }
      normDiffVelocity = cnorm - priorNorm
      normDiffAccel = normDiffVelocity - priorNormDiffVelocity
      //   println(s"Norm is $cnorm NormDiffVel=$normDiffVelocity NormDiffAccel=$normDiffAccel}")
      if (calcEigenDiffs) {
        val eigenDiff = eigenRddCollected.zip(eigenRddCollectedPrior).map {
          case ((ix, enew), eold) =>
            enew - eold
        }
        //      println(s"Norm is $cnorm NormDiff=$normDiffVelocity EigenRddCollected: "
        //        + s"${eigenRddCollected.mkString(",")} EigenDiffs: ${eigenDiff.mkString(",")}")
        //      println(s"eigenRddCollectedPrior: ${eigenRddCollectedPrior.mkString(",")}")
        System.arraycopy(eigenRddCollected.map {
          _._2
        }.toArray, 0, eigenRddCollectedPrior, 0, eigenRddCollected.length)
      }
      priorNorm = cnorm
      println(s"norm is $cnorm")

    }
    vectRdd.unpersist()

    //      val darr = new DVector(numVects)
    val eigenVect = eigenRddCollected.map(_._2).toArray
    val pdiff = eigenRddCollectedPrior.zip(eigenVect).foldLeft(0.0) { case (sum, (e1v, e2v)) =>
      sum + Math.abs(e1v - e2v)
    }
    assert(LA.withinTol(pdiff),
      s"Why is the prior eigenValue not nearly equal to present one:  diff=$pdiff")
    val lambda = LA.dot(vectRdd.take(1)(0)._2, eigenVect) / eigenVect(0)
    //      assert(withinTol(lambdaRatio - 1.0),
    //        "According to A *X = lambda * X we should have (A *X / X) ratio  = lambda " +
    //          s"but that did not happen: instead ratio=$lambdaRatio")
    //      val lambda = Math.signum(lambdaRatio) * cnorm
    //    println(s"eigenRdd: ${collectedEigenRdd.mkString(",")}")
    //      System.arraycopy(collectedEigenRdd, 0, darr, 0, darr.length)
    //      (cnorm, darr)
    (lambda, eigenVect)
  }

  def transpose(indexedRdd: RDD[IndexedVector]) = {
    val nVertices = indexedRdd.count.toInt
    val ColsPartitioner = new Partitioner() {
      override def numPartitions: Int = nVertices

      override def getPartition(key: Any): Int = {
        val index = key.asInstanceOf[Int]
        index % nVertices
      }
    }
    // Needed for the PairRDDFunctions implicits
    val columnsRdd = indexedRdd
      .mapPartitionsWithIndex({ (rx, iter) =>
      var cntr = rx - 1
      iter.map { case (rowIndex, dval) =>
        cntr += 1
        (cntr, dval)
      }
    }, preservesPartitioning = false)
      .partitionBy(ColsPartitioner)

    columnsRdd
  }

  def subtractProjection(sc: SparkContext, vectorsRdd: RDD[IndexedVector], vect: DVector):
  RDD[IndexedVector] = {
    val bcVect = sc.broadcast(vect)
    val subVectRdd = vectorsRdd.mapPartitions { iter =>
      val localVect = bcVect.value
      iter.map { case (ix, row) =>
        val subproj = LA.subtractProjection(row, localVect)
        //        println(s"Subproj for ${row.mkString(",")} =\n${subproj.mkString(",")}")
        (ix, subproj)
      }
    }
    //    println(s"Subtracted VectorsRdd\n${
    //      printMatrix(subVectRdd.collect.map(_._2),
    //        vect.length, vect.length)
    //    }")
    subVectRdd
  }

  val printDeflatedRdd: Boolean = false

  val printInputMatrix: Boolean = false

  def eigens(sc: SparkContext, matrixRdd: RDD[IndexedVector], nClusters: Int,
             nPowerIterations: Int) = {
    val lambdas = new Array[Double](nClusters)
    val eigens = new Array[RDD[Array[Double]]](nClusters)
    var deflatedRdd = matrixRdd.map(identity) // Clone the original matrix
    val nVertices = deflatedRdd.count.toInt
    if (printInputMatrix) {
      val collectedMatrixRdd = matrixRdd.collect
      println(s"Degrees Matrix:\n${
        LA.printMatrix(collectedMatrixRdd.map(_._2),
          nVertices, nVertices)
      }")
    }
    for (ex <- 0 until nClusters) {
      val (lambda, eigen) = getPrincipalEigen(sc, deflatedRdd, Some(nVertices),
        nPowerIterations)

      //      println(s"collectedEigen=\n${eigen.mkString(",")}")
      deflatedRdd = subtractProjection(sc, deflatedRdd, eigen)
      //      deflatedRdd = sc.parallelize(deflatedRddCollected, nVertices)
      if (printDeflatedRdd) {
        val deflatedRddCollected = deflatedRdd.collect
        println(s"EigensRemovedRDDCollected=\n${
          LA.printMatrix(deflatedRddCollected.map {
            _._2
          }, nVertices, nVertices)
        }")
      }
      val arrarr = new Array[Array[Double]](1)
      arrarr(0) = new Array[Double](nVertices)
      System.arraycopy(eigen, 0, arrarr(0), 0, nVertices)
      lambdas(ex) = lambda
      eigens(ex) = sc.parallelize(arrarr, 1)
      println(s"Lambda=$lambda Eigen=${LA.printMatrix(eigen, 1, nVertices)}")
    }
    val combinedEigens = eigens.reduceLeft(_.union(_))
    (lambdas, combinedEigens)
  }

  def printVertices(vertices: Array[(VertexId, Double)]) = {
    vertices.map { case (vid, dval) => s"($vid,$dval)"}.mkString(" , ")
  }

  def printMatrixFromEdges(edgesRdd: EdgeRDD[_]) = {
    val edgec = edgesRdd.collect
//    assert(edgec.size < 1e3,"Let us not print a large graph")
    val sorted = edgec.sortWith { case (e1, e2) =>
      e1.srcId < e2.srcId || (e1.srcId == e2.srcId && e1.dstId <= e2.dstId)
    }

  }
}
