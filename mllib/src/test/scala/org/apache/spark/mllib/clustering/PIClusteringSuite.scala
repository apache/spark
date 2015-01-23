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

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite


/**
 * Provides a method to run tests against a {@link SparkContext} variable that is correctly stopped
 * after each test.
 * TODO: import this from the graphx test cases package i.e. may need update to pom.xml
*/
trait LocalSparkContext {
  /** Runs `f` on a new SparkContext and ensures that it is stopped afterwards. */
  def withSpark[T](f: SparkContext => T) = {
    val conf = new SparkConf()
    GraphXUtils.registerKryoClasses(conf)
    val sc = new SparkContext("local", "test", conf)
    try {
      f(sc)
    } finally {
      sc.stop()
    }
  }
}

/**
 * SpectralClusteringWithGraphxSuite
 *
 */
class PIClusteringSuite extends FunSuite with LocalSparkContext {

  val PIC = PIClustering
  val LA = PICLinalg
  val RDDLA = RDDLinalg
  val A = Array

  test("graphxSingleEigen") {
    graphxSingleEigen
  }

  import PIClusteringSuite._
  def graphxSingleEigen() = {

    val (aMat, dxDat1) = createAffinityMatrix()
    //    val dats = Array((dat2, expDat2), (dat1, expDat1))
    val sigma = 1.0
    val nIterations = 20
    val nClusters = 3
    withSpark { sc =>
      val affinityRdd = sc.parallelize(dxDat1.zipWithIndex).map { case (dvect, ix) =>
        (ix.toLong, dvect)
      }
      val wCollected = affinityRdd.collect
      val nVertices = aMat(0).length
      println(s"AffinityMatrix:\n${LA.printMatrix(wCollected.map(_._2), nVertices, nVertices)}")
      val edgesRdd = PIC.createSparseEdgesRdd(sc, affinityRdd)
      val edgesRddCollected = edgesRdd.collect()
      println(s"edges=${edgesRddCollected.mkString(",")}")
      val rowSums = aMat.map { vect =>
        vect.foldLeft(0.0){ _ + _ }
      }
      val initialVt = PIC.createInitialVector(sc, affinityRdd.map{_._1}.collect, rowSums )
      val G = PIC.createGraphFromEdges(sc, edgesRdd, nVertices, Some(initialVt))
      val printVtVectors: Boolean = true
      if (printVtVectors) {
        val vCollected = G.vertices.collect()
        val graphInitialVt = vCollected.map {
          _._2
        }
        println(s"     initialVT vertices: ${RDDLA.printVertices(initialVt.toArray)}")
        println(s"graphInitialVt vertices: ${RDDLA.printVertices(vCollected)}")
        val initialVtVect = initialVt.map {
          _._2
        }.toArray
        println(s"graphInitialVt=${graphInitialVt.mkString(",")}\n"
          + s"initialVt=${initialVt.mkString(",")}")
//        assert(LA.compareVectors(graphInitialVt, initialVtVect))
      }
      val (g2, norm, eigvect) = PIC.getPrincipalEigen(sc, G, nIterations)
      println(s"lambda=$norm eigvect=${eigvect.mkString(",")}")
    }
  }

  test("fifteenVerticesTest") {
    val vertFile = "../data/graphx/new_lr_data.15.txt"
    val sigma = 1.0
    val nIterations = 20
    val nClusters = 3
    withSpark { sc =>
      val vertices = PIC.readVerticesfromFile(vertFile)
      val nVertices = vertices.length
      val (graph, lambda, eigen) = PIC.cluster(sc, vertices, nClusters,
        nIterations, sigma)
      val collectedRdd = eigen // .collect
      println(s"DegreeMatrix:\n${printMatrix(collectedRdd, nVertices, nVertices)}")
      println(s"Eigenvalue = $lambda EigenVectors:\n${printMatrix(collectedRdd, nClusters, nVertices)}")
//      println(s"Eigenvalues = ${lambdas.mkString(",")} EigenVectors:\n${printMatrix(collectedEigens, nClusters, nVertices)}")
    }
  }

//  test("testLinearFnGenerator") {
//    val PS = PolySpec
//    val dr = new DRange(0.0, 5.0)
//    val polyInfo = A(PS(3.0, 2.0, -1.0)
//    val noiseRatio = 0.1
//    val l = List(1,2,3)
//    l.scanLeft(
//  }

  def printMatrix(darr: Array[Double], numRows: Int, numCols: Int): String =
    LA.printMatrix(darr, numRows, numCols)

  def printMatrix(darr: Array[Array[Double]], numRows: Int, numCols: Int): String =
    LA.printMatrix(darr, numRows, numCols)
}

object PIClusteringSuite {
  val LA = PICLinalg
  val A = Array

  def toMat(dvect: Array[Double], ncols: Int) = {
    val m = dvect.toSeq.grouped(ncols).map(_.toArray).toArray
    m
  }

  def createAffinityMatrix() = {
    val dat1 = A(
      A(0, 0.4, .8, 0.9),
      A(.4, 0, .7, 0.5),
      A(0.8, 0.7, 0, 0.75),
      A(0.9, .5, .75, 0)
    )
//    val asize=10
//    val dat1 = toMat(for (ix <- 0 until asize;
//        for cx<- 0 until asize)
//    yield {
//      cx match {
//        case _ if cx < 2 => 10
//        case _ if cx <= 7 => 0.5
//        case _ => cx * 0.2
//      }
//    }, asize, asize)


    println(s"Input mat: ${LA.printMatrix(dat1, 4,4)}")
    val D = /*LA.transpose(dat1)*/ dat1.zipWithIndex.map { case (dvect, ix) =>
      val sum = dvect.foldLeft(0.0) {
        _ + _
      }
      dvect.zipWithIndex.map { case (d, dx) =>
        if (ix == dx) {
          1.0 / sum
        } else {
          0.0
        }
      }
    }
    print(s"D =\n ${LA.printMatrix(D)}")

    val DxDat1 = LA.mult(D, dat1)
    print(s"D * Dat1 =\n ${LA.printMatrix(DxDat1)}")
    (dat1, DxDat1)
  }

  def main(args: Array[String]) {
    val pictest = new PIClusteringSuite
    pictest.graphxSingleEigen()
  }
}