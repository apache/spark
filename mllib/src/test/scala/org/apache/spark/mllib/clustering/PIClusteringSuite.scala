package org.apache.spark.mllib.clustering
import org.scalatest.FunSuite
/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
/**
 * PIClusteringSuite
 *
 */
class PIClusteringSuite extends FunSuite with LocalSparkContext {
  val SP = SpectralClusteringUsingRdd
  val LA = SP.Linalg
  val A = Array
  test("graphxSingleEigen") {
    val dat1 = A(
      A(0, 0.4, .8, 0.9),
      A(.4, 0, .7, 0.5),
      A(0.8, 0.7, 0, 0.75),
      A(0.9, .5, .75, 0)
    )
    val D = /*LA.transpose(dat1)*/dat1.zipWithIndex.map { case (dvect, ix) =>
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
    // val dats = Array((dat2, expDat2), (dat1, expDat1))
    val SC = SpectralClustering
    val sigma = 1.0
    val nIterations = 20
    val nClusters = 3
    withSpark { sc =>
      val affinityRdd = sc.parallelize(DxDat1.zipWithIndex).map { case (dvect, ix) =>
        (ix.toLong, dvect)
      }
      val wCollected = affinityRdd.collect
      val nVertices = dat1(0).length
      println(s"AffinityMatrix:\n${printMatrix(wCollected.map(_._2), nVertices, nVertices)}")
      val edgesRdd = SC.createSparseEdgesRdd(sc, affinityRdd)
      val edgesRddCollected = edgesRdd.collect()
      println(s"edges=${edgesRddCollected.mkString(",")}")
      val rowSums = dat1.map { vect =>
        vect.foldLeft(0.0){ _ + _ }
      }
      val initialVt = SC.createInitialVector(sc, affinityRdd.map{_._1}.collect, rowSums )
      val G = SC.createGraphFromEdges(sc, edgesRdd, nVertices, Some(initialVt))
      val printVtVectors: Boolean = true
      if (printVtVectors) {
        val vCollected = G.vertices.collect()
        val graphInitialVt = vCollected.map {
          _._2
        }
        println(s" initialVT vertices: ${LA.printVertices(initialVt.toArray)}")
        println(s"graphInitialVt vertices: ${LA.printVertices(vCollected)}")
        val initialVtVect = initialVt.map {
          _._2
        }.toArray
        println(s"graphInitialVt=${graphInitialVt.mkString(",")}\n"
          + s"initialVt=${initialVt.mkString(",")}")
        assert(LA.compareVectors(graphInitialVt, initialVtVect))
      }
      val (g2, norm, eigvect) = SC.getPrincipalEigen(sc, G, nIterations)
      println(s"lambda=$norm eigvect=${eigvect.mkString(",")}")
    }
  }
  test("VectorArithmeticAndProjections") {
    // def A[T : ClassTag](ts: T*) = Array(ts:_*)
    // type A = Array[Double]
    var dat = A(
      A(1.0, 2.0, 3.0),
      A(3.0, 6.0, 9.0)
    )
    var firstEigen = LA.subtractProjection(dat(0), dat(1)) // collinear
    println(s"firstEigen should be all 0's ${firstEigen.mkString(",")}")
    assert(firstEigen.forall(LA.withinTol(_, 1e-8)))
    dat = A(
      A(1.0, 2.0, 3.0),
      A(-3.0, -6.0, -9.0),
      A(2.0, 4.0, 6.0),
      A(-1.0, 1.0, -.33333333333333),
      A(-2.0, 5.0, 2.0)
    )
    var proj = LA.subtractProjection(dat(0), dat(3)) // orthog
    println(s"proj after subtracting orthogonal vector should be same " +
      s"as input (1.,2.,3.) ${proj.mkString(",")}")
    assert(proj.zip(dat(0)).map { case (a, b) => a - b} .forall(LA.withinTol(_, 1e-11)))
    val addMultVect = LA.add(dat(0), LA.mult(dat(3), 3.0))
    assert(addMultVect.zip(dat(4)).map { case (a, b) => a - b} .forall(LA.withinTol(_, 1e-11)),
      "AddMult was not within tolerance")
    proj = LA.subtractProjection(addMultVect, dat(3)) // orthog
    println(s"proj should be same as parallel input (1.,2.,3.) ${proj.mkString(",")}")
    assert(proj.zip(dat(0)).map { case (a, b) => a - b} .forall(LA.withinTol(_, 1e-11)))
  }
  test("eigensTest") {
    var dat0 = toMat(A(2., 1.5, 2, .5, 3, .5, 1., .5, 4.), 3)
    val expDat0 = (A(-0.7438459, -0.4947461, -0.4493547),
      toMat(A(0.5533067, 0.3680148, 0.3342507,
        0.3680148, 0.2447737, 0.2223165, 0.3342507, 0.2223165, 0.2019197), 3)
      )
    val dat1 = A(
      A(3.0, 2.0, 4.0),
      A(2.0, 0.0, 2.0),
      A(4.0, 2.0, 3.0)
    )
    val expDat1 =
      (A(8.0, -1.0, -1.0),
        A(
          A(0.6666667, 0.7453560, 0.0000000),
          A(0.3333333, -0.2981424, -0.8944272),
          A(0.6666667, -0.5962848, 0.4472136)
        ))
    val dat2 = A(
      A(1.0, 1, -2.0),
      A(-1.0, 2.0, 1.0),
      A(0.0, 1.0, -1.0)
    )
    // val dat2 = A(
    // A(1.0, -1.0, 0.0),
    // A(1.0, 2.0, 1.0),
    // A(-2.0, 1.0, -1.0)
    // )
    val expDat2 =
      (A(2.0, -1.0, 1.0),
        A(
          A(-0.5773503, -0.1360828, -7.071068e-01),
          A(0.5773503, -0.2721655, -3.188873e-16),
          A(0.5773503, 0.9525793, 7.071068e-01)
        ))
    val dats = Array((dat0, expDat0))
    // val dats = Array((dat2, expDat2), (dat1, expDat1))
    for ((dat, expdat) <- dats) {
      val sigma = 1.0
      val nIterations = 10 // 20
      val nClusters = 3
      withSpark { sc =>
        var datRdd = LA.transpose(
          sc.parallelize((0 until dat.length).zip(dat).map { case (ix, dvect) =>
            (ix, dvect)
          }.toSeq))
        val datRddCollected = datRdd.collect()
        val (eigvals, eigvects) = LA.eigens(sc, datRdd, nClusters, nIterations)
        val collectedEigens = eigvects.collect
        val printedEigens = LA.printMatrix(collectedEigens, 3, 3)
        println(s"eigvals=${eigvals.mkString(",")} eigvects=\n$printedEigens}")
        assert(LA.compareVectors(eigvals, expdat._1))
        assert(LA.compareMatrices(eigvects.collect, expdat._2))
      }
    }
  }
  test("matrix mult") {
    val m1 = toMat(A(1., 2., 3., 4., 5., 6., 7., 8., 9.), 3)
    val m2 = toMat(A(3., 1., 2., 3., 4., 5., 6., 7., 8.), 3)
    val mprod = LA.mult(m1, m2)
    println(s"Matrix1:\n ${printMatrix(mprod, 3, 3)}")
    val m21 = toMat(A(1., 2., 3., 4., 5., 6., 7., 8., 9.), 3)
    val m22 = toMat(A(10., 11., 12., 13., 14., 15.), 2)
    val mprod2 = LA.mult(m21, m22)
    println(s"Matrix2:\n ${printMatrix(mprod2, 3, 3)}")
  }
  test("positiveEigenValues") {
    var dat2r = toMat(A(2., 1.5, 2, .5, 3, .5, 1., .5, 4.), 3)
    val expLambda = A(5.087874, 2.810807, 1.101319)
    val expdat = LA.transpose(toMat(A(0.6196451, -0.2171220, 0.9402498, 0.3200200, -0.8211316, -0.1699035, 0.7166779, 0.5278267, -0.2950646), 3))
    val nClusters = 3
    val nIterations = 20
    val numVects = 3
    LA.localPIC(dat2r, nClusters, nIterations, Some((expLambda, expdat)))
  }
  def toMat(dvect: Array[Double], ncols: Int) = {
    val m = dvect.toSeq.grouped(ncols).map(_.toArray).toArray
    m
  }
  test("positiveEigenValuesTaketwo") {
    val dat2r = toMat(A(2., 1.5, 2, .5, 3, .5, 1., .5, 2.), 3)
    val expLambda = A(4.2058717, 2.2358331, 0.5582952)
    val expdat = LA.transpose(toMat(A(-0.7438459, 0.4718612, -0.82938843,
      -0.4947461, -0.6777315, 0.05601231, -0.4493547, 0.5639389, 0.55585740), 3))
    val nClusters = 3
    val nIterations = 20
    val numVects = 3
    LA.localPIC(dat2r, nClusters, nIterations, Some((expLambda, expdat)))
  }
  test("manualPowerIt") {
    // R code
    //
    //> x = t(matrix(c(-2,-.5,2,.5,-3,.5,-1.,.5,4),nrow=3,ncol=3))
    //> x
    // [,1] [,2] [,3]
    //[1,] -2.0 -0.5 2.0
    //[2,] 0.5 -3.0 0.5
    //[3,] -1.0 0.5 4.0
    //
    //> eigen(x)
    //$values
    //[1] 3.708394 -2.639960 -2.068434
    //
    //$vectors
    // [,1] [,2] [,3]
    //[1,] 0.32182428 0.56847491 -0.85380536
    //[2,] 0.09420476 0.82235947 -0.51117379
    //[3,] 0.94210116 0.02368917 -0.09857872
    //
    //x = t(matrix(c(-2.384081, 0.387542, -2.124275, -0.612458, -3.032928, 0.170814, 0.875725, 0.170814, 0.709039 ),nrow=3,ncol=3))
    //> eigen(x)
    //$values
    //[1] -2.6400564672 -2.0684340334 0.0005205006
    //
    //$vectors
    // [,1] [,2] [,3]
    //[1,] -0.5192257 0.8053757 0.6396646
    //[2,] 0.8496238 -0.5503942 -0.1713433
    //[3,] 0.0924343 -0.2200823 -0.7493135
    // var dat2r = A(
    // A(-2.0, -0.5, 2.0),
    // A(0.5, -3.0, 0.5),
    // A(-0.5, 0.5, 4.0)
    // )
    var dat2r = toMat(A(-2., -.5, 2, .5, -3, .5, -1., .5, 4.), 3)
    val expLambda = A(3.708394, -2.639960, -2.068434)
    val expdat = LA.transpose(toMat(A(0.32182428, 0.56847491, -0.85380536, 0.09420476,
      0.82235947, -0.51117379, 0.94210116, 0.02368917, -0.09857872), 3))
    val nClusters = 3
    val nIterations = 30
    val numVects = 3
    LA.localPIC(dat2r, nClusters, nIterations, Some((expLambda, expdat)))
  }
  test("fifteenVerticesTest") {
    val vertFile = "../data/graphx/new_lr_data.15.txt"
    val sigma = 1.0
    val nIterations = 20
    val nClusters = 3
    withSpark { sc =>
      val vertices = SpectralClusteringUsingRdd.readVerticesfromFile(vertFile)
      val nVertices = vertices.length
      val (lambdas, eigens) = SpectralClusteringUsingRdd.cluster(sc, vertices, nClusters, sigma, nIterations)
      val collectedRdd = eigens.collect
      println(s"DegreeMatrix:\n${printMatrix(collectedRdd, nVertices, nVertices)}")
      val collectedEigens = eigens.collect
      println(s"Eigenvalues = ${lambdas.mkString(",")} EigenVectors:\n${printMatrix(collectedEigens, nClusters, nVertices)}")
    }
  }
  def printMatrix(darr: Array[Double], numRows: Int, numCols: Int): String =
    LA.printMatrix(darr, numRows, numCols)
  def printMatrix(darr: Array[Array[Double]], numRows: Int, numCols: Int): String =
    LA.printMatrix(darr, numRows, numCols)
}