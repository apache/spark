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

import scala.util.Random

/**
 * PICLinalg
 *
 */

object PICLinalg {

  type DVector = Array[Double]
  type DMatrix = Array[DVector]

  type LabeledVector = (String, DVector)

  type IndexedVector = (Long, DVector)

  type Vertices = Seq[LabeledVector]

  def add(v1: DVector, v2: DVector) =
    v1.zip(v2).map { x => x._1 + x._2}

  def mult(v1: DVector, d: Double) = {
    v1.map {
      _ * d
    }
  }

  def mult(v1: DVector, v2: DVector) = {
    v1.zip(v2).map { case (v1v, v2v) => v1v * v2v}
  }

  def multColByRow(v1: DVector, v2: DVector) = {
    val mat = for (v1v <- v1)
    yield mult(v2, v1v)
    //      println(s"Col by Row:\n${printMatrix(mat,
    //        v1.length, v1.length)}")
    mat
  }

  def norm(vect: DVector): Double = {
    Math.sqrt(vect.foldLeft(0.0) { case (sum, dval) => sum + Math.pow(dval, 2)})
  }

  def manhattanNorm(vect: DVector): Double = {
    val n = vect.foldLeft(0.0) { case (sum, dval) => sum + Math.abs(dval)}
    n / Math.sqrt(vect.size)
  }

  def dot(v1: DVector, v2: DVector) = {
    v1.zip(v2).foldLeft(0.0) {
      case (sum, (b, p)) => sum + b * p
    }
  }

  def onesVector(len: Int): DVector = {
    Array.fill(len)(1.0)
  }

  val calcEigenDiffs = true

  def withinTol(d: Double, tol: Double = DefaultTolerance) = Math.abs(d) <= tol

  val DefaultTolerance: Double = 1e-8

  def makeNonZero(dval: Double, tol: Double = DefaultTolerance) = {
    if (Math.abs(dval) < tol) {
      Math.signum(dval) * tol
    } else {
      dval
    }
  }

  def transpose(mat: DMatrix) = {
    val nCols = mat(0).length
    val matT = mat
      .flatten
      .zipWithIndex
      .groupBy {
      _._2 % nCols
    }
      .toSeq.sortBy {
      _._1
    }
      .map(_._2)
      //  .map(_.toSeq.sortBy(_._1))
      .map(_.map(_._1))
      .toArray
    matT
  }

  def printMatrix(mat: Array[Array[Double]]): String
  = printMatrix(mat, mat.length, mat.length)

  def printMatrix(darr: Array[DVector], numRows: Int, numCols: Int): String = {
    val flattenedArr = darr.zipWithIndex.foldLeft(new DVector(numRows * numCols)) {
      case (flatarr, (row, indx)) =>
        System.arraycopy(row, 0, flatarr, indx * numCols, numCols)
        flatarr
    }
    printMatrix(flattenedArr, numRows, numCols)
  }

  def printMatrix(darr: DVector, numRows: Int, numCols: Int): String = {
    val stride = (darr.length / numCols)
    val sb = new StringBuilder
    def leftJust(s: String, len: Int) = {
      "         ".substring(0, len - Math.min(len, s.length)) + s
    }

    assert(darr.size == numRows * numCols,
      s"Input array is not correct length (${darr.length}) given #rows/cols=$numRows/$numCols")
    for (r <- 0 until numRows) {
      for (c <- 0 until numCols) {
        sb.append(leftJust(f"${darr(r * stride + c)}%.6f", 9) + " ")
      }
      sb.append("\n")
    }
    sb.toString
  }

  def printVector(dvect: DVector) = {
    dvect.mkString(",")
  }

  def project(basisVector: DVector, inputVect: DVector) = {
    val pnorm = makeNonZero(norm(basisVector))
    val projectedVect = basisVector.map(
      _ * dot(basisVector, inputVect) / dot(basisVector, basisVector))
    projectedVect
  }

  def subtract(v1: DVector, v2: DVector) = {
    val subvect = v1.zip(v2).map { case (v1val, v2val) => v1val - v2val}
    subvect
  }

  def subtractProjection(vect: DVector, basisVect: DVector): DVector = {
    val proj = project(basisVect, vect)
    val subVect = subtract(vect, proj)
    subVect
  }

  def localPIC(matIn: DMatrix, nClusters: Int, nIterations: Int,
               optExpected: Option[(DVector, DMatrix)]) = {

    var mat = matIn.map(identity)
    val numVects = mat.length

    val (expLambda, expdat) = optExpected.getOrElse((new DVector(0), new DMatrix(0)))
    var cnorm = -1.0
    for (k <- 0 until nClusters) {
      val r = new Random()
      var eigen = Array.fill(numVects) {
        //          1.0
        r.nextDouble
      }
      val enorm = norm(eigen)
      eigen.map { e => e / enorm}

      for (iter <- 0 until nIterations) {
        eigen = mat.map { dvect =>
          dot(dvect, eigen)
        }
        cnorm = makeNonZero(norm(eigen))
        eigen = eigen.map(_ / cnorm)
      }
      val signum = Math.signum(dot(mat(0), eigen))
      val lambda = dot(mat(0), eigen) / eigen(0)
      eigen = eigen.map(_ * signum)
      println(s"lambda=$lambda eigen=${printVector(eigen)}")
      if (expLambda.length > 0) {
        val compareVect = eigen.zip(expdat(k)).map { case (a, b) => a / b}
        println(s"Ratio  to expected: lambda=${lambda / expLambda(k)} " +
          s"Vect=${compareVect.mkString("[", ",", "]")}")
      }
      if (k < nClusters - 1) {
        // TODO: decide between deflate/schurComplement
        mat = schurComplement(mat, lambda, eigen)
      }
    }
  }

  def compareVectors(v1: Array[Double], v2: Array[Double]) = {
    v1.zip(v2).forall { case (v1v, v2v) => withinTol(v1v - v2v)}
  }

  def compareMatrices(m1: DMatrix, m2: DMatrix) = {
    m1.zip(m2).forall { case (m1v, m2v) =>
      m1v.zip(m2v).forall { case (m1vv, m2vv) => withinTol(m1vv - m2vv)}
    }
  }

  def subtract(mat1: DMatrix, mat2: DMatrix) = {
    mat1.zip(mat2).map { case (m1row, m2row) =>
      m1row.zip(m2row).map { case (m1v, m2v) => m1v - m2v}
    }
  }

  def deflate(mat: DMatrix, lambda: Double, eigen: DVector) = {
    //        mat = mat.map(subtractProjection(_, mult(eigen, lambda)))
    val eigT = eigen
    val projected = multColByRow(eigen, eigT).map(mult(_, lambda))
    //        println(s"projected matrix:\n${printMatrix(projected,
    //          eigen.length, eigen.length)}")
    val matOut = mat.zip(projected).map { case (mrow, prow) =>
      subtract(mrow, prow)
    }
    println(s"Updated matrix:\n${
      printMatrix(mat,
        eigen.length, eigen.length)
    }")
    matOut
  }

  def mult(mat1: DMatrix, mat2: DMatrix) = {
    val mat2T = transpose(mat2)
    val outmatT = for {row <- mat1}
    yield {
      val outRow = mat2T.map { col =>
        dot(row, col)
      }
      outRow
    }
    outmatT
  }

  //    def mult(mat: DMatrix, vect: DVector): DMatrix  = {
  //      val outMat = mat.map { m =>
  //        mult(m, vect)
  //      }
  //      outMat
  //    }
  //
  //    def mult(vect: DVector, mat: DMatrix): DMatrix = {
  //      for {d <- vect.zip(transpose(mat)) }
  //        yield mult(d._2, d._1)
  //    }

  def scale(mat: DMatrix, d: Double): DMatrix = {
    for (row <- mat) yield mult(row, d)
  }

  def transpose(vector: DVector) = {
    vector.map { d => Array(d)}
  }

  def toMat(dvect: Array[Double], ncols: Int) = {
    val m = dvect.toSeq.grouped(ncols).map(_.toArray).toArray
    m
  }

  def schurComplement(mat: DMatrix, lambda: Double, eigen: DVector) = {
    val eigT = toMat(eigen, eigen.length) // The sense is reversed
    val eig = transpose(eigT)
    val projected = mult(eig, eigT)
    println(s"projected matrix:\n${
      printMatrix(projected,
        eigen.length, eigen.length)
    }")
    val numerat1 = mult(mat, projected)
    val numerat2 = mult(numerat1, mat)
    println(s"numerat2=\n${
      printMatrix(numerat2,
        eigen.length, eigen.length)
    }")
    val denom1 = mult(eigT, mat)
    val denom2 = mult(denom1, toMat(eigen, 1))
    val denom = denom2(0)(0)
    println(s"denom is $denom")
    val projMat = scale(numerat2, 1.0 / denom)
    println(s"Updated matrix:\n${
      printMatrix(projMat,
        eigen.length, eigen.length)
    }")
    val defMat = subtract(mat, projMat)
    println(s"deflated matrix:\n${
      printMatrix(defMat,
        eigen.length, eigen.length)
    }")
    defMat
  }

}

