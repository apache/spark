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

import org.apache.spark.mllib.linalg.Vectors

import scala.reflect.ClassTag
import scala.util.Random
import breeze.linalg.{DenseVector => BDV,DenseMatrix => BDM}

/**
 * PICLinalg
 *
 */

object PICLinalg {

  type DMatrix = BDM[Double]

  type LabeledVector = (Long, BDV[Double])

  type IndexeBDV[Double] = (Long, BDV[Double])

  type Vertices = Seq[LabeledVector]

//  implicit def arrayToVect(darr: Array[Double]): BDV[Double] = new BDV(darr)
  implicit def bdvToSeq[T](vect: BDV[T])(implicit ct: ClassTag[T]): Seq[T] = vect.toArray.toSeq
  implicit def bdvToArray[T](vect: BDV[T])(implicit ct: ClassTag[T]): Array[T] = vect.toArray
//  implicit def arrayToSeq(arr: Array[Double]) = Predef.doubleArrayOps(arr)
  def add(v1: BDV[Double], v2: BDV[Double]) =
    v1.zip(v2).map { x => x._1 + x._2}

  def norm(darr: Array[Double]): Double = {
    Math.sqrt(darr.foldLeft(0.0) { case (sum, dval) => sum + Math.pow(dval, 2)})
  }

  def norm(darr: BDV[Double]): Double = {
    darr.norm(2)
  }

//
//
//  // Implicits to convert between Breeze DenseVector's and Arrays
//  implicit def arrToSeq[T](arr: Array[T]): Seq[T] = arr.toSeq
////  implicit def arrayToBDV(darr: Array[Double]): BDV[Double]
////    = Vectors.dense(darr).asInstanceOf[BDV[Double]]
////  implicit def bdvToArray[T](vect: BDV[T])(implicit ct: ClassTag[T]): Array[T] = vect.toArray
////  implicit def bdvToSeq[T](vect: BDV[T])(implicit ct: ClassTag[T]): Seq[T] = vect.toArray.toSeq
//
//  def add(v1: BDV[Double], v2: BDV[Double]) =
//    v1.zip(v2).map { x => x._1 + x._2}

  def mult(v1: BDV[Double], d: Double) = {
    v1 * d
  }

  def mult(v1: BDV[Double], v2: BDV[Double]) = {
    v1 * v2
  }

  def multColByRow(v1: BDV[Double], v2: BDV[Double]) = {
    val mat = v1 * v2.t
    mat
  }

  def norm(vect: BDV[Double]): Double = {
    vect.norm
  }

//  def norm(darr: Array[Double]): Double = {
//    Math.sqrt(darr.foldLeft(0.0) { case (sum, dval) => sum + Math.pow(dval, 2)})
//  }

  def manhattanNorm(vect: BDV[Double]): Double = {
    vect.norm(1)
  }

  def dot(v1: BDV[Double], v2: BDV[Double]) : Double = {
    v1.dot(v2)
  }

  def onesVector(len: Int): BDV[Double] = {
    BDV.ones(len)
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
    mat.t
  }

  def printMatrix(mat: BDM[Double]): String
  = printMatrix(mat, mat.rows, mat.cols)

  def printMatrix(mat: BDM[Double], numRows: Int, numCols: Int): String = {
    printMatrix(mat.toArray, numRows, numCols)
  }

  def printMatrix(vect: Array[Double], numRows: Int, numCols: Int): String = {
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

  def printVector(dvect: BDV[Double]) = {
    dvect.mkString(",")
  }

  def project(basisVector: BDV[Double], inputVect: BDV[Double]) = {
    val pnorm = makeNonZero(norm(basisVector))
    val projectedVect = basisVector.map(
      _ * dot(basisVector, inputVect) / dot(basisVector, basisVector))
    projectedVect
  }

  def subtract(v1: BDV[Double], v2: BDV[Double]) = {
    v1 - v2
  }

  def subtractProjection(vect: BDV[Double], basisVect: BDV[Double]): BDV[Double] = {
    val proj = project(basisVect, vect)
    val subVect = subtract(vect, proj)
    subVect
  }

  def localPIC(matIn: DMatrix, nClusters: Int, nIterations: Int,
               optExpected: Option[(BDV[Double], DMatrix)]) = {

    var mat = matIn.map(identity)
    val numVects = mat.cols

    val (expLambda, expdat) = optExpected.getOrElse((new BDV(Array(0.0)), new BDM(0,0)))
    var cnorm = -1.0
    for (k <- 0 until nClusters) {
      val r = new Random()
      var eigen = new BDV(Array.fill(numVects) {
        //          1.0
        r.nextDouble
      })
      val enorm = norm(eigen)
      eigen *= 1.0 / enorm

      for (iter <- 0 until nIterations) {
        eigen = mat * eigen
        cnorm = makeNonZero(norm(eigen))
        eigen = eigen.map(_ / cnorm)
      }
      val signum = Math.signum(dot(mat(0), eigen))
      val lambda = dot(mat(0), eigen) / eigen(0)
      eigen = eigen.map(_ * signum)
      println(s"lambda=$lambda eigen=${printVector(eigen)}")
      if (expLambda.toArray.length > 0) {
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
    m1.toArray.zip(m2.toArray).forall { case (m1v, m2v) =>
       withinTol(m1v - m2v)
    }
  }

  def subtract(mat1: DMatrix, mat2: DMatrix) = {
    mat1 - mat2
  }

  def deflate(mat: DMatrix, lambda: Double, eigen: BDV[Double]) = {
    //        mat = mat.map(subtractProjection(_, mult(eigen, lambda)))
    val eigT = eigen
    val projected = (eigen * eigen.t) * lambda
    //        println(s"projected matrix:\n${printMatrix(projected,
    //          eigen.length, eigen.length)}")
    val matOut = mat - projected
    println(s"Updated matrix:\n${
      printMatrix(mat,
        eigen.length, eigen.length)
    }")
    matOut
  }

  def mult(mat1: DMatrix, mat2: DMatrix) = {
    val outMat = mat1 :* mat2
    outMat
  }

  //    def mult(mat: DMatrix, vect: BDV[Double]): DMatrix  = {
  //      val outMat = mat.map { m =>
  //        mult(m, vect)
  //      }
  //      outMat
  //    }
  //
  //    def mult(vect: BDV[Double], mat: DMatrix): DMatrix = {
  //      for {d <- vect.zip(transpose(mat)) }
  //        yield mult(d._2, d._1)
  //    }

  def scale(mat: DMatrix, d: Double): DMatrix = {
    mat * d
  }

  def transpose(vector: BDV[Double]) = {
    vector.map { d => Array(d)}
  }

  def toMat(dvect: Array[Double], ncols: Int) = {
    val m = dvect.toSeq.grouped(ncols).map(_.toArray)
    m
  }

  def schurComplement(mat: DMatrix, lambda: Double, eigen: BDV[Double]) = {
    val eig = eigen
    val eigT = eigen.t
    val projected = eig * eigT
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
    val denom1 = eigT  *  mat
    val denom2 = denom1 * eigen
    val denom = denom2.toArray(0)
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

