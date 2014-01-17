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

package org.apache.spark.mllib.linalg

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.jblas.{DoubleMatrix, Singular, MatrixFunctions}


/**
 * Class used to obtain singular value decompositions
 */
class SVD {
  private var k: Int = 1

  /**
   * Set the number of top-k singular vectors to return
   */
  def setK(k: Int): SVD = {
    this.k = k
    this
  }

   /**
   * Compute SVD using the current set parameters
   */
  def compute(matrix: SparseMatrix) : MatrixSVD = {
    SVD.sparseSVD(matrix, k)
  }
}


/**
 * Top-level methods for calling Singular Value Decomposition
 * NOTE: All matrices are in 0-indexed sparse format RDD[((int, int), value)]
 */
object SVD {
/**
 * Singular Value Decomposition for Tall and Skinny matrices.
 * Given an m x n matrix A, this will compute matrices U, S, V such that
 * A = U * S * V'
 * 
 * There is no restriction on m, but we require n^2 doubles to fit in memory.
 * Further, n should be less than m.
 * 
 * The decomposition is computed by first computing A'A = V S^2 V',
 * computing svd locally on that (since n x n is small),
 * from which we recover S and V. 
 * Then we compute U via easy matrix multiplication
 * as U =  A * V * S^-1
 * 
 * Only the k largest singular values and associated vectors are found.
 * If there are k such values, then the dimensions of the return will be:
 *
 * S is k x k and diagonal, holding the singular values on diagonal
 * U is m x k and satisfies U'U = eye(k)
 * V is n x k and satisfies V'V = eye(k)
 *
 * All input and output is expected in sparse matrix format, 0-indexed
 * as tuples of the form ((i,j),value) all in RDDs using the
 * SparseMatrix class
 *
 * @param matrix sparse matrix to factorize
 * @param k Recover k singular values and vectors
 * @return Three sparse matrices: U, S, V such that A = USV^T
 */
  def sparseSVD(
      matrix: SparseMatrix,
      k: Int)
    : MatrixSVD =
  {
    val data = matrix.data
    val m = matrix.m
    val n = matrix.n

    if (m < n || m <= 0 || n <= 0) {
      throw new IllegalArgumentException("Expecting a tall and skinny matrix")
    }

    if (k < 1 || k > n) {
      throw new IllegalArgumentException("Must request up to n singular values")
    }

    // Compute A^T A, assuming rows are sparse enough to fit in memory
    val rows = data.map(entry =>
            (entry.i, (entry.j, entry.mval))).groupByKey()
    val emits = rows.flatMap{ case (rowind, cols)  =>
      cols.flatMap{ case (colind1, mval1) =>
                    cols.map{ case (colind2, mval2) =>
                            ((colind1, colind2), mval1*mval2) } }
    }.reduceByKey(_+_)

    // Construct jblas A^T A locally
    val ata = DoubleMatrix.zeros(n, n)
    for (entry <- emits.toArray) {
      ata.put(entry._1._1, entry._1._2, entry._2)
    }

    // Since A^T A is small, we can compute its SVD directly
    val svd = Singular.sparseSVD(ata)
    val V = svd(0)
    val sigmas = MatrixFunctions.sqrt(svd(1)).toArray.filter(x => x > 1e-9)

    if (sigmas.size < k) {
      throw new Exception("Not enough singular values to return")
    } 

    val sigma = sigmas.take(k)

    val sc = data.sparkContext

    // prepare V for returning
    val retVdata = sc.makeRDD(
            Array.tabulate(V.rows, sigma.length){ (i,j) =>
                    MatrixEntry(i, j, V.get(i,j)) }.flatten)
    val retV = SparseMatrix(retVdata, V.rows, sigma.length)
     
    val retSdata = sc.makeRDD(Array.tabulate(sigma.length){
      x => MatrixEntry(x, x, sigma(x))})

    val retS = SparseMatrix(retSdata, sigma.length, sigma.length)

    // Compute U as U = A V S^-1
    // turn V S^-1 into an RDD as a sparse matrix
    val vsirdd = sc.makeRDD(Array.tabulate(V.rows, sigma.length)
                { (i,j) => ((i, j), V.get(i,j) / sigma(j))  }.flatten)

    // Multiply A by VS^-1
    val aCols = data.map(entry => (entry.j, (entry.i, entry.mval)))
    val bRows = vsirdd.map(entry => (entry._1._1, (entry._1._2, entry._2)))
    val retUdata = aCols.join(bRows).map( {case (key, ( (rowInd, rowVal), (colInd, colVal)) )
        => ((rowInd, colInd), rowVal*colVal)}).reduceByKey(_+_)
          .map{ case ((row, col), mval) => MatrixEntry(row, col, mval)}
    val retU = SparseMatrix(retUdata, m, sigma.length) 
   
    MatrixSVD(retU, retS, retV)  
  }


  def main(args: Array[String]) {
    if (args.length < 8) {
      println("Usage: SVD <master> <matrix_file> <m> <n> " +
              "<k> <output_U_file> <output_S_file> <output_V_file>")
      System.exit(1)
    }

    val (master, inputFile, m, n, k, output_u, output_s, output_v) = 
      (args(0), args(1), args(2).toInt, args(3).toInt,
      args(4).toInt, args(5), args(6), args(7))
    
    val sc = new SparkContext(master, "SVD")
    
    val rawdata = sc.textFile(inputFile)
    val data = rawdata.map { line =>
      val parts = line.split(',')
      MatrixEntry(parts(0).toInt, parts(1).toInt, parts(2).toDouble)
    }

    val decomposed = SVD.sparseSVD(SparseMatrix(data, m, n), k)
    val u = decomposed.U.data
    val s = decomposed.S.data
    val v = decomposed.V.data
    
    println("Computed " + s.toArray.length + " singular values and vectors")
    u.saveAsTextFile(output_u)
    s.saveAsTextFile(output_s)
    v.saveAsTextFile(output_v)
    System.exit(0)
  }
}


