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


package org.apache.spark.mllib.feature

import breeze.linalg.{ DenseVector => BDV, SparseVector => BSV, Vector => BV }

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD


/**
 * A feature transformer that projects vectors to a low-dimensional space using IPCA.
 *
 * @param k number of principal components
 * @param b batch size in which the input will be divided
 */
class IPCA(val k: Int, val b: Int) extends Serializable {
  require(k > 0,
    s"Number of principal components must be positive but got ${k}")
  require(b >= k,
    s"Batch size must be greater than or equal to the number principal components but got ${b}")

  /**
   * Converts a Vector to Array.
   */
  private[this] def toBreeze(v: Vector) = BV(v.toArray)

  /**
   * Converts an Array to a DenseVector.
   */
  private[this] def fromBreeze(bv: BV[Double]) = Vectors.dense(bv.toArray)

  /**
   * Utility to add two Vector.
   */
  private[this] def add(v1: Vector, v2: Vector) = fromBreeze(toBreeze(v1) + toBreeze(v2))

  /**
   * Utility to subtract two Vector.
   */
  private[this] def subtract(v1: Vector, v2: Vector) = fromBreeze(toBreeze(v1) - toBreeze(v2))

  /**
   * Utility to multiply a Vector with a Scalar value.
   */
  private[this] def scalarMultiply(a: Double, v: Vector) = fromBreeze(a * toBreeze(v))

  /**
   * Java-friendly version of `fit()`.
   */
  def fit(sources: JavaRDD[Vector]): PCAModel = fit(sources.rdd)

  /**
   * Computes a [[PCAModel]] that contains the principal components
   * of the input vectors using incremental approach.
   * @param sources source vectors
   */
  def fit(sources: RDD[Vector]): PCAModel = {
    val numFeatures = sources.first().size
    require(k <= numFeatures, s"source vector size $numFeatures must be no less than k=$k")
    var mat = new RowMatrix(sources)
    var principalComponent = mat;
    val numPartition = (mat.numRows() / b).toInt
    var newMat = mat.rows.repartition(numPartition)
    val context = newMat.context
    var densePC = DenseMatrix zeros (1, 1)
    var explainedVariance = Vectors.dense(0.0)
    var i = 0
    var mean = Vectors.dense(0.0)
    var singularValues = Vectors.dense(0.0)
    var numSampleSeen = 0
    while (i < numPartition) {
      var row = newMat.glom().collect()(i)
      i = i + 1
      var rows = context.parallelize(row)
      val mat1: RowMatrix = new RowMatrix(rows)
      val summary = mat1.computeColumnSummaryStatistics()
      var col_batch_mean = summary.mean
      var col_mean = col_batch_mean
      var size = rows.count()

      if (numSampleSeen == 0) {
        rows = rows.map { x => { subtract(x, col_mean) } }
      }
      else if (numSampleSeen > 0) {
        col_mean = add(scalarMultiply(numSampleSeen, mean), scalarMultiply(size, col_batch_mean))
        col_mean = scalarMultiply(((1.0) / (numSampleSeen + size)).toInt, col_mean)
        rows = rows.map { x => { subtract(x, col_mean) } }
        val a = Math.sqrt((numSampleSeen * size) / (numSampleSeen + size))
        var mean_correction = scalarMultiply(a, subtract(mean, col_mean))
        var j = -1
        var append = principalComponent.rows.map { x => {
          j = j + 1
          scalarMultiply(singularValues.apply(j), x)}
        }
        var mean_correction1 = context.parallelize(Array(mean_correction))
        rows = rows.union(append).union(mean_correction1)
      }

      mat = new RowMatrix(rows)
      // Compute the top k singular values and the corresponding prinicpal components
      val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(k.toInt)
      val s: Vector = svd.s
      val V: Matrix = svd.V.transpose
      principalComponent = new RowMatrix(matrixToRDD(V, context))
      val (pc, explainedVariance1) = mat.computePrincipalComponentsAndExplainedVariance(k)
      mean = col_mean
      singularValues = s
      numSampleSeen = numSampleSeen + size.toInt
      if (i == numPartition) {
        densePC = pc match {
          case dm: DenseMatrix =>
            dm
          case sm: SparseMatrix =>
            /* Convert a sparse matrix to dense.
             *
             * RowMatrix.computePrincipalComponents always returns a dense matrix.
             * The following code is a safeguard.
             */
            sm.toDense
          case m =>
            throw new IllegalArgumentException("Unsupported matrix format. Expected " +
              s"SparseMatrix or DenseMatrix. Instead got: ${m.getClass}")
        }
          explainedVariance = explainedVariance1 match {
          case dv: DenseVector =>
            dv
          case sv: SparseVector =>
            sv.toDense
        }
      }
    }
    new PCAModel(k, densePC, explainedVariance.toDense)
  }


  /**
   * Utility to Convert a Matrix to RDD.
   */
  private[this] def matrixToRDD(m: Matrix, sc: SparkContext): RDD[Vector] = {
    val columns = m.toArray.grouped(m.numRows)
    val rows = columns.toSeq.transpose
    val vectors = rows.map(row => new DenseVector(row.toArray))
    sc.parallelize(vectors)
  }
}

