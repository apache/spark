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

// scalastyle:off println
package org.apache.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}


object DataTypesExamples {

  private def localVectorExample(): Unit = {
    // $example on:local_vector$
    import org.apache.spark.mllib.linalg.{Vector, Vectors}

    // Create a dense vector (1.0, 0.0, 3.0).
    val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to
    // nonzero entries.
    val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its nonzero entries.
    val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
    // $example off:local_vector$
  }

  private def labeledPointExample(): Unit = {
    // $example on:labeled_point$
    import org.apache.spark.mllib.linalg.Vectors
    import org.apache.spark.mllib.regression.LabeledPoint

    // Create a labeled point with a positive label and a dense feature vector.
    val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))

    // Create a labeled point with a negative label and a sparse feature vector.
    val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))
    // $example off:labeled_point$
  }

  private def libsvmExample(): Unit = {
    val sc = SparkContext.getOrCreate()
    // $example on:libsvm$
    import org.apache.spark.mllib.regression.LabeledPoint
    import org.apache.spark.mllib.util.MLUtils
    import org.apache.spark.rdd.RDD

    val examples: RDD[LabeledPoint] =
      MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
    // $example off:libsvm$
  }

  private def localMatrixExample(): Unit = {
    // $example on:local_matrix$
    import org.apache.spark.mllib.linalg.{Matrix, Matrices}

    // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))

    // Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
    val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
    // $example off:local_matrix$
  }

  private def rowMatrixExample(): Unit = {
    val sc = SparkContext.getOrCreate()
    // $example on:row_matrix$
    import org.apache.spark.mllib.linalg.{Vector, Vectors}
    import org.apache.spark.mllib.linalg.distributed.RowMatrix
    import org.apache.spark.rdd.RDD

    val v1 = Vectors.dense(1.0, 10.0, 100.0)
    val v2 = Vectors.dense(2.0, 20.0, 200.0)
    val v3 = Vectors.dense(3.0, 30.0, 300.0)

    val rows: RDD[Vector] = sc.parallelize(Seq(v1, v2, v3)) // an RDD of local vectors
    // Create a RowMatrix from an RDD[Vector].
    val mat: RowMatrix = new RowMatrix(rows)

    // Get its size.
    val m = mat.numRows()
    val n = mat.numCols()

    // QR decomposition
    val qrResult = mat.tallSkinnyQR(true)
    // $example off:row_matrix$
  }

  private def indexedRowMatrixExample(): Unit = {
    val sc = SparkContext.getOrCreate()

    // $example on:indexed_row_matrix$
    import org.apache.spark.mllib.linalg.Vectors
    import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
    import org.apache.spark.rdd.RDD

    val r0 = IndexedRow(0, Vectors.dense(1, 2, 3))
    val r1 = IndexedRow(1, Vectors.dense(4, 5, 6))
    val r2 = IndexedRow(2, Vectors.dense(7, 8, 9))
    val r3 = IndexedRow(3, Vectors.dense(10, 11, 12))

    val rows: RDD[IndexedRow] = sc.parallelize(Seq(r0, r1, r2, r3)) // an RDD of indexed rows
    // Create an IndexedRowMatrix from an RDD[IndexedRow].
    val mat: IndexedRowMatrix = new IndexedRowMatrix(rows)

    // Get its size.
    val m = mat.numRows()
    val n = mat.numCols()

    // Drop its row indices.
    val rowMat: RowMatrix = mat.toRowMatrix()
    // $example off:indexed_row_matrix$
  }

  private def coordinateMatrixExample(): Unit = {
    val sc = SparkContext.getOrCreate()

    // $example on:coordinate_matrix$
    import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
    import org.apache.spark.rdd.RDD

    val me1 = MatrixEntry(0, 0, 1.2)
    val me2 = MatrixEntry(1, 0, 2.1)
    val me3 = MatrixEntry(6, 1, 3.7)

    val entries: RDD[MatrixEntry] = sc.parallelize(Seq(me1, me2, me3)) // an RDD of matrix entries
    // Create a CoordinateMatrix from an RDD[MatrixEntry].
    val mat: CoordinateMatrix = new CoordinateMatrix(entries)

    // Get its size.
    val m = mat.numRows()
    val n = mat.numCols()

    // Convert it to an IndexRowMatrix whose rows are sparse vectors.
    val indexedRowMatrix = mat.toIndexedRowMatrix()
    // $example off:coordinate_matrix$
  }

  private def blockMatrixExample(): Unit = {
    val sc = SparkContext.getOrCreate()

    // $example on:block_matrix$
    import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry}
    import org.apache.spark.rdd.RDD

    val me1 = MatrixEntry(0, 0, 1.2)
    val me2 = MatrixEntry(1, 0, 2.1)
    val me3 = MatrixEntry(6, 1, 3.7)

    // an RDD of (i, j, v) matrix entries
    val entries: RDD[MatrixEntry] = sc.parallelize(Seq(me1, me2, me3))
    // Create a CoordinateMatrix from an RDD[MatrixEntry].
    val coordMat: CoordinateMatrix = new CoordinateMatrix(entries)
    // Transform the CoordinateMatrix to a BlockMatrix
    val matA: BlockMatrix = coordMat.toBlockMatrix().cache()

    // Validate whether the BlockMatrix is set up properly.
    // Throws an Exception when it is not valid.
    // Nothing happens if it is valid.
    matA.validate()

    // Calculate A^T A.
    val ata = matA.transpose.multiply(matA)
    // $example off:block_matrix$
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataTypesExamples")
    val sc = new SparkContext(conf)

    localVectorExample()
    labeledPointExample()
    libsvmExample()
    localMatrixExample()
    rowMatrixExample()
    indexedRowMatrixExample()
    coordinateMatrixExample()
    blockMatrixExample()

    sc.stop()
  }
}
// scalastyle:on println
