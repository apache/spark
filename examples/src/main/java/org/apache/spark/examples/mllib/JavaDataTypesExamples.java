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

package org.apache.spark.examples.mllib;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

// $example on:local_vector$
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
// $example off:local_vector$
// $example on:labeled_point$
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
// $example off:labeled_point$
// $example on:libsvm$
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.api.java.JavaRDD;
// $example off:libsvm$
// $example on:local_matrix$
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Matrices;
// $example off:local_matrix$
// $example on:row_matrix$
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.linalg.QRDecomposition;
// $example off:row_matrix$
// $example on:indexed_row_matrix$
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
// $example off:indexed_row_matrix$
// $example on:coordinate_matrix$
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
// $example off:coordinate_matrix$
// $example on:block_matrix$
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
// $example off:block_matrix$


public class JavaDataTypesExamples {

  private static void localVectorExample() {
    // $example on:local_vector$
    // Create a dense vector (1.0, 0.0, 3.0).
    Vector dv = Vectors.dense(1.0, 0.0, 3.0);
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to
    // nonzero entries.
    Vector sv = Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0});
    // $example off:local_vector$
  }

  private static void labeledPointExample() {
    // $example on:labeled_point$
    // Create a labeled point with a positive label and a dense feature vector.
    LabeledPoint pos = new LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0));

    // Create a labeled point with a negative label and a sparse feature vector.
    LabeledPoint neg =
      new LabeledPoint(0.0, Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0}));
    // $example off:labeled_point$
  }

  private static void libsvmExample() {
    // $example on:libsvm$
    SparkContext sc = SparkContext.getOrCreate();
    JavaRDD<LabeledPoint> examples =
      MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").toJavaRDD();
    // $example off:libsvm$
  }

  private static void localMatrixExample() {
    // $example on:local_matrix$
    // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
    Matrix dm = Matrices.dense(3, 2, new double[] {1.0, 3.0, 5.0, 2.0, 4.0, 6.0});

    // Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
    Matrix sm =
      Matrices.sparse(3, 2, new int[] {0, 1, 3}, new int[] {0, 2, 1}, new double[] {9, 6, 8});
    // $example off:local_matrix$
  }

  private static void rowMatrixExample() {
    SparkContext sc = SparkContext.getOrCreate();
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

    // $example on:row_matrix$
    Vector v1 = Vectors.dense(1.0, 10.0, 100.0);
    Vector v2 = Vectors.dense(2.0, 20.0, 200.0);
    Vector v3 = Vectors.dense(3.0, 30.0, 300.0);

    // a JavaRDD of local vectors
    JavaRDD<Vector> rows = jsc.parallelize(Arrays.asList(v1, v2, v3));

    // Create a RowMatrix from an JavaRDD<Vector>.
    RowMatrix mat = new RowMatrix(rows.rdd());

    // Get its size.
    long m = mat.numRows();
    long n = mat.numCols();

    // QR decomposition
    QRDecomposition<RowMatrix, Matrix> result = mat.tallSkinnyQR(true);
    // $example off:row_matrix$
  }

  private static void indexedRowMatrixExample() {
    SparkContext sc = SparkContext.getOrCreate();
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

    // $example on:indexed_row_matrix$
    IndexedRow r0 = new IndexedRow(0, Vectors.dense(1, 2, 3));
    IndexedRow r1 = new IndexedRow(1, Vectors.dense(4, 5, 6));
    IndexedRow r2 = new IndexedRow(2, Vectors.dense(7, 8, 9));
    IndexedRow r3 = new IndexedRow(3, Vectors.dense(10, 11, 12));

    // a JavaRDD of indexed rows
    JavaRDD<IndexedRow> rows = jsc.parallelize(Arrays.asList(r0, r1, r2, r3));

    // Create an IndexedRowMatrix from a JavaRDD<IndexedRow>.
    IndexedRowMatrix mat = new IndexedRowMatrix(rows.rdd());

    // Get its size.
    long m = mat.numRows();
    long n = mat.numCols();

    // Drop its row indices.
    RowMatrix rowMat = mat.toRowMatrix();
    // $example off:indexed_row_matrix$
  }

  private static void coordinateMatrixExample() {
    SparkContext sc = SparkContext.getOrCreate();
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

    // $example on:coordinate_matrix$
    MatrixEntry me1 = new MatrixEntry(0, 0, 1.2);
    MatrixEntry me2 = new MatrixEntry(1, 0, 2.1);
    MatrixEntry me3 = new MatrixEntry(6, 1, 3.7);

    // a JavaRDD of matrix entries
    JavaRDD<MatrixEntry> entries = jsc.parallelize(Arrays.asList(me1, me2, me3));
    // Create a CoordinateMatrix from a JavaRDD<MatrixEntry>.
    CoordinateMatrix mat = new CoordinateMatrix(entries.rdd());

    // Get its size.
    long m = mat.numRows();
    long n = mat.numCols();

    // Convert it to an IndexRowMatrix whose rows are sparse vectors.
    IndexedRowMatrix indexedRowMatrix = mat.toIndexedRowMatrix();
    // $example off:coordinate_matrix$
  }

  private static void blockMatrixExample() {
    SparkContext sc = SparkContext.getOrCreate();
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

    // $example on:block_matrix$
    MatrixEntry me1 = new MatrixEntry(0, 0, 1.2);
    MatrixEntry me2 = new MatrixEntry(1, 0, 2.1);
    MatrixEntry me3 = new MatrixEntry(6, 1, 3.7);

    // A JavaRDD of (i, j, v) Matrix Entries
    JavaRDD<MatrixEntry> entries = jsc.parallelize(Arrays.asList(me1, me2, me3));

    // Create a CoordinateMatrix from a JavaRDD<MatrixEntry>.
    CoordinateMatrix coordMat = new CoordinateMatrix(entries.rdd());
    // Transform the CoordinateMatrix to a BlockMatrix
    BlockMatrix matA = coordMat.toBlockMatrix().cache();

    // Validate whether the BlockMatrix is set up properly.
    // Throws an Exception when it is not valid. Nothing happens if it is valid.
    matA.validate();

    // Calculate A^T A.
    BlockMatrix ata = matA.transpose().multiply(matA);
    // $example off:block_matrix$
  }

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("JavaDataTypesExample");
    SparkContext sc = new SparkContext(conf);

    localVectorExample();
    labeledPointExample();
    libsvmExample();
    localMatrixExample();
    rowMatrixExample();
    indexedRowMatrixExample();
    coordinateMatrixExample();
    blockMatrixExample();

    sc.stop();
 }
}
