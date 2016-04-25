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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
// $example on$
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.QRDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
// $example off$

public class JavaRowMatrixExample {
  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("JavaRowMatrixExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    Vector v1 = Vectors.dense(1.0, 10.0, 100.0);
    Vector v2 = Vectors.dense(2.0, 20.0, 200.0);
    Vector v3 = Vectors.dense(3.0, 30.0, 300.0);

    // $example on$
    // a JavaRDD of local vectors
    JavaRDD<Vector> rows = jsc.parallelize(Arrays.asList(v1, v2, v3));

    // Create a RowMatrix from an JavaRDD<Vector>.
    RowMatrix mat = new RowMatrix(rows.rdd());

    // Get its size.
    long m = mat.numRows();
    long n = mat.numCols();

    // QR decomposition
    QRDecomposition<RowMatrix, Matrix> result = mat.tallSkinnyQR(true);
    // $example off$

    jsc.stop();
  }
}
