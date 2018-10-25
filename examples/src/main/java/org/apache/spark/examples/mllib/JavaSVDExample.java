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

// $example on$
import java.util.Arrays;
import java.util.List;
// $example off$

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
// $example on$
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
// $example off$

/**
 * Example for SingularValueDecomposition.
 */
public class JavaSVDExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("SVD Example");
    SparkContext sc = new SparkContext(conf);
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

    // $example on$
    List<Vector> data = Arrays.asList(
            Vectors.sparse(5, new int[] {1, 3}, new double[] {1.0, 7.0}),
            Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
            Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    );

    JavaRDD<Vector> rows = jsc.parallelize(data);

    // Create a RowMatrix from JavaRDD<Vector>.
    RowMatrix mat = new RowMatrix(rows.rdd());

    // Compute the top 5 singular values and corresponding singular vectors.
    SingularValueDecomposition<RowMatrix, Matrix> svd = mat.computeSVD(5, true, 1.0E-9d);
    RowMatrix U = svd.U();  // The U factor is a RowMatrix.
    Vector s = svd.s();     // The singular values are stored in a local dense vector.
    Matrix V = svd.V();     // The V factor is a local dense matrix.
    // $example off$
    Vector[] collectPartitions = (Vector[]) U.rows().collect();
    System.out.println("U factor is:");
    for (Vector vector : collectPartitions) {
      System.out.println("\t" + vector);
    }
    System.out.println("Singular values are: " + s);
    System.out.println("V factor is:\n" + V);

    jsc.stop();
  }
}
