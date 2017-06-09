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
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
// $example off$

/**
 * Example for compute principal components on a 'RowMatrix'.
 */
public class JavaPCAExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("PCA Example");
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

    // Compute the top 4 principal components.
    // Principal components are stored in a local dense matrix.
    Matrix pc = mat.computePrincipalComponents(4);

    // Project the rows to the linear space spanned by the top 4 principal components.
    RowMatrix projected = mat.multiply(pc);
    // $example off$
    Vector[] collectPartitions = (Vector[])projected.rows().collect();
    System.out.println("Projected vector of principal component:");
    for (Vector vector : collectPartitions) {
      System.out.println("\t" + vector);
    }
    jsc.stop();
  }
}
