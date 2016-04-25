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
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
// $example on$
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
// $example off$

public class JavaBlockMatrixExample {
  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("JavaBlockMatrixExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    // $example on$
    MatrixEntry me1 = new MatrixEntry(0, 0, 1.2);
    MatrixEntry me2 = new MatrixEntry(1, 0, 2.1);
    MatrixEntry me3 = new MatrixEntry(6, 1, 3.7);

    // a JavaRDD of (i, j, v) Matrix Entries
    JavaRDD<MatrixEntry> entries = jsc.parallelize(Arrays.asList(me1, me2, me3));

    // Create a CoordinateMatrix from a JavaRDD<MatrixEntry>.
    CoordinateMatrix coordMat = new CoordinateMatrix(entries.rdd());
    // Transform the CoordinateMatrix to a BlockMatrix
    BlockMatrix matA = coordMat.toBlockMatrix().cache();

    // Validate whether the BlockMatrix is set up properly. Throws an Exception when it is not valid
    // Nothing happens if it is valid.
    matA.validate();

    // Calculate A^T A.
    BlockMatrix ata = matA.transpose().multiply(matA);
    // $example off$

    jsc.stop();

  }
}
