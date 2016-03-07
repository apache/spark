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
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
// $example off$

public class JavaCoordinateMatrixExample {
  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("JavaCoordinateMatrixExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    // $example on$
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
    // $example off$

    jsc.stop();
  }
}
