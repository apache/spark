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

package org.apache.spark.examples.mllib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, CoordinateMatrix, RowMatrix}

/**
 * Compute the similar columns of a matrix, using cosine similarity.
 *
 * The input matrix must be stored in row-oriented dense format, one line per row with its entries
 * separated by space. For example,
 * {{{
 * 0.5 1.0
 * 2.0 3.0
 * 4.0 5.0
 * }}}
 * represents a 3-by-2 matrix, whose first row is (0.5, 1.0).
 *
 * Example invocation:
 *
 * bin/run-example org.apache.spark.examples.mllib.CosineSimilarity \
 * data/mllib/sample_svm_data.txt 0.1
 */
object CosineSimilarity {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: CosineSimilarity <input> <threshold>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("CosineSimilarity")
    val sc = new SparkContext(conf)

    // Load and parse the data file.
    val rows = sc.textFile(args(0)).map { line =>
      val values = line.split(' ').map(_.toDouble)
      Vectors.dense(values)
    }
    val mat = new RowMatrix(rows)

    val threshold = args(1).toDouble

    // Compute similar columns perfectly, with brute force.
    val simsPerfect = mat.columnSimilarities().entries.collect

    // Compute similar columns with estimation focusing on pairs more similar than threshold
    val simsEstimate = mat.columnSimilarities(threshold).entries.collect

    val n = mat.numCols().toInt
    val real = Array.ofDim[Double](n, n)
    val est = Array.ofDim[Double](n, n)
    for (entry <- simsPerfect) {
      real(entry.i.toInt)(entry.j.toInt) = entry.value
    }
    for (entry <- simsEstimate) {
      est(entry.i.toInt)(entry.j.toInt) = entry.value
    }

    val errors = Array.tabulate[Double](n, n)((i, j) => math.abs(real(i)(j) - est(i)(j)))
    val avgErr = errors.flatten.sum / (n * (n - 1) / 2)

    println(s"Average error in estimate is: $avgErr")

    sc.stop()
  }
}
