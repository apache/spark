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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, CoordinateMatrix, RowMatrix}

/**
 * Compute the similar columns of a matrix, using cosine similarity.
 */
object CosineSimilarity {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CosineSimilarity")
    val sc = new SparkContext(conf)

    // Number of rows
    val M = 1000
    // Number of columns
    val U = 1000
    // Number of nonzeros per row
    val NNZ = 10
    // Number of partitions for data
    val NUMCHUNKS = 4

    // Create data
    val R = sc.parallelize(0 until M, NUMCHUNKS).flatMap{i =>
      val inds = new scala.collection.mutable.TreeSet[Int]()
      while (inds.size < NNZ) {
        inds += scala.util.Random.nextInt(U)
      }
      inds.toArray.map(j => MatrixEntry(i, j, scala.math.random))
    }

    val mat = new CoordinateMatrix(R, M, U).toRowMatrix()

    // Compute similar columns perfectly, with brute force.
    val simsPerfect = mat.columnSimilarities()

    println("Pairwise similarities are: " + simsPerfect.entries.collect.mkString(", "))

    // Compute similar columns with estimation focusing on pairs more similar than 0.8
    val simsEstimate = mat.columnSimilarities(0.8)

    println("Estimated pairwise similarities are: " + simsEstimate.entries.collect.mkString(", "))

    sc.stop()
  }
}
