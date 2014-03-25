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

package org.apache.spark.mllib.linalg

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.jblas.{DoubleMatrix, Singular, MatrixFunctions}

/**
 * Square a given matrix efficienty
 */
object MatrixSquare {
/**
 * TODO: javadoc Square a given matrix efficienty
 */
  def squareWithDIMSUM(matrix: RDD[Array[Double]],
                       colMags: Array[Double], double: gamma):
   Array[Array[Double]] = {
    val n = matrix.first.size

    if (k < 1 || k > n) {
      throw new IllegalArgumentException(
        "Request up to n singular values k=$k n=$n")
    }

    // Compute A^T A
    val fullata = matrix.mapPartitions {
      iter =>
        val localATA = Array.ofDim[Double](n, n)
        while (iter.hasNext) {
          val row = iter.next()
          var i = 0
          while (i < n) {
            var j = 0
            while (j < n) {
              localATA(i)(j) += row(i) * row(j)
              j += 1
            }
            i += 1
          }
        }
        Iterator(localATA)
    }.fold(Array.ofDim[Double](n, n)) {
      (a, b) =>
        var i = 0
        while (i < n) {
          var j = 0
          while (j < n) {
            a(i)(j) += b(i)(j)
            j += 1
          }
          i += 1
        }
        a
    }
    fullata
  }


/**
 * TODO: javadoc Square a tall skinny A^TA given matrix efficienty
 */
  def square(matrix: RDD[Array[Double]]) : Array[Array[Double]] = {
    val n = matrix.first.size

    if (k < 1 || k > n) {
      throw new IllegalArgumentException(
        "Request up to n singular values k=$k n=$n")
    }

    // Compute A^T A
    val fullata = matrix.mapPartitions {
      iter =>
        val localATA = Array.ofDim[Double](n, n)
        while (iter.hasNext) {
          val row = iter.next()
          var i = 0
          while (i < n) {
            var j = 0
            while (j < n) {
              localATA(i)(j) += row(i) * row(j)
              j += 1
            }
            i += 1
          }
        }
        Iterator(localATA)
    }.fold(Array.ofDim[Double](n, n)) {
      (a, b) =>
        var i = 0
        while (i < n) {
          var j = 0
          while (j < n) {
            a(i)(j) += b(i)(j)
            j += 1
          }
          i += 1
        }
        a
    }
    fullata
  }
}

