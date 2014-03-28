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

import org.apache.spark.rdd.RDD

import org.jblas.DoubleMatrix

/**
 * Efficient matrix operations.
 * For example square a given matrix efficiently
 */
object MatrixAlgebra {

 def columnMagnitudes(matrix: RDD[Array[Double]]):
  Array[Double] = {
   val n = matrix.first.size
    matrix.map {
      x =>
        val a = new DoubleMatrix(x)
        a.mul(a).data
    }.fold(Array.ofDim[Double](n)) {
      (a, b) =>
        val am = new DoubleMatrix(a)
        val bm = new DoubleMatrix(b)
        am.addi(bm)
        a
    }
  }


  /**
   * TODO: javadoc Square a given matrix efficienty
   */
  def squareWithDIMSUM(matrix: RDD[Array[Double]], colMags: Array[Double], gamma: Double):
  Array[Array[Double]] = {
    val n = matrix.first.size

    if (gamma < 1) {
      throw new IllegalArgumentException("Oversampling should be greater than 1: $gamma")
    }

    // Compute A^T A
    val fullata = matrix.mapPartitions {
      iter =>
        val localATA = Array.ofDim[Double](n, n)
        while (iter.hasNext) {
          val row = iter.next()
          var i = 0
          while (i < n) {
            if(Math.random < gamma / colMags(i)) {
              var j = i + 1
              while (j < n) {
                val mult = row(i) * row(j)
                localATA(i)(j) += mult
                localATA(j)(i) += mult
                j += 1
              }
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

    // undo normalization
    for(i <- 0 until n) for(j <- i until n) {
      fullata(i)(j) = if (i == j) colMags(i)
                      else if (gamma / colMags(i) > 1) fullata(i)(j)
                      else fullata(i)(j) * colMags(i) / gamma
      fullata(j)(i) = fullata(i)(j)
    }

    fullata
  }


  /**
   * TODO: javadoc Square a tall skinny A^TA given matrix efficienty
   */
  def square(matrix: RDD[Array[Double]]): Array[Array[Double]] = {
    val n = matrix.first.size

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

