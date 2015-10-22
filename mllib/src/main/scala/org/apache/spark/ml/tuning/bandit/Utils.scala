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

package org.apache.spark.ml.tuning.bandit

import scala.util.Random

import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}

object Utils {
  /**
   * Randomly choose one sample given a frequency histogram. The higher the frequency of one
   * element, the easier the element be chose.
   */
  def chooseOne(p: Vector): Int = {
    // TODO Optimize it to suitable for Sparse Vector too.
    val threshold = Random.nextDouble() * Utils.sum(p)
    var i = 0
    var sum = 0.0
    while ((sum < threshold) && (i < p.size)) {
      sum += p(i)
      i += 1
    }
    i - 1
  }

  def argMin(a: Vector): Int = {
    a match {
      case d: DenseVector =>
        d.values.zipWithIndex.minBy(_._1)._2
      case s: SparseVector => throw new UnsupportedOperationException
    }
  }

  def argSort(a: Vector): Array[Int] = {
    a match {
      case d: DenseVector => d.values.zipWithIndex.sortBy(_._1).map(_._2)
      case s: SparseVector => throw new UnsupportedOperationException
    }
  }

  def sum(a: Vector): Double = {
    a match {
      case d: DenseVector => d.toArray.sum
      case s: SparseVector => s.values.sum
    }
  }

  def sqrt(a: Vector): Unit = {
    a match {
      case d: DenseVector =>
        var i = 0
        while (i < a.size) {
          d.values(i) = math.sqrt(d(i))
          i += 1
        }
      case s: SparseVector => throw new UnsupportedOperationException
    }
  }

  def log(a: Vector): Unit = {
    a match {
      case d: DenseVector =>
        var i = 0
        while (i < a.size) {
          d.values(i) = math.log(d(i))
          i += 1
        }
      case s: SparseVector => throw new UnsupportedOperationException
    }
  }

  /**
   * y = y / x
   */
  def div(x: Vector, y: Vector): Unit = {
    y match {
      case dy: DenseVector =>
        var i = 0
        while (i < y.size) {
          dy.values(i) /= x(i)
          i += 1
        }
      case dy: SparseVector => throw new UnsupportedOperationException
    }
  }

  /**
   * y = y * x
   */
  def mul(x: Vector, y: Vector): Unit = {
    y match {
      case dy: DenseVector =>
        var i = 0
        while (i < y.size) {
          dy.values(i) *= x(i)
          i += 1
        }
      case dy: SparseVector => throw new UnsupportedOperationException
    }
  }

  /**
   * y = y - x
   */
  def sub(x: Vector, y: Vector): Unit = {
    y match {
      case dy: DenseVector =>
        var i = 0
        while (i < y.size) {
          dy.values(i) -= x(i)
          i += 1
        }
      case dy: SparseVector => throw new UnsupportedOperationException
    }
  }

  /**
   * Base 2 logarithm.
   */
  def log2(n: Double): Double = {
    if (n <= 0) throw new IllegalArgumentException()
    // TODO find more stable method?
    math.log(n) / math.log(2)
  }
}
