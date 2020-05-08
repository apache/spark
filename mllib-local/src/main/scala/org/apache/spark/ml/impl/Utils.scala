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

package org.apache.spark.ml.impl


private[spark] object Utils {

  lazy val EPSILON = {
    var eps = 1.0
    while ((1.0 + (eps / 2.0)) != 1.0) {
      eps /= 2.0
    }
    eps
  }

  /**
   * Convert an n * (n + 1) / 2 dimension array representing the upper triangular part of a matrix
   * into an n * n array representing the full symmetric matrix (column major).
   *
   * @param n The order of the n by n matrix.
   * @param triangularValues The upper triangular part of the matrix packed in an array
   *                         (column major).
   * @return A dense matrix which represents the symmetric matrix in column major.
   */
  def unpackUpperTriangular(
      n: Int,
      triangularValues: Array[Double]): Array[Double] = {
    val symmetricValues = new Array[Double](n * n)
    var r = 0
    var i = 0
    while (i < n) {
      var j = 0
      while (j <= i) {
        symmetricValues(i * n + j) = triangularValues(r)
        symmetricValues(j * n + i) = triangularValues(r)
        r += 1
        j += 1
      }
      i += 1
    }
    symmetricValues
  }

  /**
   * Sequentially group input elements to groups, and do aggregation within each group.
   * A group only contains single key, and be of size no greater than the corresponding size.
   * For example, input keys = [1, 1, 1, 2, 2, 2, 3, 3, 1],
   * group sizes are: 1->2, 2->5, 3->1,
   * then the groups are {1, 1}, {1}, {2, 2, 2}, {3}, {3}, {1}.
   *
   * @param input input iterator containing (key, value), usually sorted by key
   * @param getSize group size of each key.
   * @return aggregated iterator
   */
  def combineWithinGroups[K, V, U](
      input: Iterator[(K, V)],
      initOp: V => U,
      seqOp: (U, V) => U,
      getSize: K => Long): Iterator[(K, U)] = {
    if (input.isEmpty) return Iterator.empty

    // null.asInstanceOf[K] won't work with K=Int/Long/...
    var prevK = Option.empty[K]
    var prevU = null.asInstanceOf[U]
    var groupSize = -1L
    var groupCount = 0L

    input.flatMap { case (key, value) =>
      if (!prevK.contains(key) || groupCount == groupSize) {
        val ret = prevK.map(k => (k, prevU))

        prevK = Some(key)
        prevU = initOp(value)
        groupSize = getSize(key)
        groupCount = 1L

        ret.iterator
      } else {
        prevU = seqOp(prevU, value)
        groupCount += 1L
        Iterator.empty
      }
    } ++ prevK.iterator.map(k => (k, prevU))
  }
}
