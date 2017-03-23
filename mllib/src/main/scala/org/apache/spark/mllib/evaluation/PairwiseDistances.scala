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

package org.apache.spark.mllib.evaluation

import breeze.linalg.functions.euclideanDistance
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, BlockMatrix}
import org.apache.spark.mllib.util.MLUtils

/**
 * Compute the pairwise distances
 */
object PairwiseDistances {

  /**
   * calculate the eucliden distnaces
   * @param a
   * @param b
   * @return
   */
  def euclidean(a: Vector, b: Vector ): Double = {
    val res = a.toArray.zip(b.toArray).foldLeft(0.0)( (a: Double, b: (Double, Double)) => {
      a + Math.pow(b._1 - b._2, 2)
    })
    Math.sqrt(res)
  }

  /**
   * calculates pairwise distances for every a,b in 'matrix' where b>a
   * while a and b are the row indices
   *
   * @param matrix
   * @return
   */
  def calculatePairwiseDistances(matrix: BlockMatrix): Array[DistanceWithKey] = {
    val rows = matrix.toIndexedRowMatrix().rows.collect()
    val sortedRows = rows.sortBy(_.index)
    val pairwiseDistances = sortedRows.map { rowA =>
      val distances = rows.filter(_.index > rowA.index).sortBy(_.index).map { rowB =>
        computeDistance(rowA, rowB)
      }
      distances
    }
    pairwiseDistances.flatten.sortBy(_.key)
  }

  /**
   * compute the distance and provide a key for each comparison
   * to be able to sort the output if required
   * @param key
   * @param similarity
   */
  case class DistanceWithKey(key: Int, similarity: Double)
  def computeDistance(rowA: IndexedRow, rowB: IndexedRow): DistanceWithKey = {
    val sortKey = s"${rowA.index}${rowB.index}".toInt
    val dist = euclidean(rowA.vector, rowB.vector)
    DistanceWithKey(sortKey, Math.abs(dist))
  }
}
