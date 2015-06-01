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

package org.apache.spark.mllib.feature

import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, RowMatrix, CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import breeze.stats.distributions.{Uniform, Binomial}
import org.apache.spark.SparkContext
import org.apache.spark.ml.param.{ParamMap}
import org.apache.spark.sql.Row

class RandomProjection(intrinsicDimension: Int) {

  /**
   * recursive implementation of random, non repeating list
   * @param data
   * @param maxValue
   * @return
   */
  def nonRepeatingRandomItem(data: mutable.MutableList[Int], maxValue: Int): Int = {
    val rand = Uniform(0.0, maxValue).draw().toInt
    if (data.contains(rand)) nonRepeatingRandomItem(data, maxValue)
    else rand
  }

  /**
   * generate list of non repeating, random values
   * @param nonZeroRange
   * @param maxValue
   * @return
   */
  def drawNonZeroIndices(nonZeroRange: Range, maxValue: Int): List[Int] = {
    val nonZeroIndices = mutable.MutableList[Int]()
    nonZeroRange map { j =>
      nonZeroIndices += nonRepeatingRandomItem(nonZeroIndices, maxValue)
    }
    nonZeroIndices.toList
  }

  /**
   * draw values
   * @param nonZeroRange
   * @return
   */
  def drawNonZeroValues(nonZeroRange: Range): IndexedSeq[Double] = {
    nonZeroRange map { _ =>
      // random value, either -1 or 1
      new Binomial(1, 0.5).draw().toDouble * 2 - 1
    }
  }

  /**
   * @param value
   * @param density
   * @param newDimensions
   * @return
   */
  def scaleNonZeroRandomValue(value: Double, density: Double, newDimensions: Int): Double = {
    Math.sqrt(1 / density) / Math.sqrt(newDimensions) * value
  }

  /**
   * in RP, one row refers to one dimension of the intrinsic
   * @param origDimensions
   * @param density
   * @return
   */
  def computeRPRows(origDimensions: Int, density: Double): List[MatrixEntry] = {
    // one row for each dimension of the dataset
    val rows = 0 until origDimensions map { rowIndex =>

      /**
       * flip coin 'origDimensions' times with a probability of 'density' and count
       */
      val nonZero = new Binomial(intrinsicDimension, density).sample()
      val nonZeroRange = 0 until nonZero

      val nonZeroIndices = drawNonZeroIndices(nonZeroRange, intrinsicDimension)
      val nonZeroValues = drawNonZeroValues(nonZeroRange)

      require(nonZeroValues.length == nonZeroIndices.length,
              "nonZero values and indices must have same length")

      val merged = nonZeroIndices.zip(nonZeroValues).map { item =>
        val colIndex = item._1
        // scale each value
        val value = scaleNonZeroRandomValue(value = item._2, density, origDimensions)
        new MatrixEntry(rowIndex, colIndex, value)
      }
      merged.toList
    }

    /**
     * make sure the matrix has the correct dimension while initializing it with the
     * required dimensions
     */
    val initialSize = List(new MatrixEntry(origDimensions - 1, intrinsicDimension - 1, 0.0))
    initialSize ++ rows.flatten.toList
  }

  /**
   * copute the RP
   * @param origDimensions
   * @return
   */
  def computeRPMatrix(sparkContext: SparkContext, origDimensions: Int): BlockMatrix = {
    val density = getDensity
    val rows = computeRPRows(origDimensions, density)
    val rdd = sparkContext.parallelize(rows)
    new CoordinateMatrix(rdd).toBlockMatrix()
  }

  /**
   * calculate density for the RP
   * @return
   */
  def getDensity: Double = {
    // consider to test: 1 / (2.0*math.sqrt(origDimensions))
    1 / math.sqrt(intrinsicDimension)
  }

  /**
   * perform actual reduction (matrix multiplication
   * @param dataSetMatrix
   * @return
   */
  def reduce(randomMatrix: Matrix, dataSetMatrix: RowMatrix): RDD[Row] = {
    val reduced = dataSetMatrix.multiply(randomMatrix)
    reduced.rows.map(Row(_))
  }
}
