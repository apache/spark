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

package org.apache.spark.mllib.optimization

import breeze.linalg.{Vector => BV, DenseVector => BDV}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix

class OrdinaryLeastSquares extends Optimizer with Logging {

  @DeveloperApi
  def optimize(data: RDD[(Double, Vector)], initialWeights: Vector): Vector = {
    OrdinaryLeastSquares.fit(data, initialWeights.size)
  }
}

@DeveloperApi
object OrdinaryLeastSquares extends Logging {

  def fit(data: RDD[(Double, Vector)], rank: Int): Vector = {
    // TODO: Compute and return other statistics:
    // (R-squared, Adjusted R-squared, Std. Error of weights, t-statistics, p-value)
    val featureRows = data.map { case (y, x) => x }
    val response = data.map { case (y, x) => y }.cache()

    val featureRowMatrix = new RowMatrix(featureRows)
    val svd = featureRowMatrix.computeSVD(rank, computeU = true)
    val uRdd = svd.U.rows

    val yHatRdd = response.zipPartitions(uRdd, true) {
      (yIterator, uIterator) => new Iterator[BV[Double]] {
          def hasNext = yIterator.hasNext
          def next = {
            val yValue = yIterator.next()
            uIterator.next().toBreeze.map(_ * yValue)
          }
        }
    }

    val yHat = yHatRdd.reduce(_ + _)
    val zArray = svd.s.toArray.zipWithIndex.map { case(value, index) =>  yHat(index) / value }

    val vMat = svd.V.toBreeze
    val solution = vMat * BDV(zArray)
    Vectors.fromBreeze(solution)
  }


}