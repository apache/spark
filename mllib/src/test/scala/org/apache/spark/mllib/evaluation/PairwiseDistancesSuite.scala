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

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.feature.RandomProjection
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import java.io._


/**
 * some tests with pairwise distances
 */
class PairwiseDistancesSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("compute the pairwise distances accordingly") {
    // build the test matrix
    val values = 1 until 11
    val data = values.flatMap(x => {
      values.map { y =>
        MatrixEntry(x, y, x)
      }
    }).toList

    // make the matrix distributed
    val rdd = sc.parallelize(data)
    val coordMat: CoordinateMatrix = new CoordinateMatrix(rdd, 11, 11)
    val matA: BlockMatrix = coordMat.toBlockMatrix().cache()

    val origDimension = matA.numCols().toInt
    val origRows = matA.numRows().toInt
    // scalastyle:off println
    println(s"dataset rows: $origRows")
    println(s"dataset dimension: $origDimension")
    matA.validate()
    val fold = 0 until 2
    val dimensions = 5 until 16

    // lets print the results for further processing (graphs)
    val path = s"${new File(".").getCanonicalPath()}/../../../results_pairwise_spark.csv"
    val pw = new PrintWriter(new File(path))

    // the label of the columns
    pw.write("x,y_orig,y_reduced,y_error\r\n")
    dimensions.foreach { dimension =>
      val error = fold.map { item =>
        evaluatePairwiseDistances(matA, dimension)
      }
      val meanError = error.map(_.error).sum / error.length
      val origDistance = error.map(_.original).sum / error.length
      val reducedDistance = error.map(_.reduced).sum / error.length
      pw.write(s"$dimension,$origDistance,$reducedDistance,$meanError\r\n")
    }

    pw.close

    assert(true == true)
  }

  case class SumDistances(original: Double, reduced: Double, error: Double)

  def evaluatePairwiseDistances(dataset: BlockMatrix, intrinsicDimension: Int = 5) = {

    val distancesOriginal = PairwiseDistances.calculatePairwiseDistances(dataset)

    val rp = new RandomProjection(intrinsicDimension)
    val reduced = dataset.multiply(rp.computeRPMatrix(sc, dataset.numCols().toInt))
    val distancesReduced = PairwiseDistances.calculatePairwiseDistances(reduced)


    val accumulateDistances = distancesOriginal.zip(distancesReduced).foldLeft(SumDistances(0, 0, 0))((counter, item) => {
      // make sure,
      require(item._1._1 == item._2._1, s"error. '${item._1._1}' must be equal to '${item._2._1}'")
      val origDistances = item._1._2
      val reducedDistances = item._2._2

      //error must be positive
      val error = if (origDistances > reducedDistances) origDistances - reducedDistances else reducedDistances - origDistances
      //println(s"#${item._1._1} -> orig distance: $origDistances, reduced distances: $reducedDistances, error: $error")
      //println(s"orig sum of ${item._1._1} = ${counter.original + origDistances}")

      SumDistances(
        counter.original + origDistances,
        counter.reduced + reducedDistances,
        counter.error + error)
    })
    accumulateDistances
  }
}
