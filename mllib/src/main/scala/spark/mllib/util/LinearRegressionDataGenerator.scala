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

package spark.mllib.util

import scala.util.Random

import org.jblas.DoubleMatrix

import spark.{RDD, SparkContext}
import spark.mllib.regression.LabeledPoint

/**
 * Generate sample data used for LinearRegression. This class generates
 * uniformly random values for every feature and adds Gaussian noise with mean `eps` to the
 * response variable `Y`.
 *
 */
object LinearRegressionDataGenerator {

  /**
   * Generate an RDD containing sample data for LinearRegression.
   *
   * @param sc SparkContext to be used for generating the RDD.
   * @param nexamples Number of examples that will be contained in the RDD.
   * @param nfeatures Number of features to generate for each example.
   * @param eps Epsilon factor by which examples are scaled.
   * @param nparts Number of partitions in the RDD. Default value is 2.
   *
   * @return RDD of LabeledPoint containing sample data.
   */
  def generateLinearRDD(
    sc: SparkContext,
    nexamples: Int,
    nfeatures: Int,
    eps: Double,
    nparts: Int = 2,
    intercept: Double = 0.0) : RDD[LabeledPoint] = {
    org.jblas.util.Random.seed(42)
    // Random values distributed uniformly in [-0.5, 0.5]
    val w = DoubleMatrix.rand(nfeatures, 1).subi(0.5)
    w.put(0, 0, 10)
    w.put(1, 0, 10)

    val data: RDD[LabeledPoint] = sc.parallelize(0 until nparts, nparts).flatMap { p =>
      org.jblas.util.Random.seed(42 + p)
      val examplesInPartition = nexamples / nparts

      val X = DoubleMatrix.rand(examplesInPartition, nfeatures)
      val y = X.mmul(w).add(intercept)

      val rnd = new Random(42 + p)

      val normalValues = Array.fill[Double](examplesInPartition)(rnd.nextGaussian() * eps)
      val yObs = new DoubleMatrix(normalValues).addi(y)

      Iterator.tabulate(examplesInPartition) { i =>
        LabeledPoint(yObs.get(i, 0), X.getRow(i).toArray)
      }
    }
    data
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: LinearRegressionGenerator " +
        "<master> <output_dir> [num_examples] [num_features] [num_partitions]")
      System.exit(1)
    }

    val sparkMaster: String = args(0)
    val outputPath: String = args(1)
    val nexamples: Int = if (args.length > 2) args(2).toInt else 1000
    val nfeatures: Int = if (args.length > 3) args(3).toInt else 100
    val parts: Int = if (args.length > 4) args(4).toInt else 2
    val eps = 10

    val sc = new SparkContext(sparkMaster, "LinearRegressionDataGenerator")
    val data = generateLinearRDD(sc, nexamples, nfeatures, eps, parts)

    MLUtils.saveLabeledData(data, outputPath)
    sc.stop()
  }
}
