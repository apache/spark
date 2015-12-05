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

package org.apache.spark.mllib.util

import scala.util.Random

import org.apache.spark.annotation.{Since, DeveloperApi}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

/**
 * :: DeveloperApi ::
 * Generate test data for LogisticRegression. This class chooses positive labels
 * with probability `probOne` and scales features for positive examples by `eps`.
 */
@DeveloperApi
@Since("0.8.0")
object LogisticRegressionDataGenerator {

  /**
   * Generate an RDD containing test data for LogisticRegression.
   *
   * @param sc SparkContext to use for creating the RDD.
   * @param nexamples Number of examples that will be contained in the RDD.
   * @param nfeatures Number of features to generate for each example.
   * @param eps Epsilon factor by which positive examples are scaled.
   * @param nparts Number of partitions of the generated RDD. Default value is 2.
   * @param probOne Probability that a label is 1 (and not 0). Default value is 0.5.
   */
  @Since("0.8.0")
  def generateLogisticRDD(
    sc: SparkContext,
    nexamples: Int,
    nfeatures: Int,
    eps: Double,
    nparts: Int = 2,
    probOne: Double = 0.5): RDD[LabeledPoint] = {
    val data = sc.parallelize(0 until nexamples, nparts).map { idx =>
      val rnd = new Random(42 + idx)

      val y = if (idx % 2 == 0) 0.0 else 1.0
      val x = Array.fill[Double](nfeatures) {
        rnd.nextGaussian() + (y * eps)
      }
      LabeledPoint(y, Vectors.dense(x))
    }
    data
  }

  @Since("0.8.0")
  def main(args: Array[String]) {
    if (args.length != 5) {
      // scalastyle:off println
      println("Usage: LogisticRegressionGenerator " +
        "<master> <output_dir> <num_examples> <num_features> <num_partitions>")
      // scalastyle:on println
      System.exit(1)
    }

    val sparkMaster: String = args(0)
    val outputPath: String = args(1)
    val nexamples: Int = if (args.length > 2) args(2).toInt else 1000
    val nfeatures: Int = if (args.length > 3) args(3).toInt else 2
    val parts: Int = if (args.length > 4) args(4).toInt else 2
    val eps = 3

    val sc = new SparkContext(sparkMaster, "LogisticRegressionDataGenerator")
    val data = generateLogisticRDD(sc, nexamples, nfeatures, eps, parts)

    data.saveAsTextFile(outputPath)

    sc.stop()
  }
}
