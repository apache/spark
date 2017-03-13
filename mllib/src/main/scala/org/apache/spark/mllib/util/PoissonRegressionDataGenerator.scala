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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

@DeveloperApi
object PoissonRegressionDataGenerator {

  /**
   * Generate an RDD containing the sample data for PoissonRegression.
   *
   * We first randomly choose the parameters for the Poisson model, then generate
   * a series of samples and the corresponding labels.
   * 
   * @param sc SparkContext to use for creating the RDD.
   * @param numExamples Number of examples that will be contained in the RDD.
   * @param numFeatures Number of features to generate for each example.
   * @param useIntercept Whether to use interception in the underlying parameters.
   * @param numParts Number of partitions of the generated RDD. Default value is 2.
   */
  def generatePoissonRegRDD(
      sc: SparkContext,
      numExamples: Int,
      numFeatures: Int,
      useIntercept: Boolean,
      numParts: Int = 2): RDD[LabeledPoint] = {
    val rnd = new Random(100083)
    
    // the underlying possion regression paramters
    val parameters = Vectors dense Array.fill[Double](numFeatures)(rnd.nextDouble())
    val intercept = if (useIntercept) rnd.nextGaussian() else 0.0

    // generate the data set
    sc.parallelize(0 until numExamples, numParts) map { idx =>
      val rnd = new Random(32 + idx)
      val x = Vectors dense Array.fill[Double](numFeatures)(rnd.nextDouble() * 4.0)
      val y = math.exp(parameters.toBreeze dot x.toBreeze + intercept)
      LabeledPoint(math rint y, x)
    }
  }

  def main(args: Array[String]) {
    if (args.length != 5) {
      println("Usage: PoissonRegressionGenerator " +
        "<master> <output_dir> <num_examples> <num_features> <num_partitions>")
      System.exit(1)
    }

    val sparkMaster: String = args(0)
    val outputPath: String = args(1)
    val nexamples: Int = if (args.length > 2) args(2).toInt else 1000
    val nfeatures: Int = if (args.length > 3) args(3).toInt else 2
    val parts: Int = if (args.length > 4) args(4).toInt else 2

    val sc = new SparkContext(sparkMaster, "PoissonRegressionDataGenerator")
    val data = generatePoissonRegRDD(sc, nexamples, nfeatures, false, parts)

    data.saveAsTextFile(outputPath)
    sc.stop()
  }
}

