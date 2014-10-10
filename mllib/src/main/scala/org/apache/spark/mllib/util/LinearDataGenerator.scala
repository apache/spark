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

import scala.collection.JavaConversions._
import scala.util.Random

import org.jblas.DoubleMatrix

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * :: DeveloperApi ::
 * Generate sample data used for Linear Data. This class generates
 * uniformly random values for every feature and adds Gaussian noise with mean `eps` to the
 * response variable `Y`.
 */
@DeveloperApi
object LinearDataGenerator {

  /**
   * Return a Java List of synthetic data randomly generated according to a multi
   * collinear model.
   * @param intercept Data intercept
   * @param weights  Weights to be applied.
   * @param nPoints Number of points in sample.
   * @param seed Random seed
   * @return Java List of input.
   */
  def generateLinearInputAsList(
      intercept: Double,
      weights: Array[Double],
      nPoints: Int,
      seed: Int,
      eps: Double): java.util.List[LabeledPoint] = {
    seqAsJavaList(generateLinearInput(intercept, weights, nPoints, seed, eps))
  }

  /**
   *
   * @param intercept Data intercept
   * @param weights  Weights to be applied.
   * @param nPoints Number of points in sample.
   * @param seed Random seed
   * @param eps Epsilon scaling factor.
   * @return
   */
  def generateLinearInput(
      intercept: Double,
      weights: Array[Double],
      nPoints: Int,
      seed: Int,
      eps: Double = 0.1): Seq[LabeledPoint] = {

    val rnd = new Random(seed)
    val weightsMat = new DoubleMatrix(1, weights.length, weights:_*)
    val x = Array.fill[Array[Double]](nPoints)(
      Array.fill[Double](weights.length)(2 * rnd.nextDouble - 1.0))
    val y = x.map { xi =>
      new DoubleMatrix(1, xi.length, xi: _*).dot(weightsMat) + intercept + eps * rnd.nextGaussian()
    }
    y.zip(x).map(p => LabeledPoint(p._1, Vectors.dense(p._2)))
  }

  /**
   * Generate an RDD containing sample data for Linear Regression models - including Ridge, Lasso,
   * and uregularized variants.
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

    val data: RDD[LabeledPoint] = sc.parallelize(0 until nparts, nparts).flatMap { p =>
      val seed = 42 + p
      val examplesInPartition = nexamples / nparts
      generateLinearInput(intercept, w.toArray, examplesInPartition, seed, eps)
    }
    data
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: LinearDataGenerator " +
        "<master> <output_dir> [num_examples] [num_features] [num_partitions]")
      System.exit(1)
    }

    val sparkMaster: String = args(0)
    val outputPath: String = args(1)
    val nexamples: Int = if (args.length > 2) args(2).toInt else 1000
    val nfeatures: Int = if (args.length > 3) args(3).toInt else 100
    val parts: Int = if (args.length > 4) args(4).toInt else 2
    val eps = 10

    val sc = new SparkContext(sparkMaster, "LinearDataGenerator")
    val data = generateLinearRDD(sc, nexamples, nfeatures, eps, nparts = parts)

    data.saveAsTextFile(outputPath)

    sc.stop()
  }
}
