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

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.ml.linalg.BLAS
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * Generate sample data used for SVM. This class generates uniform random values
 * for the features and adds Gaussian noise with weight 0.1 to generate labels.
 */
@Since("0.8.0")
object SVMDataGenerator {

  @Since("0.8.0")
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      // scalastyle:off println
      println("Usage: SVMGenerator " +
        "<master> <output_dir> [num_examples] [num_features] [num_partitions]")
      // scalastyle:on println
      System.exit(1)
    }

    val sparkMaster: String = args(0)
    val outputPath: String = args(1)
    val nexamples: Int = if (args.length > 2) args(2).toInt else 1000
    val nfeatures: Int = if (args.length > 3) args(3).toInt else 2
    val parts: Int = if (args.length > 4) args(4).toInt else 2

    val sc = new SparkContext(sparkMaster, "SVMGenerator")

    val globalRnd = new Random(94720)
    val trueWeights = Array.fill[Double](nfeatures)(globalRnd.nextGaussian())

    val data: RDD[LabeledPoint] = sc.parallelize(0 until nexamples, parts).map { idx =>
      val rnd = new Random(42 + idx)

      val x = Array.fill[Double](nfeatures) {
        rnd.nextDouble() * 2.0 - 1.0
      }
      val yD = BLAS.nativeBLAS.ddot(trueWeights.length, x, 1, trueWeights, 1)
                + rnd.nextGaussian() * 0.1
      val y = if (yD < 0) 0.0 else 1.0
      LabeledPoint(y, Vectors.dense(x))
    }

    data.saveAsTextFile(outputPath)

    sc.stop()
  }
}
