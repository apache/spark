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

// scalastyle:off println
package org.apache.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.GaussianMixture
import org.apache.spark.mllib.linalg.Vectors

/**
 * An example Gaussian Mixture Model EM app. Run with
 * {{{
 * ./bin/run-example mllib.DenseGaussianMixture <input> <k> <convergenceTol>
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object DenseGaussianMixture {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("usage: DenseGmmEM <input file> <k> <convergenceTol> [maxIterations]")
    } else {
      val maxIterations = if (args.length > 3) args(3).toInt else 100
      run(args(0), args(1).toInt, args(2).toDouble, maxIterations)
    }
  }

  private def run(inputFile: String, k: Int, convergenceTol: Double, maxIterations: Int) {
    val conf = new SparkConf().setAppName("Gaussian Mixture Model EM example")
    val ctx = new SparkContext(conf)

    val data = ctx.textFile(inputFile).map { line =>
      Vectors.dense(line.trim.split(' ').map(_.toDouble))
    }.cache()

    val clusters = new GaussianMixture()
      .setK(k)
      .setConvergenceTol(convergenceTol)
      .setMaxIterations(maxIterations)
      .run(data)

    for (i <- 0 until clusters.k) {
      println("weight=%f\nmu=%s\nsigma=\n%s\n" format
        (clusters.weights(i), clusters.gaussians(i).mu, clusters.gaussians(i).sigma))
    }

    println("Cluster labels (first <= 100):")
    val clusterLabels = clusters.predict(data)
    clusterLabels.take(100).foreach { x =>
      print(" " + x)
    }
    println()
  }
}
// scalastyle:on println
