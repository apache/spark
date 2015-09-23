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

import scopt.OptionParser

import org.apache.spark.mllib.clustering.DpMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
 * An example DP-means app. Run with
 * {{{
 * ./bin/run-example mllib.DenseDpMeans <--lambdaValue> [<--convergenceTol>
 *  <--maxIterations>] <input>
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object DenseDpMeans {
  case class Params(
      input: String = null,
      lambdaValue: Double = 0.0,
      convergenceTol: Double = 0.01,
      maxIterations: Int = 20) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("DenseDpMeans") {
      head("DenseDpMeans: DP-means example application.")
      opt[Double]("lambdaValue")
        .required()
        .text("lambda value, required")
        .action((x, c) => c.copy(lambdaValue = x))
      opt[Double]("convergenceTol")
        .abbr("ct")
        .text(s"convergence threshold, default: ${defaultParams.convergenceTol}")
        .action((x, c) => c.copy(convergenceTol = x))
      opt[Int]("maxIterations")
        .abbr("iter")
        .text(s"number of iterations, default: ${defaultParams.maxIterations}")
        .action((x, c) => c.copy(maxIterations = x))
      arg[String]("<input>")
        .text("path to input data")
        .required()
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  private def run(params: Params) {
    val conf = new SparkConf().setAppName("DP-means example")
    val sc = new SparkContext(conf)

    val data = sc.textFile(params.input).map { line =>
      Vectors.dense(line.trim.split(' ').map(_.toDouble))
    }.cache()

    val clusters = new DpMeans()
      .setLambdaValue(params.lambdaValue)
      .setConvergenceTol(params.convergenceTol)
      .setMaxIterations(params.maxIterations)
      .run(data)

    val k = clusters.k
    println(s"Number of Clusters = $k.")
    println()

    println("Clusters centers ::")
    for (i <- 0 until clusters.k) {
      println(clusters.clusterCenters(i))
    }
    println()

    println("Cluster labels (first <= 20):")
    val clusterLabels = clusters.predict(data)
    clusterLabels.take(20).foreach { x =>
      print(" " + x)
    }
    println()

    val labels = clusterLabels.collect().groupBy(x => x) map { case (k, v) => k-> v.length }
    println(s"Cluster Count:: $labels")

    val cost = clusters.computeCost(data)
    println(s"Total Cost = $cost.")
    sc.stop()
  }
}

