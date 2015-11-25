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

package org.apache.spark.examples.mllib

// scalastyle:off println
import scopt.OptionParser

import org.apache.spark.mllib.clustering.BisectingKMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * An example demonstrating a bisecting k-means clustering in spark.mllib.
 *
 * Run with
 * {{{
 * bin/run-example mllib.BisectingKMeansExample [option] <input>
 * }}}
 */
object BisectingKMeansExample {

  case class Params(
    input: String = null,
    k: Int = 4) extends AbstractParams[Params]

  def main(args: Array[String]) {

    val defaultParams = Params()

    def parse(line: String): Vector = {
      Vectors.dense(line.split(",").map(_.toDouble))
    }

    val parser = new OptionParser[Params]("DenseKMeans") {
      head("BisectingKMeansExample: an example bisecting k-means app.")
      opt[Int]('k', "k")
        .required()
        .text(s"number of clusters, required")
        .action((x, c) => c.copy(k = x))
      arg[String]("<input>")
        .text("CSV file paths")
        .required()
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defaultParams).map { params =>
      val sparkConf = new SparkConf().setAppName("mllib.BisectingKMeansExample")
      val sc = new SparkContext(sparkConf)

      val data = sc.textFile(params.input).map(parse).cache()
      val k = params.k

      val bkm = new BisectingKMeans().setK(k)
      val model = bkm.run(data)

      println(s"Compute Cost: ${model.computeCost(data)}")
      model.clusterCenters.zipWithIndex.foreach { case (center, idx) =>
        println(s"Cluster Center ${idx}: ${center}")
      }

      sc.stop()
    }.getOrElse {
      sys.exit(1)
    }
  }
}
// scalastyle:on println
