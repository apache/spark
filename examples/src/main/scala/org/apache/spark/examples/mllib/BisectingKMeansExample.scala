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
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.clustering.BisectingKMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
// $example off$

/**
 * An example demonstrating a bisecting k-means clustering in spark.mllib.
 *
 * Run with
 * {{{
 * bin/run-example mllib.BisectingKMeansExample
 * }}}
 */
object BisectingKMeansExample {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("mllib.BisectingKMeansExample")
    val sc = new SparkContext(sparkConf)

    // $example on$
    // Loads and parses data
    def parse(line: String): Vector = Vectors.dense(line.split(" ").map(_.toDouble))
    val data = sc.textFile("data/mllib/kmeans_data.txt").map(parse).cache()

    // Clustering the data into 6 clusters by BisectingKMeans.
    val bkm = new BisectingKMeans().setK(6)
    val model = bkm.run(data)

    // Show the compute cost and the cluster centers
    println(s"Compute Cost: ${model.computeCost(data)}")
    model.clusterCenters.zipWithIndex.foreach { case (center, idx) =>
      println(s"Cluster Center ${idx}: ${center}")
    }
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
