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

package org.apache.spark.examples.ml

// scalastyle:off println

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.ml.clustering.{GaussianMixture, GaussianMixtureSummary}
import org.apache.spark.mllib.linalg.{Vectors, VectorUDT}
import org.apache.spark.sql.{DataFrame, SQLContext}
// $example off$

/**
 * An example demonstrating Gaussian Mixture Model (GMM).
 * Run with
 * {{{
 * bin/run-example ml.GaussianMixtureExample
 * }}}
 */
object GaussianMixtureExample {
  def main(args: Array[String]): Unit = {
    // Creates a Spark context and a SQL context
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // $example on$
    // Crates a DataFrame
    val dataset: DataFrame = sqlContext.createDataFrame(Seq(
      (1, Vectors.dense(0.0, 0.0, 0.0)),
      (2, Vectors.dense(0.1, 0.1, 0.1)),
      (3, Vectors.dense(0.2, 0.2, 0.2)),
      (4, Vectors.dense(9.0, 9.0, 9.0)),
      (5, Vectors.dense(9.1, 9.1, 9.1)),
      (6, Vectors.dense(9.2, 9.2, 9.2))
    )).toDF("id", "features")

    // Trains Gaussian Mixture Model
    val gmm = new GaussianMixture()
      .setK(2)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setTol(0.0001)
      .setMaxIter(10)
      .setSeed(10)
    val model = gmm.fit(dataset)

    // Shows the result
    val summary: GaussianMixtureSummary = model.summary
    println("Size of (number of data points in) each cluster: ")
    println(summary.clusterSizes.foreach(println))

    println("Cluster centers of the transformed data:")
    summary.cluster.show()

    println("Probability of each cluster:")
    summary.probability.show()
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
