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

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StructField, StructType}


/**
 * An example demonstrating a k-means clustering.
 * Run with
 * {{{
 * bin/run-example ml.KMeansExample <file> <k>
 * }}}
 */
object KMeansExample {

  final val FEATURES_COL = "features"

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      // scalastyle:off println
      System.err.println("Usage: ml.KMeansExample <file> <k>")
      // scalastyle:on println
      System.exit(1)
    }
    val input = args(0)
    val k = args(1).toInt

    // Creates a Spark context and a SQL context
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Loads data
    val rowRDD = sc.textFile(input).filter(_.nonEmpty)
      .map(_.split(" ").map(_.toDouble)).map(Vectors.dense).map(Row(_))
    val schema = StructType(Array(StructField(FEATURES_COL, new VectorUDT, false)))
    val dataset = sqlContext.createDataFrame(rowRDD, schema)

    // Trains a k-means model
    val kmeans = new KMeans()
      .setK(k)
      .setFeaturesCol(FEATURES_COL)
    val model = kmeans.fit(dataset)

    // Shows the result
    // scalastyle:off println
    println("Final Centers: ")
    model.clusterCenters.foreach(println)
    // scalastyle:on println

    sc.stop()
  }
}
