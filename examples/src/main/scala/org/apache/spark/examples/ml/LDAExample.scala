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
// $example on$
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.mllib.linalg.{Vectors, VectorUDT}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}
// $example off$

/**
 * An example demonstrating a LDA of ML pipeline.
 * Run with
 * {{{
 * bin/run-example ml.LDAExample
 * }}}
 */
object LDAExample {

  final val FEATURES_COL = "features"

  def main(args: Array[String]): Unit = {

    val input = "data/mllib/sample_lda_data.txt"
    // Creates a Spark context and a SQL context
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()

    // $example on$
    // Loads data
    val rowRDD = spark.read.text(input).rdd.filter(_.nonEmpty)
      .map(_.split(" ").map(_.toDouble)).map(Vectors.dense).map(Row(_))
    val schema = StructType(Array(StructField(FEATURES_COL, new VectorUDT, false)))
    val dataset = spark.createDataFrame(rowRDD, schema)

    // Trains a LDA model
    val lda = new LDA()
      .setK(10)
      .setMaxIter(10)
      .setFeaturesCol(FEATURES_COL)
    val model = lda.fit(dataset)
    val transformed = model.transform(dataset)

    val ll = model.logLikelihood(dataset)
    val lp = model.logPerplexity(dataset)

    // describeTopics
    val topics = model.describeTopics(3)

    // Shows the result
    topics.show(false)
    transformed.show(false)

    // $example off$
    spark.stop()
  }
}
// scalastyle:on println
