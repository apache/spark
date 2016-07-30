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
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

// $example off$

object KMeansPipelineExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("KMeansPipelineExample")
      .getOrCreate()

    // $example on$
    // Prepare cluster data from a list of (email, income, gender) tuples.
    val dataToCluster = spark.createDataFrame(Seq(
      ("a@email.com", 12000, "M"),
      ("b@email.com", 43000, "M"),
      ("c@email.com", 5000, "F"),
      ("d@email.com", 60000, "M")
    )).toDF("email", "income", "gender")

    // Configure an ML pipeline, which consists of four stages: Indexing, OneHotEncoding,
    // VectorAssembling and kMeans.
    val indexer = new StringIndexer()
      .setInputCol("gender")
      .setOutputCol("genderIndex")
    val oneHotEncoder = new OneHotEncoder()
      .setInputCol("genderIndex")
      .setOutputCol("genderVec")
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("income", "genderVec"))
      .setOutputCol("features")
    val kMeans = new KMeans()
      .setK(2).
      setFeaturesCol("features")
      .setPredictionCol("prediction")
    val pipeline = new Pipeline()
      .setStages(Array(indexer, oneHotEncoder, vectorAssembler, kMeans))

    // Fit the pipeline to clustering data.
    val model = pipeline.fit(dataToCluster)

    // Now we can optionally save the fitted pipeline to disk
    model.write.overwrite().save("/tmp/spark-k-means-model")

    // We can also save this unfit pipeline to disk
    pipeline.write.overwrite().save("/tmp/unfit-k-means-model")

    // And load it back in during production
    val sameModel = PipelineModel.load("/tmp/spark-k-means-model")

    // Make predictions on data which is to be clustered.
    model.transform(dataToCluster)
      .select("email", "income", "gender", "prediction")
      .collect()
      .foreach { case Row(email: String, income: Int, gender: String, prediction: Int) =>
        println(s"($email, $income, $gender) --> cluster=$prediction")
      }
    // $example off$

    spark.stop()
  }
}

// scalastyle:on println
