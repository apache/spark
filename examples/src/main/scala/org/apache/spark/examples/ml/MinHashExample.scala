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
import org.apache.spark.ml.feature.MinHash
import org.apache.spark.ml.linalg.Vectors
// $example off$
import org.apache.spark.sql.SparkSession

object MinHashExample {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("MinHashExample")
      .getOrCreate()

    // $example on$
    val dataset = spark.createDataFrame(Seq(
      (1, Vectors.sparse(100, Array(1, 3, 4), Array(1.0, 1.0, 1.0))),
      (2, Vectors.sparse(100, Array(1, 3, 4), Array(1.0, 1.0, 1.0))),
      (3, Vectors.sparse(100, Array(3, 4, 6), Array(1.0, 1.0, 1.0))),
      (4, Vectors.sparse(100, Array(2, 8), Array(1.0, 1.0)))
    )).toDF("id", "signatures")

    val minHash = new MinHash()
      .setInputCol("signatures")
      .setOutputCol("buckets")
      .setOutputDim(2)
    val model = minHash.fit(dataset)

    // basic transformation with a new hash column
    val transformedDataset = model.transform(dataset)
    transformedDataset.select("id", "signatures", "buckets").show

    // approximate nearest neighbor search with a dataset and a key
    val key = Vectors.sparse(100, Array[Int](2, 3, 4), Array[Double](1.0, 1.0, 1.0))
    val approxNearestNeighbors = model.approxNearestNeighbors(dataset, key, 3, false, "distance")
    approxNearestNeighbors.select("id", "signatures", "distance").show

    // approximate similarity join of two datasets
    val datasetToJoin = spark.createDataFrame(Seq((5, key))).toDF("id", "signatures")
    val approxSimilarityJoin = model.approxSimilarityJoin(dataset, datasetToJoin, 1)
    approxSimilarityJoin.select("datasetA", "datasetB", "distCol").show
    // $example off$

    spark.stop()
  }
}
