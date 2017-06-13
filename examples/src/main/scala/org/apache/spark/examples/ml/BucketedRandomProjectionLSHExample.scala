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
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.col
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example demonstrating BucketedRandomProjectionLSH.
 * Run with:
 *   bin/run-example ml.BucketedRandomProjectionLSHExample
 */
object BucketedRandomProjectionLSHExample {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession
    val spark = SparkSession
      .builder
      .appName("BucketedRandomProjectionLSHExample")
      .getOrCreate()

    // $example on$
    val dfA = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 1.0)),
      (1, Vectors.dense(1.0, -1.0)),
      (2, Vectors.dense(-1.0, -1.0)),
      (3, Vectors.dense(-1.0, 1.0))
    )).toDF("id", "features")

    val dfB = spark.createDataFrame(Seq(
      (4, Vectors.dense(1.0, 0.0)),
      (5, Vectors.dense(-1.0, 0.0)),
      (6, Vectors.dense(0.0, 1.0)),
      (7, Vectors.dense(0.0, -1.0))
    )).toDF("id", "features")

    val key = Vectors.dense(1.0, 0.0)

    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0)
      .setNumHashTables(3)
      .setInputCol("features")
      .setOutputCol("hashes")

    val model = brp.fit(dfA)

    // Feature Transformation
    println("The hashed dataset where hashed values are stored in the column 'hashes':")
    model.transform(dfA).show()

    // Compute the locality sensitive hashes for the input rows, then perform approximate
    // similarity join.
    // We could avoid computing hashes by passing in the already-transformed dataset, e.g.
    // `model.approxSimilarityJoin(transformedA, transformedB, 1.5)`
    println("Approximately joining dfA and dfB on Euclidean distance smaller than 1.5:")
    model.approxSimilarityJoin(dfA, dfB, 1.5, "EuclideanDistance")
      .select(col("datasetA.id").alias("idA"),
        col("datasetB.id").alias("idB"),
        col("EuclideanDistance")).show()

    // Compute the locality sensitive hashes for the input rows, then perform approximate nearest
    // neighbor search.
    // We could avoid computing hashes by passing in the already-transformed dataset, e.g.
    // `model.approxNearestNeighbors(transformedA, key, 2)`
    println("Approximately searching dfA for 2 nearest neighbors of the key:")
    model.approxNearestNeighbors(dfA, key, 2).show()
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
