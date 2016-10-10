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

package org.apache.spark.ml.feature

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

private[ml] object LSHTest {
  /**
   * For any locality sensitive function h in a metric space, we meed to verify whether
   * the following property is satisfied.
   *
   * There exist dist1, dist2, p1, p2, so that for any two elements e1 and e2,
   * If dist(e1, e2) <= dist1, then Pr{h(x) == h(y)} >= p1
   * If dist(e1, e2) >= dist2, then Pr{h(x) == h(y)} <= p2
   *
   * This is called locality sensitive property. This method checks the property on an
   * existing dataset and calculate the probabilities.
   * (https://en.wikipedia.org/wiki/Locality-sensitive_hashing#Definition)
   *
   * This method hashes each elements to hash buckets using LSH, and calculate the false positive
   * and false negative:
   * False positive: Of all (e1, e2) sharing any bucket, the probability of dist(e1, e2) > distFP
   * False positive: Of all (e1, e2) not sharing buckets, the probability of dist(e1, e2) < distFN
   *
   * @param dataset The dataset to verify the locality sensitive hashing property.
   * @param lsh The lsh instance to perform the hashing
   * @param distFP Distance threshold for false positive
   * @param distFN Distance threshold for false negative
   * @tparam T The class type of lsh
   * @return A tuple of two doubles, representing the false positive and false negative rate
   */
  def calculateLSHProperty[T <: LSHModel[T]](
      dataset: Dataset[_],
      lsh: LSH[T],
      distFP: Double,
      distFN: Double): (Double, Double) = {
    val model = lsh.fit(dataset)
    val inputCol = model.getInputCol
    val outputCol = model.getOutputCol
    val transformedData = model.transform(dataset)

    // Perform a cross join and label each pair of same_bucket and distance
    val pairs = transformedData.as("a").crossJoin(transformedData.as("b"))
    val distUDF = udf((x: Vector, y: Vector) => model.keyDistance(x, y), DataTypes.DoubleType)
    val sameBucket = udf((x: Vector, y: Vector) => model.hashDistance(x, y) == 0.0,
      DataTypes.BooleanType)
    val result = pairs
      .withColumn("same_bucket", sameBucket(col(s"a.$outputCol"), col(s"b.$outputCol")))
      .withColumn("distance", distUDF(col(s"a.$inputCol"), col(s"b.$inputCol")))

    // Compute the probabilities based on the join result
    val positive = result.filter(col("same_bucket"))
    val negative = result.filter(!col("same_bucket"))
    val falsePositiveCount = positive.filter(col("distance") > distFP).count().toDouble
    val falseNegativeCount = negative.filter(col("distance") < distFN).count().toDouble
    (falsePositiveCount / positive.count(), falseNegativeCount / negative.count())
  }

  /**
   * Compute the precision and recall of approximate nearest neighbors
   * @param lsh The lsh instance
   * @param dataset the dataset to look for the key
   * @param key The key to hash for the item
   * @param k The maximum number of items closest to the key
   * @tparam T The class type of lsh
   * @return A tuple of two doubles, representing precision and recall rate
   */
  def calculateApproxNearestNeighbors[T <: LSHModel[T]](
      lsh: LSH[T],
      dataset: Dataset[_],
      key: Vector,
      k: Int,
      singleProbing: Boolean): (Double, Double) = {
    val model = lsh.fit(dataset)

    // Compute expected
    val distUDF = udf((x: Vector) => model.keyDistance(x, key), DataTypes.DoubleType)
    val expected = dataset.sort(distUDF(col(model.getInputCol))).limit(k)

    // Compute actual
    val actual = model.approxNearestNeighbors(dataset, key, k, singleProbing, "distCol")

    // Compute precision and recall
    val correctCount = expected.join(actual, model.getInputCol).count().toDouble
    (correctCount / actual.count(), correctCount / expected.count())
  }

  /**
   * Compute the precision and recall of approximate similarity join
   * @param lsh The lsh instance
   * @param datasetA One of the datasets to join
   * @param datasetB Another dataset to join
   * @param threshold The threshold for the distance of record pairs
   * @tparam T The class type of lsh
   * @return A tuple of two doubles, representing precision and recall rate
   */
  def calculateApproxSimilarityJoin[T <: LSHModel[T]](
      lsh: LSH[T],
      datasetA: Dataset[_],
      datasetB: Dataset[_],
      threshold: Double): (Double, Double) = {
    val model = lsh.fit(datasetA)
    val inputCol = model.getInputCol

    // Compute expected
    val distUDF = udf((x: Vector, y: Vector) => model.keyDistance(x, y), DataTypes.DoubleType)
    val expected = datasetA.as("a").crossJoin(datasetB.as("b"))
      .filter(distUDF(col(s"a.$inputCol"), col(s"b.$inputCol")) < threshold)

    // Compute actual
    val actual = model.approxSimilarityJoin(datasetA, datasetB, threshold)

    // Compute precision and recall
    val correctCount = actual.filter(col("distCol") < threshold).count().toDouble
    (correctCount / actual.count(), correctCount / expected.count())
  }
}
