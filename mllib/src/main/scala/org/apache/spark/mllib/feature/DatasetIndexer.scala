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

package org.apache.spark.mllib.feature

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.OpenHashSet

/**
 * :: Experimental ::
 * Class for indexing columns in a dataset.
 *
 * This helps process a dataset of unknown vectors into a dataset with some continuous features
 * and some categorical features. The choice between continuous and categorical is based upon
 * a maxCategories parameter.
 *
 * This can also map categorical feature values to 0-based indices.
 *
 * Usage:
 *   val myData1: RDD[Vector] = ...
 *   val myData2: RDD[Vector] = ...
 *   val datasetIndexer = new DatasetIndexer(maxCategories)
 *   datasetIndexer.fit(myData1)
 *   val indexedData1: RDD[Vector] = datasetIndexer.transform(myData1)
 *   datasetIndexer.fit(myData2)
 *   val indexedData2: RDD[Vector] = datasetIndexer.transform(myData2)
 *   val categoricalFeaturesInfo: Map[Int, Int] = datasetIndexer.getCategoricalFeaturesInfo()
 */
@Experimental
class DatasetIndexer(
    val maxCategories: Int,
    val ignoreUnrecognizedCategories: Boolean = true)
  extends Logging {

  /**
   * Array (over features) of sets of distinct feature values (up to maxCategories values).
   * Null values in array indicate feature has been determined to be continuous.
   */
  private var featureValueCounts: Array[OpenHashSet[Double]] = null

  /**
   * Scans a dataset once and updates statistics about each column.
   * The statistics are used to choose categorical features and re-index them.
   *
   * Warning: Calling this on a new dataset changes the feature statistics and thus
   *          can change the behavior of [[transform]] and [[getCategoricalFeatureIndexes]].
   *          It is best to [[fit]] on all datasets before calling [[transform]] on any.
   *
   * @param data  Dataset with equal-length vectors.
   *              NOTE: A single instance of [[DatasetIndexer]] must always be given vectors of
   *              the same length.  If given non-matching vectors, this method will throw an error.
   */
  def fit(data: RDD[Vector]): Unit = {
    if (featureValueCounts == null) {
      val sample = data.take(1)
      if (sample.size == 0) {
        logWarning("DatasetIndexer given empty RDD")
        return
      }
      val numFeatures = sample(0).size
      featureValueCounts = new Array[OpenHashSet[Double]](numFeatures)
      var i = 0
      while (i < numFeatures) {
        featureValueCounts(i) = new OpenHashSet[Double]()
        i += 1
      }
    }
    val partitionIndexes: RDD[(Array[OpenHashSet[Double]], Int)] = data.mapPartitions { iter =>
      val partfeatureValueCounts: Array[OpenHashSet[Double]] = featureValueCounts
      /*
      iter.foreach { v => v match
        case dv: DenseVector =>
        case sv: SparseVector =>
      }
      */
      val violation: Int = 0
      Iterator((partfeatureValueCounts, violation))
    }
    /*
  } else {
    require(featureValueCounts.length == numFeatures,
      s"DatasetIndexer given non-matching RDDs. Current RDD of vectors have length $numFeatures, but was previously ")
      */
  }

  /**
   *
   * @param data  Dataset with equal-length vectors.
   *              NOTE: A single instance of [[DatasetIndexer]] must always be given vectors of
   *              the same length.  If given non-matching vectors, this method will throw an error.
   * @return  Dataset with categorical features modified to have 0-based indices,
   *          using the index returned by
   */
  def transform(data: RDD[Vector]): RDD[Vector] = data.map(this.transform)

  /**
   * Based on datasets given to [[fit]], compute an index of the
   */
  def getCategoricalFeatureIndexes: Map[Int, Map[Double, Int]] = ???

  private def fit(datum: Vector): Unit = {

  }

  private def transform(datum: Vector): Vector = {
  }
}

/**
 * :: Experimental ::
 */
@Experimental
object DatasetIndexer {

  def

}
