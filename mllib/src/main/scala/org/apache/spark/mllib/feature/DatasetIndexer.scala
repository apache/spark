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
import org.apache.spark.mllib.linalg.{Vectors, DenseVector, SparseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.OpenHashSet

class FeatureValueStats(val numFeatures: Int, val maxCategories: Int)
  extends Serializable {

  val featureValueSets = new Array[OpenHashSet[Double]](numFeatures)

  /**
   * Merge other [[FeatureValueStats]] into this instance, modifying this instance.
   * @param other  Other instance.  Not modified.
   * @return This instance
   */
  def merge(other: FeatureValueStats): FeatureValueStats = {
    featureValueSets.zip(other.featureValueSets).foreach { case (fvs1, fvs2) =>
      fvs2.iterator.foreach { val2 =>
        if (fvs1.size <= maxCategories) fvs1.add(val2)
      }
    }
    this
  }

  def addDenseVector(dv: DenseVector): Unit = {
    var i = 0
    while (i < dv.size) {
      if (featureValueSets(i).size <= maxCategories) {
        featureValueSets(i).add(dv(i))
      }
      i += 1
    }
  }

  def addSparseVector(sv: SparseVector): Unit = {
    // TODO: This could be made more efficient.
    var vecIndex = 0 // index into vector
    var nzIndex = 0 // index into non-zero elements
    while (vecIndex < sv.size) {
      val featureValue = if (nzIndex < sv.indices.size && vecIndex == sv.indices(nzIndex)) {
        nzIndex += 1
        sv.values(nzIndex - 1)
      } else {
        0.0
      }
      if (featureValueSets(vecIndex).size <= maxCategories) {
        featureValueSets(vecIndex).add(featureValue)
      }
      vecIndex += 1
    }
  }

}

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
   *
   * Once the number of elements in a feature's set reaches maxCategories + 1,
   * then it is declared continuous, and we stop adding elements.
   */
  private var featureValueStats: Option[FeatureValueStats] = None

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
    // For each partition, get (featureValueStats, newNumFeatures).
    // If all vectors have the same length, then newNumFeatures will be set to numFeatures.
    // If a vector with a new length is found, then newNumFeatures is set to that length.
    val partitionFeatureValueSets: RDD[(Option[FeatureValueStats], Int)] = data.mapPartitions { iter =>
      // Make local copy of featureValueStats.
      //  This will be None initially if this is the first dataset to be fitted.
      var localFeatureValueStats: Option[FeatureValueStats] = featureValueStats
      var newNumFeatures: Int = localFeatureValueStats match {
        case Some(fvs) => fvs.numFeatures
        case None => -1
      }
      /*
      // TODO: Track which features are known to be continuous already, and do not bother
      //       updating counts for them.  Probably store featureValueCounts in a linked list.
      iter.foreach { _ match {
        case dv: DenseVector =>
          localFeatureValueStats match {
            case Some(fvs) =>
              if (fvs.numFeatures == dv.size) {
                fvs.addDenseVector(dv)
              } else {
                // non-matching vector lengths
                newNumFeatures = dv.size
              }
            case None =>
              newNumFeatures = dv.size
              localFeatureValueStats = Some(new FeatureValueStats(dv.size, maxCategories))
              localFeatureValueStats.get.addDenseVector(dv)
          }
        case sv: SparseVector =>
          localFeatureValueStats match {
            case Some(fvs) =>
              if (fvs.numFeatures == sv.size) {
                fvs.addSparseVector(sv)
              } else {
                // non-matching vector lengths
                newNumFeatures = sv.size
              }
            case None =>
              newNumFeatures = sv.size
              localFeatureValueStats = Some(new FeatureValueStats(sv.size, maxCategories))
              localFeatureValueStats.get.addSparseVector(sv)
          }
      }}
      */
      Iterator((localFeatureValueStats, newNumFeatures))
    }
    val (aggFeatureValueStats: Option[FeatureValueStats], newNumFeatures: Int) = (None, -1)
    /*
      partitionFeatureValueSets.fold((None, -1)) {
        case ((Some(fvs1), newNumFeatures1), (Some(fvs2), newNumFeatures2)) =>
          if (fvs2.numFeatures == fvs1.numFeatures) {
            val mergedFVS = fvs1.merge(fvs2)
            (newNumFeatures1, newNumFeatures2) match {
              case (-1, -1) =>
                // good: vector lengths match
                (Some(mergedFVS), -1)
              case (-1, _) =>
                (Some(mergedFVS), newNumFeatures2)
              case (_, _) =>
                (Some(mergedFVS), newNumFeatures1)
            }
          } else {
            // non-matching vector lengths
            (Some(fvs1), fvs2.numFeatures)
          }
        case ((Some(fvs1), newNumFeatures1), (None, -1)) =>
          (Some(fvs1), newNumFeatures1)
        case ((None, -1), (Some(fvs2), newNumFeatures2)) =>
          (Some(fvs2), newNumFeatures2)
        case ((None, -1), (None, -1)) =>
          (None, -1)
      }
      */
    if (newNumFeatures != -1) {
      throw new RuntimeException("DatasetIndexer given records of non-matching length." +
        s" Found records with length ${aggFeatureValueStats.get.numFeatures} and length" +
        s" $newNumFeatures")
    }
    (featureValueStats, aggFeatureValueStats) match {
      case (Some(origFVS), Some(newFVS)) =>
        origFVS.merge(newFVS)
      case (None, Some(newFVS)) =>
        featureValueStats = Some(newFVS)
      case _ =>
        logDebug("DatasetIndexer.fit(rdd) called on RDD with 0 rows.")
    }
  }

  /**
   *
   * @param data  Dataset with equal-length vectors.
   *              NOTE: A single instance of [[DatasetIndexer]] must always be given vectors of
   *              the same length.  If given non-matching vectors, this method will throw an error.
   * @return  Dataset with categorical features modified to have 0-based indices,
   *          using the index returned by
   */
  def transform(data: RDD[Vector]): RDD[Vector] = {
    val catFeatIdx = getCategoricalFeatureIndexes
    data.map { v: Vector => v match {
      case dv: DenseVector =>
        catFeatIdx.foreach { case (featureIndex, categoryMap) =>
          dv.values(featureIndex) = categoryMap(dv(featureIndex))
        }
        dv.asInstanceOf[Vector]
      case sv: SparseVector =>
        // TODO: This currently converts to a dense vector. After updating
        //       getCategoricalFeatureIndexes, make this maintain sparsity when possible.
        val dv = sv.toArray
        catFeatIdx.foreach { case (featureIndex, categoryMap) =>
          dv(featureIndex) = categoryMap(dv(featureIndex))
        }
        Vectors.dense(dv)
    }}
  }

  /**
   * Based on datasets given to [[fit]], decide which features are categorical,
   * and choose indices for categories.
   * @return  Feature index.  Keys are categorical feature indices (column indices).
   *          Values are mappings from original features values to 0-based category indices.
   */
  def getCategoricalFeatureIndexes: Map[Int, Map[Double, Int]] = featureValueStats match {
    // TODO: It would be ideal to have value 0 set to index 0 to maintain sparsity if possible.
    case Some(fvs) =>
      fvs.featureValueSets.zipWithIndex
        .filter(_._1.size <= maxCategories).map { case (featureValues, featureIndex) =>
        val categoryMap: Map[Double, Int] = featureValues.iterator.toList.sorted.zipWithIndex.toMap
        (featureIndex, categoryMap)
      }.toMap
    case None =>
      throw new RuntimeException("DatasetIndexer.getCategoricalFeatureIndexes called," +
        " but no datasets have been indexed via fit() yet.")
  }

}
