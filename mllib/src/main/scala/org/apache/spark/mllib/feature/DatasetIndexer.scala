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
 *
 * TODO: Add warning if a categorical feature has only 1 category.
 *
 * TODO: Add option for allowing unknown categories:
 *       Parameter allowUnknownCategories:
 *        If true, then handle unknown categories during `transform`
 *        by assigning them to an extra category index.
 *        That unknown category index will be the largest index;
 *        e.g., if 5 categories are found during `fit`, then any
 *        unknown categories will be assigned index 5.
 *
 * @param maxCategories  Threshold for the number of values a categorical feature can take.
 *                       If a feature is found to have > maxCategories values, then it is
 *                       declared continuous.
 */
@Experimental
class DatasetIndexer(val maxCategories: Int) extends Logging with Serializable {

  require(maxCategories > 1,
    s"DatasetIndexer given maxCategories = $maxCategories, but requires maxCategories > 1.")

  private class FeatureValueStats(val numFeatures: Int, val maxCategories: Int)
    extends Serializable {

    val featureValueSets = Array.fill[OpenHashSet[Double]](numFeatures)(new OpenHashSet[Double]())

    /**
     * Merge other [[FeatureValueStats]] into this instance, modifying this instance.
     * @param other  Other instance.  Not modified.
     * @return This instance
     */
    def merge(other: FeatureValueStats): FeatureValueStats = {
      featureValueSets.zip(other.featureValueSets).foreach { case (fvs1, fvs2) =>
        fvs2.iterator.foreach { val2 =>
          // Once we have found > maxCategories values, we know the feature is continuous
          // and do not need to collect more values for it.
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
   * Note: To run this on an RDD[Double], convert to Vector via `data.map(Vectors.dense(_))`.
   *
   * @param data  Dataset with equal-length vectors.
   *              NOTE: A single instance of [[DatasetIndexer]] must always be given vectors of
   *              the same length.  If given non-matching vectors, this method will throw an error.
   */
  def fit(data: RDD[Vector]): Unit = {

    // For each partition, get (featureValueStats, newNumFeatures).
    //  If all vectors have the same length, then newNumFeatures = None.
    //  If a vector with a new length is found, then newNumFeatures is set to that length.
    val partitionFeatureValueSets: RDD[(Option[FeatureValueStats], Option[Int])] =
      data.mapPartitions { iter =>
        // Make local copy of featureValueStats.
        //  This will be None initially if this is the first dataset to be fitted.
        var localFeatureValueStats: Option[FeatureValueStats] = featureValueStats
        var newNumFeatures: Option[Int] = None
        // TODO: Track which features are known to be continuous already, and do not bother
        //       updating counts for them.  Probably store featureValueStats in a linked list.
        iter.foreach {
          case dv: DenseVector =>
            localFeatureValueStats match {
              case Some(fvs) =>
                if (fvs.numFeatures == dv.size) {
                  fvs.addDenseVector(dv)
                } else {
                  // non-matching vector lengths
                  newNumFeatures = Some(dv.size)
                }
              case None =>
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
                  newNumFeatures = Some(sv.size)
                }
              case None =>
                localFeatureValueStats = Some(new FeatureValueStats(sv.size, maxCategories))
                localFeatureValueStats.get.addSparseVector(sv)
            }
        }
        Iterator((localFeatureValueStats, newNumFeatures))
      }
    val (aggFeatureValueStats: Option[FeatureValueStats], newNumFeatures: Option[Int]) =
      partitionFeatureValueSets.fold((None, None)) {
        case ((Some(fvs1), newNumFeatures1), (Some(fvs2), newNumFeatures2)) =>
          if (fvs2.numFeatures == fvs1.numFeatures) {
            val tmpNumFeatures = (newNumFeatures1, newNumFeatures2) match {
              case (None, None) => None // good: vector lengths match
              case (None, _) => newNumFeatures2
              case (_, _) => newNumFeatures1
            }
            (Some(fvs1.merge(fvs2)), tmpNumFeatures)
          } else {
            // non-matching vector lengths
            (Some(fvs1), Some(fvs2.numFeatures))
          }
        case ((Some(fvs1), newNumFeatures1), (None, None)) =>
          (Some(fvs1), newNumFeatures1)
        case ((None, None), (Some(fvs2), newNumFeatures2)) =>
          (Some(fvs2), newNumFeatures2)
        case ((None, None), (None, None)) =>
          (None, None)
      }
    if (newNumFeatures.nonEmpty) {
      throw new RuntimeException("DatasetIndexer given records of non-matching length." +
        s" Found records with length ${aggFeatureValueStats.get.numFeatures} and length" +
        s" ${newNumFeatures.get}")
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
   * Transforms the given dataset using the indexes returned by [[getCategoricalFeatureIndexes]].
   * Categorical features are mapped to their feature value indices.
   * Continuous features (columns) are left unchanged.
   *
   * Note: To run this on an RDD[Double], convert to Vector via `data.map(Vectors.dense(_))`.
   *
   * @param data  Dataset with equal-length vectors.
   *              NOTE: A single instance of [[DatasetIndexer]] must always be given vectors of
   *              the same length.  If given non-matching vectors, this method will throw an error.
   * @return  Dataset with categorical features modified to use 0-based indices.
   */
  def transform(data: RDD[Vector]): RDD[Vector] = {
    val catFeatIdx = getCategoricalFeatureIndexes
    data.mapPartitions { iterator =>
      val sortedCategoricalFeatureIndices = catFeatIdx.keys.toArray.sorted
      iterator.map { v: Vector =>
        v match {
          case dv: DenseVector =>
            catFeatIdx.foreach { case (featureIndex, categoryMap) =>
              dv.values(featureIndex) = categoryMap(dv(featureIndex))
            }
            dv.asInstanceOf[Vector]
          case sv: SparseVector =>
            var sortedFeatInd = 0 // index into sortedCategoricalFeatureIndices
            var k = 0 // index into non-zero elements of sparse vector
            while (sortedFeatInd < sortedCategoricalFeatureIndices.size && k < sv.indices.size) {
              val featInd = sortedCategoricalFeatureIndices(sortedFeatInd)
              if (featInd < sv.indices(k)) {
                sortedFeatInd += 1
              } else if (featInd > sv.indices(k)) {
                k += 1
              } else {
                sv.values(k) = catFeatIdx(featInd)(sv.values(k))
                sortedFeatInd += 1
                k += 1
              }
            }
            sv.asInstanceOf[Vector]
        }
      }
    }
  }

  /**
   * Based on datasets given to [[fit]], decide which features are categorical,
   * and choose indices for categories.
   *
   * Sparsity: This tries to maintain sparsity by treating value 0.0 specially.
   *           If a categorical feature takes value 0.0, then value 0.0 is given index 0.
   *
   * @return  Feature value index.  Keys are categorical feature indices (column indices).
   *          Values are mappings from original features values to 0-based category indices.
   */
  def getCategoricalFeatureIndexes: Map[Int, Map[Double, Int]] = featureValueStats match {
    case Some(fvs) =>
      fvs.featureValueSets.zipWithIndex
        .filter(_._1.size <= maxCategories).map { case (featureValues, featureIndex) =>
        // Get feature values, but remove 0 to treat separately.
        // If value 0 exists, give it index 0 to maintain sparsity if possible.
        var sortedFeatureValues = featureValues.iterator.filter(_ != 0.0).toArray.sorted
        val zeroExists = sortedFeatureValues.size + 1 == featureValues.size
        if (zeroExists) {
          sortedFeatureValues = 0.0 +: sortedFeatureValues
        }
        val categoryMap: Map[Double, Int] = sortedFeatureValues.zipWithIndex.toMap
        (featureIndex, categoryMap)
      }.toMap
    case None =>
      throw new RuntimeException("DatasetIndexer.getCategoricalFeatureIndexes called," +
        " but no datasets have been indexed via fit() yet.")
  }
}
