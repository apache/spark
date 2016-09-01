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

import scala.collection.mutable.ArrayBuilder

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Chi Squared selector model.
 *
 * @param selectedFeatures list of indices to select (filter). Must be ordered asc
 */
@Since("1.3.0")
class ChiSqSelectorModel @Since("1.3.0") (
  @Since("1.3.0") val selectedFeatures: Array[Int]) extends VectorTransformer with Saveable {

  require(isSorted(selectedFeatures), "Array has to be sorted asc")

  protected def isSorted(array: Array[Int]): Boolean = {
    var i = 1
    val len = array.length
    while (i < len) {
      if (array(i) < array(i-1)) return false
      i += 1
    }
    true
  }

  /**
   * Applies transformation on a vector.
   *
   * @param vector vector to be transformed.
   * @return transformed vector.
   */
  @Since("1.3.0")
  override def transform(vector: Vector): Vector = {
    compress(vector, selectedFeatures)
  }

  /**
   * Returns a vector with features filtered.
   * Preserves the order of filtered features the same as their indices are stored.
   * Might be moved to Vector as .slice
   * @param features vector
   * @param filterIndices indices of features to filter, must be ordered asc
   */
  private def compress(features: Vector, filterIndices: Array[Int]): Vector = {
    features match {
      case SparseVector(size, indices, values) =>
        val newSize = filterIndices.length
        val newValues = new ArrayBuilder.ofDouble
        val newIndices = new ArrayBuilder.ofInt
        var i = 0
        var j = 0
        var indicesIdx = 0
        var filterIndicesIdx = 0
        while (i < indices.length && j < filterIndices.length) {
          indicesIdx = indices(i)
          filterIndicesIdx = filterIndices(j)
          if (indicesIdx == filterIndicesIdx) {
            newIndices += j
            newValues += values(i)
            j += 1
            i += 1
          } else {
            if (indicesIdx > filterIndicesIdx) {
              j += 1
            } else {
              i += 1
            }
          }
        }
        // TODO: Sparse representation might be ineffective if (newSize ~= newValues.size)
        Vectors.sparse(newSize, newIndices.result(), newValues.result())
      case DenseVector(values) =>
        val values = features.toArray
        Vectors.dense(filterIndices.map(i => values(i)))
      case other =>
        throw new UnsupportedOperationException(
          s"Only sparse and dense vectors are supported but got ${other.getClass}.")
    }
  }

  @Since("1.6.0")
  override def save(sc: SparkContext, path: String): Unit = {
    ChiSqSelectorModel.SaveLoadV1_0.save(sc, this, path)
  }

  override protected def formatVersion: String = "1.0"
}

object ChiSqSelectorModel extends Loader[ChiSqSelectorModel] {
  @Since("1.6.0")
  override def load(sc: SparkContext, path: String): ChiSqSelectorModel = {
    ChiSqSelectorModel.SaveLoadV1_0.load(sc, path)
  }

  private[feature]
  object SaveLoadV1_0 {

    private val thisFormatVersion = "1.0"

    /** Model data for import/export */
    case class Data(feature: Int)

    private[feature]
    val thisClassName = "org.apache.spark.mllib.feature.ChiSqSelectorModel"

    def save(sc: SparkContext, model: ChiSqSelectorModel, path: String): Unit = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()

      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      // Create Parquet data.
      val dataArray = Array.tabulate(model.selectedFeatures.length) { i =>
        Data(model.selectedFeatures(i))
      }
      spark.createDataFrame(dataArray).repartition(1).write.parquet(Loader.dataPath(path))
    }

    def load(sc: SparkContext, path: String): ChiSqSelectorModel = {
      implicit val formats = DefaultFormats
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val (className, formatVersion, metadata) = Loader.loadMetadata(sc, path)
      assert(className == thisClassName)
      assert(formatVersion == thisFormatVersion)

      val dataFrame = spark.read.parquet(Loader.dataPath(path))
      val dataArray = dataFrame.select("feature")

      // Check schema explicitly since erasure makes it hard to use match-case for checking.
      Loader.checkSchema[Data](dataFrame.schema)

      val features = dataArray.rdd.map {
        case Row(feature: Int) => (feature)
      }.collect()

      new ChiSqSelectorModel(features)
    }
  }
}

/**
 * Creates a ChiSquared feature selector.
 * @param numTopFeatures number of features that selector will select
 *                       (ordered by statistic value descending)
 *                       Note that if the number of features is less than numTopFeatures,
 *                       then this will select all features.
 */
@Since("1.3.0")
class ChiSqSelector @Since("1.3.0") (
  @Since("1.3.0") val numTopFeatures: Int) extends Serializable {

  /**
   * Returns a ChiSquared feature selector.
   *
   * @param data an `RDD[LabeledPoint]` containing the labeled dataset with categorical features.
   *             Real-valued features will be treated as categorical for each distinct value.
   *             Apply feature discretizer before using this function.
   */
  @Since("1.3.0")
  def fit(data: RDD[LabeledPoint]): ChiSqSelectorModel = {
    val indices = Statistics.chiSqTest(data)
      .zipWithIndex.sortBy { case (res, _) => -res.statistic }
      .take(numTopFeatures)
      .map { case (_, indices) => indices }
      .sorted
    new ChiSqSelectorModel(indices)
  }
}
/**
 * Creates a ChiSquared feature selector by Percentile.
 * @param percentile percentage of features that selector will select
 *                   (ordered by statistic value descending)
 *                   Note that if the percentile is larger than 100,
 *                   then this will select all features.
 */
@Since("2.0.0")
class PercentileChiSqSelector @Since("2.0.0") (
  @Since("2.0.0") val percentile: Int) extends Serializable {

  /**
   * Returns a ChiSquared feature selector.
   *
   * @param data an `RDD[LabeledPoint]` containing the labeled dataset with categorical features.
   *             Real-valued features will be treated as categorical for each distinct value.
   *             Apply feature discretizer before using this function.
   */
  @Since("2.0.0")
  def fit(data: RDD[LabeledPoint]): ChiSqSelectorModel = {
    val indices = Statistics.chiSqTest(data)
      .zipWithIndex.sortBy { case (res, _) => -res.statistic }
      .take((data.count() * percentile / 100).toInt)
      .map { case (_, indices) => indices }
      .sorted
    new ChiSqSelectorModel(indices)
  }
}
