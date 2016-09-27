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
 * @param selectedFeatures list of indices to select (filter).
 */
@Since("1.3.0")
class ChiSqSelectorModel @Since("1.3.0") (
  @Since("1.3.0") val selectedFeatures: Array[Int]) extends VectorTransformer with Saveable {

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
   * @param filterIndices indices of features to filter
   */
  private def compress(features: Vector, filterIndices: Array[Int]): Vector = {
    val orderedIndices = filterIndices.sorted
    features match {
      case SparseVector(size, indices, values) =>
        val newSize = orderedIndices.length
        val newValues = new ArrayBuilder.ofDouble
        val newIndices = new ArrayBuilder.ofInt
        var i = 0
        var j = 0
        var indicesIdx = 0
        var filterIndicesIdx = 0
        while (i < indices.length && j < orderedIndices.length) {
          indicesIdx = indices(i)
          filterIndicesIdx = orderedIndices(j)
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
        Vectors.dense(orderedIndices.map(i => values(i)))
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
 * The selector supports three selection methods: `kbest`, `percentile` and `fpr`.
 * `kbest` chooses the `k` top features according to a chi-squared test.
 * `percentile` is similar but chooses a fraction of all features instead of a fixed number.
 * `fpr` select features based on a false positive rate test.
 * `fdr` select features based on an estimated false discovery rate.
 * `fwe` select features based on family-wise error rate.
 * By default, the selection method is `kbest`, the default number of top features is 50.
 */
@Since("1.3.0")
class ChiSqSelector @Since("2.1.0") () extends Serializable {
  var numTopFeatures: Int = 50
  var percentile: Double = 0.1
  var alphaFPR: Double = 0.05
  var alphaFDR: Double = 0.05
  var alphaFWE: Double = 0.05
  var selectorType = ChiSqSelector.KBest

  /**
   * The is the same to call this() and setNumTopFeatures(numTopFeatures)
   */
  @Since("1.3.0")
  def this(numTopFeatures: Int) {
    this()
    this.numTopFeatures = numTopFeatures
  }

  @Since("1.6.0")
  def setNumTopFeatures(value: Int): this.type = {
    numTopFeatures = value
    this
  }

  @Since("2.1.0")
  def setPercentile(value: Double): this.type = {
    require(0.0 <= value && value <= 1.0, "Percentile must be in [0,1]")
    percentile = value
    this
  }

  @Since("2.1.0")
  def setAlphaFPR(value: Double): this.type = {
    require(0.0 <= value && value <= 1.0, "Alpha must be in [0,1]")
    alphaFPR = value
    this
  }

  @Since("2.1.0")
  def setAlphaFDR(value: Double): this.type = {
    require(0.0 <= value && value <= 1.0, "Alpha must be in [0,1]")
    alphaFDR = value
    this
  }

  @Since("2.1.0")
  def setAlphaFWE(value: Double): this.type = {
    require(0.0 <= value && value <= 1.0, "Alpha must be in [0,1]")
    alphaFWE = value
    this
  }

  @Since("2.1.0")
  def setSelectorType(value: String): this.type = {
    require(ChiSqSelector.supportedSelectorTypes.toSeq.contains(value),
      s"ChiSqSelector Type: $value was not supported.")
    selectorType = value
    this
  }

  /**
   * Returns a ChiSquared feature selector.
   *
   * @param data an `RDD[LabeledPoint]` containing the labeled dataset with categorical features.
   *             Real-valued features will be treated as categorical for each distinct value.
   *             Apply feature discretizer before using this function.
   */
  @Since("1.3.0")
  def fit(data: RDD[LabeledPoint]): ChiSqSelectorModel = {
    val chiSqTestResult = Statistics.chiSqTest(data)
      .zipWithIndex
    val features = selectorType match {
      case ChiSqSelector.KBest => chiSqTestResult
        .sortBy { case (res, _) => -res.statistic }
        .take(numTopFeatures)
      case ChiSqSelector.Percentile => chiSqTestResult
        .sortBy { case (res, _) => -res.statistic }
        .take((chiSqTestResult.length * percentile).toInt)
      case ChiSqSelector.FPR => chiSqTestResult
        .filter{ case (res, _) => res.pValue < alphaFPR }
      case ChiSqSelector.FDR =>
        val tempRDD = chiSqTestResult
          .sortBy{ case (res, _) => res.pValue }
        val maxIndex = tempRDD
          .zipWithIndex
          .filter{ case ((res, _), index) =>
            res.pValue <= alphaFDR * (index + 1) / chiSqTestResult.length }
          .map{ case (_, index) => index}
          .max
        tempRDD.take(maxIndex + 1)
      case ChiSqSelector.FWE => chiSqTestResult
        .filter{ case (res, _) => res.pValue < alphaFWE/chiSqTestResult.length }
      case errorType =>
        throw new IllegalStateException(s"Unknown ChiSqSelector Type: $errorType")
    }
    val indices = features.map { case (_, index) => index }
    new ChiSqSelectorModel(indices)
  }
}

@Since("2.1.0")
object ChiSqSelector {

  /** String name for `kbest` selector type. */
  private[spark] val KBest: String = "kbest"

  /** String name for `percentile` selector type. */
  private[spark] val Percentile: String = "percentile"

  /** String name for `fpr` selector type. */
  private[spark] val FPR: String = "fpr"

  /** String name for `fdr` selector type. */
  private[spark] val FDR: String = "fdr"

  /** String name for `fwe` selector type. */
  private[spark] val FWE: String = "fwe"

  /** Set of selector type and param pairs that ChiSqSelector supports. */
  private[spark] val supportedTypeAndParamPairs = Set(KBest -> "numTopFeatures",
    Percentile -> "percentile", FPR -> "alphaFPR", FDR -> "alphaFDR", FWE -> "alphaFWE")

  /** Set of selector types that ChiSqSelector supports. */
  private[spark] val supportedSelectorTypes = supportedTypeAndParamPairs.map(_._1)
}
