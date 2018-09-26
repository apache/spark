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

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Chi Squared selector model.
 *
 * @param selectedFeatures list of indices to select (filter).
 */
@Since("1.3.0")
class ChiSqSelectorModel @Since("1.3.0") (
  @Since("1.3.0") val selectedFeatures: Array[Int]) extends VectorTransformer with Saveable {

  private val filterIndices = selectedFeatures.sorted

  @deprecated("not intended for subclasses to use", "2.1.0")
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
    compress(vector)
  }

  /**
   * Returns a vector with features filtered.
   * Preserves the order of filtered features the same as their indices are stored.
   * Might be moved to Vector as .slice
   * @param features vector
   */
  private def compress(features: Vector): Vector = {
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
      spark.createDataFrame(sc.makeRDD(dataArray, 1)).write.parquet(Loader.dataPath(path))
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
        case Row(feature: Int) => feature
      }.collect()

      new ChiSqSelectorModel(features)
    }
  }
}

/**
 * Creates a ChiSquared feature selector.
 * The selector supports different selection methods: `numTopFeatures`, `percentile`, `fpr`,
 * `fdr`, `fwe`.
 *  - `numTopFeatures` chooses a fixed number of top features according to a chi-squared test.
 *  - `percentile` is similar but chooses a fraction of all features instead of a fixed number.
 *  - `fpr` chooses all features whose p-values are below a threshold, thus controlling the false
 *    positive rate of selection.
 *  - `fdr` uses the [Benjamini-Hochberg procedure]
 *    (https://en.wikipedia.org/wiki/False_discovery_rate#Benjamini.E2.80.93Hochberg_procedure)
 *    to choose all features whose false discovery rate is below a threshold.
 *  - `fwe` chooses all features whose p-values are below a threshold. The threshold is scaled by
 *    1/numFeatures, thus controlling the family-wise error rate of selection.
 * By default, the selection method is `numTopFeatures`, with the default number of top features
 * set to 50.
 */
@Since("1.3.0")
class ChiSqSelector @Since("2.1.0") () extends Serializable {
  var numTopFeatures: Int = 50
  var percentile: Double = 0.1
  var fpr: Double = 0.05
  var fdr: Double = 0.05
  var fwe: Double = 0.05
  var selectorType = ChiSqSelector.NumTopFeatures

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
  def setFpr(value: Double): this.type = {
    require(0.0 <= value && value <= 1.0, "FPR must be in [0,1]")
    fpr = value
    this
  }

  @Since("2.2.0")
  def setFdr(value: Double): this.type = {
    require(0.0 <= value && value <= 1.0, "FDR must be in [0,1]")
    fdr = value
    this
  }

  @Since("2.2.0")
  def setFwe(value: Double): this.type = {
    require(0.0 <= value && value <= 1.0, "FWE must be in [0,1]")
    fwe = value
    this
  }

  @Since("2.1.0")
  def setSelectorType(value: String): this.type = {
    require(ChiSqSelector.supportedSelectorTypes.contains(value),
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
    val chiSqTestResult = Statistics.chiSqTest(data).zipWithIndex
    val features = selectorType match {
      case ChiSqSelector.NumTopFeatures =>
        chiSqTestResult
          .sortBy { case (res, _) => res.pValue }
          .take(numTopFeatures)
      case ChiSqSelector.Percentile =>
        chiSqTestResult
          .sortBy { case (res, _) => res.pValue }
          .take((chiSqTestResult.length * percentile).toInt)
      case ChiSqSelector.FPR =>
        chiSqTestResult
          .filter { case (res, _) => res.pValue < fpr }
      case ChiSqSelector.FDR =>
        // This uses the Benjamini-Hochberg procedure.
        // https://en.wikipedia.org/wiki/False_discovery_rate#Benjamini.E2.80.93Hochberg_procedure
        val tempRes = chiSqTestResult
          .sortBy { case (res, _) => res.pValue }
        val selected = tempRes
          .zipWithIndex
          .filter { case ((res, _), index) =>
            res.pValue <= fdr * (index + 1) / chiSqTestResult.length }
        if (selected.isEmpty) {
          Array.empty[(ChiSqTestResult, Int)]
        } else {
          val maxIndex = selected.map(_._2).max
          tempRes.take(maxIndex + 1)
        }
      case ChiSqSelector.FWE =>
        chiSqTestResult
          .filter { case (res, _) => res.pValue < fwe / chiSqTestResult.length }
      case errorType =>
        throw new IllegalStateException(s"Unknown ChiSqSelector Type: $errorType")
    }
    val indices = features.map { case (_, index) => index }
    new ChiSqSelectorModel(indices)
  }
}

private[spark] object ChiSqSelector {

  /** String name for `numTopFeatures` selector type. */
  private[spark] val NumTopFeatures: String = "numTopFeatures"

  /** String name for `percentile` selector type. */
  private[spark] val Percentile: String = "percentile"

  /** String name for `fpr` selector type. */
  private[spark] val FPR: String = "fpr"

  /** String name for `fdr` selector type. */
  private[spark] val FDR: String = "fdr"

  /** String name for `fwe` selector type. */
  private[spark] val FWE: String = "fwe"


  /** Set of selector types that ChiSqSelector supports. */
  val supportedSelectorTypes: Array[String] = Array(NumTopFeatures, Percentile, FPR, FDR, FWE)
}
