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

import scala.collection.mutable

import org.apache.spark.Logging
import org.apache.spark.annotation.{Since, Experimental}
import org.apache.spark.ml._
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.param.{IntParam, _}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol, HasSeed}
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.random.XORShiftRandom

/**
 * Params for [[QuantileDiscretizer]].
 */
private[feature] trait QuantileDiscretizerBase extends Params
  with HasInputCol with HasOutputCol with HasSeed {

  /**
   * Maximum number of buckets (quantiles, or categories) into which data points are grouped. Must
   * be >= 2.
   * default: 2
   * @group param
   */
  val numBuckets = new IntParam(this, "numBuckets", "Maximum number of buckets (quantiles, or " +
    "categories) into which data points are grouped. Must be >= 2.",
    ParamValidators.gtEq(2))
  setDefault(numBuckets -> 2)

  /** @group getParam */
  def getNumBuckets: Int = getOrDefault(numBuckets)
}

/**
 * :: Experimental ::
 * `QuantileDiscretizer` takes a column with continuous features and outputs a column with binned
 * categorical features. The bin ranges are chosen by taking a sample of the data and dividing it
 * into roughly equal parts. The lower and upper bin bounds will be -Infinity and +Infinity,
 * covering all real values. This attempts to find numBuckets partitions based on a sample of data,
 * but it may find fewer depending on the data sample values.
 */
@Experimental
final class QuantileDiscretizer(override val uid: String)
  extends Estimator[Bucketizer] with QuantileDiscretizerBase with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("quantileDiscretizer"))

  /** @group setParam */
  def setNumBuckets(value: Int): this.type = set(numBuckets, value)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setSeed(value: Long): this.type = set(seed, value)

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), DoubleType)
    val inputFields = schema.fields
    require(inputFields.forall(_.name != $(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val attr = NominalAttribute.defaultAttr.withName($(outputCol))
    val outputFields = inputFields :+ attr.toStructField()
    StructType(outputFields)
  }

  override def fit(dataset: DataFrame): Bucketizer = {
    val samples = QuantileDiscretizer
      .getSampledInput(dataset.select($(inputCol)), $(numBuckets), $(seed))
      .map { case Row(feature: Double) => feature }
    val candidates = QuantileDiscretizer.findSplitCandidates(samples, $(numBuckets) - 1)
    val splits = QuantileDiscretizer.getSplits(candidates)
    val bucketizer = new Bucketizer(uid).setSplits(splits)
    copyValues(bucketizer)
  }

  override def copy(extra: ParamMap): QuantileDiscretizer = defaultCopy(extra)
}

@Since("1.6.0")
object QuantileDiscretizer extends DefaultParamsReadable[QuantileDiscretizer] with Logging {

  /**
   * Minimum number of samples required for finding splits, regardless of number of bins.  If
   * the dataset has fewer rows than this value, the entire dataset will be used.
   */
  private[spark] val minSamplesRequired: Int = 10000

  /**
   * Sampling from the given dataset to collect quantile statistics.
   */
  private[feature] def getSampledInput(dataset: DataFrame, numBins: Int, seed: Long): Array[Row] = {
    val totalSamples = dataset.count()
    require(totalSamples > 0,
      "QuantileDiscretizer requires non-empty input dataset but was given an empty input.")
    val requiredSamples = math.max(numBins * numBins, minSamplesRequired)
    val fraction = math.min(requiredSamples.toDouble / dataset.count(), 1.0)
    dataset.sample(withReplacement = false, fraction, new XORShiftRandom(seed).nextInt()).collect()
  }

  /**
   * Compute split points with respect to the sample distribution.
   */
  private[feature]
  def findSplitCandidates(samples: Array[Double], numSplits: Int): Array[Double] = {
    val valueCountMap = samples.foldLeft(Map.empty[Double, Int]) { (m, x) =>
      m + ((x, m.getOrElse(x, 0) + 1))
    }
    val valueCounts = valueCountMap.toSeq.sortBy(_._1).toArray ++ Array((Double.MaxValue, 1))
    val possibleSplits = valueCounts.length - 1
    if (possibleSplits <= numSplits) {
      valueCounts.dropRight(1).map(_._1)
    } else {
      val stride: Double = math.ceil(samples.length.toDouble / (numSplits + 1))
      val splitsBuilder = mutable.ArrayBuilder.make[Double]
      var index = 1
      // currentCount: sum of counts of values that have been visited
      var currentCount = valueCounts(0)._2
      // targetCount: target value for `currentCount`. If `currentCount` is closest value to
      // `targetCount`, then current value is a split threshold. After finding a split threshold,
      // `targetCount` is added by stride.
      var targetCount = stride
      while (index < valueCounts.length) {
        val previousCount = currentCount
        currentCount += valueCounts(index)._2
        val previousGap = math.abs(previousCount - targetCount)
        val currentGap = math.abs(currentCount - targetCount)
        // If adding count of current value to currentCount makes the gap between currentCount and
        // targetCount smaller, previous value is a split threshold.
        if (previousGap < currentGap) {
          splitsBuilder += valueCounts(index - 1)._1
          targetCount += stride
        }
        index += 1
      }
      splitsBuilder.result()
    }
  }

  /**
   * Adjust split candidates to proper splits by: adding positive/negative infinity to both sides as
   * needed, and adding a default split value of 0 if no good candidates are found.
   */
  private[feature] def getSplits(candidates: Array[Double]): Array[Double] = {
    val effectiveValues = if (candidates.size != 0) {
      if (candidates.head == Double.NegativeInfinity
        && candidates.last == Double.PositiveInfinity) {
        candidates.drop(1).dropRight(1)
      } else if (candidates.head == Double.NegativeInfinity) {
        candidates.drop(1)
      } else if (candidates.last == Double.PositiveInfinity) {
        candidates.dropRight(1)
      } else {
        candidates
      }
    } else {
      candidates
    }

    if (effectiveValues.size == 0) {
      Array(Double.NegativeInfinity, 0, Double.PositiveInfinity)
    } else {
      Array(Double.NegativeInfinity) ++ effectiveValues ++ Array(Double.PositiveInfinity)
    }
  }

  @Since("1.6.0")
  override def load(path: String): QuantileDiscretizer = super.load(path)
}
