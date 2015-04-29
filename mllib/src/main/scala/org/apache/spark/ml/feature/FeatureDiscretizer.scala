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

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.util.random.XORShiftRandom

/**
 * :: AlphaComponent ::
 * `FeatureDiscretizer` takes a column with continuous features and outputs a column with binned
 * categorical features.
 */
@AlphaComponent
class FeatureDiscretizer extends Transformer with HasInputCol with HasOutputCol {

  /**
   * Number of bins to collect data points, which should be a positive integer.
   * @group param
   */
  val numBins = new IntParam(this, "numBins",
    "Number of bins to collect data points, which should be a positive integer.")
  setDefault(numBins -> 1)

  /** @group getParam */
  def getNumBins: Int = getOrDefault(numBins)

  /** @group setParam */
  def setNumBins(value: Int): this.type = set(numBins, value)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = extractParamMap(paramMap)
    assert(map(numBins) >= 1, "Number of bins should be a positive integer.")
    SchemaUtils.checkColumnType(schema, map(inputCol), DoubleType)
    val inputFields = schema.fields
    val outputColName = map(outputCol)
    require(inputFields.forall(_.name != outputColName),
      s"Output column $outputColName already exists.")
    val attr = NominalAttribute.defaultAttr.withName(outputColName)
    val outputFields = inputFields :+ attr.toStructField()
    StructType(outputFields)
  }

  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    transformSchema(dataset.schema, paramMap)
    val map = extractParamMap(paramMap)
    val input = dataset.select(map(inputCol)).map { case Row(feature: Double) => feature }
    val samples = getSampledInput(input, map(numBins))
    val splits = findSplits(samples, map(numBins) - 1)
    val discretizer = udf { feature: Double => binarySearchForBins(splits, feature) }
    val outputColName = map(outputCol)
    val metadata = NominalAttribute.defaultAttr
      .withName(outputColName).withValues(splits.map(_.toString)).toMetadata()
    dataset.select(col("*"),
      discretizer(dataset(map(inputCol))).as(outputColName, metadata))
  }

  /**
   * Binary searching in several bins to place each data point.
   */
  private def binarySearchForBins(splits: Array[Double], feature: Double): Double = {
    val wrappedSplits = Array(Double.MinValue) ++ splits ++ Array(Double.MaxValue)
    var left = 0
    var right = wrappedSplits.length - 2
    while (left <= right) {
      val mid = left + (right - left) / 2
      val split = wrappedSplits(mid)
      if ((feature > split) && (feature <= wrappedSplits(mid + 1))) {
        return mid
      } else if (feature <= split) {
        right = mid - 1
      } else {
        left = mid + 1
      }
    }
    -1
  }

  /**
   * Sampling from the given dataset to collect quantile statistics.
   */
  private def getSampledInput(dataset: RDD[Double], numBins: Int): Array[Double] = {
    val totalSamples = dataset.count()
    assert(totalSamples > 0)
    val requiredSamples = math.max(numBins * numBins, 10000)
    val fraction = math.min(requiredSamples / dataset.count(), 1.0)
    dataset.sample(withReplacement = false, fraction, new XORShiftRandom().nextInt()).collect()
  }

  /**
   * Compute split points with respect to the sample distribution.
   */
  private def findSplits(samples: Array[Double], numSplits: Int): Array[Double] = {
    val valueCountMap = samples.foldLeft(Map.empty[Double, Int]) { (m, x) =>
      m + ((x, m.getOrElse(x, 0) + 1))
    }
    val valueCounts = valueCountMap.toSeq.sortBy(_._1).toArray
    val possibleSplits = valueCounts.length
    if (possibleSplits <= numSplits) {
      valueCounts.map(_._1)
    } else {
      val stride: Double = samples.length.toDouble / (numSplits + 1)
      val splitsBuilder = mutable.ArrayBuilder.make[Double]
      var index = 1
      // currentCount: sum of counts of values that have been visited
      var currentCount = valueCounts(0)._2
      // targetCount: target value for `currentCount`.
      // If `currentCount` is closest value to `targetCount`,
      // then current value is a split threshold.
      // After finding a split threshold, `targetCount` is added by stride.
      var targetCount = stride
      while (index < valueCounts.length) {
        val previousCount = currentCount
        currentCount += valueCounts(index)._2
        val previousGap = math.abs(previousCount - targetCount)
        val currentGap = math.abs(currentCount - targetCount)
        // If adding count of current value to currentCount
        // makes the gap between currentCount and targetCount smaller,
        // previous value is a split threshold.
        if (previousGap < currentGap) {
          splitsBuilder += valueCounts(index - 1)._1
          targetCount += stride
        }
        index += 1
      }
      splitsBuilder.result()
    }
  }
}

