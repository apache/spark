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

import org.apache.spark.SparkContext._
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 * Contingency feature-class table calculator for discrete features
 */
@Experimental
private[feature] trait ContingencyTableCalculator extends java.io.Serializable {

  /**
   * Returns the map of label to index
   * @param discreteData labeled discrete data
   */
  private def indexByLabelMap(discreteData: RDD[LabeledPoint]) = {
    discreteData.map(labeledPoint =>
      labeledPoint.label).distinct.collect.sorted.zipWithIndex.toMap
  }

  /**
   * Returns a map of feature index to the list of its values
   * @param discreteData labeled discrete data
   */
  private def enumerateValues(discreteData: RDD[LabeledPoint]) = {
    discreteData.flatMap(labeledPoint =>
      (0 to labeledPoint.features.size).zip(labeledPoint.features.toArray))
      .distinct()
      .combineByKey[List[Double]](
        createCombiner = (value: Double) => List(value),
        mergeValue = (c: List[Double], value: Double) => value :: c,
        mergeCombiners = (c1: List[Double], c2: List[Double]) => c1 ::: c2
      ).collectAsMap()
  }

  /**
   * Returns an RDD of feature (index, contingency table) pairs
   * @param discreteData labeled discrete data
   */
  protected def tables(discreteData: RDD[LabeledPoint]): RDD[(Int, Array[Array[Int]])] = {
    val indexByLabel = indexByLabelMap(discreteData)
    val classesCount = indexByLabel.size
    val valuesByIndex = enumerateValues(discreteData)
    discreteData.flatMap {
      labeledPoint =>
        labeledPoint.features.toArray.zipWithIndex.map {
          case (featureValue, featureIndex) =>
            /** array of feature value presence in a class */
            val featureValues = valuesByIndex(featureIndex)
            val valuesCount = featureValues.size
            val featureValueIndex = featureValues.indexOf(featureValue)
            val labelIndex = indexByLabel(labeledPoint.label)
            val counts = Array.ofDim[Int](valuesCount, classesCount)
            counts(featureValueIndex)(labelIndex) = 1
            (featureIndex, counts)
        }
    }.reduceByKey {
      case (x, y) =>
        x.zip(y).map { case (row1, row2) =>
          row1.zip(row2).map { case (a, b) =>
            a + b}}
    }
  }
}

/**
 * :: Experimental ::
 * Calculates chi-squared value based on contingency table
 */
@Experimental
private[feature] object ChiSquared {

  /**
   * Returns chi-squared value for a given contingency table
   * @param contingencyTable contingency table
   */
  def apply(contingencyTable: Array[Array[Int]]): Double = {
    /** probably use List instead of Array or breeze matrices */
    val columns = contingencyTable(0).size
    val rowSums = contingencyTable.map(row =>
      row.sum)
    val columnSums = contingencyTable.fold(Array.ofDim[Int](columns)) { case (ri, rj) =>
      ri.zip(rj).map { case (a1, b1) =>
        a1 + b1
      }
    }
    val tableSum = rowSums.sum
    val rowRatios = rowSums.map(_.toDouble / tableSum)
    val expectedTable = rowRatios.map(a =>
      columnSums.map(_ * a))
    contingencyTable.zip(expectedTable).foldLeft(0.0) { case (sum, (obsRow, expRow)) =>
      obsRow.zip(expRow).map { case (oValue, eValue) =>
        sqr(oValue - eValue) / eValue
      }.sum + sum
    }
  }

  /**
   * Returns squared value
   * @param x value
   */
  private def sqr(x: Double): Double = x * x
}

/**
 * :: Experimental ::
 * Performes chi-square feature selection for a given discrete dataset.
 * Filters a given number of features sorted by chi-squared descending
 * @param labeledDiscreteData data
 * @param numTopFeatures top N features sorted by chi-square to keep
 */
@Experimental
class ChiSquaredFeatureSelection(labeledDiscreteData: RDD[LabeledPoint], numTopFeatures: Int)
  extends java.io.Serializable with LabeledPointFeatureFilter with ContingencyTableCalculator {

  override def data: RDD[LabeledPoint] = labeledDiscreteData

  override def select: Set[Int] = {
    tables(data).map { case (featureIndex, contTable) =>
      (featureIndex, ChiSquared(contTable))
    }.collect().sortBy(-_._2).take(numTopFeatures).unzip._1.toSet
  }
}
