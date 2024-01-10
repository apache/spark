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

package org.apache.spark.ml.stat

import org.apache.commons.math3.distribution.FDistribution

import org.apache.spark.annotation.Since
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.util.collection.OpenHashMap


/**
 * ANOVA Test for continuous data.
 *
 * See <a href="https://en.wikipedia.org/wiki/Analysis_of_variance">Wikipedia</a> for more
 * information on ANOVA test.
 */
@Since("3.1.0")
private[ml] object ANOVATest {

  /**
   * @param dataset  DataFrame of categorical labels and continuous features.
   * @param featuresCol  Name of features column in dataset, of type `Vector` (`VectorUDT`)
   * @param labelCol  Name of label column in dataset, of any numerical type
   * @return DataFrame containing the test result for every feature against the label.
   *         This DataFrame will contain a single Row with the following fields:
   *          - `pValues: Vector`
   *          - `degreesOfFreedom: Array[Long]`
   *          - `fValues: Vector`
   *         Each of these fields has one value per feature.
   */
  @Since("3.1.0")
  def test(dataset: DataFrame, featuresCol: String, labelCol: String): DataFrame = {
    test(dataset, featuresCol, labelCol, false)
  }

  /**
   * @param dataset  DataFrame of categorical labels and continuous features.
   * @param featuresCol  Name of features column in dataset, of type `Vector` (`VectorUDT`)
   * @param labelCol  Name of label column in dataset, of any numerical type
   * @param flatten  If false, the returned DataFrame contains only a single Row, otherwise, one
   *                 row per feature.
   */
  @Since("3.1.0")
  def test(
      dataset: DataFrame,
      featuresCol: String,
      labelCol: String,
      flatten: Boolean): DataFrame = {
    val spark = dataset.sparkSession
    import spark.implicits._

    val resultDF = testClassification(dataset, featuresCol, labelCol)
      .toDF("featureIndex", "pValue", "degreesOfFreedom", "fValue")

    if (flatten) {
      resultDF
    } else {
      resultDF.agg(collect_list(struct("*")))
        .as[Seq[(Int, Double, Long, Double)]]
        .map { seq =>
          val results = seq.toArray.sortBy(_._1)
          val pValues = Vectors.dense(results.map(_._2))
          val degreesOfFreedom = results.map(_._3)
          val fValues = Vectors.dense(results.map(_._4))
          (pValues, degreesOfFreedom, fValues)
        }.toDF("pValues", "degreesOfFreedom", "fValues")
    }
  }

  /**
   * @param dataset  DataFrame of categorical labels and continuous features.
   * @param featuresCol  Name of features column in dataset, of type `Vector` (`VectorUDT`)
   * @param labelCol  Name of label column in dataset, of any numerical type
   */
  private[ml] def testClassification(
      dataset: Dataset[_],
      featuresCol: String,
      labelCol: String): RDD[(Int, Double, Long, Double)] = {
    val spark = dataset.sparkSession
    import spark.implicits._

    SchemaUtils.checkColumnType(dataset.schema, featuresCol, new VectorUDT)
    SchemaUtils.checkNumericType(dataset.schema, labelCol)

    val points = dataset.select(col(labelCol).cast("double"), col(featuresCol))
      .as[(Double, Vector)].rdd

    points.first()._2 match {
      case dv: DenseVector =>
        testClassificationDenseFeatures(points, dv.size)
      case sv: SparseVector =>
        testClassificationSparseFeatures(points, sv.size)
    }
  }

  private def testClassificationDenseFeatures(
      points: RDD[(Double, Vector)],
      numFeatures: Int): RDD[(Int, Double, Long, Double)] = {
    points.flatMap { case (label, features) =>
      require(features.size == numFeatures,
        s"Number of features must be $numFeatures but got ${features.size}")
      features.iterator.map { case (col, value) => (col, (label, value)) }
    }.aggregateByKey[(Double, Double, OpenHashMap[Double, Double], OpenHashMap[Double, Long])](
      (0.0, 0.0, new OpenHashMap[Double, Double], new OpenHashMap[Double, Long]))(
      seqOp = {
        case ((sum, sumOfSq, sums, counts), (label, value)) =>
          // sums: mapOfSumPerClass (key: label, value: sum of features for each label)
          // counts: mapOfCountPerClass key: label, value: count of features for each label
          sums.changeValue(label, value, _ + value)
          counts.changeValue(label, 1L, _ + 1L)
          (sum + value, sumOfSq + value * value, sums, counts)
      },
      combOp = {
        case ((sum1, sumOfSq1, sums1, counts1), (sum2, sumOfSq2, sums2, counts2)) =>
          sums2.foreach { case (v, w) => sums1.changeValue(v, w, _ + w) }
          counts2.foreach { case (v, w) => counts1.changeValue(v, w, _ + w) }
          (sum1 + sum2, sumOfSq1 + sumOfSq2, sums1, counts1)
      }
    ).map { case (col, (sum, sumOfSq, sums, counts)) =>
      val (pValue, degreesOfFreedom, fValue) =
        computeANOVA(sum, sumOfSq, sums.toMap, counts.toMap)
      (col, pValue, degreesOfFreedom, fValue)
    }
  }

  private def testClassificationSparseFeatures(
      points: RDD[(Double, Vector)],
      numFeatures: Int): RDD[(Int, Double, Long, Double)] = {
    val counts = points.map(_._1).countByValue().toMap

    val numParts = points.getNumPartitions
    points.mapPartitionsWithIndex { case (pid, iter) =>
      iter.flatMap { case (label, features) =>
        require(features.size == numFeatures,
          s"Number of features must be $numFeatures but got ${features.size}")
        features.nonZeroIterator.map { case (col, value) => (col, (label, value)) }
      } ++ {
        // append this to make sure that all columns are taken into account
        Iterator.range(pid, numFeatures, numParts).map(col => (col, null))
      }
    }.aggregateByKey[(Double, Double, OpenHashMap[Double, Double])](
      (0.0, 0.0, new OpenHashMap[Double, Double]))(
      seqOp = {
        case ((sum, sumOfSq, sums), labelAndValue) =>
          // sums: mapOfSumPerClass (key: label, value: sum of features for each label)
          if (labelAndValue != null) {
            val (label, value) = labelAndValue
            sums.changeValue(label, value, _ + value)
            (sum + value, sumOfSq + value * value, sums)
          } else {
            (sum, sumOfSq, sums)
          }
      },
      combOp = {
        case ((sum1, sumOfSq1, sums1), (sum2, sumOfSq2, sums2)) =>
          sums2.foreach { case (v, w) => sums1.changeValue(v, w, _ + w) }
          (sum1 + sum2, sumOfSq1 + sumOfSq2, sums1)
      }
    ).map { case (col, (sum, sumOfSq, sums)) =>
      counts.keysIterator.foreach { label =>
        // adjust sums if all related feature values are 0 for some label
        if (!sums.contains(label)) sums.update(label, 0.0)
      }
      val (pValue, degreesOfFreedom, fValue) =
        computeANOVA(sum, sumOfSq, sums.toMap, counts)
      (col, pValue, degreesOfFreedom, fValue)
    }
  }

  private def computeANOVA(
      sum: Double,
      sumOfSq: Double,
      sums: Map[Double, Double],
      counts: Map[Double, Long]): (Double, Long, Double) = {
    val numSamples = counts.valuesIterator.sum
    val numClasses = counts.size

    // e.g. features are [3.3, 2.5, 1.0, 3.0, 2.0] and labels are [1, 2, 1, 3, 3]
    // sum: sum of all the features (3.3+2.5+1.0+3.0+2.0)
    // sumOfSq: sum of squares of all the features (3.3^2+2.5^2+1.0^2+3.0^2+2.0^2)
    // sums: mapOfSumPerClass (key: label, value: sum of features for each label)
    //                                         ( 1 -> 3.3 + 1.0, 2 -> 2.5, 3 -> 3.0 + 2.0 )
    // counts: mapOfCountPerClass (key: label, value: count of features for each label)
    //                                         ( 1 -> 2, 2 -> 2, 3 -> 2 )
    // sqSum: square of sum of all data ((3.3+2.5+1.0+3.0+2.0)^2)
    val sqSum = sum * sum
    val ssTot = sumOfSq - sqSum / numSamples

    // totalSqSum:
    //     sum( sq_sum_classes[k] / n_samples_per_class[k] for k in range(n_classes))
    //     e.g. ((3.3+1.0)^2 / 2 + 2.5^2 / 1 + (3.0+2.0)^2 / 2)
    val totalSqSum = sums.iterator
      .map { case (label, sum) => sum * sum / counts(label) }.sum
    // Sums of Squares Between
    val ssbn = totalSqSum - (sqSum / numSamples)
    // Sums of Squares Within
    val sswn = ssTot - ssbn
    // degrees of freedom between
    val dfbn = numClasses - 1
    // degrees of freedom within
    val dfwn = numSamples - numClasses
    // mean square between
    val msb = ssbn / dfbn
    // mean square within
    val msw = sswn / dfwn
    val fValue = msb / msw
    val pValue = 1 - new FDistribution(dfbn.toDouble, dfwn.toDouble).cumulativeProbability(fValue)
    val degreesOfFreedom = dfbn + dfwn
    (pValue, degreesOfFreedom, fValue)
  }
}
