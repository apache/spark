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
package org.apache.spark.ml.optim.aggregator

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext

class LeastSquaresAggregatorSuite extends SparkFunSuite with MLlibTestSparkContext {

  import DifferentiableLossAggregatorSuite.getRegressionSummarizers

  @transient var instances: Array[Instance] = _
  @transient var instancesConstantFeature: Array[Instance] = _
  @transient var instancesConstantLabel: Array[Instance] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    instances = Array(
      Instance(0.0, 0.1, Vectors.dense(1.0, 2.0)),
      Instance(1.0, 0.5, Vectors.dense(1.5, 1.0)),
      Instance(2.0, 0.3, Vectors.dense(4.0, 0.5))
    )
    instancesConstantFeature = Array(
      Instance(0.0, 0.1, Vectors.dense(1.0, 2.0)),
      Instance(1.0, 0.5, Vectors.dense(1.0, 1.0)),
      Instance(2.0, 0.3, Vectors.dense(1.0, 0.5))
    )
    instancesConstantLabel = Array(
      Instance(1.0, 0.1, Vectors.dense(1.0, 2.0)),
      Instance(1.0, 0.5, Vectors.dense(1.5, 1.0)),
      Instance(1.0, 0.3, Vectors.dense(4.0, 0.5))
    )
  }

  /** Get summary statistics for some data and create a new LeastSquaresAggregator. */
  private def getNewAggregator(
      instances: Array[Instance],
      coefficients: Vector,
      fitIntercept: Boolean): LeastSquaresAggregator = {
    val (featuresSummarizer, ySummarizer) = getRegressionSummarizers(instances)
    val yStd = math.sqrt(ySummarizer.variance(0))
    val yMean = ySummarizer.mean(0)
    val featuresStd = featuresSummarizer.variance.toArray.map(math.sqrt)
    val bcFeaturesStd = spark.sparkContext.broadcast(featuresStd)
    val featuresMean = featuresSummarizer.mean
    val bcFeaturesMean = spark.sparkContext.broadcast(featuresMean.toArray)
    val bcCoefficients = spark.sparkContext.broadcast(coefficients)
    new LeastSquaresAggregator(yStd, yMean, fitIntercept, bcFeaturesStd,
      bcFeaturesMean)(bcCoefficients)
  }

  test("aggregator add method input size") {
    val coefficients = Vectors.dense(1.0, 2.0)
    val agg = getNewAggregator(instances, coefficients, fitIntercept = true)
    withClue("LeastSquaresAggregator features dimension must match coefficients dimension") {
      intercept[IllegalArgumentException] {
        agg.add(Instance(1.0, 1.0, Vectors.dense(2.0)))
      }
    }
  }

  test("negative weight") {
    val coefficients = Vectors.dense(1.0, 2.0)
    val agg = getNewAggregator(instances, coefficients, fitIntercept = true)
    withClue("LeastSquaresAggregator does not support negative instance weights.") {
      intercept[IllegalArgumentException] {
        agg.add(Instance(1.0, -1.0, Vectors.dense(2.0, 1.0)))
      }
    }
  }

  test("check sizes") {
    val coefficients = Vectors.dense(1.0, 2.0)
    val aggIntercept = getNewAggregator(instances, coefficients, fitIntercept = true)
    val aggNoIntercept = getNewAggregator(instances, coefficients, fitIntercept = false)
    instances.foreach(aggIntercept.add)
    instances.foreach(aggNoIntercept.add)

    // least squares agg does not include intercept in its gradient array
    assert(aggIntercept.gradient.size === 2)
    assert(aggNoIntercept.gradient.size === 2)
  }

  test("check correctness") {
    /*
    Check that the aggregator computes loss/gradient for:
      0.5 * sum_i=1^N ([sum_j=1^D beta_j * ((x_j - x_j,bar) / sigma_j)] - ((y - ybar) / sigma_y))^2
     */
    val coefficients = Vectors.dense(1.0, 2.0)
    val numFeatures = coefficients.size
    val (featuresSummarizer, ySummarizer) = getRegressionSummarizers(instances)
    val featuresStd = featuresSummarizer.variance.toArray.map(math.sqrt)
    val featuresMean = featuresSummarizer.mean.toArray
    val yStd = math.sqrt(ySummarizer.variance(0))
    val yMean = ySummarizer.mean(0)

    val agg = getNewAggregator(instances, coefficients, fitIntercept = true)
    instances.foreach(agg.add)

    // compute (y - pred) analytically
    val errors = instances.map { case Instance(l, w, f) =>
      val scaledFeatures = (0 until numFeatures).map { j =>
        (f.toArray(j) - featuresMean(j)) / featuresStd(j)
      }.toArray
      val scaledLabel = (l - yMean) / yStd
      BLAS.dot(coefficients, Vectors.dense(scaledFeatures)) - scaledLabel
    }

    // compute expected loss sum analytically
    val expectedLoss = errors.zip(instances).map { case (error, instance) =>
      instance.weight * error * error / 2.0
    }

    // compute gradient analytically from instances
    val expectedGradient = Vectors.dense(0.0, 0.0)
    errors.zip(instances).foreach { case (error, instance) =>
      val scaledFeatures = (0 until numFeatures).map { j =>
        instance.weight * instance.features.toArray(j) / featuresStd(j)
      }.toArray
      BLAS.axpy(error, Vectors.dense(scaledFeatures), expectedGradient)
    }

    val weightSum = instances.map(_.weight).sum
    BLAS.scal(1.0 / weightSum, expectedGradient)
    assert(agg.loss ~== (expectedLoss.sum / weightSum) relTol 1e-5)
    assert(agg.gradient ~== expectedGradient relTol 1e-5)
  }

  test("check with zero standard deviation") {
    val coefficients = Vectors.dense(1.0, 2.0)
    val aggConstantFeature = getNewAggregator(instancesConstantFeature, coefficients,
      fitIntercept = true)
    instances.foreach(aggConstantFeature.add)
    // constant features should not affect gradient
    assert(aggConstantFeature.gradient(0) === 0.0)

    withClue("LeastSquaresAggregator does not support zero standard deviation of the label") {
      intercept[IllegalArgumentException] {
        getNewAggregator(instancesConstantLabel, coefficients, fitIntercept = true)
      }
    }
  }
}
