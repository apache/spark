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
import org.apache.spark.ml.linalg.{BLAS, Matrices, Vector, Vectors}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext

class LogisticAggregatorSuite extends SparkFunSuite with MLlibTestSparkContext {

  import DifferentiableLossAggregatorSuite.getClassificationSummarizers

  @transient var instances: Array[Instance] = _
  @transient var instancesConstantFeature: Array[Instance] = _
  @transient var instancesConstantFeatureFiltered: Array[Instance] = _

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
    instancesConstantFeatureFiltered = Array(
      Instance(0.0, 0.1, Vectors.dense(2.0)),
      Instance(1.0, 0.5, Vectors.dense(1.0)),
      Instance(2.0, 0.3, Vectors.dense(0.5))
    )
  }

  /** Get summary statistics for some data and create a new LogisticAggregator. */
  private def getNewAggregator(
      instances: Array[Instance],
      coefficients: Vector,
      fitIntercept: Boolean,
      isMultinomial: Boolean): LogisticAggregator = {
    val (featuresSummarizer, ySummarizer) =
      DifferentiableLossAggregatorSuite.getClassificationSummarizers(instances)
    val numClasses = ySummarizer.histogram.length
    val featuresStd = featuresSummarizer.variance.toArray.map(math.sqrt)
    val bcFeaturesStd = spark.sparkContext.broadcast(featuresStd)
    val bcCoefficients = spark.sparkContext.broadcast(coefficients)
    new LogisticAggregator(bcFeaturesStd, numClasses, fitIntercept, isMultinomial)(bcCoefficients)
  }

  test("aggregator add method input size") {
    val coefArray = Array(1.0, 2.0, -2.0, 3.0, 0.0, -1.0)
    val interceptArray = Array(4.0, 2.0, -3.0)
    val agg = getNewAggregator(instances, Vectors.dense(coefArray ++ interceptArray),
      fitIntercept = true, isMultinomial = true)
    withClue("LogisticAggregator features dimension must match coefficients dimension") {
      intercept[IllegalArgumentException] {
        agg.add(Instance(1.0, 1.0, Vectors.dense(2.0)))
      }
    }
  }

  test("negative weight") {
    val coefArray = Array(1.0, 2.0, -2.0, 3.0, 0.0, -1.0)
    val interceptArray = Array(4.0, 2.0, -3.0)
    val agg = getNewAggregator(instances, Vectors.dense(coefArray ++ interceptArray),
      fitIntercept = true, isMultinomial = true)
    withClue("LogisticAggregator does not support negative instance weights") {
      intercept[IllegalArgumentException] {
        agg.add(Instance(1.0, -1.0, Vectors.dense(2.0, 1.0)))
      }
    }
  }

  test("check sizes multinomial") {
    val rng = new scala.util.Random
    val numFeatures = instances.head.features.size
    val numClasses = instances.map(_.label).toSet.size
    val coefWithIntercept = Vectors.dense(
      Array.fill(numClasses * (numFeatures + 1))(rng.nextDouble))
    val coefWithoutIntercept = Vectors.dense(
      Array.fill(numClasses * numFeatures)(rng.nextDouble))
    val aggIntercept = getNewAggregator(instances, coefWithIntercept, fitIntercept = true,
      isMultinomial = true)
    val aggNoIntercept = getNewAggregator(instances, coefWithoutIntercept, fitIntercept = false,
      isMultinomial = true)
    instances.foreach(aggIntercept.add)
    instances.foreach(aggNoIntercept.add)

    assert(aggIntercept.gradient.size === (numFeatures + 1) * numClasses)
    assert(aggNoIntercept.gradient.size === numFeatures * numClasses)
  }

  test("check sizes binomial") {
    val rng = new scala.util.Random
    val binaryInstances = instances.filter(_.label < 2.0)
    val numFeatures = binaryInstances.head.features.size
    val coefWithIntercept = Vectors.dense(Array.fill(numFeatures + 1)(rng.nextDouble))
    val coefWithoutIntercept = Vectors.dense(Array.fill(numFeatures)(rng.nextDouble))
    val aggIntercept = getNewAggregator(binaryInstances, coefWithIntercept, fitIntercept = true,
      isMultinomial = false)
    val aggNoIntercept = getNewAggregator(binaryInstances, coefWithoutIntercept,
      fitIntercept = false, isMultinomial = false)
    binaryInstances.foreach(aggIntercept.add)
    binaryInstances.foreach(aggNoIntercept.add)

    assert(aggIntercept.gradient.size === numFeatures + 1)
    assert(aggNoIntercept.gradient.size === numFeatures)
  }

  test("check correctness multinomial") {
    /*
    Check that the aggregator computes loss/gradient for:
      -sum_i w_i * (beta_y dot x_i - log(sum_k e^(beta_k dot x_i)))
     */
    val coefArray = Array(1.0, 2.0, -2.0, 3.0, 0.0, -1.0)
    val interceptArray = Array(4.0, 2.0, -3.0)
    val numFeatures = instances.head.features.size
    val numClasses = instances.map(_.label).toSet.size
    val intercepts = Vectors.dense(interceptArray)
    val (featuresSummarizer, ySummarizer) = getClassificationSummarizers(instances)
    val featuresStd = featuresSummarizer.variance.toArray.map(math.sqrt)
    val weightSum = instances.map(_.weight).sum

    val agg = getNewAggregator(instances, Vectors.dense(coefArray ++ interceptArray),
      fitIntercept = true, isMultinomial = true)
    instances.foreach(agg.add)

    // compute the loss
    val stdCoef = coefArray.indices.map(i => coefArray(i) / featuresStd(i / numClasses)).toArray
    val linearPredictors = instances.map { case Instance(l, w, f) =>
      val result = intercepts.copy.toDense
      BLAS.gemv(1.0, Matrices.dense(numClasses, numFeatures, stdCoef), f, 1.0, result)
      (l, w, result)
    }

    // sum_i w * beta_k dot x_i
    val sumLinear = linearPredictors.map { case (l, w, p) =>
      w * p(l.toInt)
    }.sum

    // sum_i w * log(sum_k e^(beta_K dot x_i))
    val sumLogs = linearPredictors.map { case (l, w, p) =>
      w * math.log(p.values.map(math.exp).sum)
    }.sum
    val loss = (sumLogs - sumLinear) / weightSum


    // compute the gradients
    val gradientCoef = new Array[Double](numFeatures * numClasses)
    val gradientIntercept = new Array[Double](numClasses)
    instances.foreach { case Instance(l, w, f) =>
      val margin = intercepts.copy.toDense
      BLAS.gemv(1.0, Matrices.dense(numClasses, numFeatures, stdCoef), f, 1.0, margin)
      val sum = margin.values.map(math.exp).sum

      gradientCoef.indices.foreach { i =>
        val fStd = f(i / numClasses) / featuresStd(i / numClasses)
        val cidx = i % numClasses
        if (cidx == l.toInt) gradientCoef(i) -= w * fStd
        gradientCoef(i) += w * math.exp(margin(cidx)) / sum * fStd
      }

      gradientIntercept.indices.foreach { i =>
        val cidx = i % numClasses
        if (cidx == l.toInt) gradientIntercept(i) -= w
        gradientIntercept(i) += w * math.exp(margin(cidx)) / sum
      }
    }
    val gradient = Vectors.dense((gradientCoef ++ gradientIntercept).map(_ / weightSum))

    assert(loss ~== agg.loss relTol 0.01)
    assert(gradient ~== agg.gradient relTol 0.01)
  }

  test("check correctness binomial") {
    /*
    Check that the aggregator computes loss/gradient for:
      -sum_i y_i * log(1 / (1 + e^(-beta dot x_i)) + (1 - y_i) * log(1 - 1 / (1 + e^(-beta dot x_i))
     */
    val binaryInstances = instances.map { instance =>
      if (instance.label <= 1.0) instance else Instance(0.0, instance.weight, instance.features)
    }
    val coefArray = Array(1.0, 2.0)
    val intercept = 1.0
    val numFeatures = binaryInstances.head.features.size
    val (featuresSummarizer, _) = getClassificationSummarizers(binaryInstances)
    val featuresStd = featuresSummarizer.variance.toArray.map(math.sqrt)
    val weightSum = binaryInstances.map(_.weight).sum

    val agg = getNewAggregator(binaryInstances, Vectors.dense(coefArray ++ Array(intercept)),
      fitIntercept = true, isMultinomial = false)
    binaryInstances.foreach(agg.add)

    // compute the loss
    val stdCoef = coefArray.indices.map(i => coefArray(i) / featuresStd(i)).toArray
    val lossSum = binaryInstances.map { case Instance(l, w, f) =>
      val margin = BLAS.dot(Vectors.dense(stdCoef), f) + intercept
      val prob = 1.0 / (1.0 + math.exp(-margin))
      -w * l * math.log(prob) - w * (1.0 - l) * math.log(1.0 - prob)
    }.sum
    val loss = lossSum / weightSum

    // compute the gradients
    val gradientCoef = new Array[Double](numFeatures)
    var gradientIntercept = 0.0
    binaryInstances.foreach { case Instance(l, w, f) =>
      val margin = BLAS.dot(f, Vectors.dense(coefArray)) + intercept
      gradientCoef.indices.foreach { i =>
        gradientCoef(i) += w * (1.0 / (1.0 + math.exp(-margin)) - l) * f(i) / featuresStd(i)
      }
      gradientIntercept += w * (1.0 / (1.0 + math.exp(-margin)) - l)
    }
    val gradient = Vectors.dense((gradientCoef ++ Array(gradientIntercept)).map(_ / weightSum))

    assert(loss ~== agg.loss relTol 0.01)
    assert(gradient ~== agg.gradient relTol 0.01)
  }

  test("check with zero standard deviation") {
    val binaryInstances = instancesConstantFeature.map { instance =>
      if (instance.label <= 1.0) instance else Instance(0.0, instance.weight, instance.features)
    }
    val binaryInstancesFiltered = instancesConstantFeatureFiltered.map { instance =>
      if (instance.label <= 1.0) instance else Instance(0.0, instance.weight, instance.features)
    }
    val coefArray = Array(1.0, 2.0, -2.0, 3.0, 0.0, -1.0)
    val coefArrayFiltered = Array(3.0, 0.0, -1.0)
    val interceptArray = Array(4.0, 2.0, -3.0)
    val aggConstantFeature = getNewAggregator(instancesConstantFeature,
      Vectors.dense(coefArray ++ interceptArray), fitIntercept = true, isMultinomial = true)
    val aggConstantFeatureFiltered = getNewAggregator(instancesConstantFeatureFiltered,
      Vectors.dense(coefArrayFiltered ++ interceptArray), fitIntercept = true, isMultinomial = true)

    instancesConstantFeature.foreach(aggConstantFeature.add)
    instancesConstantFeatureFiltered.foreach(aggConstantFeatureFiltered.add)

    // constant features should not affect gradient
    def validateGradient(grad: Vector, gradFiltered: Vector, numCoefficientSets: Int): Unit = {
      for (i <- 0 until numCoefficientSets) {
        assert(grad(i) === 0.0)
        assert(grad(numCoefficientSets + i) == gradFiltered(i))
      }
    }

    validateGradient(aggConstantFeature.gradient, aggConstantFeatureFiltered.gradient, 3)

    val binaryCoefArray = Array(1.0, 2.0)
    val binaryCoefArrayFiltered = Array(2.0)
    val intercept = 1.0
    val aggConstantFeatureBinary = getNewAggregator(binaryInstances,
      Vectors.dense(binaryCoefArray ++ Array(intercept)), fitIntercept = true,
      isMultinomial = false)
    val aggConstantFeatureBinaryFiltered = getNewAggregator(binaryInstancesFiltered,
      Vectors.dense(binaryCoefArrayFiltered ++ Array(intercept)), fitIntercept = true,
      isMultinomial = false)
    binaryInstances.foreach(aggConstantFeatureBinary.add)
    binaryInstancesFiltered.foreach(aggConstantFeatureBinaryFiltered.add)

    // constant features should not affect gradient
    validateGradient(aggConstantFeatureBinary.gradient,
      aggConstantFeatureBinaryFiltered.gradient, 1)
  }
}
