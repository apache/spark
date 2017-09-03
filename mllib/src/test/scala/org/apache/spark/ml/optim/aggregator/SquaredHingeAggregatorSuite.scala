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

class SquaredHingeAggregatorSuite extends SparkFunSuite with MLlibTestSparkContext {

  import DifferentiableLossAggregatorSuite.getClassificationSummarizers

  @transient var instances: Array[Instance] = _
  @transient var instancesConstantFeature: Array[Instance] = _
  @transient var instancesConstantFeatureFiltered: Array[Instance] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    instances = Array(
      Instance(0.0, 0.1, Vectors.dense(1.0, 2.0)),
      Instance(1.0, 0.5, Vectors.dense(1.5, 1.0)),
      Instance(0.0, 0.3, Vectors.dense(4.0, 0.5))
    )
    instancesConstantFeature = Array(
      Instance(0.0, 0.1, Vectors.dense(1.0, 2.0)),
      Instance(1.0, 0.5, Vectors.dense(1.0, 1.0)),
      Instance(1.0, 0.3, Vectors.dense(1.0, 0.5))
    )
    instancesConstantFeatureFiltered = Array(
      Instance(0.0, 0.1, Vectors.dense(2.0)),
      Instance(1.0, 0.5, Vectors.dense(1.0)),
      Instance(2.0, 0.3, Vectors.dense(0.5))
    )
  }

   /** Get summary statistics for some data and create a new SquaredHingeAggregator. */
  private def getNewAggregator(
      instances: Array[Instance],
      coefficients: Vector,
      fitIntercept: Boolean): SquaredHingeAggregator = {
    val (featuresSummarizer, ySummarizer) =
      DifferentiableLossAggregatorSuite.getClassificationSummarizers(instances)
    val featuresStd = featuresSummarizer.variance.toArray.map(math.sqrt)
    val bcFeaturesStd = spark.sparkContext.broadcast(featuresStd)
    val bcCoefficients = spark.sparkContext.broadcast(coefficients)
    new SquaredHingeAggregator(bcFeaturesStd, fitIntercept)(bcCoefficients)
  }

  test("SquaredHingeAggregator check add method input size") {
    val coefArray = Array(1.0, 2.0)
    val interceptArray = Array(2.0)
    val agg = getNewAggregator(instances, Vectors.dense(coefArray ++ interceptArray),
      fitIntercept = true)
    withClue("SquaredHingeAggregator features dimension must match coefficients dimension") {
      intercept[IllegalArgumentException] {
        agg.add(Instance(1.0, 1.0, Vectors.dense(2.0)))
      }
    }
  }

  test("SquaredHingeAggregator negative weight") {
    val coefArray = Array(1.0, 2.0)
    val interceptArray = Array(2.0)
    val agg = getNewAggregator(instances, Vectors.dense(coefArray ++ interceptArray),
      fitIntercept = true)
    withClue("SquaredHingeAggregator does not support negative instance weights") {
      intercept[IllegalArgumentException] {
        agg.add(Instance(1.0, -1.0, Vectors.dense(2.0, 1.0)))
      }
    }
  }

  test("SquaredHingeAggregator check sizes") {
    val rng = new scala.util.Random
    val numFeatures = instances.head.features.size
    val coefWithIntercept = Vectors.dense(Array.fill(numFeatures + 1)(rng.nextDouble))
    val coefWithoutIntercept = Vectors.dense(Array.fill(numFeatures)(rng.nextDouble))
    val aggIntercept = getNewAggregator(instances, coefWithIntercept, fitIntercept = true)
    val aggNoIntercept = getNewAggregator(instances, coefWithoutIntercept,
      fitIntercept = false)
    instances.foreach(aggIntercept.add)
    instances.foreach(aggNoIntercept.add)

    assert(aggIntercept.gradient.size === numFeatures + 1)
    assert(aggNoIntercept.gradient.size === numFeatures)
  }

  test("SquaredHingeAggregator check correctness") {
    val coefArray = Array(1.0, 2.0)
    val intercept = 1.0
    val numFeatures = instances.head.features.size
    val (featuresSummarizer, _) = getClassificationSummarizers(instances)
    val featuresStd = featuresSummarizer.variance.toArray.map(math.sqrt)
    val weightSum = instances.map(_.weight).sum

    val agg = getNewAggregator(instances, Vectors.dense(coefArray ++ Array(intercept)),
      fitIntercept = true)
    instances.foreach(agg.add)

    // compute the loss
    val stdCoef = coefArray.indices.map(i => coefArray(i) / featuresStd(i)).toArray
    val lossSum = instances.map { case Instance(l, w, f) =>
      val margin = BLAS.dot(Vectors.dense(stdCoef), f) + intercept
      val labelScaled = 2 * l - 1.0
      if (1.0 > labelScaled * margin) {
        val hingeLoss = 1.0 - labelScaled * margin
        hingeLoss * hingeLoss * w
      } else {
        0.0
      }
    }.sum
    val loss = lossSum / weightSum

    // compute the gradients
    val gradientCoef = new Array[Double](numFeatures)
    var gradientIntercept = 0.0
    instances.foreach { case Instance(l, w, f) =>
      val margin = BLAS.dot(f, Vectors.dense(stdCoef)) + intercept
      if (1.0 > (2 * l - 1.0) * margin) {
        val gradientScale = ((2 * l - 1) * margin - 1) * (2 * l - 1) * 2
        gradientCoef.indices.foreach { i =>
          gradientCoef(i) += f(i) * gradientScale * w / featuresStd(i)
        }
        gradientIntercept += gradientScale * w
      }
    }
    val gradient = Vectors.dense((gradientCoef ++ Array(gradientIntercept)).map(_ / weightSum))

    assert(loss ~== agg.loss relTol 0.01)
    assert(gradient ~== agg.gradient relTol 0.01)
  }

  test("check with zero standard deviation") {
    val binaryCoefArray = Array(1.0, 2.0)
    val intercept = 1.0
    val aggConstantFeatureBinary = getNewAggregator(instancesConstantFeature,
      Vectors.dense(binaryCoefArray ++ Array(intercept)), fitIntercept = true)
    instancesConstantFeature.foreach(aggConstantFeatureBinary.add)

    val aggConstantFeatureBinaryFiltered = getNewAggregator(instancesConstantFeatureFiltered,
      Vectors.dense(binaryCoefArray.tail ++ Array(intercept)), fitIntercept = true)
    instancesConstantFeatureFiltered.foreach(aggConstantFeatureBinaryFiltered.add)

    // constant features should not affect gradient
    assert(aggConstantFeatureBinary.gradient(0) === 0.0)
    assert(aggConstantFeatureBinary.gradient(1) == aggConstantFeatureBinaryFiltered.gradient(0))
  }

}
