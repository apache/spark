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
import org.apache.spark.ml.feature.{Instance, InstanceBlock}
import org.apache.spark.ml.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext

class HuberAggregatorSuite extends SparkFunSuite with MLlibTestSparkContext {

  @transient var instances: Array[Instance] = _
  @transient var instancesConstantFeature: Array[Instance] = _
  @transient var instancesConstantFeatureFiltered: Array[Instance] = _
  @transient var standardizedInstances: Array[Instance] = _

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
    standardizedInstances = standardize(instances)
  }

  /** Get summary statistics for some data and create a new HuberAggregator. */
  private def getNewAggregator(
      instances: Array[Instance],
      parameters: Vector,
      fitIntercept: Boolean,
      epsilon: Double): HuberAggregator = {
    val (featuresSummarizer, _) = Summarizer.getRegressionSummarizers(sc.parallelize(instances))
    val featuresStd = featuresSummarizer.std.toArray
    val bcFeaturesStd = spark.sparkContext.broadcast(featuresStd)
    val bcParameters = spark.sparkContext.broadcast(parameters)
    new HuberAggregator(fitIntercept, epsilon, bcFeaturesStd)(bcParameters)
  }

  /** Get summary statistics for some data and create a new BlockHingeAggregator. */
  private def getNewBlockAggregator(
      parameters: Vector,
      fitIntercept: Boolean,
      epsilon: Double): BlockHuberAggregator = {
    val bcParameters = spark.sparkContext.broadcast(parameters)
    new BlockHuberAggregator(fitIntercept, epsilon)(bcParameters)
  }

  test("aggregator add method should check input size") {
    val parameters = Vectors.dense(1.0, 2.0, 3.0, 4.0)
    val agg = getNewAggregator(instances, parameters, fitIntercept = true, epsilon = 1.35)
    withClue("HuberAggregator features dimension must match parameters dimension") {
      intercept[IllegalArgumentException] {
        agg.add(Instance(1.0, 1.0, Vectors.dense(2.0)))
      }
    }
  }

  test("negative weight") {
    val parameters = Vectors.dense(1.0, 2.0, 3.0, 4.0)
    val agg = getNewAggregator(instances, parameters, fitIntercept = true, epsilon = 1.35)
    withClue("HuberAggregator does not support negative instance weights.") {
      intercept[IllegalArgumentException] {
        agg.add(Instance(1.0, -1.0, Vectors.dense(2.0, 1.0)))
      }
    }
  }

  test("check sizes") {
    val paramWithIntercept = Vectors.dense(1.0, 2.0, 3.0, 4.0)
    val paramWithoutIntercept = Vectors.dense(1.0, 2.0, 4.0)
    val aggIntercept = getNewAggregator(instances, paramWithIntercept,
      fitIntercept = true, epsilon = 1.35)
    val aggNoIntercept = getNewAggregator(instances, paramWithoutIntercept,
      fitIntercept = false, epsilon = 1.35)
    instances.foreach(aggIntercept.add)
    instances.foreach(aggNoIntercept.add)

    assert(aggIntercept.gradient.size === 4)
    assert(aggNoIntercept.gradient.size === 3)
  }

  test("check correctness") {
    val parameters = Vectors.dense(1.0, 2.0, 3.0, 4.0)
    val numFeatures = 2
    val (featuresSummarizer, _) = Summarizer.getRegressionSummarizers(sc.parallelize(instances))
    val featuresStd = featuresSummarizer.std.toArray
    val epsilon = 1.35
    val weightSum = instances.map(_.weight).sum

    val agg = getNewAggregator(instances, parameters, fitIntercept = true, epsilon)
    instances.foreach(agg.add)

    // compute expected loss sum
    val coefficients = parameters.toArray.slice(0, 2)
    val intercept = parameters(2)
    val sigma = parameters(3)
    val stdCoef = coefficients.indices.map(i => coefficients(i) / featuresStd(i)).toArray
    val lossSum = instances.map { case Instance(label, weight, features) =>
      val margin = BLAS.dot(Vectors.dense(stdCoef), features) + intercept
      val linearLoss = label - margin
      if (math.abs(linearLoss) <= sigma * epsilon) {
        0.5 * weight * (sigma +  math.pow(linearLoss, 2.0) / sigma)
      } else {
        0.5 * weight * (sigma + 2.0 * epsilon * math.abs(linearLoss) - sigma * epsilon * epsilon)
      }
    }.sum
    val loss = lossSum / weightSum

    // compute expected gradients
    val gradientCoef = new Array[Double](numFeatures + 2)
    instances.foreach { case Instance(label, weight, features) =>
      val margin = BLAS.dot(Vectors.dense(stdCoef), features) + intercept
      val linearLoss = label - margin
      if (math.abs(linearLoss) <= sigma * epsilon) {
        features.toArray.indices.foreach { i =>
          gradientCoef(i) +=
            -1.0 * weight * (linearLoss / sigma) * (features(i) / featuresStd(i))
        }
        gradientCoef(2) += -1.0 * weight * (linearLoss / sigma)
        gradientCoef(3) += 0.5 * weight * (1.0 - math.pow(linearLoss / sigma, 2.0))
      } else {
        val sign = if (linearLoss >= 0) -1.0 else 1.0
        features.toArray.indices.foreach { i =>
          gradientCoef(i) += weight * sign * epsilon * (features(i) / featuresStd(i))
        }
        gradientCoef(2) += weight * sign * epsilon
        gradientCoef(3) += 0.5 * weight * (1.0 - epsilon * epsilon)
      }
    }
    val gradient = Vectors.dense(gradientCoef.map(_ / weightSum))

    assert(loss ~== agg.loss relTol 0.01)
    assert(gradient ~== agg.gradient relTol 0.01)

    Seq(1, 2, 4).foreach { blockSize =>
      val blocks1 = standardizedInstances
        .grouped(blockSize)
        .map(seq => InstanceBlock.fromInstances(seq))
        .toArray
      val blocks2 = blocks1.map { block =>
        new InstanceBlock(block.labels, block.weights, block.matrix.toSparseRowMajor)
      }

      Seq(blocks1, blocks2).foreach { blocks =>
        val blockAgg = getNewBlockAggregator(parameters, fitIntercept = true, epsilon)
        blocks.foreach(blockAgg.add)
        assert(agg.loss ~== blockAgg.loss relTol 1e-9)
        assert(agg.gradient ~== blockAgg.gradient relTol 1e-9)
      }
    }
  }

  test("check with zero standard deviation") {
    val parameters = Vectors.dense(1.0, 2.0, 3.0, 4.0)
    val parametersFiltered = Vectors.dense(2.0, 3.0, 4.0)
    val aggConstantFeature = getNewAggregator(instancesConstantFeature, parameters,
      fitIntercept = true, epsilon = 1.35)
    val aggConstantFeatureFiltered = getNewAggregator(instancesConstantFeatureFiltered,
      parametersFiltered, fitIntercept = true, epsilon = 1.35)
    instances.foreach(aggConstantFeature.add)
    instancesConstantFeatureFiltered.foreach(aggConstantFeatureFiltered.add)
    // constant features should not affect gradient
    def validateGradient(grad: Vector, gradFiltered: Vector): Unit = {
      assert(grad(0) === 0.0)
      assert(grad(1) ~== gradFiltered(0) relTol 0.01)
    }

    validateGradient(aggConstantFeature.gradient, aggConstantFeatureFiltered.gradient)
  }
}
