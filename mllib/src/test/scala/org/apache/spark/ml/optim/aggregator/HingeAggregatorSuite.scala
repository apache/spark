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

class HingeAggregatorSuite extends SparkFunSuite with MLlibTestSparkContext {

  @transient var instances: Array[Instance] = _
  @transient var instancesConstantFeature: Array[Instance] = _
  @transient var instancesConstantFeatureFiltered: Array[Instance] = _
  @transient var standardizedInstances: Array[Instance] = _

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
    standardizedInstances = standardize(instances)
  }

   /** Get summary statistics for some data and create a new HingeAggregator. */
  private def getNewAggregator(
      instances: Array[Instance],
      coefficients: Vector,
      fitIntercept: Boolean): HingeAggregator = {
    val (featuresSummarizer, ySummarizer) =
      Summarizer.getClassificationSummarizers(sc.parallelize(instances))
    val featuresStd = featuresSummarizer.std.toArray
    val bcFeaturesStd = spark.sparkContext.broadcast(featuresStd)
    val bcCoefficients = spark.sparkContext.broadcast(coefficients)
    new HingeAggregator(bcFeaturesStd, fitIntercept)(bcCoefficients)
  }

  /** Get summary statistics for some data and create a new BlockHingeAggregator. */
  private def getNewBlockAggregator(
      coefficients: Vector,
      fitIntercept: Boolean): BlockHingeAggregator = {
    val bcCoefficients = spark.sparkContext.broadcast(coefficients)
    new BlockHingeAggregator(fitIntercept)(bcCoefficients)
  }

  test("aggregator add method input size") {
    val coefArray = Array(1.0, 2.0)
    val interceptArray = Array(2.0)
    val agg = getNewAggregator(instances, Vectors.dense(coefArray ++ interceptArray),
      fitIntercept = true)
    withClue("HingeAggregator features dimension must match coefficients dimension") {
      intercept[IllegalArgumentException] {
        agg.add(Instance(1.0, 1.0, Vectors.dense(2.0)))
      }
    }
  }

  test("negative weight") {
    val coefArray = Array(1.0, 2.0)
    val interceptArray = Array(2.0)
    val agg = getNewAggregator(instances, Vectors.dense(coefArray ++ interceptArray),
      fitIntercept = true)
    withClue("HingeAggregator does not support negative instance weights") {
      intercept[IllegalArgumentException] {
        agg.add(Instance(1.0, -1.0, Vectors.dense(2.0, 1.0)))
      }
    }
  }

  test("check sizes") {
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

  test("check correctness") {
    val coefArray = Array(1.0, 2.0)
    val intercept = 1.0
    val numFeatures = instances.head.features.size
    val (featuresSummarizer, _) = Summarizer.getClassificationSummarizers(sc.parallelize(instances))
    val featuresStd = featuresSummarizer.std.toArray
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
        (1.0 - labelScaled * margin) * w
      } else {
        0.0
      }
    }.sum
    val loss = lossSum / weightSum

    // compute the gradients
    val gradientCoef = new Array[Double](numFeatures)
    var gradientIntercept = 0.0
    instances.foreach { case Instance(l, w, f) =>
      val margin = BLAS.dot(f, Vectors.dense(coefArray)) + intercept
      if (1.0 > (2 * l - 1.0) * margin) {
        gradientCoef.indices.foreach { i =>
          gradientCoef(i) += f(i) * -(2 * l - 1.0) * w / featuresStd(i)
        }
        gradientIntercept += -(2 * l - 1.0) * w
      }
    }
    val gradient = Vectors.dense((gradientCoef ++ Array(gradientIntercept)).map(_ / weightSum))

    assert(loss ~== agg.loss relTol 1e-9)
    assert(gradient ~== agg.gradient relTol 1e-9)

    Seq(1, 2, 4).foreach { blockSize =>
      val blocks1 = standardizedInstances
        .grouped(blockSize)
        .map(seq => InstanceBlock.fromInstances(seq))
        .toArray
      val blocks2 = blocks1.map { block =>
        new InstanceBlock(block.labels, block.weights, block.matrix.toSparseRowMajor)
      }

      Seq(blocks1, blocks2).foreach { blocks =>
        val blockAgg = getNewBlockAggregator(Vectors.dense(coefArray ++ Array(intercept)),
          fitIntercept = true)
        blocks.foreach(blockAgg.add)
        assert(agg.loss ~== blockAgg.loss relTol 1e-9)
        assert(agg.gradient ~== blockAgg.gradient relTol 1e-9)
      }
    }
  }

  test("check with zero standard deviation") {
    val binaryCoefArray = Array(1.0, 2.0)
    val intercept = 1.0
    val aggConstantFeatureBinary = getNewAggregator(instancesConstantFeature,
      Vectors.dense(binaryCoefArray ++ Array(intercept)), fitIntercept = true)
    instancesConstantFeature.foreach(aggConstantFeatureBinary.add)

    val aggConstantFeatureBinaryFiltered = getNewAggregator(instancesConstantFeatureFiltered,
      Vectors.dense(binaryCoefArray ++ Array(intercept)), fitIntercept = true)
    instancesConstantFeatureFiltered.foreach(aggConstantFeatureBinaryFiltered.add)

    // constant features should not affect gradient
    assert(aggConstantFeatureBinary.gradient(0) === 0.0)
    assert(aggConstantFeatureBinary.gradient(1) == aggConstantFeatureBinaryFiltered.gradient(0))
  }
}
