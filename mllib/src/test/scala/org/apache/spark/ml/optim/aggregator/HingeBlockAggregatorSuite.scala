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
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.util.ArrayImplicits._

class HingeBlockAggregatorSuite extends SparkFunSuite with MLlibTestSparkContext {

  @transient var instances: Array[Instance] = _
  @transient var instancesConstantFeature: Array[Instance] = _
  @transient var instancesConstantFeatureFiltered: Array[Instance] = _
  @transient var scaledInstances: Array[Instance] = _

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
      Instance(1.0, 0.3, Vectors.dense(0.5))
    )
    scaledInstances = standardize(instances)
  }


  /** Get summary statistics for some data and create a new HingeBlockAggregator. */
  private def getNewAggregator(
      instances: Array[Instance],
      coefficients: Vector,
      fitIntercept: Boolean): HingeBlockAggregator = {
    val (featuresSummarizer, _) =
      Summarizer.getClassificationSummarizers(sc.parallelize(instances.toImmutableArraySeq))
    val featuresStd = featuresSummarizer.std.toArray
    val featuresMean = featuresSummarizer.mean.toArray
    val inverseStd = featuresStd.map(std => if (std != 0) 1.0 / std else 0.0)
    val scaledMean = inverseStd.zip(featuresMean).map(t => t._1 * t._2)
    val bcInverseStd = sc.broadcast(inverseStd)
    val bcScaledMean = sc.broadcast(scaledMean)
    val bcCoefficients = sc.broadcast(coefficients)
    new HingeBlockAggregator(bcInverseStd, bcScaledMean, fitIntercept)(bcCoefficients)
  }

  test("sparse coefficients") {
    val bcInverseStd = sc.broadcast(Array(1.0))
    val bcScaledMean = sc.broadcast(Array(2.0))
    val bcCoefficients = sc.broadcast(Vectors.sparse(2, Array(0), Array(1.0)))
    val binaryAgg = new HingeBlockAggregator(bcInverseStd, bcScaledMean,
      fitIntercept = false)(bcCoefficients)
    val block = InstanceBlock.fromInstances(Seq(Instance(1.0, 1.0, Vectors.dense(1.0))))
    val thrownBinary = withClue("aggregator cannot handle sparse coefficients") {
      intercept[IllegalArgumentException] {
        binaryAgg.add(block)
      }
    }
    assert(thrownBinary.getMessage.contains("coefficients only supports dense"))
  }

  test("aggregator add method input size") {
    val coefArray = Array(1.0, 2.0)
    val interceptValue = 4.0
    val agg = getNewAggregator(instances, Vectors.dense(coefArray :+ interceptValue),
      fitIntercept = true)
    val block = InstanceBlock.fromInstances(Seq(Instance(1.0, 1.0, Vectors.dense(2.0))))
    withClue("BinaryLogisticBlockAggregator features dimension must match coefficients dimension") {
      intercept[IllegalArgumentException] {
        agg.add(block)
      }
    }
  }

  test("negative weight") {
    val coefArray = Array(1.0, 2.0)
    val interceptValue = 4.0
    val agg = getNewAggregator(instances, Vectors.dense(coefArray :+ interceptValue),
      fitIntercept = true)
    val block = InstanceBlock.fromInstances(Seq(Instance(1.0, -1.0, Vectors.dense(2.0, 1.0))))
    withClue("BinaryLogisticBlockAggregator does not support negative instance weights") {
      intercept[IllegalArgumentException] {
        agg.add(block)
      }
    }
  }

  test("check sizes") {
    val rng = new scala.util.Random
    val numFeatures = instances.head.features.size
    val coefWithIntercept = Vectors.dense(Array.fill(numFeatures + 1)(rng.nextDouble()))
    val coefWithoutIntercept = Vectors.dense(Array.fill(numFeatures)(rng.nextDouble()))
    val block = InstanceBlock.fromInstances(instances.toImmutableArraySeq)

    val aggIntercept = getNewAggregator(instances, coefWithIntercept, fitIntercept = true)
    aggIntercept.add(block)
    assert(aggIntercept.gradient.size === numFeatures + 1)

    val aggNoIntercept = getNewAggregator(instances, coefWithoutIntercept, fitIntercept = false)
    aggNoIntercept.add(block)
    assert(aggNoIntercept.gradient.size === numFeatures)
  }

  test("check correctness: fitIntercept = false") {
    val coefVec = Vectors.dense(1.0, 2.0)
    val numFeatures = instances.head.features.size
    val (featuresSummarizer, _) =
      Summarizer.getClassificationSummarizers(sc.parallelize(instances.toImmutableArraySeq))
    val featuresStd = featuresSummarizer.std
    val stdCoefVec = Vectors.dense(Array.tabulate(coefVec.size)(i => coefVec(i) / featuresStd(i)))
    val weightSum = instances.map(_.weight).sum

    // compute the loss and the gradients
    var lossSum = 0.0
    val gradientCoef = Array.ofDim[Double](numFeatures)
    instances.foreach { case Instance(l, w, f) =>
      val margin = BLAS.dot(stdCoefVec, f)
      val labelScaled = 2 * l - 1.0
      if (1.0 > labelScaled * margin) {
        lossSum += (1.0 - labelScaled * margin) * w
        gradientCoef.indices.foreach { i =>
          gradientCoef(i) += f(i) * -(2 * l - 1.0) * w / featuresStd(i)
        }
      }
    }
    val loss = lossSum / weightSum
    val gradient = Vectors.dense(gradientCoef.map(_ / weightSum))

    Seq(1, 2, 4).foreach { blockSize =>
      val blocks1 = scaledInstances
        .grouped(blockSize)
        .map(seq => InstanceBlock.fromInstances(seq.toImmutableArraySeq))
        .toArray
      val blocks2 = blocks1.map { block =>
        new InstanceBlock(block.labels, block.weights, block.matrix.toSparseRowMajor)
      }

      Seq(blocks1, blocks2).foreach { blocks =>
        val agg = getNewAggregator(instances, coefVec, fitIntercept = false)
        blocks.foreach(agg.add)
        assert(agg.loss ~== loss relTol 1e-9)
        assert(agg.gradient ~== gradient relTol 1e-9)
      }
    }
  }

  test("check correctness: fitIntercept = true") {
    val coefVec = Vectors.dense(1.0, 2.0)
    val interceptValue = 1.0
    val numFeatures = instances.head.features.size
    val (featuresSummarizer, _) =
      Summarizer.getClassificationSummarizers(sc.parallelize(instances.toImmutableArraySeq))
    val featuresStd = featuresSummarizer.std
    val featuresMean = featuresSummarizer.mean
    val stdCoefVec = Vectors.dense(Array.tabulate(coefVec.size)(i => coefVec(i) / featuresStd(i)))
    val weightSum = instances.map(_.weight).sum

    // compute the loss and the gradients
    var lossSum = 0.0
    val gradientCoef = Array.ofDim[Double](numFeatures)
    var gradientIntercept = 0.0
    instances.foreach { case Instance(l, w, f) =>
      val centered = f.toDense.copy
      BLAS.axpy(-1.0, featuresMean, centered)
      val margin = BLAS.dot(stdCoefVec, centered) + interceptValue
      val labelScaled = 2 * l - 1.0
      if (1.0 > labelScaled * margin) {
        lossSum += (1.0 - labelScaled * margin) * w
        gradientCoef.indices.foreach { i =>
          gradientCoef(i) += (f(i) - featuresMean(i)) * -(2 * l - 1.0) * w / featuresStd(i)
        }
        gradientIntercept += -(2 * l - 1.0) * w
      }
    }
    val loss = lossSum / weightSum
    val gradient = Vectors.dense((gradientCoef :+ gradientIntercept).map(_ / weightSum))

    Seq(1, 2, 4).foreach { blockSize =>
      val blocks1 = scaledInstances
        .grouped(blockSize)
        .map(seq => InstanceBlock.fromInstances(seq.toImmutableArraySeq))
        .toArray
      val blocks2 = blocks1.map { block =>
        new InstanceBlock(block.labels, block.weights, block.matrix.toSparseRowMajor)
      }

      Seq(blocks1, blocks2).foreach { blocks =>
        val agg = getNewAggregator(instances, Vectors.dense(coefVec.toArray :+ interceptValue),
          fitIntercept = true)
        blocks.foreach(agg.add)
        assert(agg.loss ~== loss relTol 1e-9)
        assert(agg.gradient ~== gradient relTol 1e-9)
      }
    }
  }

  test("check with zero standard deviation") {
    val coefArray = Array(1.0, 2.0)
    val coefArrayFiltered = Array(2.0)
    val interceptValue = 1.0

    Seq(false, true).foreach { fitIntercept =>
      val coefVec = if (fitIntercept) {
        Vectors.dense(coefArray :+ interceptValue)
      } else {
        Vectors.dense(coefArray)
      }
      val aggConstantFeature = getNewAggregator(instancesConstantFeature,
        coefVec, fitIntercept = fitIntercept)
      aggConstantFeature
        .add(InstanceBlock.fromInstances(standardize(instancesConstantFeature).toImmutableArraySeq))
      val grad = aggConstantFeature.gradient

      val coefVecFiltered = if (fitIntercept) {
        Vectors.dense(coefArrayFiltered :+ interceptValue)
      } else {
        Vectors.dense(coefArrayFiltered)
      }
      val aggConstantFeatureFiltered = getNewAggregator(instancesConstantFeatureFiltered,
        coefVecFiltered, fitIntercept = fitIntercept)
      aggConstantFeatureFiltered
        .add(InstanceBlock.fromInstances(standardize(instancesConstantFeatureFiltered)
          .toImmutableArraySeq))
      val gradFiltered = aggConstantFeatureFiltered.gradient

      // constant features should not affect gradient
      assert(aggConstantFeature.loss ~== aggConstantFeatureFiltered.loss relTol 1e-9)
      assert(grad(0) === 0)
      assert(grad(1) ~== gradFiltered(0) relTol 1e-9)
      if (fitIntercept) {
        assert(grad.toArray.last ~== gradFiltered.toArray.last relTol 1e-9)
      }
    }
  }
}
