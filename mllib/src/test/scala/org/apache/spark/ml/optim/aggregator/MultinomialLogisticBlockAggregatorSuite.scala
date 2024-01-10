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

class MultinomialLogisticBlockAggregatorSuite extends SparkFunSuite with MLlibTestSparkContext {

  @transient var instances: Array[Instance] = _
  @transient var instancesConstantFeature: Array[Instance] = _
  @transient var instancesConstantFeatureFiltered: Array[Instance] = _
  @transient var scaledInstances: Array[Instance] = _

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
    scaledInstances = standardize(instances)
  }

  /** Get summary statistics for some data and create a new MultinomialLogisticBlockAggregator. */
  private def getNewAggregator(
      instances: Array[Instance],
      coefficients: Vector,
      fitIntercept: Boolean,
      fitWithMean: Boolean): MultinomialLogisticBlockAggregator = {
    val (featuresSummarizer, _) =
      Summarizer.getClassificationSummarizers(sc.parallelize(instances.toImmutableArraySeq))
    val featuresStd = featuresSummarizer.std.toArray
    val featuresMean = featuresSummarizer.mean.toArray
    val inverseStd = featuresStd.map(std => if (std != 0) 1.0 / std else 0.0)
    val scaledMean = inverseStd.zip(featuresMean).map(t => t._1 * t._2)
    val bcInverseStd = sc.broadcast(inverseStd)
    val bcScaledMean = sc.broadcast(scaledMean)
    val bcCoefficients = sc.broadcast(coefficients)
    new MultinomialLogisticBlockAggregator(bcInverseStd,
      bcScaledMean, fitIntercept, fitWithMean)(bcCoefficients)
  }

  test("sparse coefficients") {
    val bcInverseStd = sc.broadcast(Array(1.0))
    val bcScaledMean = sc.broadcast(Array(2.0))
    val bcCoefficients = sc.broadcast(Vectors.sparse(4, Array(0), Array(1.0)))
    val binaryAgg = new MultinomialLogisticBlockAggregator(bcInverseStd, bcScaledMean,
      fitIntercept = true, fitWithMean = false)(bcCoefficients)
    val block = InstanceBlock.fromInstances(Seq(Instance(1.0, 1.0, Vectors.dense(1.0))))
    val thrownBinary = withClue("aggregator cannot handle sparse coefficients") {
      intercept[IllegalArgumentException] {
        binaryAgg.add(block)
      }
    }
    assert(thrownBinary.getMessage.contains("coefficients only supports dense"))
  }

  test("aggregator add method input size") {
    val coefArray = Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
    val interceptArray = Array(7.0, 8.0, 9.0)
    val agg = getNewAggregator(instances, Vectors.dense(coefArray ++ interceptArray),
      fitIntercept = true, fitWithMean = true)
    val block = InstanceBlock.fromInstances(Seq(Instance(1.0, 1.0, Vectors.dense(2.0))))
    withClue("BinaryLogisticBlockAggregator features dimension must match coefficients dimension") {
      intercept[IllegalArgumentException] {
        agg.add(block)
      }
    }
  }

  test("negative weight") {
    val coefArray = Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
    val interceptArray = Array(7.0, 8.0, 9.0)
    val agg = getNewAggregator(instances, Vectors.dense(coefArray ++ interceptArray),
      fitIntercept = true, fitWithMean = true)
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
    val numClasses = instances.map(_.label).distinct.length
    val coefWithIntercept = Vectors.dense(
      Array.fill(numClasses * (numFeatures + 1))(rng.nextDouble()))
    val coefWithoutIntercept = Vectors.dense(
      Array.fill(numClasses * numFeatures)(rng.nextDouble()))
    val block = InstanceBlock.fromInstances(instances.toImmutableArraySeq)

    val aggIntercept = getNewAggregator(instances, coefWithIntercept,
      fitIntercept = true, fitWithMean = false)
    aggIntercept.add(block)
    assert(aggIntercept.gradient.size === (numFeatures + 1) * numClasses)

    val aggNoIntercept = getNewAggregator(instances, coefWithoutIntercept,
      fitIntercept = false, fitWithMean = false)
    aggNoIntercept.add(block)
    assert(aggNoIntercept.gradient.size === numFeatures * numClasses)
  }

  test("check correctness: fitIntercept = false") {
    val coefArray = Array(1.0, 2.0, -2.0, 3.0, 0.0, -1.0)
    val numFeatures = instances.head.features.size
    val numClasses = instances.map(_.label).toSet.size
    val (featuresSummarizer, _) =
      Summarizer.getClassificationSummarizers(sc.parallelize(instances.toImmutableArraySeq))
    val featuresStd = featuresSummarizer.std
    val stdCoefMat = Matrices.dense(numClasses, numFeatures,
      Array.tabulate(coefArray.length)(i => coefArray(i) / featuresStd(i / numClasses)))
    val weightSum = instances.map(_.weight).sum

    // compute the loss
    val linearPredictors = instances.map { case Instance(l, w, f) =>
      val result = new DenseVector(Array.ofDim[Double](numClasses))
      BLAS.gemv(1.0, stdCoefMat, f, 1.0, result)
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
    instances.foreach { case Instance(l, w, f) =>
      val margin = new DenseVector(Array.ofDim[Double](numClasses))
      BLAS.gemv(1.0, stdCoefMat, f, 1.0, margin)
      val sum = margin.values.map(math.exp).sum

      gradientCoef.indices.foreach { i =>
        val fStd = f(i / numClasses) / featuresStd(i / numClasses)
        val cidx = i % numClasses
        if (cidx == l.toInt) gradientCoef(i) -= w * fStd
        gradientCoef(i) += w * math.exp(margin(cidx)) / sum * fStd
      }
    }
    val gradient = Vectors.dense((gradientCoef).map(_ / weightSum))

    Seq(1, 2, 4).foreach { blockSize =>
      val blocks1 = scaledInstances
        .grouped(blockSize)
        .map(seq => InstanceBlock.fromInstances(seq.toImmutableArraySeq))
        .toArray
      val blocks2 = blocks1.map { block =>
        new InstanceBlock(block.labels, block.weights, block.matrix.toSparseRowMajor)
      }

      Seq(blocks1, blocks2).foreach { blocks =>
        val agg = getNewAggregator(instances, Vectors.dense(coefArray),
          fitIntercept = false, fitWithMean = false)
        blocks.foreach(agg.add)
        assert(agg.loss ~== loss relTol 1e-9)
        assert(agg.gradient ~== gradient relTol 1e-9)
      }
    }
  }

  test("check correctness: fitIntercept = true, fitWithMean = false") {
    val coefArray = Array(1.0, 2.0, -2.0, 3.0, 0.0, -1.0)
    val interceptArray = Array(4.0, 2.0, -3.0)
    val numFeatures = instances.head.features.size
    val numClasses = instances.map(_.label).toSet.size
    val intercepts = Vectors.dense(interceptArray)
    val (featuresSummarizer, _) =
      Summarizer.getClassificationSummarizers(sc.parallelize(instances.toImmutableArraySeq))
    val featuresStd = featuresSummarizer.std
    val stdCoefMat = Matrices.dense(numClasses, numFeatures,
      Array.tabulate(coefArray.length)(i => coefArray(i) / featuresStd(i / numClasses)))
    val weightSum = instances.map(_.weight).sum

    // compute the loss
    val linearPredictors = instances.map { case Instance(l, w, f) =>
      val result = intercepts.copy.toDense
      BLAS.gemv(1.0, stdCoefMat, f, 1.0, result)
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
      BLAS.gemv(1.0, stdCoefMat, f, 1.0, margin)
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

    Seq(1, 2, 4).foreach { blockSize =>
      val blocks1 = scaledInstances
        .grouped(blockSize)
        .map(seq => InstanceBlock.fromInstances(seq.toImmutableArraySeq))
        .toArray
      val blocks2 = blocks1.map { block =>
        new InstanceBlock(block.labels, block.weights, block.matrix.toSparseRowMajor)
      }

      Seq(blocks1, blocks2).foreach { blocks =>
        val agg = getNewAggregator(instances, Vectors.dense(coefArray ++ interceptArray),
          fitIntercept = true, fitWithMean = false)
        blocks.foreach(agg.add)
        assert(agg.loss ~== loss relTol 1e-9)
        assert(agg.gradient ~== gradient relTol 1e-9)
      }
    }
  }

  test("check correctness: fitIntercept = true, fitWithMean = true") {
    val coefArray = Array(1.0, 2.0, -2.0, 3.0, 0.0, -1.0)
    val interceptArray = Array(4.0, 2.0, -3.0)
    val numFeatures = instances.head.features.size
    val numClasses = instances.map(_.label).toSet.size
    val intercepts = Vectors.dense(interceptArray)
    val (featuresSummarizer, _) =
      Summarizer.getClassificationSummarizers(sc.parallelize(instances.toImmutableArraySeq))
    val featuresStd = featuresSummarizer.std
    val featuresMean = featuresSummarizer.mean
    val stdCoefMat = Matrices.dense(numClasses, numFeatures,
      Array.tabulate(coefArray.length)(i => coefArray(i) / featuresStd(i / numClasses)))
    val weightSum = instances.map(_.weight).sum

    // compute the loss
    val linearPredictors = instances.map { case Instance(l, w, f) =>
      val centered = f.toDense.copy
      BLAS.axpy(-1.0, featuresMean, centered)
      val result = intercepts.copy.toDense
      BLAS.gemv(1.0, stdCoefMat, centered, 1.0, result)
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
      val centered = f.toDense.copy
      BLAS.axpy(-1.0, featuresMean, centered)
      val margin = intercepts.copy.toDense
      BLAS.gemv(1.0, stdCoefMat, centered, 1.0, margin)
      val sum = margin.values.map(math.exp).sum

      gradientCoef.indices.foreach { i =>
        val fStd = centered(i / numClasses) / featuresStd(i / numClasses)
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

    Seq(1, 2, 4).foreach { blockSize =>
      val blocks1 = scaledInstances
        .grouped(blockSize)
        .map(seq => InstanceBlock.fromInstances(seq.toImmutableArraySeq))
        .toArray
      val blocks2 = blocks1.map { block =>
        new InstanceBlock(block.labels, block.weights, block.matrix.toSparseRowMajor)
      }

      Seq(blocks1, blocks2).foreach { blocks =>
        val agg = getNewAggregator(instances, Vectors.dense(coefArray ++ interceptArray),
          fitIntercept = true, fitWithMean = true)
        blocks.foreach(agg.add)
        assert(agg.loss ~== loss relTol 1e-9)
        assert(agg.gradient ~== gradient relTol 1e-9)
      }
    }
  }

  test("check with zero standard deviation") {
    val coefArray = Array(1.0, 2.0, -2.0, 3.0, 0.0, -1.0)
    val coefArrayFiltered = Array(3.0, 0.0, -1.0)
    val interceptArray = Array(4.0, 2.0, -3.0)

    Seq((false, false), (true, false), (true, true)).foreach { case (fitIntercept, fitWithMean) =>
      val coefVec = if (fitIntercept) {
        Vectors.dense(coefArray ++ interceptArray)
      } else {
        Vectors.dense(coefArray)
      }
      val aggConstantFeature = getNewAggregator(instancesConstantFeature,
        coefVec, fitIntercept = fitIntercept, fitWithMean = fitWithMean)
      aggConstantFeature
        .add(InstanceBlock.fromInstances(standardize(instancesConstantFeature).toImmutableArraySeq))
      val grad = aggConstantFeature.gradient

      val coefVecFiltered = if (fitIntercept) {
        Vectors.dense(coefArrayFiltered ++ interceptArray)
      } else {
        Vectors.dense(coefArrayFiltered)
      }
      val aggConstantFeatureFiltered = getNewAggregator(instancesConstantFeatureFiltered,
        coefVecFiltered, fitIntercept = fitIntercept, fitWithMean = fitWithMean)
      aggConstantFeatureFiltered
        .add(InstanceBlock.fromInstances(standardize(instancesConstantFeatureFiltered)
          .toImmutableArraySeq))
      val gradFiltered = aggConstantFeatureFiltered.gradient

      // constant features should not affect gradient
      assert(Vectors.dense(grad.toArray.take(3)) === Vectors.zeros(3))
      assert(Vectors.dense(grad.toArray.slice(3, 6)) ~==
        Vectors.dense(gradFiltered.toArray.take(3)) relTol 1e-9)
      if (fitIntercept) {
        assert(Vectors.dense(grad.toArray.takeRight(3)) ~==
          Vectors.dense(gradFiltered.toArray.takeRight(3)) relTol 1e-9)
      }
    }
  }
}
