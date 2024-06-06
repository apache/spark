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

class LeastSquaresBlockAggregatorSuite extends SparkFunSuite with MLlibTestSparkContext {

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

  /** Get summary statistics for some data and create a new LeastSquaresBlockAggregator. */
  private def getNewAggregator(
      instances: Array[Instance],
      coefficients: Vector,
      fitIntercept: Boolean): LeastSquaresBlockAggregator = {
    val (featuresSummarizer, ySummarizer) =
      Summarizer.getRegressionSummarizers(sc.parallelize(instances.toImmutableArraySeq))
    val yStd = ySummarizer.std(0)
    val yMean = ySummarizer.mean(0)
    val featuresStd = featuresSummarizer.std.toArray
    val featuresMean = featuresSummarizer.mean.toArray
    val inverseStd = featuresStd.map(std => if (std != 0) 1.0 / std else 0.0)
    val scaledMean = inverseStd.zip(featuresMean).map(t => t._1 * t._2)
    val bcInverseStd = sc.broadcast(inverseStd)
    val bcScaledMean = sc.broadcast(scaledMean)
    val bcCoefficients = sc.broadcast(coefficients)
    new LeastSquaresBlockAggregator(bcInverseStd, bcScaledMean, fitIntercept, yStd,
      yMean)(bcCoefficients)
  }

  test("sparse coefficients") {
    val bcInverseStd = sc.broadcast(Array(1.0))
    val bcScaledMean = sc.broadcast(Array(2.0))
    val bcCoefficients = sc.broadcast(Vectors.sparse(1, Array(0), Array(1.0)))
    val binaryAgg = new LeastSquaresBlockAggregator(bcInverseStd, bcScaledMean,
      fitIntercept = false, 1.0, 2.0)(bcCoefficients)
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
    val agg = getNewAggregator(instances, Vectors.dense(coefArray), fitIntercept = true)
    val block = InstanceBlock.fromInstances(Seq(Instance(1.0, 1.0, Vectors.dense(2.0))))
    withClue("BinaryLogisticBlockAggregator features dimension must match coefficients dimension") {
      intercept[IllegalArgumentException] {
        agg.add(block)
      }
    }
  }

  test("negative weight") {
    val coefArray = Array(1.0, 2.0)
    val agg = getNewAggregator(instances, Vectors.dense(coefArray), fitIntercept = true)
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
    val coefVec = Vectors.dense(Array.fill(numFeatures)(rng.nextDouble()))
    val block = InstanceBlock.fromInstances(instances.toImmutableArraySeq)

    val agg = getNewAggregator(instances, coefVec, fitIntercept = true)

    agg.add(block)
    assert(agg.gradient.size === numFeatures)
  }

  test("check correctness") {
    /*
    Check that the aggregator computes loss/gradient for:
      0.5 * sum_i=1^N ([sum_j=1^D beta_j * ((x_j - x_j,bar) / sigma_j)] - ((y - ybar) / sigma_y))^2
     */
    val coefficients = Vectors.dense(1.0, 2.0)
    val numFeatures = coefficients.size
    val (featuresSummarizer, ySummarizer) =
      Summarizer.getRegressionSummarizers(sc.parallelize(instances.toImmutableArraySeq))
    val featuresStd = featuresSummarizer.std.toArray
    val featuresMean = featuresSummarizer.mean.toArray
    val yStd = ySummarizer.std(0)
    val yMean = ySummarizer.mean(0)

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

    Seq(1, 2, 4).foreach { blockSize =>
      val blocks1 = scaledInstances
        .grouped(blockSize)
        .map(seq => InstanceBlock.fromInstances(seq.toImmutableArraySeq))
        .toArray
      val blocks2 = blocks1.map { block =>
        new InstanceBlock(block.labels, block.weights, block.matrix.toSparseRowMajor)
      }

      Seq(blocks1, blocks2).foreach { blocks =>
        val agg = getNewAggregator(instances, coefficients, fitIntercept = true)
        blocks.foreach(agg.add)
        assert(agg.loss ~== (expectedLoss.sum / weightSum) relTol 1e-9)
        assert(agg.gradient ~== expectedGradient relTol 1e-9)
      }
    }
  }
}
