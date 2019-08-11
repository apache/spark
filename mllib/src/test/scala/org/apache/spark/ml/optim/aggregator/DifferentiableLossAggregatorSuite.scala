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
import org.apache.spark.ml.classification.MultiClassSummarizer
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer

class DifferentiableLossAggregatorSuite extends SparkFunSuite {

  import DifferentiableLossAggregatorSuite.TestAggregator

  private val instances1 = Array(
    Instance(0.0, 0.1, Vectors.dense(1.0, 2.0)),
    Instance(1.0, 0.5, Vectors.dense(1.5, 1.0)),
    Instance(2.0, 0.3, Vectors.dense(4.0, 0.5))
  )
  private val instances2 = Seq(
    Instance(0.2, 0.4, Vectors.dense(0.8, 2.5)),
    Instance(0.8, 0.9, Vectors.dense(2.0, 1.3)),
    Instance(1.5, 0.2, Vectors.dense(3.0, 0.2))
  )

  private def assertEqual[T, Agg <: DifferentiableLossAggregator[T, Agg]](
      agg1: DifferentiableLossAggregator[T, Agg],
      agg2: DifferentiableLossAggregator[T, Agg]): Unit = {
    assert(agg1.weight === agg2.weight)
    assert(agg1.loss === agg2.loss)
    assert(agg1.gradient === agg2.gradient)
  }

  test("empty aggregator") {
    val numFeatures = 5
    val coef = Vectors.dense(Array.fill(numFeatures)(1.0))
    val agg = new TestAggregator(numFeatures)(coef)
    withClue("cannot get loss for empty aggregator") {
      intercept[IllegalArgumentException] {
        agg.loss
      }
    }
    withClue("cannot get gradient for empty aggregator") {
      intercept[IllegalArgumentException] {
        agg.gradient
      }
    }
  }

  test("aggregator initialization") {
    val numFeatures = 3
    val coef = Vectors.dense(Array.fill(numFeatures)(1.0))
    val agg = new TestAggregator(numFeatures)(coef)
    agg.add(Instance(1.0, 0.3, Vectors.dense(Array.fill(numFeatures)(1.0))))
    assert(agg.gradient.size === 3)
    assert(agg.weight === 0.3)
  }

  test("merge aggregators") {
    val coefficients = Vectors.dense(0.5, -0.1)
    val agg1 = new TestAggregator(2)(coefficients)
    val agg2 = new TestAggregator(2)(coefficients)
    val aggBadDim = new TestAggregator(1)(Vectors.dense(0.5))
    aggBadDim.add(Instance(1.0, 1.0, Vectors.dense(1.0)))
    instances1.foreach(agg1.add)

    // merge incompatible aggregators
    withClue("cannot merge aggregators with different dimensions") {
      intercept[IllegalArgumentException] {
        agg1.merge(aggBadDim)
      }
    }

    // merge empty other
    val mergedEmptyOther = agg1.merge(agg2)
    assertEqual(mergedEmptyOther, agg1)
    assert(mergedEmptyOther === agg1)

    // merge empty this
    val agg3 = new TestAggregator(2)(coefficients)
    val mergedEmptyThis = agg3.merge(agg1)
    assertEqual(mergedEmptyThis, agg1)
    assert(mergedEmptyThis !== agg1)

    instances2.foreach(agg2.add)
    val (loss1, weight1, grad1) = (agg1.loss, agg1.weight, agg1.gradient)
    val (loss2, weight2, grad2) = (agg2.loss, agg2.weight, agg2.gradient)
    val merged = agg1.merge(agg2)

    // check pointers are equal
    assert(merged === agg1)

    // loss should be weighted average of the two individual losses
    assert(merged.loss === (loss1 * weight1 + loss2 * weight2) / (weight1 + weight2))
    assert(merged.weight === weight1 + weight2)

    // gradient should be weighted average of individual gradients
    val addedGradients = Vectors.dense(grad1.toArray.clone())
    BLAS.scal(weight1, addedGradients)
    BLAS.axpy(weight2, grad2, addedGradients)
    BLAS.scal(1 / (weight1 + weight2), addedGradients)
    assert(merged.gradient === addedGradients)
  }

  test("loss, gradient, weight") {
    val coefficients = Vectors.dense(0.5, -0.1)
    val agg = new TestAggregator(2)(coefficients)
    instances1.foreach(agg.add)
    val errors = instances1.map { case Instance(label, _, features) =>
      label - BLAS.dot(features, coefficients)
    }
    val expectedLoss = errors.zip(instances1).map { case (error: Double, instance: Instance) =>
      instance.weight * error * error / 2.0
    }
    val expectedGradient = Vectors.dense(0.0, 0.0)
    errors.zip(instances1).foreach { case (error, instance) =>
      BLAS.axpy(instance.weight * error, instance.features, expectedGradient)
    }
    BLAS.scal(1.0 / agg.weight, expectedGradient)
    val weightSum = instances1.map(_.weight).sum

    assert(agg.weight ~== weightSum relTol 1e-5)
    assert(agg.loss ~== expectedLoss.sum / weightSum relTol 1e-5)
    assert(agg.gradient ~== expectedGradient relTol 1e-5)
  }
}

object DifferentiableLossAggregatorSuite {
  /**
   * Dummy aggregator that represents least squares cost with no intercept.
   */
  class TestAggregator(numFeatures: Int)(coefficients: Vector)
    extends DifferentiableLossAggregator[Instance, TestAggregator] {

    protected override val dim: Int = numFeatures

    override def add(instance: Instance): TestAggregator = {
      val error = instance.label - BLAS.dot(coefficients, instance.features)
      weightSum += instance.weight
      lossSum += instance.weight * error * error / 2.0
      (0 until dim).foreach { j =>
        gradientSumArray(j) += instance.weight * error * instance.features(j)
      }
      this
    }
  }

  /** Get feature and label summarizers for provided data. */
  private[ml] def getRegressionSummarizers(
      instances: Array[Instance]): (MultivariateOnlineSummarizer, MultivariateOnlineSummarizer) = {
    val seqOp = (c: (MultivariateOnlineSummarizer, MultivariateOnlineSummarizer),
                 instance: Instance) =>
      (c._1.add(instance.features, instance.weight),
        c._2.add(Vectors.dense(instance.label), instance.weight))

    val combOp = (c1: (MultivariateOnlineSummarizer, MultivariateOnlineSummarizer),
                  c2: (MultivariateOnlineSummarizer, MultivariateOnlineSummarizer)) =>
      (c1._1.merge(c2._1), c1._2.merge(c2._2))

    instances.aggregate(
      (new MultivariateOnlineSummarizer, new MultivariateOnlineSummarizer)
    )(seqOp, combOp)
  }

  /** Get feature and label summarizers for provided data. */
  private[ml] def getClassificationSummarizers(
      instances: Array[Instance]): (MultivariateOnlineSummarizer, MultiClassSummarizer) = {
    val seqOp = (c: (MultivariateOnlineSummarizer, MultiClassSummarizer),
                 instance: Instance) =>
      (c._1.add(instance.features, instance.weight),
        c._2.add(instance.label, instance.weight))

    val combOp = (c1: (MultivariateOnlineSummarizer, MultiClassSummarizer),
                  c2: (MultivariateOnlineSummarizer, MultiClassSummarizer)) =>
      (c1._1.merge(c2._1), c1._2.merge(c2._2))

    instances.aggregate(
      (new MultivariateOnlineSummarizer, new MultiClassSummarizer)
    )(seqOp, combOp)
  }
}
