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
import org.apache.spark.ml.linalg._
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.mllib.util.MLlibTestSparkContext

class LeastSquaresAggregatorSuite extends SparkFunSuite with MLlibTestSparkContext {

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

  def getSummarizers(
    instances: Array[Instance]): (MultivariateOnlineSummarizer, MultivariateOnlineSummarizer) = {
    val seqOp = (c: (MultivariateOnlineSummarizer, MultivariateOnlineSummarizer),
                 instance: Instance) =>
      (c._1.add(instance.features, instance.weight),
        c._2.add(Vectors.dense(instance.label), instance.weight))

    val combOp = (c1: (MultivariateOnlineSummarizer, MultivariateOnlineSummarizer),
                  c2: (MultivariateOnlineSummarizer, MultivariateOnlineSummarizer)) =>
      (c1._1.merge(c2._1), c1._2.merge(c2._2))

    instances.aggregate(
      new MultivariateOnlineSummarizer, new MultivariateOnlineSummarizer
    )(seqOp, combOp)
  }

  /** Get summary statistics for some data and create a new LeastSquaresAggregator. */
  def getNewAggregator(
      instances: Array[Instance],
      coefficients: Vector,
      fitIntercept: Boolean): LeastSquaresAggregator = {
    val (featuresSummarizer, ySummarizer) = getSummarizers(instances)
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

  test("check initialization params") {
    val coefficients = Vectors.dense(1.0, 2.0)
    val aggIntercept = getNewAggregator(instances, coefficients, fitIntercept = true)
    val aggNoIntercept = getNewAggregator(instances, coefficients, fitIntercept = false)
    instances.foreach(aggIntercept.add)
    instances.foreach(aggNoIntercept.add)

    // least squares agg does not include intercept in its gradient array
    assert(aggIntercept.gradient.size === 2)
    assert(aggNoIntercept.gradient.size === 2)
  }

  test("check correctness with/without standardization") {

  }

  test("check with zero standardization") {
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
