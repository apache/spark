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

package org.apache.spark.mllib.optimization

import scala.util.Random
import scala.collection.JavaConversions._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.util.LocalSparkContext

object GradientDescentSuite {

  def generateLogisticInputAsList(
      offset: Double,
      scale: Double,
      nPoints: Int,
      seed: Int): java.util.List[LabeledPoint] = {
    seqAsJavaList(generateGDInput(offset, scale, nPoints, seed))
  }

  // Generate input of the form Y = logistic(offset + scale * X)
  def generateGDInput(
      offset: Double,
      scale: Double,
      nPoints: Int,
      seed: Int): Seq[LabeledPoint]  = {
    val rnd = new Random(seed)
    val x1 = Array.fill[Double](nPoints)(rnd.nextGaussian())

    val unifRand = new scala.util.Random(45)
    val rLogis = (0 until nPoints).map { i =>
      val u = unifRand.nextDouble()
      math.log(u) - math.log(1.0-u)
    }

    val y: Seq[Int] = (0 until nPoints).map { i =>
      val yVal = offset + scale * x1(i) + rLogis(i)
      if (yVal > 0) 1 else 0
    }

    val testData = (0 until nPoints).map(i => LabeledPoint(y(i), Array(x1(i))))
    testData
  }
}

class GradientDescentSuite extends FunSuite with LocalSparkContext with ShouldMatchers {

  test("Assert the loss is decreasing.") {
    val nPoints = 10000
    val A = 2.0
    val B = -1.5

    val initialB = -1.0
    val initialWeights = Array(initialB)

    val gradient = new LogisticGradient()
    val updater = new SimpleUpdater()
    val stepSize = 1.0
    val numIterations = 10
    val regParam = 0
    val miniBatchFrac = 1.0

    // Add a extra variable consisting of all 1.0's for the intercept.
    val testData = GradientDescentSuite.generateGDInput(A, B, nPoints, 42)
    val data = testData.map { case LabeledPoint(label, features) =>
      label -> Array(1.0, features: _*)
    }

    val dataRDD = sc.parallelize(data, 2).cache()
    val initialWeightsWithIntercept = Array(1.0, initialWeights: _*)

    val (_, loss) = GradientDescent.runMiniBatchSGD(
      dataRDD,
      gradient,
      updater,
      stepSize,
      numIterations,
      regParam,
      miniBatchFrac,
      initialWeightsWithIntercept)

    assert(loss.last - loss.head < 0, "loss isn't decreasing.")

    val lossDiff = loss.init.zip(loss.tail).map { case (lhs, rhs) => lhs - rhs }
    assert(lossDiff.count(_ > 0).toDouble / lossDiff.size > 0.8)
  }
}
