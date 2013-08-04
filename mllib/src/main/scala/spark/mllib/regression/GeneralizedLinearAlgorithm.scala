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

package spark.mllib.regression

import spark.{Logging, RDD, SparkContext, SparkException}
import spark.mllib.optimization._
import spark.mllib.util.MLUtils

import scala.math.round

import org.jblas.DoubleMatrix

/**
 * GeneralizedLinearModel (GLM) represents a model trained using 
 * GeneralizedLinearAlgorithm. GLMs consist of a weight vector,
 * an intercept.
 */
abstract class GeneralizedLinearModel[T: ClassManifest](
    val weights: Array[Double],
    val intercept: Double)
  extends Serializable {

  // Create a column vector that can be used for predictions
  private val weightsMatrix = new DoubleMatrix(weights.length, 1, weights:_*)

  def predictPoint(dataMatrix: DoubleMatrix, weightMatrix: DoubleMatrix,
    intercept: Double): T

  def predict(testData: spark.RDD[Array[Double]]): RDD[T] = {
    // A small optimization to avoid serializing the entire model. Only the weightsMatrix
    // and intercept is needed.
    val localWeights = weightsMatrix
    val localIntercept = intercept

    testData.map { x =>
      val dataMatrix = new DoubleMatrix(1, x.length, x:_*)
      predictPoint(dataMatrix, localWeights, localIntercept)
    }
  }

  def predict(testData: Array[Double]): T = {
    val dataMat = new DoubleMatrix(1, testData.length, testData:_*)
    predictPoint(dataMat, weightsMatrix, intercept)
  }
}

/**
 * GeneralizedLinearAlgorithm abstracts out the training for all GLMs. 
 * This class should be mixed in with an Optimizer to create a new GLM.
 *
 * NOTE(shivaram): This is an abstract class rather than a trait as we use
 * a view bound to convert labels to Double.
 */
abstract class GeneralizedLinearAlgorithm[T, M](implicit
    t: T => Double,
    tManifest: Manifest[T],
    methodEv: M <:< GeneralizedLinearModel[T])
  extends Logging with Serializable {

  // We need an optimizer mixin to solve the GLM
  self : Optimizer =>

  var addIntercept: Boolean

  def createModel(weights: Array[Double], intercept: Double): M

  /**
   * Set if the algorithm should add an intercept. Default true.
   */
  def setIntercept(addIntercept: Boolean): this.type = {
    this.addIntercept = addIntercept
    this
  }

  def train(input: RDD[(T, Array[Double])]) : M = {
    val nfeatures: Int = input.first()._2.length
    val initialWeights = Array.fill(nfeatures)(1.0)
    train(input, initialWeights)
  }

  def train(
      input: RDD[(T, Array[Double])],
      initialWeights: Array[Double])
    : M = {

    // Add a extra variable consisting of all 1.0's for the intercept.
    val data = if (addIntercept) {
      input.map { case (y, features) =>
        (y.toDouble, Array(1.0, features:_*))
      }
    } else {
      input.map { case (y, features) =>
        (y.toDouble, features)
      }
    }

    val initialWeightsWithIntercept = if (addIntercept) {
      Array(1.0, initialWeights:_*)
    } else {
      initialWeights
    }

    val weights = optimize(data, initialWeightsWithIntercept)
    val intercept = weights(0)
    val weightsScaled = weights.tail

    val model = createModel(weightsScaled, intercept)

    logInfo("Final model weights " + model.weights.mkString(","))
    logInfo("Final model intercept " + model.intercept)
    model
  }
}
