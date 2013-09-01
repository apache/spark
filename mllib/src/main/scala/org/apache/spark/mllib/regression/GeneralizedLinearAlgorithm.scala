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

package org.apache.spark.mllib.regression

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.optimization._

import org.jblas.DoubleMatrix

/**
 * GeneralizedLinearModel (GLM) represents a model trained using 
 * GeneralizedLinearAlgorithm. GLMs consist of a weight vector and
 * an intercept.
 *
 * @param weights Weights computed for every feature.
 * @param intercept Intercept computed for this model.
 */
abstract class GeneralizedLinearModel(val weights: Array[Double], val intercept: Double)
  extends Serializable {

  // Create a column vector that can be used for predictions
  private val weightsMatrix = new DoubleMatrix(weights.length, 1, weights:_*)

  /**
   * Predict the result given a data point and the weights learned.
   * 
   * @param dataMatrix Row vector containing the features for this data point
   * @param weightMatrix Column vector containing the weights of the model
   * @param intercept Intercept of the model.
   */
  def predictPoint(dataMatrix: DoubleMatrix, weightMatrix: DoubleMatrix,
    intercept: Double): Double

  /**
   * Predict values for the given data set using the model trained.
   *
   * @param testData RDD representing data points to be predicted
   * @return RDD[Double] where each entry contains the corresponding prediction
   */
  def predict(testData: RDD[Array[Double]]): RDD[Double] = {
    // A small optimization to avoid serializing the entire model. Only the weightsMatrix
    // and intercept is needed.
    val localWeights = weightsMatrix
    val localIntercept = intercept

    testData.map { x =>
      val dataMatrix = new DoubleMatrix(1, x.length, x:_*)
      predictPoint(dataMatrix, localWeights, localIntercept)
    }
  }

  /**
   * Predict values for a single data point using the model trained.
   *
   * @param testData array representing a single data point
   * @return Double prediction from the trained model
   */
  def predict(testData: Array[Double]): Double = {
    val dataMat = new DoubleMatrix(1, testData.length, testData:_*)
    predictPoint(dataMat, weightsMatrix, intercept)
  }
}

/**
 * GeneralizedLinearAlgorithm implements methods to train a Genearalized Linear Model (GLM).
 * This class should be extended with an Optimizer to create a new GLM.
 */
abstract class GeneralizedLinearAlgorithm[M <: GeneralizedLinearModel]
  extends Logging with Serializable {

  protected val validators: Seq[RDD[LabeledPoint] => Boolean] = List()

  val optimizer: Optimizer

  protected var addIntercept: Boolean = true

  protected var validateData: Boolean = true

  /**
   * Create a model given the weights and intercept
   */
  protected def createModel(weights: Array[Double], intercept: Double): M

  /**
   * Set if the algorithm should add an intercept. Default true.
   */
  def setIntercept(addIntercept: Boolean): this.type = {
    this.addIntercept = addIntercept
    this
  }

  /**
   * Set if the algorithm should validate data before training. Default true.
   */
  def setValidateData(validateData: Boolean): this.type = {
    this.validateData = validateData
    this
  }

  /**
   * Run the algorithm with the configured parameters on an input
   * RDD of LabeledPoint entries.
   */
  def run(input: RDD[LabeledPoint]) : M = {
    val nfeatures: Int = input.first().features.length
    val initialWeights = Array.fill(nfeatures)(1.0)
    run(input, initialWeights)
  }

  /**
   * Run the algorithm with the configured parameters on an input RDD
   * of LabeledPoint entries starting from the initial weights provided.
   */
  def run(input: RDD[LabeledPoint], initialWeights: Array[Double]) : M = {

    // Check the data properties before running the optimizer
    if (validateData && !validators.forall(func => func(input))) {
      throw new SparkException("Input validation failed.")
    }

    // Add a extra variable consisting of all 1.0's for the intercept.
    val data = if (addIntercept) {
      input.map(labeledPoint => (labeledPoint.label, Array(1.0, labeledPoint.features:_*)))
    } else {
      input.map(labeledPoint => (labeledPoint.label, labeledPoint.features))
    }

    val initialWeightsWithIntercept = if (addIntercept) {
      Array(1.0, initialWeights:_*)
    } else {
      initialWeights
    }

    val weights = optimizer.optimize(data, initialWeightsWithIntercept)
    val intercept = weights(0)
    val weightsScaled = weights.tail

    val model = createModel(weightsScaled, intercept)

    logInfo("Final model weights " + model.weights.mkString(","))
    logInfo("Final model intercept " + model.intercept)
    model
  }
}
