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

package org.apache.spark.mllib.grouped

import org.apache.spark.mllib.regression.{LabeledPoint, GeneralizedLinearAlgorithm}
import org.apache.spark.mllib.classification.{SVMWithSGD, SVMModel}
import org.apache.spark.mllib.optimization.{GradientDescent, SquaredL2Updater, HingeGradient}
import org.apache.spark.mllib.util.DataValidators
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag


class GroupedSVMWithSGD[K] private (
                           private var stepSize: Double,
                           private var numIterations: Int,
                           private var regParam: Double,
                           private var miniBatchFraction: Double) (implicit tag:ClassTag[K])
  extends GroupedGeneralizedLinearAlgorithm[K,SVMModel] with Serializable {

  private val gradient = new HingeGradient()
  private val updater = new SquaredL2Updater()
  override val optimizer = new GroupedGradientDescent[K](gradient, updater)
    .setStepSize(stepSize)
    .setNumIterations(numIterations)
    .setRegParam(regParam)
    .setMiniBatchFraction(miniBatchFraction)
  override protected val validators = List(DataValidators.binaryLabelValidator)

  /**
   * Construct a SVM object with default parameters
   */
  def this()(implicit tag:ClassTag[K]) = this(1.0, 100, 1.0, 1.0)

  /**
   * Create a model given the weights and intercept
   */
  override protected def createModel(weights: Vector, intercept: Double) = {
    new SVMModel(weights, intercept)
  }
}


object GroupedSVMWithSGD {

  /**
   * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate the gradient. The weights used in
   * gradient descent are initialized using the initial weights provided.
   *
   * NOTE: Labels used in SVM should be {0, 1}.
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param regParam Regularization parameter.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   * @param initialWeights Initial set of weights to be used. Array should be equal in size to
   *        the number of features in the data.
   */
  def train[K](
             input: RDD[(K,LabeledPoint)],
             numIterations: Int,
             stepSize: Double,
             regParam: Double,
             miniBatchFraction: Double,
             initialWeights: Map[K,Vector]) (implicit tag:ClassTag[K]) : Map[K,SVMModel] = {
    new GroupedSVMWithSGD[K](stepSize, numIterations, regParam, miniBatchFraction)
      .run(input, initialWeights)
  }

  /**
   * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate the gradient.
   * NOTE: Labels used in SVM should be {0, 1}
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param regParam Regularization parameter.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   */
  def train[K](
             input: RDD[(K,LabeledPoint)],
             numIterations: Int,
             stepSize: Double,
             regParam: Double,
             miniBatchFraction: Double) (implicit tag : ClassTag[K]) : Map[K,SVMModel] = {
    new GroupedSVMWithSGD(stepSize, numIterations, regParam, miniBatchFraction).run(input)
  }

  /**
   * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. We use the entire data set to
   * update the gradient in each iteration.
   * NOTE: Labels used in SVM should be {0, 1}
   *
   * @param input RDD of (label, array of features) pairs.
   * @param stepSize Step size to be used for each iteration of Gradient Descent.
   * @param regParam Regularization parameter.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a SVMModel which has the weights and offset from training.
   */
  def train[K <: Serializable](
             input: RDD[(K,LabeledPoint)],
             numIterations: Int,
             stepSize: Double,
             regParam: Double) (implicit tag : ClassTag[K]) : Map[K,SVMModel] = {
    train(input, numIterations, stepSize, regParam, 1.0)
  }

  /**
   * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using a step size of 1.0. We use the entire data set to
   * update the gradient in each iteration.
   * NOTE: Labels used in SVM should be {0, 1}
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a SVMModel which has the weights and offset from training.
   */
  def train[K](input: RDD[(K,LabeledPoint)], numIterations: Int)
              (implicit tag : ClassTag[K]) :
  Map[K,SVMModel] = {
    train(input, numIterations, 1.0, 1.0, 1.0)
  }
}
