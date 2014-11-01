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

package org.apache.spark.mllib.tree.configuration

import scala.beans.BeanProperty

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.{Gini, Variance}
import org.apache.spark.mllib.tree.loss.{LogLoss, SquaredError, Loss}

/**
 * :: Experimental ::
 * Stores all the configuration options for the boosting algorithms
 * @param algo  Learning goal.  Supported:
 *              [[org.apache.spark.mllib.tree.configuration.Algo.Classification]],
 *              [[org.apache.spark.mllib.tree.configuration.Algo.Regression]]
 * @param numEstimators Number of estimators used in boosting stages. In other words,
 *                      number of boosting iterations performed.
 * @param loss Loss function used for minimization during gradient boosting.
 * @param learningRate Learning rate for shrinking the contribution of each estimator. The
 *                     learning rate should be between in the interval (0, 1]
 * @param subsamplingRate  Fraction of the training data used for learning the decision tree.
 * @param numClassesForClassification Number of classes for classification.
 *                                    (Ignored for regression.)
 *                                    Default value is 2 (binary classification).
 * @param categoricalFeaturesInfo A map storing information about the categorical variables and the
 *                                number of discrete values they take. For example, an entry (n ->
 *                                k) implies the feature n is categorical with k categories 0,
 *                                1, 2, ... , k-1. It's important to note that features are
 *                                zero-indexed.
 * @param weakLearnerParams Parameters for weak learners. Currently only decision trees are
 *                          supported.
 */
@Experimental
case class BoostingStrategy(
    // Required boosting parameters
    algo: Algo,
    @BeanProperty var numEstimators: Int,
    @BeanProperty var loss: Loss,
    // Optional boosting parameters
    @BeanProperty var learningRate: Double = 0.1,
    @BeanProperty var subsamplingRate: Double = 1.0,
    @BeanProperty var numClassesForClassification: Int = 2,
    @BeanProperty var categoricalFeaturesInfo: Map[Int, Int] = Map[Int, Int](),
    @BeanProperty var weakLearnerParams: Strategy) extends Serializable {

  require(learningRate <= 1, "Learning rate should be <= 1. Provided learning rate is " +
    s"$learningRate.")
  require(learningRate > 0, "Learning rate should be > 0. Provided learning rate is " +
    s"$learningRate.")

  // Ensure values for weak learner are the same as what is provided to the boosting algorithm.
  weakLearnerParams.categoricalFeaturesInfo = categoricalFeaturesInfo
  weakLearnerParams.numClassesForClassification = numClassesForClassification
  weakLearnerParams.subsamplingRate = subsamplingRate

}

@Experimental
object BoostingStrategy {

  /**
   * Returns default configuration for the boosting algorithm
   * @param algo Learning goal.  Supported:
   *             [[org.apache.spark.mllib.tree.configuration.Algo.Classification]],
   *             [[org.apache.spark.mllib.tree.configuration.Algo.Regression]]
   * @return Configuration for boosting algorithm
   */
  def defaultParams(algo: Algo): BoostingStrategy = {
    val treeStrategy = defaultWeakLearnerParams(algo)
    algo match {
      case Classification =>
        new BoostingStrategy(algo, 100, LogLoss, weakLearnerParams = treeStrategy)
      case Regression =>
        new BoostingStrategy(algo, 100, SquaredError, weakLearnerParams = treeStrategy)
      case _ =>
        throw new IllegalArgumentException(s"$algo is not supported by the boosting.")
    }
  }

  /**
   * Returns default configuration for the weak learner (decision tree) algorithm
   * @param algo   Learning goal.  Supported:
   *              [[org.apache.spark.mllib.tree.configuration.Algo.Classification]],
   *              [[org.apache.spark.mllib.tree.configuration.Algo.Regression]]
   * @return Configuration for weak learner
   */
  def defaultWeakLearnerParams(algo: Algo): Strategy = {
    // Note: Regression tree used even for classification for GBT.
    new Strategy(Regression, Variance, 3)
  }

}
