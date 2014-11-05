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
import org.apache.spark.mllib.tree.loss.{LogLoss, SquaredError, Loss}

/**
 * :: Experimental ::
 * Stores all the configuration options for the boosting algorithms
 * @param algo  Learning goal.  Supported:
 *              [[org.apache.spark.mllib.tree.configuration.Algo.Classification]],
 *              [[org.apache.spark.mllib.tree.configuration.Algo.Regression]]
 * @param numIterations Number of iterations of boosting.  In other words, the number of
 *                      weak hypotheses used in the final model.
 * @param loss Loss function used for minimization during gradient boosting.
 * @param learningRate Learning rate for shrinking the contribution of each estimator. The
 *                     learning rate should be between in the interval (0, 1]
 * @param numClassesForClassification Number of classes for classification.
 *                                    (Ignored for regression.)
 *                                    This setting overrides any setting in [[weakLearnerParams]].
 *                                    Default value is 2 (binary classification).
 * @param weakLearnerParams Parameters for weak learners. Currently only decision trees are
 *                          supported.
 */
@Experimental
case class BoostingStrategy(
    // Required boosting parameters
    @BeanProperty var algo: Algo,
    @BeanProperty var numIterations: Int,
    @BeanProperty var loss: Loss,
    // Optional boosting parameters
    @BeanProperty var learningRate: Double = 0.1,
    @BeanProperty var numClassesForClassification: Int = 2,
    @BeanProperty var weakLearnerParams: Strategy) extends Serializable {

  // Ensure values for weak learner are the same as what is provided to the boosting algorithm.
  weakLearnerParams.numClassesForClassification = numClassesForClassification

  /**
   * Sets Algorithm using a String.
   */
  def setAlgo(algo: String): Unit = algo match {
    case "Classification" => setAlgo(Classification)
    case "Regression" => setAlgo(Regression)
  }

  /**
   * Check validity of parameters.
   * Throws exception if invalid.
   */
  private[tree] def assertValid(): Unit = {
    algo match {
      case Classification =>
        require(numClassesForClassification == 2)
      case Regression =>
        // nothing
      case _ =>
        throw new IllegalArgumentException(
          s"BoostingStrategy given invalid algo parameter: $algo." +
            s"  Valid settings are: Classification, Regression.")
    }
    require(learningRate > 0 && learningRate <= 1,
      "Learning rate should be in range (0, 1]. Provided learning rate is " + s"$learningRate.")
  }
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
  def defaultParams(algo: String): BoostingStrategy = {
    val treeStrategy = Strategy.defaultStrategy("Regression")
    treeStrategy.maxDepth = 3
    algo match {
      case "Classification" =>
        new BoostingStrategy(Algo.withName(algo), 100, LogLoss, weakLearnerParams = treeStrategy)
      case "Regression" =>
        new BoostingStrategy(Algo.withName(algo), 100, SquaredError,
          weakLearnerParams = treeStrategy)
      case _ =>
        throw new IllegalArgumentException(s"$algo is not supported by the boosting.")
    }
  }
}
