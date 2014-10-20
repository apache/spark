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

package org.apache.spark.mllib.tree.model

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.rdd.RDD

@Experimental
class GradientBoostingModel(trees: Array[DecisionTreeModel], strategy: BoostingStrategy)
  extends Serializable {

  require(numTrees > 0, s"GradientBoostingModel cannot be created with empty trees collection.")

  /**
   * Predict values for a single data point using the model trained.
   *
   * @param features array representing a single data point
   * @return predicted category from the trained model
   */
  private def predictRaw(features: Vector): Double = {
    val treePredictions = trees.map(tree => tree.predict(features))
    if (numTrees == 1){
      treePredictions(0)
    } else {
      var prediction = treePredictions(0)
      var index = 1
      while (index < numTrees) {
        prediction += strategy.learningRate * treePredictions(index)
        index += 1
      }
      prediction
    }
  }

  /**
   * Predict values for a single data point using the model trained.
   *
   * @param features array representing a single data point
   * @return predicted category from the trained model
   */
  def predict(features: Vector): Double = {
    val algo = strategy.algo
    algo match {
      case Regression => predictRaw(features)
      case Classification => if (predictRaw(features) > 0 ) 1.0 else 0.0
      case _ => throw new IllegalArgumentException(
        s"GradientBoostingModel given unknown algo parameter: $algo.")
    }
  }

  /**
   * Predict values for the given data set.
   *
   * @param features RDD representing data points to be predicted
   * @return RDD[Double] where each entry contains the corresponding prediction
   */
  def predict(features: RDD[Vector]): RDD[Double] = features.map(x => predict(x))

  /**
   * Get number of trees in forest.
   */
  def numTrees: Int = trees.size

  /**
   * Print full model.
   */
  override def toString: String = {
    val algo = strategy.algo
    val header = algo match {
      case Classification =>
        s"GradientBoostingModel classifier with $numTrees trees\n"
      case Regression =>
        s"GradientBoostingModel regressor with $numTrees trees\n"
      case _ => throw new IllegalArgumentException(
        s"GradientBoostingModel given unknown algo parameter: $algo.")
    }
    header + trees.zipWithIndex.map { case (tree, treeIndex) =>
      s"  Tree $treeIndex:\n" + tree.topNode.subtreeToString(4)
    }.fold("")(_ + _)
  }

}
