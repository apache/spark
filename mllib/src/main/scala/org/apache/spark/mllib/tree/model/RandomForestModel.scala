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

import scala.collection.mutable

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 * Random forest model for classification or regression.
 * This model stores a collection of [[DecisionTreeModel]] instances and uses them to make
 * aggregate predictions.
 * @param trees Trees which make up this forest.  This cannot be empty.
 * @param algo algorithm type -- classification or regression
 */
@Experimental
class RandomForestModel(val trees: Array[DecisionTreeModel], val algo: Algo) extends Serializable {

  require(trees.size > 0, s"RandomForestModel cannot be created with empty trees collection.")

  /**
   * Predict values for a single data point.
   *
   * @param features array representing a single data point
   * @return Double prediction from the trained model
   */
  def predict(features: Vector): Double = {
    algo match {
      case Classification =>
        val predictionToCount = new mutable.HashMap[Int, Int]()
        trees.foreach { tree =>
          val prediction = tree.predict(features).toInt
          predictionToCount(prediction) = predictionToCount.getOrElse(prediction, 0) + 1
        }
        predictionToCount.maxBy(_._2)._1
      case Regression =>
        trees.map(_.predict(features)).sum / trees.size
    }
  }

  /**
   * Predict values for the given data set.
   *
   * @param features RDD representing data points to be predicted
   * @return RDD[Double] where each entry contains the corresponding prediction
   */
  def predict(features: RDD[Vector]): RDD[Double] = {
    features.map(x => predict(x))
  }

  /**
   * Get number of trees in forest.
   */
  def numTrees: Int = trees.size

  /**
   * Get total number of nodes, summed over all trees in the forest.
   */
  def totalNumNodes: Int = trees.map(tree => tree.numNodes).sum

  /**
   * Print a summary of the model.
   */
  override def toString: String = algo match {
    case Classification =>
      s"RandomForestModel classifier with $numTrees trees and $totalNumNodes total nodes"
    case Regression =>
      s"RandomForestModel regressor with $numTrees trees and $totalNumNodes total nodes"
    case _ => throw new IllegalArgumentException(
      s"RandomForestModel given unknown algo parameter: $algo.")
  }

  /**
   * Print the full model to a string.
   */
  def toDebugString: String = {
    val header = toString + "\n"
    header + trees.zipWithIndex.map { case (tree, treeIndex) =>
      s"  Tree $treeIndex:\n" + tree.topNode.subtreeToString(4)
    }.fold("")(_ + _)
  }

}

private[tree] object RandomForestModel {

  def build(trees: Array[DecisionTreeModel]): RandomForestModel = {
    require(trees.size > 0, s"RandomForestModel cannot be created with empty trees collection.")
    val algo: Algo = trees(0).algo
    require(trees.forall(_.algo == algo),
      "RandomForestModel cannot combine trees which have different output types" +
      " (classification/regression).")
    new RandomForestModel(trees, algo)
  }

}
