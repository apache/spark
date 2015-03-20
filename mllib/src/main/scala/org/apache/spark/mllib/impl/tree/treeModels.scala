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

package org.apache.spark.mllib.impl.tree

import org.apache.spark.api.java.{JavaDoubleRDD, JavaRDD}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD


abstract class PredictionModel extends Serializable {

  /**
   * Predict the label for a single data point
   *
   * @param features  feature vector for a single data point
   * @return  prediction computed by this model
   */
  def predict(features: Vector): Double

  /**
   * Predict labels for an RDD of data points
   *
   * @param features  RDD of feature vectors for data points
   * @return  RDD of predictions for each of the given data points
   */
  def predict(features: RDD[Vector]): RDD[Double] = {
    features.map(predict)
  }

  /** Java-friendly version of batch [[predict()]] */
  def predict(features: JavaRDD[Vector]): JavaDoubleRDD = {
    JavaDoubleRDD.fromRDD(predict(features.rdd))
  }

}

abstract class DecisionTreeModel(val rootNode: Node) extends PredictionModel with Serializable {

  override def predict(features: Vector): Double = {
    rootNode.predict(features)
  }

  /** Number of nodes in tree, including leaf nodes. */
  lazy val numNodes: Int = {
    1 + rootNode.numDescendants
  }

  /**
   * Depth of the tree.
   * E.g.: Depth 0 means 1 leaf node.  Depth 1 means 1 internal node and 2 leaf nodes.
   */
  lazy val depth: Int = {
    rootNode.subtreeDepth
  }

  /** Summary of the model */
  override def toString: String = {
    // Implementing classes should generally override this method to be more descriptive.
    s"DecisionTreeModel of depth $depth with $numNodes nodes"
  }

  /** Full description of model */
  def toDebugString: String = {
    val header = toString + "\n"
    header + rootNode.subtreeToString(2)
  }
}

abstract class TreeEnsembleModel

abstract class RandomForestModel extends TreeEnsembleModel

abstract class GBTModel extends TreeEnsembleModel
