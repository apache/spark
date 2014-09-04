package org.apache.spark.mllib.regression

import org.apache.spark.mllib.tree.model.DecisionTreeModel

import scala.collection.mutable.ArrayBuffer

/**
 * Created by olgaoskina on 06/08/14.
 *
 */

class ResultFunction (
     private var initValue: Double,
     private val learningRate: Double) extends Serializable {

  private var trees: ArrayBuffer[DecisionTreeModel] = new ArrayBuffer[DecisionTreeModel]()

  def this(learning_rate: Double) = {
    this(0, learning_rate)
  }

  def computeValue(feature_x: org.apache.spark.mllib.linalg.Vector): Double = {
    var re_res = initValue

    if (trees.size == 0) {
      return re_res
    }
    trees.foreach(tree => re_res += learningRate * tree.predict(feature_x))
    re_res
  }

  def addTree(tree : DecisionTreeModel) = {
    trees += tree
  }

  def setInitValue (value : Double) = {
    initValue = value
  }
}
