package org.apache.spark.mllib.regression

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo.Algo
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.impurity.Impurity
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.{DoubleRDDFunctions, RDD}
import scala.util.Random

/**
 *
 * Read about the algorithm "Gradient boosting" here:
 * http://www.montefiore.ulg.ac.be/services/stochastic/pubs/2007/GWD07/geurts-icml2007.pdf
 *
 * Libraries that implement the algorithm "Gradient boosting" similar way
 * https://code.google.com/p/jforests/
 * https://code.google.com/p/jsgbm/
 *
 */
class StochasticGradientBoosting {

  /**
   * Train a Gradient Boosting model given an RDD of (label, features) pairs.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   * @param leaningRate Learning rate
   * @param countOfTrees Number of trees.
   * @param samplingSizeRatio Size of random sample, percent of ${input} size.
   * @param strategy The configuration parameters for the tree algorithm which specify the type
   *                 of algorithm (classification, regression, etc.), feature type (continuous,
   *                 categorical), depth of the tree, quantile calculation strategy, etc.
   * @return StochasticGradientBoostingModel that can be used for prediction
   */
  // TODO: think about correct scaladoc format
  def run(
       input : RDD[LabeledPoint],
       leaningRate : Double,
       countOfTrees : Int,
       samplingSizeRatio : Double,
       strategy: Strategy): StochasticGradientBoostingModel = {

    val featureDimension = input.count()
    val mean = new DoubleRDDFunctions(input.map(l => l.label)).mean()
    val boostingModel = new StochasticGradientBoostingModel(countOfTrees, mean, leaningRate)

    for (i <- 0 to countOfTrees - 1) {
      val gradient = input.map(l => l.label - boostingModel.computeValue(l.features))

      val newInput: RDD[LabeledPoint] = input
        .zip(gradient)
        .map{case(inputVal, gradientVal) => new LabeledPoint(gradientVal, inputVal.features)}

      val randomSample = newInput.sample(
        false,
        (samplingSizeRatio * featureDimension).asInstanceOf[Int],
        Random.nextInt()
      )

      val model = DecisionTree.train(randomSample, strategy)
      boostingModel.addTree(model)
    }
    boostingModel
  }
}

/**
 * Model that can be used for prediction.
 *
 * @param countOfTrees Number of trees.
 * @param initValue Initialize model with this value.
 * @param learningRate Learning rate.
 */
class StochasticGradientBoostingModel (
    private val countOfTrees: Int,
    private var initValue: Double,
    private val learningRate: Double) extends Serializable with RegressionModel {

  val trees: Array[DecisionTreeModel] = new Array[DecisionTreeModel](countOfTrees)
  var index: Int = 0

  def this(countOfTrees:Int, learning_rate: Double) = {
    this(countOfTrees, 0, learning_rate)
  }

  def computeValue(feature_x: Vector): Double = {
    var re_res = initValue

    if (index == 0) {
      return re_res
    }
    for (i <- 0 to index - 1) {
      re_res += learningRate * trees(i).predict(feature_x)
    }
    re_res
  }

  def addTree(tree : DecisionTreeModel) = {
    trees(index) = tree
    index += 1
  }

  def setInitValue (value : Double) = {
    initValue = value
  }

  override def predict(testData: RDD[Vector]): RDD[Double] = {
    testData.map(v => predict(v))
  }

  override def predict(testData: Vector): Double = {
    computeValue(testData)
  }
}


object StochasticGradientBoosting {

  /**
   * Train a Gradient Boosting model given an RDD of (label, features) pairs.
   * 
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   * @param algo Algorithm, classification or regression.
   * @param impurity Impurity criterion used for information gain calculation.
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * @return StochasticGradientBoostingModel that can be used for prediction.
   */
  def train(
       input : RDD[LabeledPoint],
       algo : Algo,
       impurity : Impurity,
       maxDepth : Int) : StochasticGradientBoostingModel= {
    train(input, 0.05, 100, 0.5, algo, impurity, maxDepth)
  }

  /**
   * Train a Gradient Boosting model given an RDD of (label, features) pairs.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   * @param leaningRate Learning rate.
   * @param countOfTrees Number of trees.
   * @param samplingSizeRatio Size of random sample, percent of {input} size.
   * @param algo Algorithm, classification or regression.
   * @param impurity Impurity criterion used for information gain calculation.
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * @return StochasticGradientBoostingModel that can be used for prediction.
   */
  def train(
       input : RDD[LabeledPoint],
       leaningRate : Double,
       countOfTrees : Int,
       samplingSizeRatio : Double,
       algo : Algo,
       impurity : Impurity,
       maxDepth : Int) : StochasticGradientBoostingModel= {
    val strategy : Strategy = new Strategy(
      algo,
      impurity,
      maxDepth
    )

    new StochasticGradientBoosting().run(
      input,
      leaningRate,
      countOfTrees,
      samplingSizeRatio,
      strategy
    )
  }
}

