package org.apache.spark.mllib.regression

import java.util.Random
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo.Algo
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.impurity.Impurity
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.{DoubleRDDFunctions, RDD}

/**
 * Created by olgaoskina on 06/08/14.
 *
 * Read about the algorithm "Gradient boosting" here:
 * http://www.montefiore.ulg.ac.be/services/stochastic/pubs/2007/GWD07/geurts-icml2007.pdf
 *
 * Libraries that implement the algorithm "Gradient boosting"
 * https://code.google.com/p/jforests/
 * https://code.google.com/p/jsgbm/
 *
 */
class StochasticGradientBoosting ()
  extends Serializable {

  def run(
       input : RDD[LabeledPoint],
       leaningRate : Double,
       M : Int,
       samplingSizeRatio : Double,
       strategy: Strategy): StochasticGradientBoostingModel = {

    val featureDimension = input.count()
    val mean = new DoubleRDDFunctions(input.map(l => l.label)).mean()
    val boostingModel = new StochasticGradientBoostingModel(M, mean, leaningRate)

    for (i <- 0 to M - 1) {
      val gradient = input.map(l => l.label - boostingModel.computeValue(l.features))

      val newInput: RDD[LabeledPoint] = input
        .zip(gradient)
        .map{case(inputVal, gradientVal) => new LabeledPoint(gradientVal, inputVal.features)}

      val rand = new Random()
      val randomSample = newInput.sample(
        false,
        (samplingSizeRatio * featureDimension).asInstanceOf[Int],
        rand.nextInt()
      )

      val model = DecisionTree.train(randomSample, strategy)
      boostingModel.addTree(model)
    }
    boostingModel
  }
}

class StochasticGradientBoostingModel (
    private val countOfTrees: Int,
    private var initValue: Double,
    private val learningRate: Double) extends Serializable with RegressionModel {

  val trees: Array[DecisionTreeModel] = new Array[DecisionTreeModel](countOfTrees)
  var index: Int = 0

  def this(M:Int, learning_rate: Double) = {
    this(M, 0, learning_rate)
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

  /**
   * Predict values for the given data set using the model trained.
   *
   * @param testData RDD representing data points to be predicted
   * @return RDD[Double] where each entry contains the corresponding prediction
   */
  override def predict(testData: RDD[Vector]): RDD[Double] = {
    testData.map(v => predict(v))
  }

  /**
   * Predict values for a single data point using the model trained.
   *
   * @param testData array representing a single data point
   * @return Double prediction from the trained model
   */
  override def predict(testData: Vector): Double = {
    computeValue(testData)
  }
}

object StochasticGradientBoosting {
  def train(
       input : RDD[LabeledPoint],
       algo : Algo,
       impurity : Impurity,
       maxDepth : Int) : StochasticGradientBoostingModel= {
    train(input, 0.05, 100, 0.5, algo, impurity, maxDepth)
  }

  def train(
       input : RDD[LabeledPoint],
       leaningRate : Double,
       M : Int,
       m_sampling_size_ratio : Double,
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
      M,
      m_sampling_size_ratio,
      strategy
    )
  }
}

