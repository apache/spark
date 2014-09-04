package org.apache.spark.examples.gradient_boosting

import org.apache.spark.mllib.regression.StochasticGradientBoosting
import org.apache.spark.mllib.tree.configuration.Algo.Regression
import org.apache.spark.mllib.tree.impurity.Variance
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.DoubleRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by olgaoskina on 07/08/14.
 *
 */
object Main {
    private val pathToTrainData: String = "/Users/olgaoskina/git/spark/data/mllib/train.txt"
    private val pathToValidData: String = "/Users/olgaoskina/git/spark/data/mllib/valid.txt"
    private val maxDepth: Int = 3

    def main(args: Array[String]) {
        val config = new SparkConf().setAppName("Main").setMaster("local")
        val sc = new SparkContext(config)

        val trainData = MLUtils.loadLabeledPoints(sc, pathToTrainData)
        val validData = MLUtils.loadLabeledPoints(sc, pathToValidData)

        val model = StochasticGradientBoosting.train(trainData, Regression, Variance, maxDepth)

        val valuesAndPredictions = validData.map { point =>
            val prediction = model.predict(point.features)
            (point.label, prediction)
        }
        val MSE = valuesAndPredictions.map{ case(v, p) => math.pow(v - p, 2)}
        val error = new DoubleRDDFunctions(MSE).mean()

        sc.stop()
        println("training Mean Squared Error = " + error)
    }
}
