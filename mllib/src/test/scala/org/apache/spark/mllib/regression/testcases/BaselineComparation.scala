package org.apache.spark.mllib.regression.testcases

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.baseline.MeanRegressionLearner
import org.apache.spark.mllib.regression.{LabeledPoint, RegressionLearner, RegressionModel}
import org.apache.spark.rdd.{DoubleRDDFunctions, RDD}

import scala.util.Random


object BaselineComparation {

  def testCompareToBaseline(learner: RegressionLearner, trainSet: RDD[LabeledPoint], better: Double) {

    val baselineLearner = new MeanRegressionLearner
    val baselineModel = baselineLearner.train(trainSet)

    val testableModel = learner.train(trainSet)

    val testSet = trainSet.sample(false, 0.1)

    assert(compareTwoModels(baselineModel, testableModel, testSet) > better)

  }

  private def compareTwoModels(first: RegressionModel, second: RegressionModel, testSet: RDD[LabeledPoint]) = {
    val firstResult = first.predict(testSet.map(_.features))
    val secondResult = second.predict(testSet.map(_.features))

    val expecteed = testSet.map(_.label)
    val firstError = calculateError(expecteed, firstResult)
    val secondError = calculateError(expecteed, secondResult)

    secondError / firstError

  }

  private def calculateError(expected: RDD[Double], actual: RDD[Double]): Double = {
    // bad code. task for better code  - SPARK-3510
    val expectedRepartitioned = expected.cache().repartition(1) // need for zip operation
    val actualRepartitioned = actual.cache().repartition(1)
    new DoubleRDDFunctions(
      expectedRepartitioned
        .zip(actualRepartitioned)
        .map(a => {
        Math.pow(a._1 - a._2, 2)
      })).mean()
  }


  def randomRegressionLabeledFeatureSet(sc: SparkContext, size: Int, featureNumber: Int) = {
    // bad code. task for better code  - SPARK-3509
    val seed = Random.nextLong();
    sc.parallelize(1 to size, 10).map(i => {
      val features = (1 to featureNumber).map(_ => Random.nextDouble()).toArray
      var seedCopy = seed
      val result = features.reduceLeft((a, b) => {
        if (seedCopy % 3 == 0) {
          seedCopy = seedCopy / 3
          a * b
        } else {
          seedCopy = seedCopy / 2
          a + b
        }
      })
      new LabeledPoint(result, Vectors.dense(features))
    })
  }


}
