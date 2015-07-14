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

package org.apache.spark.mllib.classification

import scala.util.Random

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, argmax => brzArgmax, sum => brzSum}
import breeze.stats.distributions.{Multinomial => BrzMultinomial}

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.{LocalClusterSparkContext, MLlibTestSparkContext}
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.util.Utils

object NaiveBayesSuite {

  import NaiveBayes.{Multinomial, Bernoulli}

  private def calcLabel(p: Double, pi: Array[Double]): Int = {
    var sum = 0.0
    for (j <- 0 until pi.length) {
      sum += pi(j)
      if (p < sum) return j
    }
    -1
  }

  // Generate input of the form Y = (theta * x).argmax()
  def generateNaiveBayesInput(
    pi: Array[Double],            // 1XC
    theta: Array[Array[Double]],  // CXD
    nPoints: Int,
    seed: Int,
    modelType: String = Multinomial,
    sample: Int = 10): Seq[LabeledPoint] = {
    val D = theta(0).length
    val rnd = new Random(seed)
    val _pi = pi.map(math.pow(math.E, _))
    val _theta = theta.map(row => row.map(math.pow(math.E, _)))

    for (i <- 0 until nPoints) yield {
      val y = calcLabel(rnd.nextDouble(), _pi)
      val xi = modelType match {
        case Bernoulli => Array.tabulate[Double] (D) { j =>
            if (rnd.nextDouble () < _theta(y)(j) ) 1 else 0
        }
        case Multinomial =>
          val mult = BrzMultinomial(BDV(_theta(y)))
          val emptyMap = (0 until D).map(x => (x, 0.0)).toMap
          val counts = emptyMap ++ mult.sample(sample).groupBy(x => x).map {
            case (index, reps) => (index, reps.size.toDouble)
          }
          counts.toArray.sortBy(_._1).map(_._2)
        case _ =>
          // This should never happen.
          throw new UnknownError(s"Invalid modelType: $modelType.")
      }

      LabeledPoint(y, Vectors.dense(xi))
    }
  }

  /** Bernoulli NaiveBayes with binary labels, 3 features */
  private val binaryBernoulliModel = new NaiveBayesModel(labels = Array(0.0, 1.0),
    pi = Array(0.2, 0.8), theta = Array(Array(0.1, 0.3, 0.6), Array(0.2, 0.4, 0.4)), Bernoulli)

  /** Multinomial NaiveBayes with binary labels, 3 features */
  private val binaryMultinomialModel = new NaiveBayesModel(labels = Array(0.0, 1.0),
    pi = Array(0.2, 0.8), theta = Array(Array(0.1, 0.3, 0.6), Array(0.2, 0.4, 0.4)), Multinomial)
}

class NaiveBayesSuite extends SparkFunSuite with MLlibTestSparkContext {

  import NaiveBayes.{Multinomial, Bernoulli}

  def validatePrediction(predictions: Seq[Double], input: Seq[LabeledPoint]) {
    val numOfPredictions = predictions.zip(input).count {
      case (prediction, expected) =>
        prediction != expected.label
    }
    // At least 80% of the predictions should be on.
    assert(numOfPredictions < input.length / 5)
  }

  def validateModelFit(
      piData: Array[Double],
      thetaData: Array[Array[Double]],
      model: NaiveBayesModel): Unit = {
    def closeFit(d1: Double, d2: Double, precision: Double): Boolean = {
      (d1 - d2).abs <= precision
    }
    val modelIndex = (0 until piData.length).zip(model.labels.map(_.toInt))
    for (i <- modelIndex) {
      assert(closeFit(math.exp(piData(i._2)), math.exp(model.pi(i._1)), 0.05))
    }
    for (i <- modelIndex) {
      for (j <- 0 until thetaData(i._2).length) {
        assert(closeFit(math.exp(thetaData(i._2)(j)), math.exp(model.theta(i._1)(j)), 0.05))
      }
    }
  }

  def validatePredictionsProbabilities(predictionsProbabilities: Seq[Array[Double]],
                                       input: Seq[LabeledPoint],
                                       modelType: String = Multinomial) = {
    predictionsProbabilities.foreach { probabilities =>
      val sum = probabilities.sum
      // Check that prediction probabilities sum up to one
      // with an epsilon of 10^-5
      assert(sum ~== 1.0 relTol 0.00001)
    }

    val wrongPredictions = predictionsProbabilities.zip(input).count {
      case (prediction, expected) =>
        prediction.indexOf(prediction.max).toDouble != expected.label
    }
    // At least 80% of the predictions should be on.
    assert(wrongPredictions < input.length / 5)

    comparePosteriorsWithR(predictionsProbabilities.take(10), modelType)
  }

  /**
   * The following is the instruction to reproduce the model using R's e1071 package.
   *
   * First of all, using the following scala code to save the data into `path`.
   *
   *    testRDD.map { x =>
   *      s"${x.label}, ${x.features.toArray.mkString(", ")}"
   *    }.saveAsTextFile("path")
   *
   * Using the following R code to load the data and train the model using e1071 package.
   *
   *    library(e1071)
   *    data <- read.csv("path", header = FALSE)
   *    labels <- factor(data$V1)
   *    features <- data.frame(data$V2, data$V3, data$V4, data$V5)
   *    model <- naiveBayes(features, labels)
   *    predictions <- predict(model, features[1:10, -1], type = "raw")
   *
   */
  def comparePosteriorsWithR(predictionsProbabilities: Seq[Array[Double]],
                             modelType: String = Multinomial,
                             epsilon: Double = 0.1) = {
    require(predictionsProbabilities.length == 10)

    val posteriorsFromR = modelType match {
      case Multinomial =>
        Array(
          Array(2.942994e-07, 5.467545e-11, 9.999997e-01),
          Array(2.931850e-07, 4.922381e-12, 9.999997e-01),
          Array(9.997708e-01, 3.424392e-06, 2.257879e-04),
          Array(9.991757e-01, 6.132008e-04, 2.110877e-04),
          Array(9.281650e-14, 8.199463e-17, 1.000000e+00),
          Array(8.099445e-01, 3.821142e-05, 1.900173e-01),
          Array(2.667884e-01, 7.331288e-01, 8.276015e-05),
          Array(9.999776e-01, 2.163690e-06, 2.023486e-05),
          Array(9.997814e-01, 2.441990e-06, 2.161960e-04),
          Array(8.850206e-14, 6.692205e-18, 1.000000e+00)
        )
      case Bernoulli =>
        Array(
          Array(1.048099e-09, 1.000000e+00, 1.578642e-09),
          Array(1.831993e-19, 9.999999e-01, 1.190036e-07),
          Array(4.664977e-12, 1.000000e+00, 1.291666e-12),
          Array(3.224249e-11, 2.433594e-02, 9.756641e-01),
          Array(9.610916e-01, 1.256859e-13, 3.890841e-02),
          Array(8.318820e-01, 1.097496e-01, 5.836849e-02),
          Array(8.318820e-01, 1.097496e-01, 5.836849e-02),
          Array(9.610916e-01, 1.256859e-13, 3.890841e-02),
          Array(8.318820e-01, 1.097496e-01, 5.836849e-02),
          Array(8.318820e-01, 1.097496e-01, 5.836849e-02)
        )
    }

    predictionsProbabilities.zip(posteriorsFromR).foreach {
      case (probs, fromR) =>
        val p = probs.indexOf(probs.max)
        val r = fromR.indexOf(fromR.max)
        // Checking that the prediction is the same
        if (p == r) {
          probs.zip(fromR).foreach {
            case (prob, probFromR) => assert(prob > (probFromR - epsilon) && prob < (probFromR + epsilon))
          }
        }
    }
  }

  test("model types") {
    assert(Multinomial === "multinomial")
    assert(Bernoulli === "bernoulli")
  }

  test("get, set params") {
    val nb = new NaiveBayes()
    nb.setLambda(2.0)
    assert(nb.getLambda === 2.0)
    nb.setLambda(3.0)
    assert(nb.getLambda === 3.0)
  }

  test("Naive Bayes Multinomial") {
    val nPoints = 1000
    val pi = Array(0.5, 0.1, 0.4).map(math.log)
    val theta = Array(
      Array(0.70, 0.10, 0.10, 0.10), // label 0
      Array(0.10, 0.70, 0.10, 0.10), // label 1
      Array(0.10, 0.10, 0.70, 0.10)  // label 2
    ).map(_.map(math.log))

    val testData = NaiveBayesSuite.generateNaiveBayesInput(pi, theta, nPoints, 42, Multinomial)
    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    val model = NaiveBayes.train(testRDD, 1.0, Multinomial)
    validateModelFit(pi, theta, model)

    val validationData = NaiveBayesSuite.generateNaiveBayesInput(
      pi, theta, nPoints, 17, Multinomial)
    val validationRDD = sc.parallelize(validationData, 2)

    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)

    // Test prediction probabilities on RDD.
    validatePredictionsProbabilities(model.predictProbabilities(validationRDD.map(_.features)).map(_.toArray).collect(), validationData)

    // Test prediction probabilities on Array.
    validatePredictionsProbabilities(validationData.map(row => model.predictProbabilities(row.features)).map(_.toArray), validationData)
  }

  test("Naive Bayes Bernoulli") {
    val nPoints = 10000
    val pi = Array(0.5, 0.3, 0.2).map(math.log)
    val theta = Array(
      Array(0.50, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.40), // label 0
      Array(0.02, 0.70, 0.10, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02), // label 1
      Array(0.02, 0.02, 0.60, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.30)  // label 2
    ).map(_.map(math.log))

    val testData = NaiveBayesSuite.generateNaiveBayesInput(
      pi, theta, nPoints, 45, Bernoulli)
    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    val model = NaiveBayes.train(testRDD, 1.0, Bernoulli)
    validateModelFit(pi, theta, model)

    val validationData = NaiveBayesSuite.generateNaiveBayesInput(
      pi, theta, nPoints, 20, Bernoulli)
    val validationRDD = sc.parallelize(validationData, 2)

    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)

    // Test prediction probabilities on RDD.
    validatePredictionsProbabilities(model.predictProbabilities(validationRDD.map(_.features)).map(_.toArray).collect(),
      validationData, Bernoulli)

    // Test prediction probabilities on Array.
    validatePredictionsProbabilities(validationData.map(row => model.predictProbabilities(row.features)).map(_.toArray),
      validationData, Bernoulli)
  }

  test("detect negative values") {
    val dense = Seq(
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(0.0, Vectors.dense(-1.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(1.0, Vectors.dense(0.0)))
    intercept[SparkException] {
      NaiveBayes.train(sc.makeRDD(dense, 2))
    }
    val sparse = Seq(
      LabeledPoint(1.0, Vectors.sparse(1, Array(0), Array(1.0))),
      LabeledPoint(0.0, Vectors.sparse(1, Array(0), Array(-1.0))),
      LabeledPoint(1.0, Vectors.sparse(1, Array(0), Array(1.0))),
      LabeledPoint(1.0, Vectors.sparse(1, Array.empty, Array.empty)))
    intercept[SparkException] {
      NaiveBayes.train(sc.makeRDD(sparse, 2))
    }
    val nan = Seq(
      LabeledPoint(1.0, Vectors.sparse(1, Array(0), Array(1.0))),
      LabeledPoint(0.0, Vectors.sparse(1, Array(0), Array(Double.NaN))),
      LabeledPoint(1.0, Vectors.sparse(1, Array(0), Array(1.0))),
      LabeledPoint(1.0, Vectors.sparse(1, Array.empty, Array.empty)))
    intercept[SparkException] {
      NaiveBayes.train(sc.makeRDD(nan, 2))
    }
  }

  test("detect non zero or one values in Bernoulli") {
    val badTrain = Seq(
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(0.0, Vectors.dense(2.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(1.0, Vectors.dense(0.0)))

    intercept[SparkException] {
      NaiveBayes.train(sc.makeRDD(badTrain, 2), 1.0, Bernoulli)
    }

    val okTrain = Seq(
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(0.0, Vectors.dense(0.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(0.0, Vectors.dense(0.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(1.0, Vectors.dense(1.0))
    )

    val badPredict = Seq(
      Vectors.dense(1.0),
      Vectors.dense(2.0),
      Vectors.dense(1.0),
      Vectors.dense(0.0))

    val model = NaiveBayes.train(sc.makeRDD(okTrain, 2), 1.0, Bernoulli)
    intercept[SparkException] {
      model.predict(sc.makeRDD(badPredict, 2)).collect()
    }
  }

  test("model save/load: 2.0 to 2.0") {
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    Seq(NaiveBayesSuite.binaryBernoulliModel, NaiveBayesSuite.binaryMultinomialModel).map {
      model =>
        // Save model, load it back, and compare.
        try {
          model.save(sc, path)
          val sameModel = NaiveBayesModel.load(sc, path)
          assert(model.labels === sameModel.labels)
          assert(model.pi === sameModel.pi)
          assert(model.theta === sameModel.theta)
          assert(model.modelType === sameModel.modelType)
        } finally {
          Utils.deleteRecursively(tempDir)
        }
    }
  }

  test("model save/load: 1.0 to 2.0") {
    val model = NaiveBayesSuite.binaryMultinomialModel

    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    // Save model as version 1.0, load it back, and compare.
    try {
      val data = NaiveBayesModel.SaveLoadV1_0.Data(model.labels, model.pi, model.theta)
      NaiveBayesModel.SaveLoadV1_0.save(sc, path, data)
      val sameModel = NaiveBayesModel.load(sc, path)
      assert(model.labels === sameModel.labels)
      assert(model.pi === sameModel.pi)
      assert(model.theta === sameModel.theta)
      assert(model.modelType === Multinomial)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}

class NaiveBayesClusterSuite extends SparkFunSuite with LocalClusterSparkContext {

  test("task size should be small in both training and prediction") {
    val m = 10
    val n = 200000
    val examples = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map { i =>
        LabeledPoint(random.nextInt(2), Vectors.dense(Array.fill(n)(random.nextDouble())))
      }
    }
    // If we serialize data directly in the task closure, the size of the serialized task
    // would be greater than 1MB and hence Spark would throw an error.
    val model = NaiveBayes.train(examples)
    val predictions = model.predict(examples.map(_.features))
  }
}
