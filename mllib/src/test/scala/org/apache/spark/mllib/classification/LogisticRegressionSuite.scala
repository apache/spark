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

import scala.collection.JavaConversions._
import scala.util.Random
import scala.util.control.Breaks._

import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.util.{LocalClusterSparkContext, MLlibTestSparkContext}
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.util.Utils


object LogisticRegressionSuite {

  def generateLogisticInputAsList(
    offset: Double,
    scale: Double,
    nPoints: Int,
    seed: Int): java.util.List[LabeledPoint] = {
    seqAsJavaList(generateLogisticInput(offset, scale, nPoints, seed))
  }

  // Generate input of the form Y = logistic(offset + scale*X)
  def generateLogisticInput(
      offset: Double,
      scale: Double,
      nPoints: Int,
      seed: Int): Seq[LabeledPoint] = {
    val rnd = new Random(seed)
    val x1 = Array.fill[Double](nPoints)(rnd.nextGaussian())

    val y = (0 until nPoints).map { i =>
      val p = 1.0 / (1.0 + math.exp(-(offset + scale * x1(i))))
      if (rnd.nextDouble() < p) 1.0 else 0.0
    }

    val testData = (0 until nPoints).map(i => LabeledPoint(y(i), Vectors.dense(Array(x1(i)))))
    testData
  }

  /**
   * Generates `k` classes multinomial synthetic logistic input in `n` dimensional space given the
   * model weights and mean/variance of the features. The synthetic data will be drawn from
   * the probability distribution constructed by weights using the following formula.
   *
   * P(y = 0 | x) = 1 / norm
   * P(y = 1 | x) = exp(x * w_1) / norm
   * P(y = 2 | x) = exp(x * w_2) / norm
   * ...
   * P(y = k-1 | x) = exp(x * w_{k-1}) / norm
   * where norm = 1 + exp(x * w_1) + exp(x * w_2) + ... + exp(x * w_{k-1})
   *
   * @param weights matrix is flatten into a vector; as a result, the dimension of weights vector
   *                will be (k - 1) * (n + 1) if `addIntercept == true`, and
   *                if `addIntercept != true`, the dimension will be (k - 1) * n.
   * @param xMean the mean of the generated features. Lots of time, if the features are not properly
   *              standardized, the algorithm with poor implementation will have difficulty
   *              to converge.
   * @param xVariance the variance of the generated features.
   * @param addIntercept whether to add intercept.
   * @param nPoints the number of instance of generated data.
   * @param seed the seed for random generator. For consistent testing result, it will be fixed.
   */
  def generateMultinomialLogisticInput(
      weights: Array[Double],
      xMean: Array[Double],
      xVariance: Array[Double],
      addIntercept: Boolean,
      nPoints: Int,
      seed: Int): Seq[LabeledPoint] = {
    val rnd = new Random(seed)

    val xDim = xMean.length
    val xWithInterceptsDim = if (addIntercept) xDim + 1 else xDim
    val nClasses = weights.length / xWithInterceptsDim + 1

    val x = Array.fill[Vector](nPoints)(Vectors.dense(Array.fill[Double](xDim)(rnd.nextGaussian())))

    x.foreach { vector =>
      // This doesn't work if `vector` is a sparse vector.
      val vectorArray = vector.toArray
      var i = 0
      val len = vectorArray.length
      while (i < len) {
        vectorArray(i) = vectorArray(i) * math.sqrt(xVariance(i)) + xMean(i)
        i += 1
      }
    }

    val y = (0 until nPoints).map { idx =>
      val xArray = x(idx).toArray
      val margins = Array.ofDim[Double](nClasses)
      val probs = Array.ofDim[Double](nClasses)

      for (i <- 0 until nClasses - 1) {
        for (j <- 0 until xDim) margins(i + 1) += weights(i * xWithInterceptsDim + j) * xArray(j)
        if (addIntercept) margins(i + 1) += weights((i + 1) * xWithInterceptsDim - 1)
      }
      // Preventing the overflow when we compute the probability
      val maxMargin = margins.max
      if (maxMargin > 0) for (i <- 0 until nClasses) margins(i) -= maxMargin

      // Computing the probabilities for each class from the margins.
      val norm = {
        var temp = 0.0
        for (i <- 0 until nClasses) {
          probs(i) = math.exp(margins(i))
          temp += probs(i)
        }
        temp
      }
      for (i <- 0 until nClasses) probs(i) /= norm

      // Compute the cumulative probability so we can generate a random number and assign a label.
      for (i <- 1 until nClasses) probs(i) += probs(i - 1)
      val p = rnd.nextDouble()
      var y = 0
      breakable {
        for (i <- 0 until nClasses) {
          if (p < probs(i)) {
            y = i
            break
          }
        }
      }
      y
    }

    val testData = (0 until nPoints).map(i => LabeledPoint(y(i), x(i)))
    testData
  }

  /** Binary labels, 3 features */
  private val binaryModel = new LogisticRegressionModel(
    weights = Vectors.dense(0.1, 0.2, 0.3), intercept = 0.5, numFeatures = 3, numClasses = 2)

  /** 3 classes, 2 features */
  private val multiclassModel = new LogisticRegressionModel(
    weights = Vectors.dense(0.1, 0.2, 0.3, 0.4), intercept = 1.0, numFeatures = 2, numClasses = 3)

  private def checkModelsEqual(a: LogisticRegressionModel, b: LogisticRegressionModel): Unit = {
    assert(a.weights == b.weights)
    assert(a.intercept == b.intercept)
    assert(a.numClasses == b.numClasses)
    assert(a.numFeatures == b.numFeatures)
    assert(a.getThreshold == b.getThreshold)
  }
}


class LogisticRegressionSuite extends SparkFunSuite with MLlibTestSparkContext with Matchers {
  def validatePrediction(
      predictions: Seq[Double],
      input: Seq[LabeledPoint],
      expectedAcc: Double = 0.83) {
    val numOffPredictions = predictions.zip(input).count { case (prediction, expected) =>
      prediction != expected.label
    }
    // At least 83% of the predictions should be on.
    ((input.length - numOffPredictions).toDouble / input.length) should be > expectedAcc
  }

  // Test if we can correctly learn A, B where Y = logistic(A + B*X)
  test("logistic regression with SGD") {
    val nPoints = 10000
    val A = 2.0
    val B = -1.5

    val testData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 42)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()
    val lr = new LogisticRegressionWithSGD().setIntercept(true)
    lr.optimizer
      .setStepSize(10.0)
      .setRegParam(0.0)
      .setNumIterations(20)
      .setConvergenceTol(0.0005)

    val model = lr.run(testRDD)

    // Test the weights
    assert(model.weights(0) ~== B relTol 0.02)
    assert(model.intercept ~== A relTol 0.02)

    val validationData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 17)
    val validationRDD = sc.parallelize(validationData, 2)
    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }

  // Test if we can correctly learn A, B where Y = logistic(A + B*X)
  test("logistic regression with LBFGS") {
    val nPoints = 10000
    val A = 2.0
    val B = -1.5

    val testData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 42)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()
    val lr = new LogisticRegressionWithLBFGS().setIntercept(true)

    val model = lr.run(testRDD)

    // Test the weights
    assert(model.weights(0) ~== B relTol 0.02)
    assert(model.intercept ~== A relTol 0.02)

    val validationData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 17)
    val validationRDD = sc.parallelize(validationData, 2)
    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }

  test("logistic regression with initial weights with SGD") {
    val nPoints = 10000
    val A = 2.0
    val B = -1.5

    val testData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 42)

    val initialB = -1.0
    val initialWeights = Vectors.dense(initialB)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    // Use half as many iterations as the previous test.
    val lr = new LogisticRegressionWithSGD().setIntercept(true)
    lr.optimizer
      .setStepSize(10.0)
      .setRegParam(0.0)
      .setNumIterations(10)

    val model = lr.run(testRDD, initialWeights)

    // Test the weights
    assert(model.weights(0) ~== B relTol 0.02)
    assert(model.intercept ~== A relTol 0.02)

    val validationData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 17)
    val validationRDD = sc.parallelize(validationData, 2)
    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }

  test("logistic regression with initial weights and non-default regularization parameter") {
    val nPoints = 10000
    val A = 2.0
    val B = -1.5

    val testData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 42)

    val initialB = -1.0
    val initialWeights = Vectors.dense(initialB)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    // Use half as many iterations as the previous test.
    val lr = new LogisticRegressionWithSGD().setIntercept(true)
    lr.optimizer.
      setStepSize(1.0).
      setNumIterations(10).
      setRegParam(1.0)

    val model = lr.run(testRDD, initialWeights)

    // Test the weights
    // With regularization, the resulting weights will be smaller.
    assert(model.weights(0) ~== -0.14 relTol 0.02)
    assert(model.intercept ~== 0.25 relTol 0.02)

    val validationData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 17)
    val validationRDD = sc.parallelize(validationData, 2)
    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData, 0.8)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData, 0.8)
  }

  test("logistic regression with initial weights with LBFGS") {
    val nPoints = 10000
    val A = 2.0
    val B = -1.5

    val testData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 42)

    val initialB = -1.0
    val initialWeights = Vectors.dense(initialB)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    // Use half as many iterations as the previous test.
    val lr = new LogisticRegressionWithLBFGS().setIntercept(true)

    val model = lr.run(testRDD, initialWeights)

    // Test the weights
    assert(model.weights(0) ~== B relTol 0.02)
    assert(model.intercept ~== A relTol 0.02)

    val validationData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 17)
    val validationRDD = sc.parallelize(validationData, 2)
    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }

  test("numerical stability of scaling features using logistic regression with LBFGS") {
    /**
     * If we rescale the features, the condition number will be changed so the convergence rate
     * and the solution will not equal to the original solution multiple by the scaling factor
     * which it should be.
     *
     * However, since in the LogisticRegressionWithLBFGS, we standardize the training dataset first,
     * no matter how we multiple a scaling factor into the dataset, the convergence rate should be
     * the same, and the solution should equal to the original solution multiple by the scaling
     * factor.
     */

    val nPoints = 10000
    val A = 2.0
    val B = -1.5

    val testData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 42)

    val initialWeights = Vectors.dense(0.0)

    val testRDD1 = sc.parallelize(testData, 2)

    val testRDD2 = sc.parallelize(
      testData.map(x => LabeledPoint(x.label, Vectors.fromBreeze(x.features.toBreeze * 1.0E3))), 2)

    val testRDD3 = sc.parallelize(
      testData.map(x => LabeledPoint(x.label, Vectors.fromBreeze(x.features.toBreeze * 1.0E6))), 2)

    testRDD1.cache()
    testRDD2.cache()
    testRDD3.cache()

    val numIteration = 10

    val lrA = new LogisticRegressionWithLBFGS().setIntercept(true)
    lrA.optimizer.setNumIterations(numIteration)
    val lrB = new LogisticRegressionWithLBFGS().setIntercept(true).setFeatureScaling(false)
    lrB.optimizer.setNumIterations(numIteration)

    val modelA1 = lrA.run(testRDD1, initialWeights)
    val modelA2 = lrA.run(testRDD2, initialWeights)
    val modelA3 = lrA.run(testRDD3, initialWeights)

    val modelB1 = lrB.run(testRDD1, initialWeights)
    val modelB2 = lrB.run(testRDD2, initialWeights)
    val modelB3 = lrB.run(testRDD3, initialWeights)

    // For model trained with feature standardization, the weights should
    // be the same in the scaled space. Note that the weights here are already
    // in the original space, we transform back to scaled space to compare.
    assert(modelA1.weights(0) ~== modelA2.weights(0) * 1.0E3 absTol 0.01)
    assert(modelA1.weights(0) ~== modelA3.weights(0) * 1.0E6 absTol 0.01)

    // Training data with different scales without feature standardization
    // will not yield the same result in the scaled space due to poor
    // convergence rate.
    assert(modelB1.weights(0) !~== modelB2.weights(0) * 1.0E3 absTol 0.1)
    assert(modelB1.weights(0) !~== modelB3.weights(0) * 1.0E6 absTol 0.1)
  }

  test("multinomial logistic regression with LBFGS") {
    val nPoints = 10000

    /**
     * The following weights and xMean/xVariance are computed from iris dataset with lambda = 0.2.
     * As a result, we are actually drawing samples from probability distribution of built model.
     */
    val weights = Array(
      -0.57997, 0.912083, -0.371077, -0.819866, 2.688191,
      -0.16624, -0.84355, -0.048509, -0.301789, 4.170682)

    val xMean = Array(5.843, 3.057, 3.758, 1.199)
    val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)

    val testData = LogisticRegressionSuite.generateMultinomialLogisticInput(
      weights, xMean, xVariance, true, nPoints, 42)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    val lr = new LogisticRegressionWithLBFGS().setIntercept(true).setNumClasses(3)
    lr.optimizer.setConvergenceTol(1E-15).setNumIterations(200)

    val model = lr.run(testRDD)

    val numFeatures = testRDD.map(_.features.size).first()
    val initialWeights = Vectors.dense(new Array[Double]((numFeatures + 1) * 2))
    val model2 = lr.run(testRDD, initialWeights)

    LogisticRegressionSuite.checkModelsEqual(model, model2)

    /**
     * The following is the instruction to reproduce the model using R's glmnet package.
     *
     * First of all, using the following scala code to save the data into `path`.
     *
     *    testRDD.map(x => x.label+ ", " + x.features(0) + ", " + x.features(1) + ", " +
     *      x.features(2) + ", " + x.features(3)).saveAsTextFile("path")
     *
     * Using the following R code to load the data and train the model using glmnet package.
     *
     *    library("glmnet")
     *    data <- read.csv("path", header=FALSE)
     *    label = factor(data$V1)
     *    features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
     *    weights = coef(glmnet(features,label, family="multinomial", alpha = 0, lambda = 0))
     *
     * The model weights of mutinomial logstic regression in R have `K` set of linear predictors
     * for `K` classes classification problem; however, only `K-1` set is required if the first
     * outcome is chosen as a "pivot", and the other `K-1` outcomes are separately regressed against
     * the pivot outcome. This can be done by subtracting the first weights from those `K-1` set
     * weights. The mathematical discussion and proof can be found here:
     * http://en.wikipedia.org/wiki/Multinomial_logistic_regression
     *
     *    weights1 = weights$`1` - weights$`0`
     *    weights2 = weights$`2` - weights$`0`
     *
     *    > weights1
     *    5 x 1 sparse Matrix of class "dgCMatrix"
     *                    s0
     *             2.6228269
     *    data.V2 -0.5837166
     *    data.V3  0.9285260
     *    data.V4 -0.3783612
     *    data.V5 -0.8123411
     *    > weights2
     *    5 x 1 sparse Matrix of class "dgCMatrix"
     *                     s0
     *             4.11197445
     *    data.V2 -0.16918650
     *    data.V3 -0.81104784
     *    data.V4 -0.06463799
     *    data.V5 -0.29198337
     */

    val weightsR = Vectors.dense(Array(
      -0.5837166, 0.9285260, -0.3783612, -0.8123411, 2.6228269,
      -0.1691865, -0.811048, -0.0646380, -0.2919834, 4.1119745))

    assert(model.weights ~== weightsR relTol 0.05)

    val validationData = LogisticRegressionSuite.generateMultinomialLogisticInput(
      weights, xMean, xVariance, true, nPoints, 17)
    val validationRDD = sc.parallelize(validationData, 2)
    // The validation accuracy is not good since this model (even the original weights) doesn't have
    // very steep curve in logistic function so that when we draw samples from distribution, it's
    // very easy to assign to another labels. However, this prediction result is consistent to R.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData, 0.47)

  }

  test("model save/load: binary classification") {
    // NOTE: This will need to be generalized once there are multiple model format versions.
    val model = LogisticRegressionSuite.binaryModel

    model.clearThreshold()
    assert(model.getThreshold.isEmpty)

    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    // Save model, load it back, and compare.
    try {
      model.save(sc, path)
      val sameModel = LogisticRegressionModel.load(sc, path)
      LogisticRegressionSuite.checkModelsEqual(model, sameModel)
    } finally {
      Utils.deleteRecursively(tempDir)
    }

    // Save model with threshold.
    try {
      model.setThreshold(0.7)
      model.save(sc, path)
      val sameModel = LogisticRegressionModel.load(sc, path)
      LogisticRegressionSuite.checkModelsEqual(model, sameModel)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  test("model save/load: multiclass classification") {
    // NOTE: This will need to be generalized once there are multiple model format versions.
    val model = LogisticRegressionSuite.multiclassModel

    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    // Save model, load it back, and compare.
    try {
      model.save(sc, path)
      val sameModel = LogisticRegressionModel.load(sc, path)
      LogisticRegressionSuite.checkModelsEqual(model, sameModel)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

}

class LogisticRegressionClusterSuite extends SparkFunSuite with LocalClusterSparkContext {

  test("task size should be small in both training and prediction using SGD optimizer") {
    val m = 4
    val n = 200000
    val points = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => LabeledPoint(1.0, Vectors.dense(Array.fill(n)(random.nextDouble()))))
    }.cache()
    // If we serialize data directly in the task closure, the size of the serialized task would be
    // greater than 1MB and hence Spark would throw an error.
    val model = LogisticRegressionWithSGD.train(points, 2)

    val predictions = model.predict(points.map(_.features))

    // Materialize the RDDs
    predictions.count()
  }

  test("task size should be small in both training and prediction using LBFGS optimizer") {
    val m = 4
    val n = 200000
    val points = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => LabeledPoint(1.0, Vectors.dense(Array.fill(n)(random.nextDouble()))))
    }.cache()
    // If we serialize data directly in the task closure, the size of the serialized task would be
    // greater than 1MB and hence Spark would throw an error.
    val lr = new LogisticRegressionWithLBFGS().setIntercept(true)
    lr.optimizer.setNumIterations(2)
    val model = lr.run(points)

    val predictions = model.predict(points.map(_.features))

    // Materialize the RDDs
    predictions.count()
  }

}
