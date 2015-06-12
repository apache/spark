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
