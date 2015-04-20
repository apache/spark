package org.apache.spark.mllib.regression

import com.github.fommil.netlib.BLAS._
import com.github.fommil.netlib.BLAS.{getInstance => blas}

import org.scalatest.FunSuite
import scala.util.Random

import org.apache.spark.mllib.linalg.{Matrix, DenseMatrix, Vectors}
import org.apache.spark.mllib.util.{LocalClusterSparkContext, MLlibTestSparkContext}
import org.apache.spark.util.Utils

/**
 * Created by zrf on 4/20/15.
 */

private object FactorizationMachineSuite {

  /**
   * model with numFactors = 8 and numFeatures = 10
   */
  val modle = new FMModel(new DenseMatrix(8, 10, Array.fill(80)(Random.nextGaussian())),
    Some(Vectors.dense(Array.fill(10)(Random.nextDouble()))), 1.0)
}

class FactorizationMachineSuite extends FunSuite with MLlibTestSparkContext {

  def predictionError(predictions: Seq[Double], input: Seq[LabeledPoint]): Double = {
    predictions.zip(input).map { case (prediction, expected) =>
      (prediction - expected.label) * (prediction - expected.label)
    }.reduceLeft(_ + _) / predictions.size
  }


  def generateMultiLinearInput(intercept: Double,
                               weights: Array[Double],
                               factors: Matrix,
                               nPoints: Int,
                               seed: Int,
                               eps: Double = 0.1): Seq[LabeledPoint] = {
    require(weights.length == factors.numCols)

    val rnd = new Random(seed)
    val x = Array.fill[Array[Double]](nPoints)(
      Array.fill[Double](weights.length)(2 * rnd.nextDouble - 1.0))
    val y = x.map {
      xi =>
        var l = blas.ddot(weights.length, xi, 1, weights, 1) + intercept + eps * rnd.nextGaussian()

        for (i <- 0 until weights.length; j <- i + 1 until weights.length) {
          var d = 0.0
          for (f <- 0 until factors.numRows) {
            d += factors(f, i) * factors(f, j)
          }
          l += xi(i) * xi(j) * d
        }
        l
    }

    y.zip(x).map(p => LabeledPoint(p._1, Vectors.dense(p._2)))
  }


  def validatePrediction(predictions: Seq[Double], input: Seq[LabeledPoint]) {
    val numOffPredictions = predictions.zip(input).count { case (prediction, expected) =>
      // A prediction is off if the prediction is more than 0.5 away from expected value.
      math.abs(prediction - expected.label) > 0.5
    }
    // At least 80% of the predictions should be on.
    assert(numOffPredictions < input.length / 5)
  }


  // Test if we can correctly learn Y = 0.1 + 1.2*X1 - 1.3*X2 + 20*X1*X2
  test("regression with weights and intercept") {

    val testRDD = sc.parallelize(generateMultiLinearInput(0.1, Array(1.2, -1.3),
      new DenseMatrix(2, 2, Array(0.0, 1.0, 0.0, 20)), 1000, 42), 2).cache()

    val model = FMWithSGD.train(testRDD, numIterations = 100, stepSize = 1,
      miniBatchFraction = 0.1, dim = (true, true, 2), regParam = (0, 0.01, 0.01))

    val w0 = model.intercept
    assert(w0 >= 0.05 && w0 <= 0.15, w0 + " not in [0.05, 0.15]")

    val w = model.weightVector
    assert(w.isDefined && w.get.size == 2)
    val w1 = w.get(0)
    assert(w1 >= 1.1 && w1 <= 1.3, w1 + " not in [1.1, 1.3]")
    val w2 = w.get(1)
    assert(w2 >= -1.4 && w2 <= -1.2, w2 + " not in [-1.4, -1.2]")

    val v = model.factorMatrix
    val v12 = v(0, 0) * v(0, 1) + v(1, 0) * v(1, 1)
    assert(v12 >= 18 && v12 <= 22, v12 + " not in [18, 22]")

    val validationData = generateMultiLinearInput(0.1, Array(1.2, -1.3),
      new DenseMatrix(2, 2, Array(0.0, 1.0, 0.0, 20)), 1000, 17)

    val validationRDD = sc.parallelize(validationData, 2)

    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }


  // Test if we can correctly learn Y = 1.2*X1 - 1.3*X2 + 20*X1*X2
  test("regression with weights but without intercept") {

    val testRDD = sc.parallelize(generateMultiLinearInput(0.0, Array(1.2, -1.3),
      new DenseMatrix(2, 2, Array(0.0, 1.0, 0.0, 20)), 1000, 42), 2).cache()

    val model = FMWithSGD.train(testRDD, numIterations = 100, stepSize = 1,
      miniBatchFraction = 0.1, dim = (false, true, 2), regParam = (0, 0.01, 0.01))

    val w0 = model.intercept
    assert(w0 == 0.0, w0 + " not equal 0")

    val w = model.weightVector
    assert(w.isDefined && w.get.size == 2)
    val w1 = w.get(0)
    assert(w1 >= 1.1 && w1 <= 1.3, w1 + " not in [1.1, 1.3]")
    val w2 = w.get(1)
    assert(w2 >= -1.4 && w2 <= -1.2, w2 + " not in [-1.4, -1.2]")

    val v = model.factorMatrix
    val v12 = v(0, 0) * v(0, 1) + v(1, 0) * v(1, 1)
    assert(v12 >= 18 && v12 <= 22, v12 + " not in [18, 22]")

    val validationData = generateMultiLinearInput(0.0, Array(1.2, -1.3),
      new DenseMatrix(2, 2, Array(0.0, 1.0, 0.0, 20)), 1000, 17)

    val validationRDD = sc.parallelize(validationData, 2)

    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }



  // Test if we can correctly learn Y = 0.1 + 20*X1*X2
  test("regression with intercept but without weights") {

    val testRDD = sc.parallelize(generateMultiLinearInput(0.1, Array(0.0, 0.0),
      new DenseMatrix(2, 2, Array(0.0, 1.0, 0.0, 20)), 1000, 42), 2).cache()

    val model = FMWithSGD.train(testRDD, numIterations = 100, stepSize = 1,
      miniBatchFraction = 0.1, dim = (true, false, 2), regParam = (0, 0.01, 0.01))

    val w0 = model.intercept
    assert(w0 >= 0.05 && w0 <= 0.15, w0 + " not in [0.05, 0.15]")

    val w = model.weightVector
    assert(w == None)

    val v = model.factorMatrix
    val v12 = v(0, 0) * v(0, 1) + v(1, 0) * v(1, 1)
    assert(v12 >= 18 && v12 <= 22, v12 + " not in [18, 22]")

    val validationData = generateMultiLinearInput(0.1, Array(0.0, 0.0),
      new DenseMatrix(2, 2, Array(0.0, 1.0, 0.0, 20)), 1000, 17)

    val validationRDD = sc.parallelize(validationData, 2)

    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }


  // Test if we can correctly learn Y = 20*X1*X2
  test("regression without weights or intercept") {

    val testRDD = sc.parallelize(generateMultiLinearInput(0.0, Array(0.0, 0.0),
      new DenseMatrix(2, 2, Array(0.0, 1.0, 0.0, 20)), 1000, 42), 2).cache()

    val model = FMWithSGD.train(testRDD, numIterations = 100, stepSize = 1,
      miniBatchFraction = 0.1, dim = (false, false, 2), regParam = (0, 0.01, 0.01))

    val w0 = model.intercept
    assert(w0 == 0.0, w0 + " not equal 0")

    val w = model.weightVector
    assert(w == None)

    val v = model.factorMatrix
    val v12 = v(0, 0) * v(0, 1) + v(1, 0) * v(1, 1)
    assert(v12 >= 18 && v12 <= 22, v12 + " not in [18, 22]")

    val validationData = generateMultiLinearInput(0.0, Array(0.0, 0.0),
      new DenseMatrix(2, 2, Array(0.0, 1.0, 0.0, 20)), 1000, 17)

    val validationRDD = sc.parallelize(validationData, 2)

    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }


  test("model save/load") {
    val model = FactorizationMachineSuite.modle

    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    // Save model, load it back, and compare.
    try {
      model.save(sc, path)
      val sameModel = FMModel.load(sc, path)
      assert(model.factorMatrix == sameModel.factorMatrix)
      assert(model.weightVector == sameModel.weightVector)
      assert(model.intercept == sameModel.intercept)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}


class FactorizationMachineSuiteClusterSuite extends FunSuite with LocalClusterSparkContext {

  test("task size should be small in both training and prediction") {
    val m = 4
    val n = 200000
    val points = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => LabeledPoint(1.0, Vectors.dense(Array.fill(n)(random.nextDouble()))))
    }.cache()
    // If we serialize data directly in the task closure, the size of the serialized task would be
    // greater than 1MB and hence Spark would throw an error.
    val model = FMWithSGD.train(points, numIterations = 2, dim = (true, true, 8))
    val predictions = model.predict(points.map(_.features))
  }
}