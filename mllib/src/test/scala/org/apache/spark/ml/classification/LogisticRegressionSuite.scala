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

package org.apache.spark.ml.classification

import scala.collection.JavaConverters._
import scala.util.Random
import scala.util.control.Breaks._

import org.scalatest.Assertions._

import org.apache.spark.SparkException
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.classification.LogisticRegressionSuite._
import org.apache.spark.ml.feature.{Instance, LabeledPoint, VectorAssembler}
import org.apache.spark.ml.linalg.{DenseMatrix, Matrices, Matrix, SparseMatrix, Vector, Vectors}
import org.apache.spark.ml.optim.aggregator.LogisticAggregator
import org.apache.spark.ml.param.{ParamMap, ParamsSuite}
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit, rand}
import org.apache.spark.sql.types.LongType

// scalastyle:off println
class LogisticRegressionSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  private val seed = 42
  @transient var smallBinaryDataset: DataFrame = _
  @transient var smallMultinomialDataset: DataFrame = _
  @transient var binaryDataset: DataFrame = _
  @transient var multinomialDataset: DataFrame = _
  @transient var multinomialDatasetWithZeroVar: DataFrame = _
  private val eps: Double = 1e-5

  override def beforeAll(): Unit = {
    super.beforeAll()

    smallBinaryDataset = generateLogisticInput(1.0, 1.0, nPoints = 100, seed = seed).toDF()

    smallMultinomialDataset = {
      val nPoints = 100
      val coefficients = Array(
        -0.57997, 0.912083, -0.371077,
        -0.16624, -0.84355, -0.048509)

      val xMean = Array(5.843, 3.057)
      val xVariance = Array(0.6856, 0.1899)

      val testData = generateMultinomialLogisticInput(
        coefficients, xMean, xVariance, addIntercept = true, nPoints, seed)

      val df = sc.parallelize(testData, 4).toDF()
      df.cache()
      df
    }

    binaryDataset = {
      val nPoints = 10000
      val coefficients = Array(-0.57997, 0.912083, -0.371077, -0.819866, 2.688191)
      val xMean = Array(5.843, 3.057, 3.758, 1.199)
      val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)

      val testData =
        generateMultinomialLogisticInput(coefficients, xMean, xVariance,
          addIntercept = true, nPoints, seed)

      val df = sc.parallelize(testData, 4).toDF().withColumn("weight", rand(seed))
      df.cache()
      df
    }

    multinomialDataset = {
      val nPoints = 10000
      val coefficients = Array(
        -0.57997, 0.912083, -0.371077, -0.819866, 2.688191,
        -0.16624, -0.84355, -0.048509, -0.301789, 4.170682)

      val xMean = Array(5.843, 3.057, 3.758, 1.199)
      val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)

      val testData = generateMultinomialLogisticInput(
        coefficients, xMean, xVariance, addIntercept = true, nPoints, seed)

      val df = sc.parallelize(testData, 4).toDF().withColumn("weight", rand(seed))
      df.cache()
      df
    }

    multinomialDatasetWithZeroVar = {
      val nPoints = 100
      val coefficients = Array(
        -0.57997, 0.912083, -0.371077,
        -0.16624, -0.84355, -0.048509)

      val xMean = Array(5.843, 3.0)
      val xVariance = Array(0.6856, 0.0)

      val testData = generateMultinomialLogisticInput(
        coefficients, xMean, xVariance, addIntercept = true, nPoints, seed)

      val df = sc.parallelize(testData, 4).toDF().withColumn("weight", lit(1.0))
      df.cache()
      df
    }
  }


  test("BLR") {
    val centered = false
    val regParam = 1.0e-8
    val num_distribution_samplings = 1000
    val num_rows_per_sampling = 1000
    val theta_1 = 0.3f
    val theta_2 = 0.2f
    val intercept = -4.0f

    val (feature1, feature2, target) = generate_blr_data(theta_1, theta_2, intercept, centered,
      num_distribution_samplings, num_rows_per_sampling)

    val num_rows = num_distribution_samplings * num_rows_per_sampling

    val const_feature = Array.fill(num_rows)(1.0f)
    (0 until num_rows / 10).foreach { i => const_feature(i) = 0.9f }


    val data = (0 until num_rows).map { i =>
      (feature1(i), feature2(i), const_feature(i), target(i))
    }

    val spark_df = spark.createDataFrame(data)
      .toDF("feature1", "feature2", "const_feature", "label").cache()


    val lr = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(regParam)
      .setElasticNetParam(0.5)
      .setFitIntercept(true)


    val vec = new VectorAssembler()
      .setInputCols(Array("feature1", "feature2"))
      .setOutputCol(("features"))
    val spark_df1 = vec.transform(spark_df).cache()

    val lrModel = lr.fit(spark_df1)
    println("Just the blr data")
    println("Coefficients: " + lrModel.coefficients)
    println("Intercept: " + lrModel.intercept)
    println(s"objectives: ${lrModel.summary.objectiveHistory.mkString(",")}")

    val vec2 = new VectorAssembler()
      .setInputCols(Array("feature1", "feature2", "const_feature"))
      .setOutputCol(("features"))
    val spark_df2 = vec2.transform(spark_df).cache()

    val lrModel2 = lr.fit(spark_df2)
    println("blr data plus one vector that is filled with 1's and .9's")
    println("Coefficients: " + lrModel2.coefficients)
    println("Intercept: " + lrModel2.intercept)
    println(s"objectives: ${lrModel2.summary.objectiveHistory.mkString(",")}")
  }

  def generate_blr_data(
      theta_1: Float,
      theta_2: Float,
      intercept: Float,
      centered: Boolean,
      num_distribution_samplings: Int,
      num_rows_per_sampling: Int): (Array[Float], Array[Float], Array[Int]) = {
    val random = new Random(12345L)
    val uniforms = Array.fill(num_distribution_samplings)(random.nextFloat())
    val uniforms2 = Array.fill(num_distribution_samplings)(random.nextFloat())

    if (centered) {
      uniforms.transform(f => f - 0.5f)
      uniforms2.transform(f => 2.0f * f - 1.0f)
    } else {
      uniforms2.transform(f => f + 1.0f)
    }

    val h_theta = uniforms.zip(uniforms2)
      .map { case (a, b) => intercept + theta_1 * a + theta_2 * b }
    val prob = h_theta.map(t => 1.0 / (1.0 + math.exp(-t)))
    val array = Array.ofDim[Int](num_distribution_samplings, num_rows_per_sampling)
    array.indices.foreach { i =>
      (0 until math.round(num_rows_per_sampling * prob(i)).toInt).foreach { j =>
        array(i)(j) = 1
      }
    }

    val feature_1 = uniforms.flatMap(f => Array.fill(num_rows_per_sampling)(f))
    val feature_2 = uniforms2.flatMap(f => Array.fill(num_rows_per_sampling)(f))
    val target = array.flatten

    (feature_1, feature_2, target)
  }
}
// scalastyle:on println

object LogisticRegressionSuite {

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = ProbabilisticClassifierSuite.allParamSettings ++ Map(
    "probabilityCol" -> "myProbability",
    "thresholds" -> Array(0.4, 0.6),
    "regParam" -> 0.01,
    "elasticNetParam" -> 0.1,
    "maxIter" -> 2,  // intentionally small
    "fitIntercept" -> true,
    "tol" -> 0.8,
    "standardization" -> false,
    "threshold" -> 0.6
  )

  def generateLogisticInputAsList(
    offset: Double,
    scale: Double,
    nPoints: Int,
    seed: Int): java.util.List[LabeledPoint] = {
    generateLogisticInput(offset, scale, nPoints, seed).asJava
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

  /**
   * When no regularization is applied, the multinomial coefficients lack identifiability
   * because we do not use a pivot class. We can add any constant value to the coefficients
   * and get the same likelihood. If fitting under bound constrained optimization, we don't
   * choose the mean centered coefficients like what we do for unbound problems, since they
   * may out of the bounds. We use this function to check whether two coefficients are equivalent.
   */
  def checkCoefficientsEquivalent(coefficients1: Matrix, coefficients2: Matrix): Unit = {
    coefficients1.colIter.zip(coefficients2.colIter).foreach { case (col1: Vector, col2: Vector) =>
      (col1.asBreeze - col2.asBreeze).toArray.toSeq.sliding(2).foreach {
        case Seq(v1, v2) => assert(v1 ~= v2 absTol 1E-3)
      }
    }
  }
}
