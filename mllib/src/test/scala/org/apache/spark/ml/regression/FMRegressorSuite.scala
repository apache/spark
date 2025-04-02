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

package org.apache.spark.ml.regression

import scala.util.Random

import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.regression.FactorizationMachines._
import org.apache.spark.ml.regression.FMRegressorSuite._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col}

class FMRegressorSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  private val seed = 10
  @transient var crossDataset: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val (crossDatasetTmp, _) = generateFactorInteractionInput(
      spark, 2, 10, 1000, seed, true, true)
    crossDataset = crossDatasetTmp
  }

  test("params") {
    ParamsSuite.checkParams(new FMRegressor)
    val model = new FMRegressionModel("fmr_test", 0.0, Vectors.dense(0.0),
      new DenseMatrix(1, 8, new Array[Double](8)))
    ParamsSuite.checkParams(model)
  }

  test("FMRegressor validate input dataset") {
    testInvalidRegressionLabels(new FMRegressor().fit(_))
    testInvalidVectors(new FMRegressor().fit(_))
  }

  test("combineCoefficients") {
    val numFeatures = 2
    val factorSize = 4
    val b = 0.1
    val w = Vectors.dense(Array(0.2, 0.3))
    val v = new DenseMatrix(numFeatures, factorSize,
      Array(0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1), true)

    val expectList = Array(
      (true, true, Array(0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 0.2, 0.3, 0.1)),
      (false, true, Array(0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 0.2, 0.3)),
      (true, false, Array(0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 0.1)),
      (false, false, Array(0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1)))

    expectList.foreach { case (fitIntercept, fitLinear, expectCoeffs) =>
      assert(combineCoefficients(b, w, v, fitIntercept, fitLinear) === Vectors.dense(expectCoeffs))
    }
  }

  test("splitCoefficients") {
    val numFeatures = 2
    val factorSize = 4
    val b = 0.1
    val w = Vectors.dense(Array(0.2, 0.3))
    val v = new DenseMatrix(numFeatures, factorSize,
      Array(0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1), true)
    val emptyB = 0.0
    val emptyW = Vectors.sparse(numFeatures, Seq.empty)

    val expectList = Array(
      (true, true, b, w, v, Array(0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 0.2, 0.3, 0.1)),
      (false, true, emptyB, w, v, Array(0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 0.2, 0.3)),
      (true, false, b, emptyW, v, Array(0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 0.1)),
      (false, false, emptyB, emptyW, v, Array(0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1)))

    expectList.foreach { case (fitIntercept, fitLinear, b1, w1, v1, coeffs) =>
      val (b2, w2, v2) = splitCoefficients(Vectors.dense(coeffs),
        numFeatures, factorSize, fitIntercept, fitLinear)
      assert(b1 === b2)
      assert(w1 === w2)
      assert(v1 === v2)
    }
  }

  def checkMSE(fitIntercept: Boolean, fitLinear: Boolean): Unit = {
    val numFeatures = 3
    val numSamples = 200
    val factorSize = 2
    val (data, coefficients) = generateFactorInteractionInput(
      spark, factorSize, numFeatures, numSamples, seed, fitIntercept, fitLinear)
    val (b, w, v) = splitCoefficients(new DenseVector(coefficients),
      numFeatures, factorSize, fitIntercept, fitLinear)

    val fm = new FMRegressor()
      .setSolver("adamW")
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setFactorSize(factorSize)
      .setFitIntercept(fitIntercept)
      .setFitLinear(fitLinear)
      .setInitStd(0.01)
      .setMaxIter(100)
      .setMiniBatchFraction(1.0)
      .setStepSize(1.0)
      .setRegParam(0.0)
      .setTol(1E-6)
    val fmModel = fm.fit(data)
    val res = fmModel.transform(data)

    // check mse value
    val mse = res.select((col("prediction") - col("label")).as("error"))
      .select((col("error") * col("error")).as("error_square"))
      .agg(avg("error_square"))
      .collect()(0).getAs[Double](0)
    assert(mse ~== 0.0 absTol 1E-4)

    // check coefficients
    assert(b ~== fmModel.intercept absTol 1E-2)
    assert(w ~== fmModel.linear absTol 1E-2)
    (0 until numFeatures).foreach { i =>
      ((i + 1) until numFeatures).foreach { j =>
        // assert <v_i, v_j> is same
        var innerProd1 = 0.0
        var innerProd2 = 0.0
        (0 until factorSize).foreach { k =>
          innerProd1 += v(i, k) * v(j, k)
          innerProd2 += fmModel.factors(i, k) * fmModel.factors(j, k)
        }
        assert(innerProd1 ~== innerProd2 absTol 1E-2)
      }
    }
  }

  test("MSE with intercept and linear") {
    checkMSE(true, true)
  }

  test("MSE with intercept but without linear") {
    checkMSE(true, false)
  }

  test("MSE with linear but without intercept") {
    checkMSE(false, true)
  }

  test("MSE without intercept or linear") {
    checkMSE(false, false)
  }

  test("read/write") {
    def checkModelData(
      model: FMRegressionModel,
      model2: FMRegressionModel
    ): Unit = {
      assert(model.intercept === model2.intercept)
      assert(model.linear.toArray === model2.linear.toArray)
      assert(model.factors.toArray === model2.factors.toArray)
      assert(model.numFeatures === model2.numFeatures)
    }
    val fm = new FMRegressor()
    val data = crossDataset
      .withColumnRenamed("features", allParamSettings("featuresCol").toString)
      .withColumnRenamed("label", allParamSettings("labelCol").toString)
    testEstimatorAndModelReadWrite(fm, data, allParamSettings,
      allParamSettings, checkModelData)
  }

  test("model size estimation: dense data") {
    val rng = new Random(1)

    Seq(10, 100, 1000, 10000, 100000).foreach { n =>
      val df = Seq(
        (Vectors.dense(Array.fill(n)(rng.nextDouble())), 0.0),
        (Vectors.dense(Array.fill(n)(rng.nextDouble())), 1.0)
      ).toDF("features", "label")

      val fm = new FMRegressor().setMaxIter(1)
      val size1 = fm.estimateModelSize(df)
      val model = fm.fit(df)
      assert(model.linear.isInstanceOf[DenseVector])
      assert(model.factors.isInstanceOf[DenseMatrix])
      val size2 = model.estimatedSize

      // the model is dense, the estimation should be relatively accurate
      //      (n, size1, size2)
      //      (10,5081,5081) <- when the model is small, model.params matters
      //      (100,11561,11561)
      //      (1000,76361,76361)
      //      (10000,724361,724361)
      //      (100000,7204361,7204361)
      val rel = (size1 - size2).toDouble / size2
      assert(math.abs(rel) < 0.05, (n, size1, size2))
    }
  }

  test("model size estimation: sparse data") {
    val rng = new Random(1)

    Seq(100, 1000, 10000, 100000).foreach { n =>
      val df = Seq(
        (Vectors.sparse(n, Array.range(0, 10), Array.fill(10)(rng.nextDouble())), 0.0),
        (Vectors.sparse(n, Array.range(0, 10), Array.fill(10)(rng.nextDouble())), 1.0)
      ).toDF("features", "label")

      val fm = new FMRegressor().setMaxIter(1).setRegParam(10.0)
      val size1 = fm.estimateModelSize(df)
      val model = fm.fit(df)
      val size2 = model.estimatedSize

      // the model is sparse, the estimated size is likely larger
      //      (n, size1, size2)
      //      (100,11561,10897)
      //      (1000,76361,68497)
      //      (10000,724361,644497)
      //      (100000,7204361,6404497)
      assert(size1 > size2, (n, size1, size2))
    }
  }
}

object FMRegressorSuite {

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "featuresCol" -> "myFeatures",
    "labelCol" -> "myLabel",
    "predictionCol" -> "prediction",
    "factorSize" -> 2,
    "fitIntercept" -> false,
    "fitLinear" -> false,
    "regParam" -> 0.01,
    "miniBatchFraction" -> 0.1,
    "initStd" -> 0.01,
    "maxIter" -> 2,
    "stepSize" -> 0.1,
    "tol" -> 1e-4,
    "solver" -> "gd",
    "seed" -> 11L
  )

  def generateFactorInteractionInput(
    spark: SparkSession,
    factorSize: Int,
    numFeatures: Int,
    numSamples: Int,
    seed: Int,
    fitIntercept: Boolean,
    fitLinear: Boolean
  ): (DataFrame, Array[Double]) = {
    import spark.implicits._
    val sc = spark.sparkContext

    // generate FM coefficients randomly
    val rnd = new Random(seed)
    val coefficientsSize = factorSize * numFeatures +
      (if (fitLinear) numFeatures else 0) + (if (fitIntercept) 1 else 0)
    val coefficients = Array.fill(coefficientsSize)(rnd.nextDouble() - 0.5)
    val (intercept, linear, factors) = splitCoefficients(
      Vectors.dense(coefficients), numFeatures, factorSize, fitIntercept, fitLinear)

    // generate samples randomly
    val X: DataFrame = sc.parallelize(0 until numSamples).map { i =>
      val x = new DenseVector(Array.fill(numFeatures)(rnd.nextDouble() - 0.5))
      (i, x)
    }.toDF("id", "features")

    // calculate FM prediction
    val fmModel = new FMRegressionModel(
      "fmr_test", intercept, linear, factors)
    fmModel.set(fmModel.factorSize, factorSize)
    fmModel.set(fmModel.fitIntercept, fitIntercept)
    fmModel.set(fmModel.fitLinear, fitLinear)
    val data = fmModel.transform(X)
      .withColumnRenamed("prediction", "label")
      .select("features", "label")
    (data, coefficients)
  }
}
