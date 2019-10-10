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

import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.regression.FactorizationMachinesSuite._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col}

class FactorizationMachinesSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  private val seed = 10
  @transient var crossDataset: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    crossDataset = generateFactorizationCrossInput(spark, 2, 10, 1000, seed)
  }

  test("params") {
    ParamsSuite.checkParams(new FactorizationMachines)
    val model = new FactorizationMachinesModel("fm_test", OldVectors.dense(0.0), 0)
    ParamsSuite.checkParams(model)
  }

  test("factorization machines squaredError") {
    val fm = new FactorizationMachines()
      .setSolver("adamW")
      .setLoss("squaredError")
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setNumFactors(2)
      .setInitStd(0.01)
      .setMaxIter(1000)
      .setMiniBatchFraction(1.0)
      .setStepSize(1.0)
      .setRegParam(0.0)
      .setTol(1E-6)
    val fmModel = fm.fit(crossDataset)
    val res = fmModel.transform(crossDataset)

    val mse = res.select((col("prediction") - col("label")).as("error"))
      .select((col("error") * col("error")).as("error_square"))
      .agg(avg("error_square"))
      .collect()(0).getAs[Double](0)

    assert(mse ~== 0.0 absTol 1E-6)
  }

  test("read/write") {
    def checkModelData(
      model: FactorizationMachinesModel,
      model2: FactorizationMachinesModel
    ): Unit = {
      assert(model.coefficients.toArray === model2.coefficients.toArray)
      assert(model.numFeatures === model2.numFeatures)
    }
    val fm = new FactorizationMachines()
    val data = crossDataset
      .withColumnRenamed("features", allParamSettings("featuresCol").toString)
      .withColumnRenamed("label", allParamSettings("labelCol").toString)
    testEstimatorAndModelReadWrite(fm, crossDataset, allParamSettings,
      allParamSettings, checkModelData)
  }
}

object FactorizationMachinesSuite {

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "featuresCol" -> "features",
    "labelCol" -> "label",
    "predictionCol" -> "prediction",
    "numFactors" -> 2,
    "fitBias" -> false,
    "fitLinear" -> false,
    "regParam" -> 0.01,
    "miniBatchFraction" -> 0.1,
    "initStd" -> 0.01,
    "maxIter" -> 2,
    "stepSize" -> 0.1,
    "tol" -> 1e-4,
    "solver" -> "gd",
    "loss" -> "squaredError",
    "verbose" -> true
  )

  def generateFactorizationCrossInput(
    spark: SparkSession,
    numFactors: Int,
    numFeatures: Int,
    numSamples: Int,
    seed: Int
  ): DataFrame = {
    import spark.implicits._
    val sc = spark.sparkContext

    val rnd = new Random(seed)
    val coefficientsSize = numFactors * numFeatures + numFeatures + 1
    val coefficients = Array.fill(coefficientsSize)(rnd.nextDouble() - 0.5)

    val X: DataFrame = sc.parallelize(0 until numSamples).map { i =>
      val x = new DenseVector(Array.fill(numFeatures)(rnd.nextDouble() - 0.5))
      (i, x)
    }.toDF("id", "features")

    val fmModel = new FactorizationMachinesModel(
      "fm_test", OldVectors.dense(coefficients), numFeatures)
    fmModel.set(fmModel.loss, "squaredError")
    fmModel.set(fmModel.numFactors, numFactors)
    fmModel.set(fmModel.fitBias, true)
    fmModel.set(fmModel.fitLinear, true)
    val data = fmModel.transform(X)
      .withColumn("label", col("prediction"))
      .select("features", "label")
    data
  }
}
