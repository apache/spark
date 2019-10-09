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
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.linalg.{Vectors => oldVectors}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col}

class FactorizationMachinesSuite extends MLTest {
  @transient var data: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val spark2 = spark
    import spark2.implicits._

    val numFeatures = 10
    val numFactors = 2
    val numSamples = 10000
    val rnd = new Random(10)
    val coefficientsSize = numFactors * numFeatures + numFeatures + 1
    val coefficients = Array.fill(coefficientsSize)(rnd.nextDouble() - 0.5)

    val X: DataFrame = sc.parallelize(0 until numSamples).map { i =>
      val x = new DenseVector(Array.fill(numFeatures)(rnd.nextDouble() - 0.5))
      (i, x)
    }.toDF("id", "features")

    val fmModel = new FactorizationMachinesModel(
      "fm_test", oldVectors.dense(coefficients), numFeatures)
    fmModel.set(fmModel.loss, "squaredError")
    fmModel.set(fmModel.numFactors, numFactors)
    fmModel.set(fmModel.fitBias, true)
    fmModel.set(fmModel.fitLinear, true)
    data = fmModel.transform(X)
      .withColumn("label", col("prediction"))
      .select("features", "label")
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
    val fmModel = fm.fit(data)
    val res = fmModel.transform(data)

    val mse = res.select((col("prediction") - col("label")).as("error"))
      .select((col("error") * col("error")).as("error_square"))
      .agg(avg("error_square"))
      .collect()(0).getAs[Double](0)

    assert(mse ~== 0.0 absTol 1E-6)
  }
}
