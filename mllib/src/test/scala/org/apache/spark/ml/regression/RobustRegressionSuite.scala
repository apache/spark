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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.{Instance, LabeledPoint}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.{LinearDataGenerator, MLlibTestSparkContext}
import org.apache.spark.sql._

class RobustRegressionSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  private val seed: Int = 42
  @transient var dataset: DataFrame = _
  @transient var datasetWithWeight: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val inlierData = LinearDataGenerator.generateLinearInput(
      intercept = 6.3, weights = Array(4.7, 7.2), xMean = Array(0.9, -1.3),
      xVariance = Array(0.7, 1.2), nPoints = 900, seed, eps = 0.1).map(_.asML)
    val outlierData = LinearDataGenerator.generateLinearInput(
      intercept = -2.1, weights = Array(0.6, -1.2), xMean = Array(0.9, -1.3),
      xVariance = Array(1.5, 0.8), nPoints = 100, seed, eps = 0.1).map(_.asML)
    val data = inlierData ++ outlierData

    dataset = spark.createDataFrame(sc.parallelize(data, 2))

    val r = new Random(seed)
    val dataWithWeight = data.map { case LabeledPoint(label: Double, features: Vector) =>
      val weight = r.nextInt(10)
      Instance(label, weight, features)
    }
    datasetWithWeight = spark.createDataFrame(sc.parallelize(dataWithWeight, 2))
  }

  /**
   * Enable the ignored test to export the dataset into CSV format,
   * so we can validate the training accuracy compared with Python's scikit-learn package.
   */
  ignore("export test data into CSV format") {
    dataset.rdd.map { case Row(label: Double, features: Vector) =>
      label + "," + features.toArray.mkString(",")
    }.repartition(1).saveAsTextFile("target/tmp/RobustRegressionSuite/dataset")

    datasetWithWeight.rdd.map { case Row(label: Double, weight: Double, features: Vector) =>
      label + "," + weight + "," + features.toArray.mkString(",")
    }.repartition(1).saveAsTextFile("target/tmp/RobustRegressionSuite/datasetWithWeight")
  }

  test("params") {
    ParamsSuite.checkParams(new RobustRegression)
    val model = new RobustRegressionModel("robReg", Vectors.dense(0.0), 0.0, 0.0)
    ParamsSuite.checkParams(model)
  }

  test("robust regression: default params") {
    val rr = new RobustRegression
    assert(rr.getLabelCol === "label")
    assert(rr.getFeaturesCol === "features")
    assert(rr.getPredictionCol === "prediction")
    assert(rr.getRegParam === 0.0)
    assert(rr.getM === 1.35)
    assert(!rr.isDefined(rr.weightCol))
    assert(rr.getFitIntercept)
    assert(rr.getStandardization)
    val model = rr.fit(dataset)

    // copied model must have the same parent.
    MLTestingUtils.checkCopy(model)

    assert(model.getFeaturesCol === "features")
    assert(model.getPredictionCol === "prediction")
    assert(model.intercept !== 0.0)
    assert(model.hasParent)
  }

  test("robust regression w/ intercept w/o regularization") {
    val trainer1 = (new RobustRegression).setFitIntercept(true).setStandardization(true)
    val trainer2 = (new RobustRegression).setFitIntercept(true).setStandardization(false)

    val model1 = trainer1.fit(dataset)
    val model2 = trainer2.fit(dataset)

    /*
      import pandas as pd
      import numpy as np
      from sklearn.linear_model import HuberRegressor
      df = pd.read_csv("path", header = None)
      X = df[df.columns[1:3]]
      y = np.array(df[df.columns[0]])
      huber = HuberRegressor(fit_intercept=True, alpha=0.0, max_iter=100, epsilon=1.35)
      huber.fit(X, y)

      >>> huber.coef_
      array([ 4.68998007,  7.19429011])
      >>> huber.intercept_
      6.3002404351083037
      >>> huber.scale_
      0.077810159205220747
     */
    val coefficientsPy = Vectors.dense(4.68998007, 7.19429011)
    val interceptPy = 6.30024044
    val scalePy = 0.07781016

    assert(model1.coefficients ~= coefficientsPy relTol 1E-3)
    assert(model1.intercept ~== interceptPy relTol 1E-3)
    assert(model1.scale ~== scalePy relTol 1E-3)

    // Without regularization, with or without standardization will converge to the same solution.
    assert(model2.coefficients ~= coefficientsPy relTol 1E-3)
    assert(model2.intercept ~== interceptPy relTol 1E-3)
    assert(model2.scale ~== scalePy relTol 1E-3)
  }

  test("robust regression w/o intercept w/o regularization") {
    val trainer1 = (new RobustRegression).setFitIntercept(false).setStandardization(true)
    val trainer2 = (new RobustRegression).setFitIntercept(false).setStandardization(false)

    val model1 = trainer1.fit(dataset)
    val model2 = trainer2.fit(dataset)

    /*
      import pandas as pd
      import numpy as np
      from sklearn.linear_model import HuberRegressor
      df = pd.read_csv("path", header = None)
      X = df[df.columns[1:3]]
      y = np.array(df[df.columns[0]])
      huber = HuberRegressor(fit_intercept=False, alpha=0.0, max_iter=100, epsilon=1.35)
      huber.fit(X, y)

      >>> huber.coef_
      array([ 6.71756703,  5.08873222])
      >>> huber.intercept_
      0.0
      >>> huber.scale_
      2.5560209922722317
     */
    val coefficientsPy = Vectors.dense(6.71756703, 5.08873222)
    val interceptPy = 0.0
    val scalePy = 2.55602099

    assert(model1.coefficients ~= coefficientsPy relTol 1E-3)
    assert(model1.intercept === interceptPy)
    assert(model1.scale ~== scalePy relTol 1E-3)

    // Without regularization, with or without standardization will converge to the same solution.
    assert(model2.coefficients ~= coefficientsPy relTol 1E-3)
    assert(model2.intercept === interceptPy)
    assert(model2.scale ~== scalePy relTol 1E-3)
  }

  test("robust regression w/ intercept w/ regularization") {
    // Since Python scikit-learn does not support standardization, we can not compare that case.
    val trainer = (new RobustRegression).setFitIntercept(true).setRegParam(2.1)
      .setStandardization(false)
    val model = trainer.fit(dataset)

    /*
      import pandas as pd
      import numpy as np
      from sklearn.linear_model import HuberRegressor
      df = pd.read_csv("path", header = None)
      X = df[df.columns[1:3]]
      y = np.array(df[df.columns[0]])
      huber = HuberRegressor(fit_intercept=True, alpha=2.1, max_iter=100, epsilon=1.35)
      huber.fit(X, y)

      >>> huber.coef_
      array([ 4.68836213,  7.19283181])
      >>> huber.intercept_
      6.2997900552575956
      >>> huber.scale_
      0.078078316418777688
     */
    val coefficientsPy = Vectors.dense(4.68836213, 7.19283181)
    val interceptPy = 6.29979006
    val scalePy = 0.07807832

    assert(model.coefficients ~= coefficientsPy relTol 1E-3)
    assert(model.intercept ~== interceptPy relTol 1E-3)
    assert(model.scale ~== scalePy relTol 1E-3)
  }

  test("robust regression w/o intercept w/ regularization") {
    // Since Python scikit-learn does not support standardization, we can not compare that case.
    val trainer = (new RobustRegression).setFitIntercept(false).setRegParam(2.1)
      .setStandardization(false)
    val model = trainer.fit(dataset)

    /*
      import pandas as pd
      import numpy as np
      from sklearn.linear_model import HuberRegressor
      df = pd.read_csv("path", header = None)
      X = df[df.columns[1:3]]
      y = np.array(df[df.columns[0]])
      huber = HuberRegressor(fit_intercept=False, alpha=2.1, max_iter=100, epsilon=1.35)
      huber.fit(X, y)

      >>> huber.coef_
      array([ 6.65843427,  5.05270876])
      >>> huber.intercept_
      0.0
      >>> huber.scale_
      2.5699129758439119
     */
    val coefficientsPy = Vectors.dense(6.65843427, 5.05270876)
    val interceptPy = 0.0
    val scalePy = 2.56991298

    assert(model.coefficients ~= coefficientsPy relTol 1E-3)
    assert(model.intercept === interceptPy)
    assert(model.scale ~== scalePy relTol 1E-3)
  }

  test("robust regression w/ weighted data") {
    val trainer1 = (new RobustRegression).setWeightCol("weight").setFitIntercept(true)
      .setStandardization(true)
    val trainer2 = (new RobustRegression).setWeightCol("weight").setFitIntercept(true)
      .setStandardization(false)

    val model1 = trainer1.fit(datasetWithWeight)
    val model2 = trainer2.fit(datasetWithWeight)

    /*
      import pandas as pd
      import numpy as np
      from sklearn.linear_model import HuberRegressor
      df = pd.read_csv("path", header = None)
      X = df[df.columns[2:4]]
      y = np.array(df[df.columns[0]])
      w = np.array(df[df.columns[1]])
      huber = HuberRegressor(fit_intercept=True, alpha=0.0, max_iter=100, epsilon=1.35)
      huber.fit(X, y, w)

      >>> huber.coef_
      array([ 4.68951232,  7.19607089])
      >>> huber.intercept_
      6.3063008457490168
      >>> huber.scale_
      0.077512216330943975
     */
    val coefficientsPy = Vectors.dense(4.68951232, 7.19607089)
    val interceptPy = 6.30630085
    val scalePy = 0.07751222

    assert(model1.coefficients ~= coefficientsPy relTol 1E-3)
    assert(model1.intercept ~== interceptPy relTol 1E-3)
    assert(model1.scale ~== scalePy relTol 1E-3)

    // Without regularization, with or without standardization will converge to the same solution.
    assert(model2.coefficients ~= coefficientsPy relTol 1E-3)
    assert(model2.intercept ~== interceptPy relTol 1E-3)
    assert(model2.scale ~== scalePy relTol 1E-3)
  }

  test("robust regression match linear regression for large m") {
    val rr = new RobustRegression().setM(1E5)
    val rrModel = rr.fit(dataset)

    val lr = new LinearRegression()
    val lrModel = lr.fit(dataset)

    assert(rrModel.coefficients ~== lrModel.coefficients relTol 1E-3)
    assert(rrModel.intercept ~== lrModel.intercept relTol 1E-3)
  }
}
