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

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.LabeledPoint
import org.apache.spark.ml.param.{Params, ParamMap, HasMaxIter, HasRegParam}
import org.apache.spark.mllib.linalg.{BLAS, Vector}
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Params for linear regression.
 */
private[regression] trait LinearRegressionParams extends RegressorParams
  with HasRegParam with HasMaxIter

/**
 * :: AlphaComponent ::
 * Logistic regression.
 */
@AlphaComponent
class LinearRegression extends Regressor[LinearRegression, LinearRegressionModel]
  with LinearRegressionParams {

  setRegParam(0.1)
  setMaxIter(100)

  def setRegParam(value: Double): this.type = set(regParam, value)
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /**
   * Same as [[fit()]], but using strong types.
   * NOTE: This does NOT support instance weights.
   * @param dataset  Training data.  Instance weights are ignored.
   * @param paramMap  Parameters for training.
   *                  These values override any specified in this Estimator's embedded ParamMap.
   */
  override def train(dataset: RDD[LabeledPoint], paramMap: ParamMap): LinearRegressionModel = {
    val oldDataset = dataset.map { case LabeledPoint(label: Double, features: Vector, weight) =>
      org.apache.spark.mllib.regression.LabeledPoint(label, features)
    }
    val handlePersistence = oldDataset.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      oldDataset.persist(StorageLevel.MEMORY_AND_DISK)
    }
    val lr = new LinearRegressionWithSGD()
    lr.optimizer
      .setRegParam(paramMap(regParam))
      .setNumIterations(paramMap(maxIter))
    val model = lr.run(oldDataset)
    val lrm = new LinearRegressionModel(this, paramMap, model.weights, model.intercept)
    if (handlePersistence) {
      oldDataset.unpersist()
    }
    lrm
  }

  /**
   * Same as [[fit()]], but using strong types.
   * NOTE: This does NOT support instance weights.
   * @param dataset  Training data.  Instance weights are ignored.
   */
  def train(dataset: RDD[LabeledPoint]): LinearRegressionModel = train(dataset, new ParamMap())
}

/**
 * :: AlphaComponent ::
 * Model produced by [[LinearRegression]].
 */
@AlphaComponent
class LinearRegressionModel private[ml] (
    override val parent: LinearRegression,
    override val fittingParamMap: ParamMap,
    val weights: Vector,
    val intercept: Double)
  extends RegressionModel[LinearRegressionModel]
  with LinearRegressionParams {

  override def predict(features: Vector): Double = {
    BLAS.dot(features, weights) + intercept
  }

  private[ml] override def copy(): LinearRegressionModel = {
    val m = new LinearRegressionModel(parent, fittingParamMap, weights, intercept)
    Params.inheritValues(this.paramMap, this, m)
    m
  }
}
