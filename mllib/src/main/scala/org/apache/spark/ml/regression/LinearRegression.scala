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
import org.apache.spark.ml.param.{Params, ParamMap}
import org.apache.spark.ml.param.shared._
import org.apache.spark.mllib.linalg.{BLAS, Vector}
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel


/**
 * Params for linear regression.
 */
private[regression] trait LinearRegressionParams extends RegressorParams
  with HasRegParam with HasMaxIter


/**
 * :: AlphaComponent ::
 *
 * Linear regression.
 */
@AlphaComponent
class LinearRegression extends Regressor[Vector, LinearRegression, LinearRegressionModel]
  with LinearRegressionParams {

  setDefault(regParam -> 0.1, maxIter -> 100)

  /** @group setParam */
  def setRegParam(value: Double): this.type = set(regParam, value)

  /** @group setParam */
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  override protected def train(dataset: DataFrame, paramMap: ParamMap): LinearRegressionModel = {
    // Extract columns from data.  If dataset is persisted, do not persist oldDataset.
    val oldDataset = extractLabeledPoints(dataset, paramMap)
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      oldDataset.persist(StorageLevel.MEMORY_AND_DISK)
    }

    // Train model
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
}

/**
 * :: AlphaComponent ::
 *
 * Model produced by [[LinearRegression]].
 */
@AlphaComponent
class LinearRegressionModel private[ml] (
    override val parent: LinearRegression,
    override val fittingParamMap: ParamMap,
    val weights: Vector,
    val intercept: Double)
  extends RegressionModel[Vector, LinearRegressionModel]
  with LinearRegressionParams {

  override protected def predict(features: Vector): Double = {
    BLAS.dot(features, weights) + intercept
  }

  override protected def copy(): LinearRegressionModel = {
    val m = new LinearRegressionModel(parent, fittingParamMap, weights, intercept)
    Params.inheritValues(extractParamMap(), this, m)
    m
  }
}
