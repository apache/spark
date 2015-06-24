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

package org.apache.spark.ml.tuning

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.{DoubleParam, ParamMap, ParamValidators}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.DataFrame

/**
 * Params for [[TrainValidationSplit]] and [[TrainValidationSplitModel]].
 */
private[ml] trait TrainValidationSplitParams extends ValidationParams {
  /**
   * Param for ratio between train and validation data. Must be between 0 and 1.
   * Default: 0.75
   * @group param
   */
  val trainRatio: DoubleParam = new DoubleParam(this, "numFolds",
    "ratio between training set and validation set (>= 0 && <= 1)", ParamValidators.inRange(0, 1))

  /** @group getParam */
  def getTrainPercent: Double = $(trainRatio)

  setDefault(trainRatio -> 0.75)
}

/**
 * :: Experimental ::
 * Validation for hyper-parameter tuning.
 * Randomly splits the input dataset into train and validation sets.
 * And uses evaluation metric on the validation set to select the best model.
 * Similar to CrossValidator, but only splits the set once.
 */
@Experimental
class TrainValidationSplit(uid: String)
  extends Validation[TrainValidationSplitModel, TrainValidationSplit](uid)
  with TrainValidationSplitParams with Logging {

  def this() = this(Identifiable.randomUID("cv"))

  /** @group setParam */
  def setTrainRatio(value: Double): this.type = set(trainRatio, value)

  override protected[ml] def validationLogic(
      dataset: DataFrame,
      est: Estimator[_],
      eval: Evaluator,
      epm: Array[ParamMap],
      numModels: Int): Array[Double] = {

    val schema = dataset.schema
    transformSchema(schema, logging = true)
    val sqlCtx = dataset.sqlContext

    val splits = MLUtils.sample(dataset.rdd, $(trainRatio), 1)
    val trainingDataset = sqlCtx.createDataFrame(splits._1, schema).cache()
    val validationDataset = sqlCtx.createDataFrame(splits._2, schema).cache()
    measureModels(trainingDataset, validationDataset, est, eval, epm, numModels)
  }

  override protected[ml] def createModel(
      uid: String,
      bestModel: Model[_],
      metrics: Array[Double]): TrainValidationSplitModel = {
    copyValues(new TrainValidationSplitModel(uid, bestModel, metrics).setParent(this))
  }
}

/**
 * :: Experimental ::
 * Model from train validation split.
 */
@Experimental
class TrainValidationSplitModel private[ml] (
    uid: String,
    bestModel: Model[_],
    avgMetrics: Array[Double])
  extends ValidationModel[TrainValidationSplitModel](uid, bestModel, avgMetrics)
  with TrainValidationSplitParams {

  override def copy(extra: ParamMap): TrainValidationSplitModel = {
    val copied = new TrainValidationSplitModel (
      uid,
      bestModel.copy(extra).asInstanceOf[Model[_]],
      avgMetrics.clone())
    copyValues(copied, extra)
  }
}
