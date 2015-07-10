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

import com.github.fommil.netlib.F2jBLAS

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml._
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.DataFrame

/**
 * Params for [[CrossValidator]] and [[CrossValidatorModel]].
 */
private[ml] trait CrossValidatorParams extends ValidatorParams {
  /**
   * Param for number of folds for cross validation.  Must be >= 2.
   * Default: 3
   * @group param
   */
  val numFolds: IntParam = new IntParam(this, "numFolds",
    "number of folds for cross validation (>= 2)", ParamValidators.gtEq(2))

  /** @group getParam */
  def getNumFolds: Int = $(numFolds)

  setDefault(numFolds -> 3)
}

/**
 * :: Experimental ::
 * K-fold cross validation.
 */
@Experimental
class CrossValidator(uid: String)
  extends Validator[CrossValidatorModel, CrossValidator](uid)
  with CrossValidatorParams with Logging {

  def this() = this(Identifiable.randomUID("cv"))

  private val f2jBLAS = new F2jBLAS

  /** @group setParam */
  def setNumFolds(value: Int): this.type = set(numFolds, value)

  override protected[ml] def validationLogic(
      dataset: DataFrame,
      est: Estimator[_],
      eval: Evaluator,
      epm: Array[ParamMap],
      numModels: Int): Array[Double] = {

    val schema = dataset.schema
    transformSchema(schema, logging = true)
    val sqlCtx = dataset.sqlContext

    val metrics = new Array[Double](epm.length)
    val splits = MLUtils.kFold(dataset.rdd, $(numFolds), 0)

    splits.zipWithIndex.foreach { case ((training, validation), splitIndex) =>
      val trainingDataset = sqlCtx.createDataFrame(training, schema).cache()
      val validationDataset = sqlCtx.createDataFrame(validation, schema).cache()
      logDebug(s"Train split $splitIndex with multiple sets of parameters.")
      val metricsPerSplit =
        measureModels(trainingDataset, validationDataset, est, eval, epm, numModels)

      var i = 0
      while (i < numModels) {
        metrics(i) += metricsPerSplit(i)
        i += 1
      }
    }

    f2jBLAS.dscal(numModels, 1.0 / $(numFolds), metrics, 1)
    metrics
  }

  override protected[ml] def createModel(
      uid: String,
      bestModel: Model[_],
      metrics: Array[Double]): CrossValidatorModel = {
    copyValues(new CrossValidatorModel(uid, bestModel, metrics).setParent(this))
  }
}

/**
 * :: Experimental ::
 * Model from k-fold cross validation.
 */
@Experimental
class CrossValidatorModel private[ml] (
    uid: String,
    bestModel: Model[_],
    avgMetrics: Array[Double])
  extends ValidatorModel[CrossValidatorModel](uid, bestModel, avgMetrics)
  with CrossValidatorParams {

  override def copy(extra: ParamMap): CrossValidatorModel = {
    val copied = new CrossValidatorModel(
      uid,
      bestModel.copy(extra).asInstanceOf[Model[_]],
      avgMetrics.clone())
    copyValues(copied, extra)
  }
}
