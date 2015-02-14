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
import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml._
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, Params}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * Params for [[CrossValidator]] and [[CrossValidatorModel]].
 */
private[ml] trait CrossValidatorParams extends Params {
  /**
   * param for the estimator to be cross-validated
   * @group param
   */
  val estimator: Param[Estimator[_]] = new Param(this, "estimator", "estimator for selection")

  /** @group getParam */
  def getEstimator: Estimator[_] = get(estimator)

  /**
   * param for estimator param maps
   * @group param
   */
  val estimatorParamMaps: Param[Array[ParamMap]] =
    new Param(this, "estimatorParamMaps", "param maps for the estimator")

  /** @group getParam */
  def getEstimatorParamMaps: Array[ParamMap] = get(estimatorParamMaps)

  /**
   * param for the evaluator for selection
   * @group param
   */
  val evaluator: Param[Evaluator] = new Param(this, "evaluator", "evaluator for selection")

  /** @group getParam */
  def getEvaluator: Evaluator = get(evaluator)

  /**
   * param for number of folds for cross validation
   * @group param
   */
  val numFolds: IntParam =
    new IntParam(this, "numFolds", "number of folds for cross validation", Some(3))

  /** @group getParam */
  def getNumFolds: Int = get(numFolds)
}

/**
 * :: AlphaComponent ::
 * K-fold cross validation.
 */
@AlphaComponent
class CrossValidator extends Estimator[CrossValidatorModel] with CrossValidatorParams with Logging {

  private val f2jBLAS = new F2jBLAS

  /** @group setParam */
  def setEstimator(value: Estimator[_]): this.type = set(estimator, value)

  /** @group setParam */
  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  /** @group setParam */
  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  /** @group setParam */
  def setNumFolds(value: Int): this.type = set(numFolds, value)

  override def fit(dataset: DataFrame, paramMap: ParamMap): CrossValidatorModel = {
    val map = this.paramMap ++ paramMap
    val schema = dataset.schema
    transformSchema(dataset.schema, paramMap, logging = true)
    val sqlCtx = dataset.sqlContext
    val est = map(estimator)
    val eval = map(evaluator)
    val epm = map(estimatorParamMaps)
    val numModels = epm.size
    val metrics = new Array[Double](epm.size)
    val splits = MLUtils.kFold(dataset.rdd, map(numFolds), 0)
    splits.zipWithIndex.foreach { case ((training, validation), splitIndex) =>
      val trainingDataset = sqlCtx.createDataFrame(training, schema).cache()
      val validationDataset = sqlCtx.createDataFrame(validation, schema).cache()
      // multi-model training
      logDebug(s"Train split $splitIndex with multiple sets of parameters.")
      val models = est.fit(trainingDataset, epm).asInstanceOf[Seq[Model[_]]]
      var i = 0
      while (i < numModels) {
        val metric = eval.evaluate(models(i).transform(validationDataset, epm(i)), map)
        logDebug(s"Got metric $metric for model trained with ${epm(i)}.")
        metrics(i) += metric
        i += 1
      }
    }
    f2jBLAS.dscal(numModels, 1.0 / map(numFolds), metrics, 1)
    logInfo(s"Average cross-validation metrics: ${metrics.toSeq}")
    val (bestMetric, bestIndex) = metrics.zipWithIndex.maxBy(_._1)
    logInfo(s"Best set of parameters:\n${epm(bestIndex)}")
    logInfo(s"Best cross-validation metric: $bestMetric.")
    val bestModel = est.fit(dataset, epm(bestIndex)).asInstanceOf[Model[_]]
    val cvModel = new CrossValidatorModel(this, map, bestModel)
    Params.inheritValues(map, this, cvModel)
    cvModel
  }

  private[ml] override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = this.paramMap ++ paramMap
    map(estimator).transformSchema(schema, paramMap)
  }
}

/**
 * :: AlphaComponent ::
 * Model from k-fold cross validation.
 */
@AlphaComponent
class CrossValidatorModel private[ml] (
    override val parent: CrossValidator,
    override val fittingParamMap: ParamMap,
    val bestModel: Model[_])
  extends Model[CrossValidatorModel] with CrossValidatorParams {

  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    bestModel.transform(dataset, paramMap)
  }

  private[ml] override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    bestModel.transformSchema(schema, paramMap)
  }
}
