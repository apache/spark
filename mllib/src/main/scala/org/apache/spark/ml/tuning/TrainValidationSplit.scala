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

import scala.reflect.ClassTag

import com.github.fommil.netlib.F2jBLAS

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.{DoubleParam, ParamMap, ParamValidators}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.{RDD, PartitionwiseSampledRDD}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils
import org.apache.spark.util.random.BernoulliCellSampler


/**
 * Params for [[TrainValidatorSplit]] and [[TrainValidatorSplitModel]].
 */
private[ml] trait TrainValidatorSplitParams extends ValidatorParams {
  /**
   * Param for ratio between train and validation data. Must be between 0 and 1.
   * Default: 0.75
   * @group param
   */
  val trainRatio: DoubleParam = new DoubleParam(this, "trainRatio",
    "ratio between training set and validation set (>= 0 && <= 1)", ParamValidators.inRange(0, 1))

  /** @group getParam */
  def getTrainRatio: Double = $(trainRatio)

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
class TrainValidatorSplit(override val uid: String) extends Estimator[TrainValidatorSplitModel]
  with TrainValidatorSplitParams with Logging {

  def this() = this(Identifiable.randomUID("cv"))

  private val f2jBLAS = new F2jBLAS

  /** @group setParam */
  def setEstimator(value: Estimator[_]): this.type = set(estimator, value)

  /** @group setParam */
  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  /** @group setParam */
  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  /** @group setParam */
  def setTrainRatio(value: Double): this.type = set(trainRatio, value)

  private[this] def sample[T: ClassTag](
      rdd: RDD[T],
      lb: Double,
      ub: Double,
      seed: Int = Utils.random.nextInt()): (RDD[T], RDD[T]) = {
    val sampler = new BernoulliCellSampler[T](lb, ub, complement = false)
    val validation = new PartitionwiseSampledRDD(rdd, sampler, true, seed)
    val training = new PartitionwiseSampledRDD(rdd, sampler.cloneComplement(), true, seed)
    (training, validation)
  }

  override def fit(dataset: DataFrame): TrainValidatorSplitModel = {
    val schema = dataset.schema
    transformSchema(schema, logging = true)
    val sqlCtx = dataset.sqlContext
    val est = $(estimator)
    val eval = $(evaluator)
    val epm = $(estimatorParamMaps)
    val numModels = epm.length
    val metrics = new Array[Double](epm.length)
    val (training, validation) = sample(dataset.rdd, $(trainRatio), 1)
    val trainingDataset = sqlCtx.createDataFrame(training, schema).cache()
    val validationDataset = sqlCtx.createDataFrame(validation, schema).cache()

    // multi-model training
    val models = est.fit(trainingDataset, epm).asInstanceOf[Seq[Model[_]]]
    trainingDataset.unpersist()
    var i = 0
    while (i < numModels) {
      // TODO: duplicate evaluator to take extra params from input
      val metric = eval.evaluate(models(i).transform(validationDataset, epm(i)))
      logDebug(s"Got metric $metric for model trained with ${epm(i)}.")
      metrics(i) += metric
      i += 1
    }
    validationDataset.unpersist()

    logInfo(s"Train validation split metrics: ${metrics.toSeq}")
    val (bestMetric, bestIndex) = metrics.zipWithIndex.maxBy(_._1)
    logInfo(s"Best set of parameters:\n${epm(bestIndex)}")
    logInfo(s"Best train validation split metric: $bestMetric.")
    val bestModel = est.fit(dataset, epm(bestIndex)).asInstanceOf[Model[_]]
    copyValues(new TrainValidatorSplitModel(uid, bestModel, metrics).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    $(estimator).transformSchema(schema)
  }

  override def validateParams(): Unit = {
    super.validateParams()
    val est = $(estimator)
    for (paramMap <- $(estimatorParamMaps)) {
      est.copy(paramMap).validateParams()
    }
  }

  override def copy(extra: ParamMap): TrainValidatorSplit = {
    val copied = defaultCopy(extra).asInstanceOf[TrainValidatorSplit]
    if (copied.isDefined(estimator)) {
      copied.setEstimator(copied.getEstimator.copy(extra))
    }
    if (copied.isDefined(evaluator)) {
      copied.setEvaluator(copied.getEvaluator.copy(extra))
    }
    copied
  }
}

/**
 * :: Experimental ::
 * Model from train validation split.
 */
@Experimental
class TrainValidatorSplitModel private[ml] (
    override val uid: String,
    val bestModel: Model[_],
    val avgMetrics: Array[Double])
  extends Model[TrainValidatorSplitModel] with TrainValidatorSplitParams {

  override def validateParams(): Unit = {
    bestModel.validateParams()
  }

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    bestModel.transform(dataset)
  }

  override def transformSchema(schema: StructType): StructType = {
    bestModel.transformSchema(schema)
  }

  override def copy(extra: ParamMap): TrainValidatorSplitModel = {
    val copied = new TrainValidatorSplitModel (
      uid,
      bestModel.copy(extra).asInstanceOf[Model[_]],
      avgMetrics.clone())
    copyValues(copied, extra)
  }
}
