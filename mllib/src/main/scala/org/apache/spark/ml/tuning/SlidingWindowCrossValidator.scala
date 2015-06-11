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
import org.apache.spark.sql.types.StructType

/**
 * Params for [[SlidingWindowCrossValidator]] and [[SlidingWindowCrossValidatorModel]].
 */
private[ml] trait SlidingWindowCrossValidatorParams extends Params {

  /**
   * param for the estimator to be cross-validated
   * @group param
   */
  val estimator: Param[Estimator[_]] = new Param(this, "estimator", "estimator for selection")

  /** @group getParam */
  def getEstimator: Estimator[_] = $(estimator)

  /**
   * param for estimator param maps
   * @group param
   */
  val estimatorParamMaps: Param[Array[ParamMap]] =
    new Param(this, "estimatorParamMaps", "param maps for the estimator")

  /** @group getParam */
  def getEstimatorParamMaps: Array[ParamMap] = $(estimatorParamMaps)

  /**
   * param for the evaluator used to select hyper-parameters that maximize the cross-validated
   * metric
   * @group param
   */
  val evaluator: Param[Evaluator] = new Param(this, "evaluator",
    "evaluator used to select hyper-parameters that maximize the cross-validated metric")

  /** @group getParam */
  def getEvaluator: Evaluator = $(evaluator)

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

  /**
   * Param for the cutoff index (exclusive) for the first training data. Must be > 0.
   * Default: 1
   * @group param
   */
  val firstCutoffIndex: LongParam = new LongParam(this, "firstCutoffIndex",
    "cutoff index (exclusive) for the first training data", ParamValidators.gt(0))

  setDefault(firstCutoffIndex -> 1L)

  /** @group getParam */
  def getFirstCutoffIndex: Long = $(firstCutoffIndex)

  /**
   * Param for the size of the validation data window. Must be > 0.
   * Default: 1
   * @group param
   */
  val windowSize: LongParam = new LongParam(this, "windowSize",
    "size of the validation data window", ParamValidators.gt(0))

  setDefault(windowSize -> 1L)

  /** @group getParam */
  def getWindowSize: Long = $(windowSize)

  /**
   * Param for index column name.
   * Default: "index"
   * @group param
   */
  val indexCol: Param[String] = new Param[String](this, "indexCol", "index column name")

  setDefault(indexCol, "index")

  /** @group getParam */
  def getIndexCol: String = $(indexCol)
}

private [tuning] object SlidingWindowCrossValidator {
  def split(dataset: DataFrame, firstCutoffIndex: Long, windowSize: Long, numFolds: Int,
    indexCol: String): Array[(DataFrame, DataFrame)] = {
    (0 until numFolds).map { idx =>
      val trainingUntil = firstCutoffIndex + idx * windowSize
      val validationUntil = firstCutoffIndex + (idx + 1) * windowSize
      val trainingSet = dataset.filter(dataset(indexCol) < trainingUntil)
      val validationSet = dataset.filter(
        dataset(indexCol) >= trainingUntil && dataset(indexCol) < validationUntil)
      (trainingSet, validationSet)
    }
    .toArray
  }
}


/**
 * :: Experimental ::
 * Sliding window cross validation.
 *
 * [[SlidingWindowCrossValidator]] provides a cross-validation method by splitting the data set by
 * a cutoff index. The cutoff index is used to separate training and validation data set. Rows
 * smaller than the cutoff index go to the training set and rows larger than it go to the validation
 * set. This splitting method is useful for time-sensitive problem like fraud detection and stock
 * price prediction.
 *
 * 3 parameters govern the splitting: numFolds is the number of splits, firstCutoffIndex denotes the
 * the cutoff index of the first training data, and windowSize defines the index range of a
 * validation set. In particular, training data is constructed with index range in [-inf, firstCutoffIndex
 * + i * windowSize) and validation data is constructed with index range in [firstCutoffIndex +
 * i * windowSize, firstCutoffIndex + (i + 1) * windowSize).
 */
@Experimental
class SlidingWindowCrossValidator(override val uid: String)
  extends Estimator[SlidingWindowCrossValidatorModel]
  with SlidingWindowCrossValidatorParams with Logging {

  def this() = this(Identifiable.randomUID("swcv"))

  private val f2jBLAS = new F2jBLAS

  /** @group setParam */
  def setEstimator(value: Estimator[_]): this.type = set(estimator, value)

  /** @group setParam */
  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  /** @group setParam */
  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  /** @group setParam */
  def setNumFolds(value: Int): this.type = set(numFolds, value)

  /** @group setParam */
  def setFirstCutoffIndex(value: Long): this.type = set(firstCutoffIndex, value)

  /** @group setParam */
  def setWindowSize(value: Long): this.type = set(windowSize, value)

  /** @group setParam */
  def setIndexCol(value: String): this.type = set(indexCol, value)

  override def fit(dataset: DataFrame): SlidingWindowCrossValidatorModel = {
    val schema = dataset.schema
    transformSchema(schema, logging = true)
    val sqlCtx = dataset.sqlContext
    val est = $(estimator)
    val eval = $(evaluator)
    val epm = $(estimatorParamMaps)
    val numModels = epm.length
    val metrics = new Array[Double](epm.length)
    val splits = SlidingWindowCrossValidator.split(
      dataset, $(firstCutoffIndex), $(windowSize), $(numFolds), $(indexCol))
    splits.zipWithIndex.foreach { case ((trainingDataset, validationDataset), splitIndex) =>
      // multi-model training
      logDebug(s"Train split $splitIndex with multiple sets of parameters.")
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
    }
    f2jBLAS.dscal(numModels, 1.0 / $(numFolds), metrics, 1)
    logInfo(s"Average cross-validation metrics: ${metrics.toSeq}")
    val (bestMetric, bestIndex) = metrics.zipWithIndex.maxBy(_._1)
    logInfo(s"Best set of parameters:\n${epm(bestIndex)}")
    logInfo(s"Best cross-validation metric: $bestMetric.")
    val bestModel = est.fit(dataset, epm(bestIndex)).asInstanceOf[Model[_]]
    copyValues(new SlidingWindowCrossValidatorModel(uid, bestModel, metrics).setParent(this))
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
}

/**
 * :: Experimental ::
 * Model from k-fold cross validation.
 */
@Experimental
class SlidingWindowCrossValidatorModel private[ml] (
    override val uid: String,
    val bestModel: Model[_],
    val avgMetrics: Array[Double])
  extends Model[SlidingWindowCrossValidatorModel] with SlidingWindowCrossValidatorParams {

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

  override def copy(extra: ParamMap): SlidingWindowCrossValidatorModel = {
    val copied = new SlidingWindowCrossValidatorModel(
      uid,
      bestModel.copy(extra).asInstanceOf[Model[_]],
      avgMetrics.clone())
    copyValues(copied, extra)
  }
}

