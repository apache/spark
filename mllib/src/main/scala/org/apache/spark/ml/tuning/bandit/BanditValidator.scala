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

package org.apache.spark.ml.tuning.bandit

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.shared.HasMaxIter
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, _}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * Params for [[BanditValidator]] and [[BanditValidatorModel]].
 */
trait BanditValidatorParams[M <: Model[M]] extends HasMaxIter {

  /**
   * param for the estimator to be validated
   * @group param
   */
  val estimator: Param[Estimator[M] with Controllable[M]] =
    new Param(this, "estimator", "estimator for selection")

  /** @group getParam */
  def getEstimator: Estimator[M] with Controllable[M] = $(estimator)

  /**
   * param for estimator param maps
   * @group param
   */
  val estimatorParamMaps: Param[Array[ParamMap]] =
    new Param(this, "estimatorParamMaps", "param maps for the estimator")

  /** @group getParam */
  def getEstimatorParamMaps: Array[ParamMap] = $(estimatorParamMaps)

  /**
   * param for the evaluator used to select hyper-parameters that maximize the validated metric
   * @group param
   */
  val evaluator: Param[Evaluator] = new Param(this, "evaluator",
    "evaluator used to select hyper-parameters that maximize the validated metric")

  /** @group getParam */
  def getEvaluator: Evaluator = $(evaluator)

  /**
   * Step control for one pulling of an arm.
   * @group param
   */
  val stepsPerPulling: IntParam =
    new IntParam(this, "stepsPerPulling", "the count of iterative steps in one pulling")

  /** @group getParam */
  def getStepsPerPulling: Int = $(stepsPerPulling)

  /**
   * Param for number of folds for cross validation.  Must be >= 2.
   * Default: 3
   * @group param
   */
  val numFolds: IntParam = new IntParam(this, "numFolds",
    "number of folds for cross validation (>= 2)", ParamValidators.gtEq(2))

  /** @group getParam */
  def getNumFolds: Int = $(numFolds)

  /**
   * Multi-arm bandit search strategy to be used in the validator. All strategies are listed in
   * [[Search]]. Different strategy has different behavior when it pulling arms.
   * @group param
   */
  val searchStrategy: Param[Search] =
    new Param(this, "searchStrategy", "search strategy to pull arms")

  /** @group getParam */
  def getSearchStrategy: Search = $(searchStrategy)

  setDefault(maxIter -> math.pow(2, 6).toInt, stepsPerPulling -> 1, numFolds -> 3)
}

/**
 * :: Experimental ::
 * Multi-arm bandit hyper-parameters selection.
 */
@Experimental
class BanditValidator[M <: Model[M]](override val uid: String)
  extends Estimator[BanditValidatorModel[M]] with BanditValidatorParams[M] with Logging {

  def this() = this(Identifiable.randomUID("bandit validation"))

  def transformSchema(schema: StructType): StructType = {
    $(estimator).transformSchema(schema)
  }

  def copy(extra: ParamMap): BanditValidator[M] = {
    val copied = defaultCopy(extra).asInstanceOf[BanditValidator[M]]
    if (copied.isDefined(estimator)) {
      // copied.setEstimator(copied.getEstimator.copy(extra))
    }
    if (copied.isDefined(evaluator)) {
      copied.setEvaluator(copied.getEvaluator.copy(extra))
    }
    copied
  }

  /** @group setParam */
  def setEstimator(value: Estimator[M] with Controllable[M]): this.type = set(estimator, value)

  /** @group setParam */
  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  /** @group setParam */
  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  /** @group setParam */
  def setNumFolds(value: Int): this.type = set(numFolds, value)

  /** @group setParam */
  def setSearchStrategy(value: Search): this.type = set(searchStrategy, value)

  /** @group setParam */
  def setStepsPerPulling(value: Int): this.type = set(stepsPerPulling, value)

  /** @group setParam */
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  override def fit(dataset: DataFrame): BanditValidatorModel[M] = {
    val schema = dataset.schema
    transformSchema(schema, logging = true)
    val sqlCtx = dataset.sqlContext
    // Get all parameters first
    val est = $(estimator)
    val eval = $(evaluator)
    val epm = $(estimatorParamMaps)

    // Split data into k-fold
    val splits = MLUtils.kFold(dataset.rdd, $(numFolds), 0)

    val totalBudget = if ($(maxIter) > 2 * epm.length) {
      $(maxIter)
    } else {
      logInfo("The `maxIter` should be larger than `2 * arms size`.")
      2 * epm.length + 1
    }

    // Find the best arm with k-fold bandit validation
    val bestArms = splits.zipWithIndex.map { case ((training, validation), splitIndex) =>
      val trainingDataset = sqlCtx.createDataFrame(training, schema).cache()
      val validationDataset = sqlCtx.createDataFrame(validation, schema).cache()
      logDebug(s"Train split $splitIndex with multiple sets of parameters.")

      // For each parameter map, create a corresponding arm
      val arms = epm.map(new Arm[M](est, None, _, eval, $(stepsPerPulling)))

      // Find the best arm with pre-defined search strategies
      val bestArm = $(searchStrategy)
        .search(totalBudget, arms, trainingDataset, validationDataset, eval.isLargerBetter)
      (bestArm, bestArm.getValidationResult(validationDataset))
    }

    val bestArm = bestArms.minBy(_._2)._1
    val bestModel = bestArm.getModel

    copyValues(new BanditValidatorModel[M](uid, bestModel).setParent(this))
  }
}

/**
 * :: Experimental ::
 * Model from multi-arm bandit validation.
 *
 * @param bestModel The best model selected from multi-arm bandit validation.
 */
class BanditValidatorModel[M <: Model[M]] private[ml] (
    override val uid: String,
    val bestModel: Model[M])
  extends Model[BanditValidatorModel[M]] with BanditValidatorParams[M] {

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

  override def copy(extra: ParamMap): BanditValidatorModel[M] = {
    val copied = new BanditValidatorModel(uid, bestModel.copy(extra).asInstanceOf[Model[M]])
    copyValues(copied, extra)
  }
}

/**
 * Multi-bandit arm for hyper-parameter selection. An arm is a composition of an estimator, a model
 * and an evaluator. Pulling an arm means performs a single iterative step for the estimator, which
 * consumes a current model and produce a new one. The evaluator computes the error given a target
 * column and a predicted column.
 */
class Arm[M <: Model[M]](
    val estimator: Estimator[M] with Controllable[M],
    val initialModel: Option[M],
    val estimatorParamMap: ParamMap,
    val evaluator: Evaluator,
    val stepsPerPulling: Int) {

  /**
   * Perf-test variable to record arm pulling history. `Int` in the beginning stands for the time
   * to pull this arm. The last two `Double`s to record evaluation result on training set and
   * validation set.
   * Since it's very costly to record these two results every time the code iterating, it will not
   * be computed in production code.
   */
  private val history = new ArrayBuffer[(Int, Double, Double)]()

  def setHistory(iter: Int, evalOnTrainingSet: Double, evalOnValidationSet: Double): this.type = {
    history.append((iter, evalOnTrainingSet, evalOnValidationSet))
    this
  }

  def getHistory: Array[(Int, Double, Double)] = history.toArray

  /**
   * Inner model to record intermediate training result.
   */
  private var model: Option[M] = None

  def getModel: M = model.get

  /**
   * Keep record of the number of pulls for computations in some search strategies.
   */
  private var numPulls: Int = 0

  def getNumPulls: Int = numPulls

  /**
   * Pull the arm to perform maxIter steps of the iterative [Estimator]. Model will be updated
   * after the pulling.
   */
  def pull(
      dataset: DataFrame,
      iter: Int,
      validationSet: Option[DataFrame] = None,
      record: Boolean = false): this.type = {
    this.numPulls += 1
    if (model.isEmpty && initialModel.isDefined) {
      this.model = initialModel
    }
    estimator.set(estimator.initialModel, model)
    estimator.set(estimator.maxIter, stepsPerPulling)
    this.model = Some(estimator.fit(dataset, estimatorParamMap))
    if (record) {
      val validationResult = validationSet match {
        case Some(data) => evaluator.evaluate(model.get.transform(data))
        case None => Double.NaN
      }
      setHistory(iter, evaluator.evaluate(model.get.transform(dataset)), validationResult)
    }
    this
  }

  /**
   * Evaluate the model according to a validation dataset.
   */
  def getValidationResult(validationSet: DataFrame): Double = {
    if (model.isEmpty) {
      throw new Exception("model is empty")
    } else {
      evaluator.evaluate(model.get.transform(validationSet))
    }
  }
}

