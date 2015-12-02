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
import org.apache.spark.ml.tuning.ValidatorParams
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * Params for [[BanditValidator]] and [[BanditValidatorModel]].
 */
trait BanditValidatorParams extends ValidatorParams with HasMaxIter {
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
class BanditValidator(override val uid: String)
  extends Estimator[BanditValidatorModel] with BanditValidatorParams with Logging {

  def this() = this(Identifiable.randomUID("BanditValidator"))

  def transformSchema(schema: StructType): StructType = {
    $(estimator).transformSchema(schema)
  }

  def copy(extra: ParamMap): BanditValidator = {
    val copied = defaultCopy(extra).asInstanceOf[BanditValidator]
    if (copied.isDefined(estimator)) {
      copied.setEstimator(
        copied.getEstimator.copy(extra).asInstanceOf[Estimator[_] with Controllable])
    }
    if (copied.isDefined(evaluator)) {
      copied.setEvaluator(copied.getEvaluator.copy(extra))
    }
    copied
  }

  /** @group setParam */
  def setEstimator(value: Estimator[_] with Controllable): this.type = set(estimator, value)

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

  /**
   * Aggregate all results in the summary variable. We record behaviors of every arm in the whole
   * validation process. So there are `$(estimatorParamMaps).length * $(numFolds)` arms in total.
   * Each arm records a `Tuple3` of "iteration", "evaluation result on training set", and
   * "evaluation result on validation test".
   */
  private var trainingSummary: Option[Array[Array[(Int, Double, Double)]]] = None

  /**
   * Re-organize the training summary into a readable format, for output to read by humans.
   */
  def readableSummary(): Array[String] = {
    val resultBuilder = new ArrayBuffer[String]()
    val sepStr = Array.fill(100)("=").mkString("")
    trainingSummary match {
      case None =>
        val error = "No summary recorded!"
        resultBuilder.append(error)
        println(error)
      case Some(x) =>
        var i = 0
        while (i < $(numFolds)) {
          var j = 0
          while (j < $(estimatorParamMaps).length) {
            val hint = s"For #${i}-fold training, #${j}-arm, we get"
            resultBuilder.append(hint)
            val history = s"\t${x(i * $(estimatorParamMaps).length + j).mkString(", ")}"
            resultBuilder.append(history)
            j += 1
          }
          resultBuilder.append(sepStr)
          i += 1
        }
    }
    resultBuilder.toArray
  }

  /**
   * Re-organize the training summary into a paintable format, for drawing pictures to compare
   * between each search strategy.
   * @return [ #fold, [ #iteration, #arm, trainingResult, validationResult ] ]
   */
  def paintableSummary(): Array[(Int, Array[(Int, Int, Double, Double)])] = {
    val numArms = $(estimatorParamMaps).length
    trainingSummary match {
      case None =>
        throw new NullPointerException("The value trainingSummary is None.")
      case Some(x) =>
        (0 until $(numFolds)).toArray.map { i =>
          val retVal = x.slice(i * numArms, (i + 1) * numArms).zipWithIndex.flatMap {
            case (armResult, armIdx) =>
              armResult.map { case (iter, trainingResult, validationResult) =>
                (iter, armIdx, trainingResult, validationResult)
              }
          }.sortBy(_._1)
          (i, retVal)
        }
    }
  }

  override def fit(dataset: DataFrame): BanditValidatorModel = {
    assert($(estimator).isInstanceOf[Controllable],
      s"Estimator ${$(estimator).getClass.getSimpleName} is not controllable.")

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
      val arms = epm.map(
        new Arm(
          est.asInstanceOf[Estimator[_] with Controllable], None, _, eval, $(stepsPerPulling)
        )
      )

      // Find the best arm with pre-defined search strategies
      val bestArm = $(searchStrategy).search(totalBudget, arms, trainingDataset, validationDataset,
        eval.isLargerBetter, needRecord = true)

      arms.zipWithIndex.foreach { case (arm, idx) =>
        if (trainingSummary.isEmpty) {
          trainingSummary = Some(Array.ofDim($(estimatorParamMaps).length * $(numFolds)))
        }
        trainingSummary.get(splitIndex * arms.length + idx) = arm.getHistory
      }

      (bestArm, bestArm.getValidationResult(validationDataset))
    }

    val bestArm = bestArms.minBy(_._2)._1
    val bestModel = bestArm.getModel

    copyValues(new BanditValidatorModel(uid, bestModel).setParent(this))
  }
}

/**
 * :: Experimental ::
 * Model from multi-arm bandit validation.
 *
 * @param bestModel The best model selected from multi-arm bandit validation.
 */
class BanditValidatorModel private[ml] (
    override val uid: String,
    val bestModel: Model[_])
  extends Model[BanditValidatorModel] with BanditValidatorParams {

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

  override def copy(extra: ParamMap): BanditValidatorModel = {
    val copied = new BanditValidatorModel(uid, bestModel.copy(extra).asInstanceOf[Model[_]])
    copyValues(copied, extra)
  }
}

/**
 * Multi-bandit arm for hyper-parameter selection. An arm is a composition of an estimator, a model
 * and an evaluator. Pulling an arm means performs a single iterative step for the estimator, which
 * consumes a current model and produce a new one. The evaluator computes the error given a target
 * column and a predicted column.
 */
class Arm(
    val estimator: Estimator[_] with Controllable,
    val initialModel: Option[Model[_]],
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
  private var model: Option[Model[_]] = None

  def getModel: Model[_] = model.get

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
    this.model = Some(estimator.fit(dataset, estimatorParamMap)).asInstanceOf[Option[Model[_]]]
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

