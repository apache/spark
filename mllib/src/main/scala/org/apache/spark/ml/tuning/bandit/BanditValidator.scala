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

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.param.shared.{HasMaxIter, HasSeed}
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
   *
   * @group param
   */
  final val stepsPerPulling: IntParam =
    new IntParam(this, "stepsPerPulling", "the count of iterative steps in one pulling")

  /** @group getParam */
  final def getStepsPerPulling: Int = $(stepsPerPulling)

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
   * An array of search strategies to use.
   *
   * @group param
   */
  val searchStrategy: Param[Search] = new Param(this, "searchStrategies", "")

  /** @group getParam */
  def getSearchStrategy: Search = $(searchStrategy)

  setDefault(maxIter -> math.pow(2, 6).toInt, stepsPerPulling -> 1, numFolds -> 3)
}

/**
 * :: Experimental ::
 * K-fold cross validation.
 */
@Experimental
class BanditValidator(override val uid: String)
  extends Estimator[BanditValidatorModel] with BanditValidatorParams with Logging {

  def this() = this(Identifiable.randomUID("bandit validation"))

  // TODO
  def transformSchema(schema: StructType): StructType = {
    schema
  }

  // TODO
  def copy(extra: ParamMap): BanditValidator = ???

  /** @group setParam */
  def setSearchStrategy(value: Search): this.type = set(searchStrategy, value)

  /** @group setParam */
  def setStepsPerPulling(value: Int): this.type = set(stepsPerPulling, value)

  /** @group setParam */
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  override def fit(dataset: DataFrame): BanditValidatorModel = {
    val schema = dataset.schema
    transformSchema(schema, logging = true)
    val sqlCtx = dataset.sqlContext
    val est = $(estimator)
    val eval = $(evaluator)
    val epm = $(estimatorParamMaps)
    val splits = MLUtils.kFold(dataset.rdd, $(numFolds), 0)
    val bestArms = splits.zipWithIndex.map { case ((training, validation), splitIndex) =>
      val trainingDataset = sqlCtx.createDataFrame(training, schema).cache()
      val validationDataset = sqlCtx.createDataFrame(validation, schema).cache()
      // multi-model training
      logDebug(s"Train split $splitIndex with multiple sets of parameters.")

      val arms = epm.map { parameter =>
        val arm = new Arm().setMaxIter($(stepsPerPulling)).setEstimator(est).setEstimatorParamMap(parameter).setEvaluator(eval)
        arm
      }

      val bestArm = $(searchStrategy).search($(maxIter), arms, trainingDataset, validationDataset)
      (bestArm, bestArm.getValidationResult(validationDataset))
    }

    val bestArm = bestArms.minBy(_._2)._1
    val bestModel = bestArm.getModel


    copyValues(new BanditValidatorModel(uid, bestModel).setParent(this))
  }
}


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

