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

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.shared.HasMaxIter
import org.apache.spark.ml.param.{Param, _}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.DataFrame

/**
 * Params for [[Arm]].
 */
trait ArmParams[M <: Model[M]] extends Params with HasMaxIter {

  /**
   * param for the estimator to be validated
   * @group param
   */
  val estimator: Param[Estimator[M]] = new Param(this, "estimator", "estimator for selection")

  /** @group getParam */
  def getEstimator: Estimator[M] = $(estimator)

  /**
   * Param for the initial model of a given estimator. Default None.
   * @group param
   */
  val initialModel: Param[Option[M]] =
    new Param(this, "initialModel", "initial model for warm-start")

  /** @group getParam */
  val getInitialModel: Option[M] = $(initialModel)

  /**
   * param for estimator param maps
   * @group param
   */
  val estimatorParamMap: Param[ParamMap] =
    new Param(this, "estimatorParamMap", "param map for the estimator")

  /** @group getParam */
  def getEstimatorParamMap: ParamMap = $(estimatorParamMap)

  /**
   * param for the evaluator used to select hyper-parameters that maximize the validated metric
   * @group param
   */
  val evaluator: Param[Evaluator] = new Param(this, "evaluator",
    "evaluator used to select hyper-parameters that maximize the validated metric")

  /** @group getParam */
  def getEvaluator: Evaluator = $(evaluator)

  setDefault(initialModel -> None, maxIter -> 1)
}


/**
 * Multi-bandit arm for hyper-parameter selection. An arm is a composition of an estimator, a model
 * and an evaluator. Pulling an arm means performs a single iterative step for the estimator, which
 * consumes a current model and produce a new one. The evaluator computes the error given a target
 * column and a predicted column.
 */
class Arm[M <: Model[M]](override val uid: String) extends ArmParams[M] {

  def this() = this(Identifiable.randomUID("arm"))

  /** @group setParam */
  def setEstimator(value: Estimator[M]): this.type = set(estimator, value)

  /** @group setParam */
  def setInitialModel(value: Option[M]): this.type = set(initialModel, value)

  /** @group setParam */
  def setEstimatorParamMap(value: ParamMap): this.type = set(estimatorParamMap, value)

  /** @group setParam */
  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  /** @group setParam */
  def setMaxIter(value: Int):this.type = set(maxIter, value)

  /**
   * Inner model to record intermediate training result.
   */
  private var model: Option[M] = $(initialModel)

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
  def pull(dataset: DataFrame): this.type = {
    this.numPulls += 1
    val epm = $(estimatorParamMap).put(initialModel, model).put(maxIter, $(maxIter))
    this.model = Some($(estimator).fit(dataset, epm))
    this
  }

  /**
   * Evaluate the model according to a validation dataset.
   */
  def getValidationResult(validationSet: DataFrame): Double = {
    if (model.isEmpty) {
      throw new Exception("model is empty")
    } else {
      $(evaluator).evaluate(model.get.transform(validationSet))
    }
  }

  override def copy(extra: ParamMap): Arm[M] = {
    val copied = defaultCopy(extra).asInstanceOf[Arm[M]]
    copied
  }
}
