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
trait Controllable[M <: Model[M]] extends Params with HasMaxIter {

  /**
   * Param for the initial model of a given estimator. Default None.
   * @group param
   */
  val initialModel: Param[Option[M]] =
    new Param(this, "initialModel", "initial model for warm-start")

  /** @group getParam */
  def getInitialModel: Option[M] = $(initialModel)

  /** @group setParam */
  def setInitialModel(model: Option[M]): this.type = set(initialModel, model)

  /** @group setParam */
  def setMaxIter(iter: Int): this.type = set(maxIter, iter)

  setDefault(initialModel -> None, maxIter -> 1)
}



