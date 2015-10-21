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

import org.apache.spark.ml.param.{DoubleParam, IntParam, Params}

private[ml] trait HasStepControl extends Params {

  /**
   * The step control parameter for any iterative program. Note that the `step` is used to be
   * modified in that program, in order to keep record of the iterated steps.
   *
   * @group param
   */
  final val step: IntParam = new IntParam(this, "step", "current step in partial training")
  setDefault(step -> 0)

  /** @group getParam */
  final def getStep: Int = $(step)
}

private[ml] trait HasDownSamplingFactor extends Params {

  /**
   * Downsampling factor for datasets.
   *
   * @group param
   */
  final val downSamplingFactor: DoubleParam = new DoubleParam(this, "downSamplingFactor",
    "down sampling factor")
  setDefault(downSamplingFactor -> 1)

  /** @group getParam */
  final def getDownSamplingFactor: Double = $(downSamplingFactor)
}

private[ml] trait HasStepsPerPulling extends Params {
  /**
   * Step control for one pulling of an arm.
   *
   * @group param
   */
  final val stepsPerPulling: IntParam = new IntParam(this, "stepsPerPulling",
    "the count of iterative steps in one pulling")
  setDefault(stepsPerPulling -> 1)

  /** @group getParam */
  final def getStepsPerPulling: Int = $(stepsPerPulling)
}
