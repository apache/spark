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

package org.apache.spark.ml.param.shared

import org.apache.spark.ml.param.{IntParam, Params, ParamValidators}

/**
 * Trait to define a level of parallelism for algorithms that are able to use
 * multithreaded execution, and provide a thread-pool based execution context.
 */
private[ml] trait HasParallelism extends Params {
  // TODO: Move HasParallelism into sharedParames

  /**
   * The number of threads to use when running parallel algorithms.
   * Default is 1 for serial execution
   *
   * @group expertParam
   */
  val parallelism = new IntParam(this, "parallelism",
    "the number of threads to use when running parallel algorithms", ParamValidators.gtEq(1))

  setDefault(parallelism -> 1)

  /** @group expertGetParam */
  def getParallelism: Int = $(parallelism)
}
