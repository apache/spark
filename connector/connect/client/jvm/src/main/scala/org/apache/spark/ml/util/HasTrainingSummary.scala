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

package org.apache.spark.ml.util

import org.apache.spark.annotation.Since
import org.apache.spark.ml.Model

/**
 * Trait for models that provides Training summary.
 *
 * @tparam T Summary instance type
 */
@Since("3.0.0")
private[ml] trait HasTrainingSummary[T, M <: Model[M]] extends Model[M] {

  /** Indicates whether a training summary exists for this model instance. */
  @Since("3.0.0")
  def hasSummary: Boolean = getModelAttr("hasSummary").asInstanceOf[Boolean]

  /**
   * Gets summary of model on training set. An exception is
   * thrown if if `hasSummary` is false.
   */
  @Since("3.0.0")
  def summary: T
}
