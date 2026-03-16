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

package org.apache.spark.ml

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.util.SizeEstimator

/**
 * A fitted model, i.e., a [[Transformer]] produced by an [[Estimator]].
 *
 * @tparam M model type
 */
abstract class Model[M <: Model[M]] extends Transformer { self =>
  /**
   * The parent estimator that produced this model.
   * @note For ensembles' component Models, this value can be null.
   */
  @transient var parent: Estimator[M] = _

  /**
   * Sets the parent of this model (Java API).
   */
  def setParent(parent: Estimator[M]): M = {
    this.parent = parent
    this.asInstanceOf[M]
  }

  /** Indicates whether this [[Model]] has a corresponding parent. */
  def hasParent: Boolean = parent != null

  override def copy(extra: ParamMap): M

  /**
   * For ml connect only.
   * Estimate the size of this model in bytes.
   * This is an approximation, the real size might be different.
   * 1, Both driver side memory usage and distributed objects size (like DataFrame,
   * RDD, Graph, Summary) are counted.
   * 2, Lazy vals are not counted, e.g., an auxiliary object used in prediction.
   * 3, The default implementation uses `org.apache.spark.util.SizeEstimator.estimate`,
   *    some models override the default implementation to achieve more precise estimation.
   * 4, For 3-rd extension, if external languages are used, it is recommended to override
   * this method and return a proper size.
   */
  private[spark] def estimatedSize: Long = SizeEstimator.estimate(self)
}
