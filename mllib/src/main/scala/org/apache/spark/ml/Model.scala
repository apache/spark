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
abstract class Model[M <: Model[M]] extends Transformer {
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
   * Estimate the upper-bound size of this model in bytes, This is an approximation,
   * the real size might be different.
   * 1, If there is no enough information to get an accurate size, try to estimate the
   * upper-bound size, e.g.
   *    - Given a LogisticRegression estimator, assume the coefficients are dense, even
   *      though the actual fitted model might be sparse (by L1 penalty).
   *    - Given a tree model, assume all underlying trees are complete binary trees, even
   *      though some branches might be pruned or truncated.
   * 2, Only driver side memory usage is counted, distributed objects (like DataFrame,
   * RDD, Graph, Training Summary) are ignored.
   * 3, Using SizeEstimator to estimate the driver memory usage of distributed objects
   * is not accurate, because the size of SparkSession/SparkContext is also included, e.g.
   *    val df = spark.range(1)
   *    SizeEstimator.estimate(df)                   -> 3310984
   *    SizeEstimator.estimate(df.rdd)               -> 3331352
   *    SizeEstimator.estimate(df.sparkSession)      -> 3249464
   *    SizeEstimator.estimate(df.rdd.sparkContext)  -> 3249744
   * 4, For 3-rd extension, if external languages are used, it is recommended to override
   * this method and return a proper size.
   */
  private[spark] def estimatedSize: Long = SizeEstimator.estimate(this)
}
