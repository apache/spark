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

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.param.ParamMap

/**
 * :: AlphaComponent ::
 * A fitted model, i.e., a [[Transformer]] produced by an [[Estimator]].
 *
 * @tparam M model type
 */
@AlphaComponent
abstract class Model[M <: Model[M]] extends Transformer {
  /**
   * The parent estimator that produced this model.
   * Note: For ensembles' component Models, this value can be null.
   */
  val parent: Estimator[M]

  override def copy(extra: ParamMap): M = {
    // The default implementation of Params.copy doesn't work for models.
    throw new NotImplementedError(s"${this.getClass} doesn't implement copy(extra: ParamMap)")
  }
}
