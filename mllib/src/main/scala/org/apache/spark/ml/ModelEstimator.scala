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

abstract class ModelEstimator[T <: ModelEstimator[T] with Model[T]]
  extends Estimator[T] with Model[T] {

  final override def parent: Estimator[T] = this

  def newInstance(uid: String): T = {
    // This reflection trick is ugly, we should force users to reimplement this method.
    this.getClass.getConstructor(classOf[String]).newInstance(uid).asInstanceOf[T]
  }

  // Because of the current way of testing the signature, this does not work properly.
  override def copy(extra: ParamMap): T = {
    val that = newInstance(uid)
    copyValues(that, extra).setParent(this)
  }

}
