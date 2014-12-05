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

import scala.beans.BeanInfo

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.mllib.linalg.Vector

/**
 * :: AlphaComponent ::
 * Class that represents an instance (data point) for prediction tasks.
 *
 * @param label Label to predict
 * @param features List of features describing this instance
 * @param weight Instance weight
 */
@AlphaComponent
@BeanInfo
case class LabeledPoint(label: Double, features: Vector, weight: Double) {

  override def toString: String = {
    "(%s,%s,%s)".format(label, features, weight)
  }
}

/**
 * :: AlphaComponent ::
 */
@AlphaComponent
object LabeledPoint {
  /** Constructor which sets instance weight to 1.0 */
  def apply(label: Double, features: Vector) = new LabeledPoint(label, features, 1.0)
}
