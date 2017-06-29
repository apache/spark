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

package org.apache.spark.ml.feature

import scala.beans.BeanInfo

import org.apache.spark.annotation.Since
import org.apache.spark.ml.linalg.Vector

/**
 *
 * Class that represents the features and label of a data point.
 *
 * @param label Label for this data point.
 * @param features List of features for this data point.
 */
@Since("2.0.0")
@BeanInfo
case class LabeledPoint(@Since("2.0.0") label: Double, @Since("2.0.0") features: Vector) {
  override def toString: String = {
    s"($label,$features)"
  }
}
