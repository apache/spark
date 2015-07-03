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

/**
 * (private[ml]) Trait for support HasThreshold & HashThresholds in the
 * same class.
 */
private[ml] trait HasThresholdInfo extends HasThreshold with HasThresholds {
  /** @group getParam
   * Return the threshold info if it is set (or any of the defaults are set) */
  def getThresholdInfo: Option[Array[Double]] = {
    val thresholds = getThresholds
    val threshold = getThreshold
    if (thresholds != null) {
      Some(thresholds)
    } else if (threshold != null) {
      Some(Array())
    } else {
      None
    }
  }
}
