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

package org.apache.spark.partial

/**
 * A Double value with error bars and associated confidence.
 */
class BoundedDouble(val mean: Double, val confidence: Double, val low: Double, val high: Double) {

  override def toString(): String = "[%.3f, %.3f]".format(low, high)

  override def hashCode: Int =
    this.mean.hashCode ^ this.confidence.hashCode ^ this.low.hashCode ^ this.high.hashCode

  /**
   * @note Consistent with Double, any NaN value will make equality false
   */
  override def equals(that: Any): Boolean =
    that match {
      case that: BoundedDouble =>
        this.mean == that.mean &&
        this.confidence == that.confidence &&
        this.low == that.low &&
        this.high == that.high
      case _ => false
    }
}
