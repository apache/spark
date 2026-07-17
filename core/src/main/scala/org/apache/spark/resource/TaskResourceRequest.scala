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

package org.apache.spark.resource

import org.apache.spark.annotation.{Since, Stable}

/**
 * A task resource request. This is used in conjunction with the [[ResourceProfile]] to
 * programmatically specify the resources needed for an RDD that will be applied at the
 * stage level.
 *
 * Use [[TaskResourceRequests]] class as a convenience API.
 *
 * @param resourceName Resource name
 * @param amount Amount requesting as a Double to support fractional resource requests.
 *               For custom resources, valid values are less than or equal to 1.0 or whole
 *               numbers, since a task's amount must map onto discrete resource addresses -
 *               ie amount equals 0.5 translates into 2 tasks per resource address. CPUs
 *               (resource name "cpus") are a plain quantity drawn from the executor's core
 *               pool rather than an addressable resource, so any amount of at least 1e-9 is
 *               valid, e.g. 1.5.
 */
@Stable
@Since("3.1.0")
class TaskResourceRequest(val resourceName: String, val amount: Double)
  extends Serializable {

  if (resourceName == ResourceProfile.CPUS) {
    // A positive amount below half of the internal accounting scale (1e-9) would silently
    // round to zero cpus downstream; reject it here where the original value is still visible.
    assert(!amount.isNaN && !amount.isInfinity &&
      CpuAmount.normalize(BigDecimal(amount.toString)).signum > 0,
      s"The cpus amount ${amount} must be at least 1e-9.")
  } else {
    assert(amount <= 1.0 || amount % 1 == 0,
      s"The resource amount ${amount} must be either <= 1.0, or a whole number.")
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: TaskResourceRequest =>
        that.getClass == this.getClass &&
          that.resourceName == resourceName && that.amount == amount
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Seq(resourceName, amount).hashCode()

  override def toString(): String = {
    s"name: $resourceName, amount: $amount"
  }
}
