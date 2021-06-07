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

import org.apache.spark.annotation.{Evolving, Since}

/**
 * A task resource request. This is used in conjunction with the [[ResourceProfile]] to
 * programmatically specify the resources needed for an RDD that will be applied at the
 * stage level.
 *
 * Use [[TaskResourceRequests]] class as a convenience API.
 *
 * @param resourceName Resource name
 * @param amount Amount requesting as a Double to support fractional resource requests.
 *               Valid values are less than or equal to 0.5 or whole numbers. This essentially
 *               lets you configure X number of tasks to run on a single resource,
 *               ie amount equals 0.5 translates into 2 tasks per resource address.
 */
@Evolving
@Since("3.1.0")
class TaskResourceRequest(val resourceName: String, val amount: Double)
  extends Serializable {

  assert(amount <= 0.5 || amount % 1 == 0,
    s"The resource amount ${amount} must be either <= 0.5, or a whole number.")

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
