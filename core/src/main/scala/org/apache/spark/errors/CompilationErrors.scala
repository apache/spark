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
package org.apache.spark.errors

import java.io.File

import org.apache.spark.SparkException
import org.apache.spark.resource.ResourceID

private [spark] object CompilationErrors {
  def acquireAnAddressNotExist(resourceName: String, address: String): Throwable = {
    new SparkException(s"Try to acquire an address that doesn't exist. $resourceName " +
      s"address $address doesn't exist.")
  }

  def acquireAnAddressNotAvailable(resourceName: String, address: String): Throwable = {
    new SparkException("Try to acquire an address that is not available. " +
      s"$resourceName address $address is not available.")
  }

  def releaseAnAddressNotExist(resourceName: String, address: String): Throwable = {
    new SparkException(s"Try to release an address that doesn't exist. $resourceName " +
      s"address $address doesn't exist.")
  }

  def releaseAnAddressNotAssigned(resourceName: String, address: String): Throwable = {
    new SparkException(s"Try to release an address that is not assigned. $resourceName " +
      s"address $address is not assigned.")
  }

  def specifyAmountForResource(resourceId: ResourceID): Throwable = {
    new SparkException(s"You must specify an amount for ${resourceId.resourceName}")
  }

  def specifyAmountConfigForResource(
                                      key: String,
                                      componentName: String,
                                      RESOURCE_PREFIX: String): Throwable = {
    new SparkException(s"You must specify an amount config for resource: $key " +
      s"config: $componentName.$RESOURCE_PREFIX.$key")
  }
  def conditionOfResourceAmount(doubleAmount: Double) : Throwable = {
    new SparkException(s"The resource amount ${doubleAmount} must be either <= 0.5," +
      s" or a whole number.")
  }

  def onlySupportFractionalResource(componentName: String): Throwable = {
    new SparkException(s"Only tasks support fractional resources," +
      s" please check your $componentName settings")
  }

}
