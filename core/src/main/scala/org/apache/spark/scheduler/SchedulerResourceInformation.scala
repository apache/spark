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

package org.apache.spark.scheduler

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ResourceInformation.UNKNOWN
import org.apache.spark.annotation.Evolving

/**
 * Class to hold information about a type of Resource used by the scheduler. This is a separate
 * class from the ResourceInformation because here its mutable because the scheduler has to update
 * the addresses based on what its assigned and what is available.
 */
@Evolving
private[spark] class SchedulerResourceInformation(
    private val name: String,
    private val availableAddresses: ArrayBuffer[String] = ArrayBuffer.empty) extends Serializable {

  private val allocatedAddresses: ArrayBuffer[String] = ArrayBuffer.empty

  def getName(): String = name

  def getAvailableAddresses(): ArrayBuffer[String] = availableAddresses

  def acquireAddresses(num: Int): Seq[String] = {
    assert(num <= availableAddresses.size, s"Required to take $num $name addresses but only " +
      s"${availableAddresses.size} available.")
    val addrs = availableAddresses.take(num)
    allocatedAddresses ++= addrs
    addrs
  }

  def releaseAddresses(addrs: Array[String]): Unit = {
    addrs.foreach { address =>
      assert(allocatedAddresses.contains(address), s"Try to release $name address $address, but " +
        "it is not allocated.")
      availableAddresses += address
      allocatedAddresses -= address
    }
  }
}

private[spark] object SchedulerResourceInformation {
  def empty: SchedulerResourceInformation =
    new SchedulerResourceInformation(UNKNOWN, ArrayBuffer.empty[String])
}
