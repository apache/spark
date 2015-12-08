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

package org.apache.spark.executor

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Metrics tracked during the execution of an executor.
 *
 * So, when adding new fields, take into consideration that the whole object can be serialized for
 * shipping off at any time to consumers of the SparkListener interface.
 */
@DeveloperApi
class ExecutorMetrics extends Serializable {

  /**
   * Host's name the executor runs on
   */
  private var _hostname: String = ""
  def hostname: String = _hostname
  private[spark] def setHostname(value: String) = _hostname = value

  /**
   * Host's port the executor runs on
   */
  private var _port: Int = _
  def port: Int = _port
  private[spark] def setPort(value: Int) = _port = value

  private[spark] def hostPort: String = hostname + ":" + port

  private var _transportMetrics: TransportMetrics = new TransportMetrics
  def transportMetrics: TransportMetrics = _transportMetrics
  private[spark] def setTransportMetrics(value: TransportMetrics) = {
    _transportMetrics = value
  }

  // for test only
  def metricsDetails: (String, Long, Long, Long) = {
    (hostname, transportMetrics.timeStamp, transportMetrics.onHeapSize,
      transportMetrics.offHeapSize)
  }
}

object ExecutorMetrics extends Serializable {
  def apply(
      hostName: String,
      port: Int,
      transportMetrics: TransportMetrics): ExecutorMetrics = {
    val execMetrics = new ExecutorMetrics
    execMetrics.setHostname(hostName)
    execMetrics.setPort(port)
    execMetrics.setTransportMetrics(transportMetrics)
    execMetrics
  }
}

/**
 * :: DeveloperApi ::
 * Metrics for network layer
 */
@DeveloperApi
class TransportMetrics (
    val timeStamp: Long = System.currentTimeMillis,
    val onHeapSize: Long = 0L,
    val offHeapSize: Long = 0L) extends Serializable

object TransportMetrics extends Serializable {
  def apply(
      timeStamp: Long,
      onHeapSize: Long,
      offHeapSize: Long): TransportMetrics = {
    new TransportMetrics(timeStamp, onHeapSize, offHeapSize)
  }
}
