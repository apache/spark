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
  private var _hostname: String = _
  def hostname: String = _hostname
  private[spark] def setHostname(value: String) = _hostname = value

  /**
   * Host's port the executor runs on
   */
  private var _port: Option[Int] = _
  def port: Option[Int] = _port
  private[spark] def setPort(value: Int) = _port = Some(value)

  def hostPort: String = hostname + ":" + port.getOrElse(0)

  private var _transportMetrics: Option[TransportMetrics] = None
  def transportMetrics: Option[TransportMetrics] = _transportMetrics
  private[spark] def setTransportMetrics(value: TransportMetrics) = {
    _transportMetrics = Some(value)
  }
}

/**
 * :: DeveloperApi ::
 * Metrics for network layer
 */
@DeveloperApi
case class TransportMetrics(
    timeStamp: Long,
    clientOnheapSize: Long,
    clientDirectheapSize: Long,
    serverOnheapSize: Long,
    serverDirectheapSize: Long)