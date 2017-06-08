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
 */
@DeveloperApi
class ExecutorMetrics extends Serializable {

  private var _hostname: String = ""
  def hostname: String = _hostname
  private[spark] def setHostname(value: String) = _hostname = value

  private var _port: Option[Int] = None
  def port: Option[Int] = _port
  private[spark] def setPort(value: Option[Int]) = _port = value

  private[spark] def hostPort: String = {
    port match {
      case None => hostname
      case value => hostname + ":" + value.get
    }
  }

  private var _transportMetrics: TransportMetrics =
    new TransportMetrics(System.currentTimeMillis(), 0L, 0L)
  def transportMetrics: TransportMetrics = _transportMetrics
  private[spark] def setTransportMetrics(value: TransportMetrics) = {
    _transportMetrics = value
  }
}

object ExecutorMetrics extends Serializable {
  def apply(
      hostName: String,
      port: Option[Int],
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
 */
@DeveloperApi
case class TransportMetrics (
    val timeStamp: Long,
    val onHeapSize: Long,
    val offHeapSize: Long) extends Serializable

