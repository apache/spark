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

package org.apache.spark.ps

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.HashMap

/**
 * PSServerManager
 */
private[spark] class PSServerManager {

  val nextServerId = new AtomicLong(0)

  val containerId2Server = new HashMap[String, ServerInfo]()
  val executorId2Server = new HashMap[String, ServerInfo]()
  val executorId2ServerId = new HashMap[String, Long]()

  def addPSServer(
      executorId: String,
      hostPort: String,
      containerId: String,
      serverInfo: ServerInfo) {
    containerId2Server(containerId) = serverInfo
    executorId2Server(executorId) = serverInfo
    executorId2ServerId(executorId) = serverInfo.serverId
  }

  def newServerId(): Long = nextServerId.getAndIncrement

  def getAllServers: Iterator[(String, ServerInfo)] = executorId2Server.toArray.toIterator

}
