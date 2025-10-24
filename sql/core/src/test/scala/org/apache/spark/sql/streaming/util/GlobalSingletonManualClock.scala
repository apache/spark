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

package org.apache.spark.sql.streaming.util

import org.apache.spark.SparkContext.DRIVER_IDENTIFIER
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.util.RpcUtils

/**
 * A global manual clock that is backed by a singleton.
 * Should use if the whole query is running in one process
 */
class GlobalSingletonManualClock extends StreamManualClock with Serializable {
  override def getTimeMillis(): Long = {
    GlobalSingletonManualClock.currentTime
  }

  override def advance(timeToAdd: Long): Unit = {
    GlobalSingletonManualClock.currentTime = GlobalSingletonManualClock.currentTime + timeToAdd
  }
}

object GlobalSingletonManualClock {
  @volatile var currentTime: Long = 0

  def reset(): Unit = {
    GlobalSingletonManualClock.currentTime = 0
  }
}

/**
 * Creates a manual clock that can be synced across driver and workers in separate processes.
 * A clock server is started on the driver and workers will connect to that server to get the
 * current time.
 */
class GlobalManualClock(endpointName: String)
  extends StreamManualClock with Serializable with Logging {

  private var clockServer: Option[GlobalSyncClockServer] = None
  private var clockClient: Option[GlobalSyncClockClient] = None

  private def getClock(): StreamManualClock = {
    if (isDriver) {
      if (clockServer.isEmpty) {
        clockServer = Some(new GlobalSyncClockServer(endpointName))
        clockServer.get.setup()
      }
      clockServer.get
    } else {
      if (clockClient.isEmpty) {
        clockClient = Some(new GlobalSyncClockClient(endpointName))
      }
      clockClient.get
    }
  }

  private def isDriver: Boolean = {
    val executorId = SparkEnv.get.executorId
    // Check for null to match the behavior of executorId == DRIVER_IDENTIFIER
    executorId != null && executorId.startsWith(DRIVER_IDENTIFIER)
  }

  override def getTimeMillis(): Long = {
    getClock().getTimeMillis()
  }

  override def advance(timeToAdd: Long): Unit = {
    getClock().advance(timeToAdd)
  }
}

class GlobalSyncClockServer(endpointName: String)
  extends StreamManualClock with Serializable with Logging {
  @volatile var currentTime: Long = 0

  def setup(): Unit = {
    val endpoint = new GlobalClockEndpoint(this)
    val endpointRef = endpoint.rpcEnv.setupEndpoint(endpointName, endpoint)
  }


  override def getTimeMillis(): Long = {
    currentTime
  }

  override def advance(timeToAdd: Long): Unit = {
    currentTime += timeToAdd
  }
}

class GlobalSyncClockClient(driverEndpointName: String)
  extends StreamManualClock with Serializable with Logging {
  @volatile var currentTime: Long = 0

  private lazy val endpoint = RpcUtils.makeDriverRef(
    driverEndpointName,
    SparkEnv.get.conf,
    SparkEnv.get.rpcEnv)

  override def getTimeMillis(): Long = {
    val result = endpoint.askSync[Long]()
    result
  }

  override def advance(timeToAdd: Long): Unit = {
    // scalastyle:off throwerror
    throw new NotImplementedError()
    // scalastyle:on throwerror
  }
}

class GlobalClockEndpoint(clock: GlobalSyncClockServer)
  extends ThreadSafeRpcEndpoint with Logging with Serializable {

  override val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _ =>
      context.reply(clock.getTimeMillis())
  }
}
