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

package org.apache.spark.sql.connect.client

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import com.google.protobuf.util.Timestamps

import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging

/**
 * SessionHeartbeat controls a thread in the client that can periodically send ExtendSession
 * requests to the server to keep the session alive. See SessionHeartbeat.Configuration for all
 * available options.
 */
private[client] class SessionHeartbeat(stubState: SparkConnectStubState) extends Logging {

  private val heartbeatConfig = stubState.configuration.extendSessionHeartbeat

  private val clientConfig = stubState.configuration

  private var expirationTime = System.currentTimeMillis()

  private var lastActivityTime = System.currentTimeMillis()

  private val bstub = proto.SparkConnectServiceGrpc.newBlockingStub(stubState.channel)

  private val activityLock = new Object()

  private val started = new AtomicBoolean(false)

  private val stopped = new AtomicBoolean(false)

  private val pingThread = new Thread() {
    override def run(): Unit = {
      while (!stubState.channel.isTerminated && !stopped.get()) {
        // If configured to stop after inactivity, wait until there is activity.
        if (heartbeatConfig.stopAfterInactivity.isDefined) {
          activityLock.synchronized {
            while (System.currentTimeMillis() - lastActivityTime <
                heartbeatConfig.stopAfterInactivity.get.toMillis) {
              activityLock.wait()
            }
          }
        }

        // Then wait until it's time to send the next ping.
        val now = System.currentTimeMillis()
        if (expirationTime < now) {
          // If already past expirationTime, assume it's 1 minute in the future.
          // We are either too late and will get an error anyway, or something else has gone wrong,
          // in which case we don't want to flood calling the server.
          expirationTime = now + heartbeatConfig.maximumInterval.toMillis
        }
        // Send ping at least every maximumInterval,
        // but if the expirationTime is approaching send it sooner.
        val nextPingTime =
          now + Math.min(heartbeatConfig.maximumInterval.toMillis, (expirationTime - now) / 2)

        Thread.sleep(Math.max(0, nextPingTime - System.currentTimeMillis()))

        if (!stubState.channel.isTerminated) {
          logDebug(s"Executing ExtendSession in session ${clientConfig.sessionId}")
          // Ignore any errors when sending ExtendSession pings.
          // Retries are handled by the outer loop and will become more frequent as the expiration
          // time approaches. If there is an unrecoverable error in the session's connection,
          // other foreground errors will be thrown, and the session will be closed, shutting down
          // the heartbeat thread then, don't shut down after an error on its own.
          try {
            val request = proto.ExtendSessionRequest
              .newBuilder()
              .setSessionId(clientConfig.sessionId)
              .setUserContext(stubState.userContext)
              .setClientType(clientConfig.userAgent)
              .build()
            val response = bstub.extendSession(request)
            expirationTime = Timestamps.toMillis(response.getExpirationTime)
            logDebug(
              s"ExtendSession response in session ${clientConfig.sessionId}, " +
                s"new expirationTime: $expirationTime")
          } catch {
            case NonFatal(e) =>
              logDebug(
                "Caught exception during ExtendSession " +
                  s"in session ${clientConfig.sessionId}",
                e)
          }
        }
      }
    }
  }

  /**
   * Register that there is an RPC activity.
   *
   * In case of lazy start, it will start the ping thread. In case it should stop after
   * inactivity, it refreshes activity time
   */
  def ping(): Unit = {
    if (heartbeatConfig.enabled) {
      if (started.getAndSet(true) == false) {
        pingThread.start()
      }
      if (heartbeatConfig.stopAfterInactivity.isDefined) {
        activityLock.synchronized {
          lastActivityTime = System.currentTimeMillis()
          activityLock.notifyAll()
        }
      }
    }
  }

  def shutdown(): Unit = {
    stopped.set(true)
    pingThread.interrupt()
    pingThread.join()
  }

  // If configuration says to not be lazy, start at constructor time.
  if (!heartbeatConfig.lazyStart) {
    ping()
  }
}

object SessionHeartbeat {

  /**
   * Client configuration of ExtendSession heartbeat.
   *
   * After receiving ExtendSession, server will extend the session by CONNECT_SESSION_EXTEND_TIME
   * server config (default: 10 minutes).
   *
   * If there is ever an ExtendSession heartbeat RPC sent by the client for the session, only
   * ExtendSession RPCs will extend the session lifetime moving forward.
   *
   * If there has not ever been any ExtendSession heartbeat RPC sent by the client for the
   * session, the server will extend the session lifetime after any RPC from the client, by
   * CONNECT_SESSION_MANAGER_DEFAULT_SESSION_TIMEOUT (default: 1 hour). This is the legacy
   * behavior of session management before ExtendSession was introduced.
   *
   * Heartbeat ExtendSession RPCs will be sent until the session is stopped with session.stop(),
   * and optionally paused if there are no other RPCs for pauseAfterInactivity period.
   *
   * @param enabled
   *   Whether session heartbeat is enabled. When disabled, server will default to keeping the
   *   session alive for CONNECT_SESSION_MANAGER_DEFAULT_SESSION_TIMEOUT after the last RPC. When
   *   enabled, only the ExtendSession RPCs send by the heartbeat extend the session lifetime.
   * @param lazyStart
   *   When true, only start sending heartbeat requests after the first RPC. When false, start
   *   sending heartbeat requests immediately when the client is created.
   * @param maximumInterval
   *   Maximum Interval with which ExtendSession requests will be sent to the server. RPCs will be
   *   send at the minimum of this interval, and half the time until the session expires.
   * @param stopAfterInactivity
   *   If set, the heartbeat will stop sending ExtendSession requests after there was no other
   *   activity (sending other requests) in the client for the given duration. The heartbeat will
   *   resume after any request is sent, but in the meanwhile, the session can expire.
   */
  case class Configuration(
      enabled: Boolean = true,
      lazyStart: Boolean = true,
      maximumInterval: FiniteDuration = FiniteDuration(2, "min"),
      stopAfterInactivity: Option[FiniteDuration] = Some(FiniteDuration(1, "hour")))
}
