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
package org.apache.spark.udf.worker.grpc

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import io.grpc.{ConnectivityState, ManagedChannel}
import io.grpc.netty.NettyChannelBuilder
import io.netty.channel.EventLoopGroup
import io.netty.channel.unix.DomainSocketAddress

import org.apache.spark.annotation.Experimental
import org.apache.spark.udf.worker.core.{WorkerConnection, WorkerLogger}

/**
 * :: Experimental ::
 * A [[WorkerConnection]] backed by a gRPC `ManagedChannel` over a Netty
 * Unix domain socket transport.
 *
 * Owns:
 *  - the [[ManagedChannel]] used by sessions to open Execute streams,
 *  - a dedicated Netty [[EventLoopGroup]] for that channel (one per worker so
 *    a slow worker cannot starve others sharing a global loop).
 *
 * Does NOT own the socket file at `socketPath`: the worker creates it and the
 * dispatcher removes it (`cleanupEndpointAddress` / `closeTransport`). Keeping
 * a single creation/deletion site avoids double-cleanup and ambiguous
 * ownership.
 *
 * Lifecycle:
 *  - [[isActive]] is true while the channel reports any non-`SHUTDOWN` state.
 *    Transient failures keep the connection usable -- gRPC will reconnect on
 *    the next RPC.
 *  - [[close]] shuts the channel down and drains the event loop. Idempotent.
 *
 * Construction does NOT block on the worker actually being reachable; the
 * dispatcher proves liveness separately via its
 * `waitForReady`/init-timeout machinery before handing the connection to a
 * session.
 *
 * @param socketPath path of the UDS the worker server binds.
 * @param logger logger for shutdown diagnostics; defaults to NoOp.
 * @param shutdownPhaseTimeoutMs upper bound on each individual phase of
 *                               close() -- graceful channel drain, forced
 *                               channel drain, and event-loop drain are
 *                               three separate phases that each get this
 *                               budget. Total worst-case close time is up
 *                               to three times this value. Defaults to 5s
 *                               per phase.
 */
@Experimental
class GrpcWorkerChannel(
    val socketPath: String,
    logger: WorkerLogger = WorkerLogger.NoOp,
    shutdownPhaseTimeoutMs: Long = GrpcWorkerChannel.DEFAULT_SHUTDOWN_PHASE_TIMEOUT_MS)
  extends WorkerConnection {

  private val transport: UnixDomainSocketTransport = UnixDomainSocketTransport.detect()
  private val eventLoopGroup: EventLoopGroup = transport.newEventLoopGroup()

  /**
   * Underlying gRPC channel. Sessions open one bidirectional Execute stream
   * per session on this channel.
   */
  val channel: ManagedChannel = try {
    NettyChannelBuilder
      .forAddress(new DomainSocketAddress(socketPath))
      .channelType(transport.channelType)
      .eventLoopGroup(eventLoopGroup)
      .usePlaintext()
      .build()
  } catch {
    case NonFatal(e) =>
      UnixDomainSocketTransport.shutdown(eventLoopGroup, shutdownPhaseTimeoutMs)
      throw e
  }

  private val closed = new AtomicBoolean(false)

  override def isActive: Boolean = {
    if (closed.get()) return false
    channel.getState(false) != ConnectivityState.SHUTDOWN
  }

  /**
   * Idempotently tears down the channel and event loop. Steps are guarded so
   * a failure in one does not skip the others. The socket file is owned and
   * removed by the dispatcher, not here.
   */
  override def close(): Unit = {
    if (!closed.compareAndSet(false, true)) return

    try {
      channel.shutdown()
      if (!channel.awaitTermination(shutdownPhaseTimeoutMs, TimeUnit.MILLISECONDS)) {
        channel.shutdownNow()
        channel.awaitTermination(shutdownPhaseTimeoutMs, TimeUnit.MILLISECONDS)
      }
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
      case NonFatal(e) =>
        logger.warn(s"Error shutting down gRPC channel for $socketPath", e)
    }

    try {
      UnixDomainSocketTransport.shutdown(eventLoopGroup, shutdownPhaseTimeoutMs)
    } catch {
      case NonFatal(e) =>
        logger.warn(s"Error shutting down event loop for $socketPath", e)
    }
  }
}

object GrpcWorkerChannel {
  /**
   * Default per-phase upper bound on close() drain time. Each of the three
   * drain phases (graceful channel, forced channel, event loop) is capped
   * by this value, so total worst-case close time is up to 3x.
   */
  val DEFAULT_SHUTDOWN_PHASE_TIMEOUT_MS: Long = 5000L
}
