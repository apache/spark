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

import java.util.Locale
import java.util.concurrent.TimeUnit

import io.netty.channel.{Channel, EventLoopGroup, MultiThreadIoEventLoopGroup, ServerChannel}

/**
 * Selects the Netty native transport that supports Unix domain sockets on the
 * current OS.
 *
 * gRPC's `NettyChannelBuilder` / `NettyServerBuilder` can talk over UDS only when
 * paired with a native transport that exposes a `DomainSocketChannel`:
 * `epoll` on Linux and `kqueue` on macOS. NIO does not support UDS in a way
 * gRPC's Netty transport can use.
 *
 * Only `epoll` and `kqueue` are wired up here. Netty also ships an `io_uring`
 * transport (`io.netty.channel.uring`) that supports UDS on recent Linux
 * kernels; it could be added as a preferred Linux option in [[detect]] once its
 * native artifact is on the classpath, but is intentionally left out for now to
 * keep the dependency set minimal.
 *
 * Callers obtain a [[UnixDomainSocketTransport]] via [[UnixDomainSocketTransport.detect]],
 * then ask it for matching `channelType` / `serverChannelType` / `EventLoopGroup`
 * objects to configure the builder.
 */
private[grpc] sealed trait UnixDomainSocketTransport {
  def channelType: Class[_ <: Channel]
  def serverChannelType: Class[_ <: ServerChannel]
  def newEventLoopGroup(): EventLoopGroup
  def name: String
}

private[grpc] object UnixDomainSocketTransport {

  /** Linux epoll. */
  private case object EpollTransport extends UnixDomainSocketTransport {
    override val channelType: Class[_ <: Channel] =
      classOf[io.netty.channel.epoll.EpollDomainSocketChannel]
    override val serverChannelType: Class[_ <: ServerChannel] =
      classOf[io.netty.channel.epoll.EpollServerDomainSocketChannel]
    override def newEventLoopGroup(): EventLoopGroup =
      new MultiThreadIoEventLoopGroup(io.netty.channel.epoll.EpollIoHandler.newFactory())
    override val name: String = "epoll"
  }

  /** macOS/BSD kqueue. */
  private case object KQueueTransport extends UnixDomainSocketTransport {
    override val channelType: Class[_ <: Channel] =
      classOf[io.netty.channel.kqueue.KQueueDomainSocketChannel]
    override val serverChannelType: Class[_ <: ServerChannel] =
      classOf[io.netty.channel.kqueue.KQueueServerDomainSocketChannel]
    override def newEventLoopGroup(): EventLoopGroup =
      new MultiThreadIoEventLoopGroup(io.netty.channel.kqueue.KQueueIoHandler.newFactory())
    override val name: String = "kqueue"
  }

  // NOTE: io.netty.channel.epoll.Epoll.isAvailable triggers a one-shot
  // native library load attempt the first time it is called. On platforms
  // that don't ship epoll (e.g. macOS) the load fails silently inside
  // Netty and is cached. The OS-aware paths in `detect()` below short-
  // circuit on os.name first so we don't pay this on the dominant happy
  // path -- only the fallback paths (wrong os.name string, container with
  // mismatched native libs) reach this side-effect.
  private def epollAvailable: Boolean = io.netty.channel.epoll.Epoll.isAvailable
  private def epollUnavailable: Throwable =
    io.netty.channel.epoll.Epoll.unavailabilityCause()
  private def kqueueAvailable: Boolean = io.netty.channel.kqueue.KQueue.isAvailable
  private def kqueueUnavailable: Throwable =
    io.netty.channel.kqueue.KQueue.unavailabilityCause()

  private def isLinux(os: String): Boolean = os.contains("linux")
  private def isMacOrBsd(os: String): Boolean =
    os.contains("mac") || os.contains("darwin") || os.contains("bsd")

  /**
   * Returns the transport for the current OS, or throws
   * [[UnsupportedOperationException]] if neither epoll nor kqueue is
   * available.
   *
   * Resolution order:
   *  1. Linux + epoll available (the dominant happy path).
   *  2. macOS/BSD + kqueue available.
   *  3. Either native transport available regardless of `os.name`
   *     (covers misreported os.name, containers that ship the wrong
   *     native libs, etc.). The `&&` short-circuits in steps 1 and 2
   *     mean we only trigger the "wrong-OS" native load attempt in
   *     this fallback branch.
   */
  def detect(): UnixDomainSocketTransport = {
    val os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT)
    if (isLinux(os) && epollAvailable) {
      EpollTransport
    } else if (isMacOrBsd(os) && kqueueAvailable) {
      KQueueTransport
    } else if (epollAvailable) {
      EpollTransport
    } else if (kqueueAvailable) {
      KQueueTransport
    } else {
      // No socket-based UDS fallback exists: gRPC's Netty transport can only
      // speak UDS through a native epoll/kqueue DomainSocketChannel. (Java NIO
      // exposes UDS via java.nio.channels.SocketChannel, but Netty/gRPC cannot
      // use it as the channel type for a managed channel.) When neither native
      // transport is present the only alternative is a different transport
      // entirely -- e.g. TCP loopback -- which is a deployment choice, not a
      // fallback we silently substitute here, so we fail loudly instead.
      val epollErr = Option(epollUnavailable).map(_.getMessage).getOrElse("unknown")
      val kqueueErr = Option(kqueueUnavailable).map(_.getMessage).getOrElse("unknown")
      throw new UnsupportedOperationException(
        s"No Netty native UDS transport available on os=$os " +
          s"(epoll: $epollErr, kqueue: $kqueueErr). " +
          "UDS-backed gRPC requires netty-transport-native-epoll on Linux or " +
          "netty-transport-native-kqueue on macOS.")
    }
  }

  /**
   * Shut an event loop group down. Best-effort: preserves the interrupt
   * flag on `InterruptedException` (does not propagate the exception) so
   * callers in a cleanup path are not forced to handle it.
   */
  def shutdown(elg: EventLoopGroup, timeoutMs: Long): Unit = {
    if (elg == null || elg.isShuttingDown) return
    val future = elg.shutdownGracefully(0L, timeoutMs, TimeUnit.MILLISECONDS)
    try {
      future.await(timeoutMs, TimeUnit.MILLISECONDS)
    } catch {
      case _: InterruptedException => Thread.currentThread().interrupt()
    }
  }
}
