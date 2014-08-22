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

package org.apache.spark.network.netty

import org.apache.spark.SparkConf

/**
 * A central location that tracks all the settings we exposed to users.
 */
private[spark]
class NettyConfig(conf: SparkConf) {

  /** Port the server listens on. Default to a random port. */
  private[netty] val serverPort = conf.getInt("spark.shuffle.io.port", 0)

  /** IO mode: nio, oio, epoll, or auto (try epoll first and then nio). */
  private[netty] val ioMode = conf.get("spark.shuffle.io.mode", "nio").toLowerCase

  /** Connect timeout in secs. Default 60 secs. */
  private[netty] val connectTimeoutMs = conf.getInt("spark.shuffle.io.connectionTimeout", 60) * 1000

  /**
   * Percentage of the desired amount of time spent for I/O in the child event loops.
   * Only applicable in nio and epoll.
   */
  private[netty] val ioRatio = conf.getInt("spark.shuffle.io.netty.ioRatio", 80)

  /** Requested maximum length of the queue of incoming connections. */
  private[netty] val backLog: Option[Int] = conf.getOption("spark.shuffle.io.backLog").map(_.toInt)

  /**
   * Receive buffer size (SO_RCVBUF).
   * Note: the optimal size for receive buffer and send buffer should be
   *  latency * network_bandwidth.
   * Assuming latency = 1ms, network_bandwidth = 10Gbps
   *  buffer size should be ~ 1.25MB
   */
  private[netty] val receiveBuf: Option[Int] =
    conf.getOption("spark.shuffle.io.sendBuffer").map(_.toInt)

  /** Send buffer size (SO_SNDBUF). */
  private[netty] val sendBuf: Option[Int] =
    conf.getOption("spark.shuffle.io.sendBuffer").map(_.toInt)
}
