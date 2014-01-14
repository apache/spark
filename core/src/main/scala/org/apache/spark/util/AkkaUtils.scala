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

package org.apache.spark.util

import scala.collection.JavaConversions.mapAsJavaMap
import scala.concurrent.duration.{Duration, FiniteDuration}

import akka.actor.{ActorSystem, ExtendedActorSystem, IndestructibleActorSystem}
import com.typesafe.config.ConfigFactory

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf

/**
 * Various utility classes for working with Akka.
 */
private[spark] object AkkaUtils {

  /**
   * Creates an ActorSystem ready for remoting, with various Spark features. Returns both the
   * ActorSystem itself and its port (which is hard to get from Akka).
   *
   * Note: the `name` parameter is important, as even if a client sends a message to right
   * host + port, if the system name is incorrect, Akka will drop the message.
   *
   * If indestructible is set to true, the Actor System will continue running in the event
   * of a fatal exception. This is used by [[org.apache.spark.executor.Executor]].
   */
  def createActorSystem(name: String, host: String, port: Int, indestructible: Boolean = false,
    conf: SparkConf): (ActorSystem, Int) = {

    val akkaThreads   = conf.getInt("spark.akka.threads", 4)
    val akkaBatchSize = conf.getInt("spark.akka.batchSize", 15)

    val akkaTimeout = conf.getInt("spark.akka.timeout", 100)

    val akkaFrameSize = conf.getInt("spark.akka.frameSize", 10)
    val akkaLogLifecycleEvents = conf.getBoolean("spark.akka.logLifecycleEvents", false)
    val lifecycleEvents = if (akkaLogLifecycleEvents) "on" else "off"
    if (!akkaLogLifecycleEvents) {
      // As a workaround for Akka issue #3787, we coerce the "EndpointWriter" log to be silent.
      // See: https://www.assembla.com/spaces/akka/tickets/3787#/
      Option(Logger.getLogger("akka.remote.EndpointWriter")).map(l => l.setLevel(Level.FATAL))
    }

    val logAkkaConfig = if (conf.getBoolean("spark.akka.logAkkaConfig", false)) "on" else "off"

    val akkaHeartBeatPauses = conf.getInt("spark.akka.heartbeat.pauses", 600)
    val akkaFailureDetector =
      conf.getDouble("spark.akka.failure-detector.threshold", 300.0)
    val akkaHeartBeatInterval = conf.getInt("spark.akka.heartbeat.interval", 1000)

    val akkaConf = ConfigFactory.parseMap(conf.getAkkaConf.toMap[String, String]).withFallback(
      ConfigFactory.parseString(
      s"""
      |akka.daemonic = on
      |akka.loggers = [""akka.event.slf4j.Slf4jLogger""]
      |akka.stdout-loglevel = "ERROR"
      |akka.jvm-exit-on-fatal-error = off
      |akka.remote.transport-failure-detector.heartbeat-interval = $akkaHeartBeatInterval s
      |akka.remote.transport-failure-detector.acceptable-heartbeat-pause = $akkaHeartBeatPauses s
      |akka.remote.transport-failure-detector.threshold = $akkaFailureDetector
      |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      |akka.remote.netty.tcp.transport-class = "akka.remote.transport.netty.NettyTransport"
      |akka.remote.netty.tcp.hostname = "$host"
      |akka.remote.netty.tcp.port = $port
      |akka.remote.netty.tcp.tcp-nodelay = on
      |akka.remote.netty.tcp.connection-timeout = $akkaTimeout s
      |akka.remote.netty.tcp.maximum-frame-size = ${akkaFrameSize}MiB
      |akka.remote.netty.tcp.execution-pool-size = $akkaThreads
      |akka.actor.default-dispatcher.throughput = $akkaBatchSize
      |akka.log-config-on-start = $logAkkaConfig
      |akka.remote.log-remote-lifecycle-events = $lifecycleEvents
      |akka.log-dead-letters = $lifecycleEvents
      |akka.log-dead-letters-during-shutdown = $lifecycleEvents
      """.stripMargin))

    val actorSystem = if (indestructible) {
      IndestructibleActorSystem(name, akkaConf)
    } else {
      ActorSystem(name, akkaConf)
    }

    val provider = actorSystem.asInstanceOf[ExtendedActorSystem].provider
    val boundPort = provider.getDefaultAddress.port.get
    (actorSystem, boundPort)
  }

  /** Returns the default Spark timeout to use for Akka ask operations. */
  def askTimeout(conf: SparkConf): FiniteDuration = {
    Duration.create(conf.getLong("spark.akka.askTimeout", 30), "seconds")
  }

  /** Returns the default Spark timeout to use for Akka remote actor lookup. */
  def lookupTimeout(conf: SparkConf): FiniteDuration = {
    Duration.create(conf.get("spark.akka.lookupTimeout", "30").toLong, "seconds")
  }
}
