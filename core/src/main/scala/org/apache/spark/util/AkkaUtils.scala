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

import akka.actor.{ActorSystem, ExtendedActorSystem}
import com.typesafe.config.ConfigFactory
import akka.util.duration._
import akka.remote.RemoteActorRefProvider


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
   */
  def createActorSystem(name: String, host: String, port: Int): (ActorSystem, Int) = {
    val akkaThreads = System.getProperty("spark.akka.threads", "4").toInt
    val akkaBatchSize = System.getProperty("spark.akka.batchSize", "15").toInt
    val akkaTimeout = System.getProperty("spark.akka.timeout", "60").toInt
    val akkaFrameSize = System.getProperty("spark.akka.frameSize", "10").toInt
    val lifecycleEvents = if (System.getProperty("spark.akka.logLifecycleEvents", "false").toBoolean) "on" else "off"
    // 10 seconds is the default akka timeout, but in a cluster, we need higher by default.
    val akkaWriteTimeout = System.getProperty("spark.akka.writeTimeout", "30").toInt
    
    val akkaConf = ConfigFactory.parseString("""
      akka.daemonic = on
      akka.event-handlers = ["akka.event.slf4j.Slf4jEventHandler"]
      akka.stdout-loglevel = "ERROR"
      akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      akka.remote.transport = "akka.remote.netty.NettyRemoteTransport"
      akka.remote.netty.hostname = "%s"
      akka.remote.netty.port = %d
      akka.remote.netty.connection-timeout = %ds
      akka.remote.netty.message-frame-size = %d MiB
      akka.remote.netty.execution-pool-size = %d
      akka.actor.default-dispatcher.throughput = %d
      akka.remote.log-remote-lifecycle-events = %s
      akka.remote.netty.write-timeout = %ds
      """.format(host, port, akkaTimeout, akkaFrameSize, akkaThreads, akkaBatchSize,
        lifecycleEvents, akkaWriteTimeout))

    val actorSystem = ActorSystem(name, akkaConf)

    // Figure out the port number we bound to, in case port was passed as 0. This is a bit of a
    // hack because Akka doesn't let you figure out the port through the public API yet.
    val provider = actorSystem.asInstanceOf[ExtendedActorSystem].provider
    val boundPort = provider.asInstanceOf[RemoteActorRefProvider].transport.address.port.get
    return (actorSystem, boundPort)
  }
}
