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

package org.apache.spark.network.rpc

import java.nio.ByteBuffer
import org.apache.spark.SparkConf
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.server._
import org.apache.spark.network.util.JavaUtils
import org.slf4j.Logger


class SimpleRpcClient(conf: SparkConf) {
  private val transportConf = SparkTransportConf.fromSparkConf(conf, 1)
  val transportContext = new TransportContext(transportConf, new RpcHandler {
    override def getStreamManager: StreamManager = new OneForOneStreamManager

    override def receive(
        client: TransportClient, message: Array[Byte], callback: RpcResponseCallback): Unit = {
      println("gotten some message " + JavaUtils.bytesToString(ByteBuffer.wrap(message)))
      callback.onSuccess(new Array[Byte](0))
    }
  })
  val clientF = transportContext.createClientFactory()
  val client = clientF.createClient("localhost", 12345)

  def sendMessage(message: Any): Unit = {
    client.sendRpcSync(JavaUtils.serialize(message), 5000)
  }
}


abstract class SimpleRpcServer(conf: SparkConf) {

  protected def log: Logger

  private val transportConf = SparkTransportConf.fromSparkConf(conf, 1)

  val transportContext = new TransportContext(transportConf, new RpcHandler {
    override def getStreamManager: StreamManager = new OneForOneStreamManager

    override def receive(
        client: TransportClient, message: Array[Byte], callback: RpcResponseCallback): Unit = {
      callback.onSuccess(new Array[Byte](0))
      val received = JavaUtils.deserialize[Any](message)
      println("got mesage " + received)
      remote = client
      if (receiveWithLogging.isDefinedAt(received)) {
        receiveWithLogging.apply(received)
      }
    }
  })

  private[this] val clientFactory = transportContext.createClientFactory()
  private[this] var server: TransportServer = _

  startServer()
  private[this] val client = clientFactory.createClient("localhost", 12345)

  def startServer(): Unit = {
    server = transportContext.createServer(12345)
    log.info("RPC server created on " + server.getPort)
  }

  var remote: TransportClient = _

  def reply(message: Any): Unit = {
//    val c = clientFactory.createClient("localhost",
//      remote.channel.remoteAddress.asInstanceOf[InetSocketAddress].getPort)
//    c.sendRpc(JavaUtils.serialize(message), new RpcResponseCallback {
//      override def onSuccess(response: Array[Byte]): Unit = {}
//      override def onFailure(e: Throwable): Unit = {}
//    })
//    remote.sendRpc(JavaUtils.serialize(message), new RpcResponseCallback {
//      override def onFailure(e: Throwable): Unit = {}
//      override def onSuccess(response: Array[Byte]): Unit = {}
//    })
    remote.sendRpcSync(JavaUtils.serialize(message), 5000)
  }

  def sendMessage(message: Any): Unit = {
    client.sendRpcSync(JavaUtils.serialize(message), 5000)
  }

  def receiveWithLogging: PartialFunction[Any, Unit]
}
