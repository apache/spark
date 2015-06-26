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
package org.apache.spark.streaming.flume


import java.net.InetSocketAddress
import java.util.concurrent.{LinkedBlockingQueue, Executors}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.avro.ipc.NettyTransceiver
import org.apache.avro.ipc.specific.SpecificRequestor
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.flume.sink._

/**
 * A [[ReceiverInputDStream]] that can be used to read data from several Flume agents running
 * [[org.apache.spark.streaming.flume.sink.SparkSink]]s.
 * @param _ssc Streaming context that will execute this input stream
 * @param addresses List of addresses at which SparkSinks are listening
 * @param maxBatchSize Maximum size of a batch
 * @param parallelism Number of parallel connections to open
 * @param storageLevel The storage level to use.
 * @tparam T Class type of the object of this stream
 */
private[streaming] class FlumePollingInputDStream[T: ClassTag](
    @transient _ssc: StreamingContext,
    val addresses: Seq[InetSocketAddress],
    val maxBatchSize: Int,
    val parallelism: Int,
    storageLevel: StorageLevel
  ) extends ReceiverInputDStream[SparkFlumeEvent](_ssc) {

  override def getReceiver(): Receiver[SparkFlumeEvent] = {
    new FlumePollingReceiver(addresses, maxBatchSize, parallelism, storageLevel)
  }
}

private[streaming] class FlumePollingReceiver(
    addresses: Seq[InetSocketAddress],
    maxBatchSize: Int,
    parallelism: Int,
    storageLevel: StorageLevel
  ) extends Receiver[SparkFlumeEvent](storageLevel) with Logging {

  lazy val channelFactoryExecutor =
    Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).
      setNameFormat("Flume Receiver Channel Thread - %d").build())

  lazy val channelFactory =
    new NioClientSocketChannelFactory(channelFactoryExecutor, channelFactoryExecutor)

  lazy val receiverExecutor = Executors.newFixedThreadPool(parallelism,
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Flume Receiver Thread - %d").build())

  private lazy val connections = new LinkedBlockingQueue[FlumeConnection]()

  override def onStart(): Unit = {
    // Create the connections to each Flume agent.
    addresses.foreach(host => {
      val transceiver = new NettyTransceiver(host, channelFactory)
      val client = SpecificRequestor.getClient(classOf[SparkFlumeProtocol.Callback], transceiver)
      connections.add(new FlumeConnection(transceiver, client))
    })
    for (i <- 0 until parallelism) {
      logInfo("Starting Flume Polling Receiver worker threads..")
      // Threads that pull data from Flume.
      receiverExecutor.submit(new FlumeBatchFetcher(this))
    }
  }

  override def onStop(): Unit = {
    logInfo("Shutting down Flume Polling Receiver")
    receiverExecutor.shutdownNow()
    connections.foreach(connection => {
      connection.transceiver.close()
    })
    channelFactory.releaseExternalResources()
  }

  private[flume] def getConnections: LinkedBlockingQueue[FlumeConnection] = {
    this.connections
  }

  private[flume] def getMaxBatchSize: Int = {
    this.maxBatchSize
  }
}

/**
 * A wrapper around the transceiver and the Avro IPC API.
 * @param transceiver The transceiver to use for communication with Flume
 * @param client The client that the callbacks are received on.
 */
private[flume] class FlumeConnection(val transceiver: NettyTransceiver,
  val client: SparkFlumeProtocol.Callback)



