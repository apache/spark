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


import java.io.{ObjectOutput, ObjectInput, Externalizable}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.{TimeUnit, Executors}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.avro.ipc.NettyTransceiver
import org.apache.avro.ipc.specific.SpecificRequestor
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory

import org.apache.spark.Logging
import org.apache.spark.flume.{SparkSinkEvent, SparkFlumeProtocol}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

/**
 * A [[ReceiverInputDStream]] that can be used to read data from several Flume agents running
 * [[org.apache.spark.flume.sink.SparkSink]]s.
 * @param ssc_ Streaming context that will execute this input stream
 * @param addresses List of addresses at which SparkSinks are listening
 * @param maxBatchSize Maximum size of a batch
 * @param parallelism Number of parallel connections to open
 * @param storageLevel The storage level to use.
 * @tparam T Class type of the object of this stream
 */
class FlumePollingInputDStream[T: ClassTag](
  @transient ssc_ : StreamingContext,
  val addresses: Seq[InetSocketAddress],
  val maxBatchSize: Int,
  val parallelism: Int,
  storageLevel: StorageLevel
) extends ReceiverInputDStream[SparkFlumePollingEvent](ssc_) {
  /**
   * Gets the receiver object that will be sent to the worker nodes
   * to receive data. This method needs to defined by any specific implementation
   * of a NetworkInputDStream.
   */
  override def getReceiver(): Receiver[SparkFlumePollingEvent] = {
    new FlumePollingReceiver(addresses, maxBatchSize, parallelism, storageLevel)
  }
}

private[streaming] class FlumePollingReceiver(
  addresses: Seq[InetSocketAddress],
  maxBatchSize: Int,
  parallelism: Int,
  storageLevel: StorageLevel
) extends Receiver[SparkFlumePollingEvent](storageLevel) with Logging {

  lazy val channelFactoryExecutor =
    Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).
      setNameFormat("Flume Receiver Channel Thread - %d").build())

  lazy val channelFactory =
    new NioClientSocketChannelFactory(channelFactoryExecutor, channelFactoryExecutor)

  lazy val receiverExecutor = Executors.newFixedThreadPool(parallelism,
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Flume Receiver Thread - %d").build())

  private var connections = Array.empty[FlumeConnection] // temporarily empty, filled in later

  override def onStart(): Unit = {
    // Create the connections to each Flume agent.
    connections = addresses.map(host => {
      val transceiver = new NettyTransceiver(host, channelFactory)
      val client = SpecificRequestor.getClient(classOf[SparkFlumeProtocol.Callback], transceiver)
      new FlumeConnection(transceiver, client)
    }).toArray

    // Threads that pull data from Flume.
    val dataReceiver = new Runnable {
      override def run(): Unit = {
        var counter = 0
        while (true) {
          counter = counter % connections.size
          val client = connections(counter).client
          counter += 1
          val batch = client.getEventBatch(maxBatchSize)
          val seq = batch.getSequenceNumber
          val events: java.util.List[SparkSinkEvent] = batch.getEvents
          logDebug("Received batch of " + events.size() + " events with sequence number: " + seq)
          try {
            // Convert each Flume event to a serializable SparkPollingEvent
            events.foreach(event => store(SparkFlumePollingEvent.fromSparkSinkEvent(event)))
            // Send an ack to Flume so that Flume discards the events from its channels.
            client.ack(seq)
          } catch {
            case e: Throwable =>
              try {
                // Let Flume know that the events need to be pushed back into the channel.
                client.nack(seq) // If the agent is down, even this could fail and throw
              } catch {
                case e: Throwable => logError("Sending Nack also failed. A Flume agent is down.")
              }
              TimeUnit.SECONDS.sleep(2L) // for now just leave this as a fixed 2 seconds.
              logWarning("Error while attempting to store events", e)
          }
        }
      }
    }
    // Create multiple threads and start all of them.
    for (i <- 0 until parallelism) {
      logInfo("Starting Flume Polling Receiver worker threads starting..")
      receiverExecutor.submit(dataReceiver)
    }
  }

  override def store(dataItem: SparkFlumePollingEvent) {
    // Not entirely sure store is thread-safe for all storage levels - so wrap it in synchronized
    // This takes a performance hit, since the parallelism is useful only for pulling data now.
    this.synchronized {
      super.store(dataItem)
    }
  }

  override def onStop(): Unit = {
    logInfo("Shutting down Flume Polling Receiver")
    receiverExecutor.shutdownNow()
    connections.map(connection => {
      connection.transceiver.close()
    })
    channelFactory.releaseExternalResources()
  }
}

/**
 * A wrapper around the transceiver and the Avro IPC API. 
 * @param transceiver The transceiver to use for communication with Flume
 * @param client The client that the callbacks are received on.
 */
private class FlumeConnection(val transceiver: NettyTransceiver,
                              val client: SparkFlumeProtocol.Callback)

private[streaming] object SparkFlumePollingEvent {
  def fromSparkSinkEvent(in: SparkSinkEvent): SparkFlumePollingEvent = {
    val event = new SparkFlumePollingEvent()
    event.event = in
    event
  }
}
/*
 * Unfortunately Avro does not allow including pre-compiled classes - so even though
 * SparkSinkEvent is identical to AvroFlumeEvent, we need to create a new class and a wrapper
 * around that to make it externalizable.
 */
class SparkFlumePollingEvent() extends Externalizable with Logging {
  var event : SparkSinkEvent = new SparkSinkEvent()

  /* De-serialize from bytes. */
  def readExternal(in: ObjectInput) {
    val (headers, bodyBuff) = EventTransformer.readExternal(in)
    event.setBody(ByteBuffer.wrap(bodyBuff))
    event.setHeaders(headers)
  }

  /* Serialize to bytes. */
  def writeExternal(out: ObjectOutput) {
    EventTransformer.writeExternal(out, event.getHeaders, event.getBody.array())
  }
}


