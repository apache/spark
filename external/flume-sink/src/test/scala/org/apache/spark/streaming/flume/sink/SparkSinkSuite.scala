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
package org.apache.spark.streaming.flume.sink

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, Executors}

import scala.collection.JavaConversions._
import scala.concurrent.{Promise, Future}
import scala.util.{Failure, Success, Try}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.avro.ipc.NettyTransceiver
import org.apache.avro.ipc.specific.SpecificRequestor
import org.apache.flume.Context
import org.apache.flume.channel.MemoryChannel
import org.apache.flume.event.EventBuilder
import org.apache.spark.streaming.TestSuiteBase
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory

class SparkSinkSuite extends TestSuiteBase {
  val eventsPerBatch = 1000
  val channelCapacity = 5000

  test("Success test") {
    val (channel, sink) = initializeChannelAndSink(None)
    channel.start()
    sink.start()

    putEvents(channel, eventsPerBatch)

    val port = sink.getPort
    val address = new InetSocketAddress("0.0.0.0", port)

    val (transceiver, client) = getTransceiverAndClient(address, 1)(0)
    val events = client.getEventBatch(1000)
    client.ack(events.getSequenceNumber)
    assert(events.getEvents.size() === 1000)
    assertChannelIsEmpty(channel)
    sink.stop()
    channel.stop()
    transceiver.close()
  }

  test("Nack") {
    val (channel, sink) = initializeChannelAndSink(None)
    channel.start()
    sink.start()
    putEvents(channel, eventsPerBatch)

    val port = sink.getPort
    val address = new InetSocketAddress("0.0.0.0", port)

    val (transceiver, client) = getTransceiverAndClient(address, 1)(0)
    val events = client.getEventBatch(1000)
    assert(events.getEvents.size() === 1000)
    client.nack(events.getSequenceNumber)
    assert(availableChannelSlots(channel) === 4000)
    sink.stop()
    channel.stop()
    transceiver.close()
  }

  test("Timeout") {
    val (channel, sink) = initializeChannelAndSink(Option(Map(SparkSinkConfig
      .CONF_TRANSACTION_TIMEOUT -> 1.toString)))
    channel.start()
    sink.start()
    putEvents(channel, eventsPerBatch)
    val port = sink.getPort
    val address = new InetSocketAddress("0.0.0.0", port)

    val (transceiver, client) = getTransceiverAndClient(address, 1)(0)
    val events = client.getEventBatch(1000)
    assert(events.getEvents.size() === 1000)
    Thread.sleep(1000)
    assert(availableChannelSlots(channel) === 4000)
    sink.stop()
    channel.stop()
    transceiver.close()
  }

  test("Multiple consumers") {
    multipleClients(failSome = false)
  }

  test("Multiple consumers With Some Failures") {
    multipleClients(failSome = true)
  }

  def multipleClients(failSome: Boolean): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val (channel, sink) = initializeChannelAndSink(None)
    channel.start()
    sink.start()
    (1 to 5).map(_ => putEvents(channel, eventsPerBatch))
    val port = sink.getPort
    val address = new InetSocketAddress("0.0.0.0", port)

    val transAndClient = getTransceiverAndClient(address, 5)
    val batchCounter = new CountDownLatch(5)
    val counter = new AtomicInteger(0)
    transAndClient.foreach(x => {
      val promise = Promise[EventBatch]()
      val future = promise.future
      Future {
        val client = x._2
        var events: EventBatch = null
        Try {
          events = client.getEventBatch(1000)
          if(!failSome || counter.getAndIncrement() % 2 == 0) {
            client.ack(events.getSequenceNumber)
          } else {
            client.nack(events.getSequenceNumber)
          }
        }.map(_ => promise.success(events)).recover({
          case e => promise.failure(e)
        })
      }
      future.onComplete {
        case Success(events) => assert(events.getEvents.size() === 1000)
          batchCounter.countDown()
        case Failure(t) => batchCounter.countDown()
          throw t
      }
    })
    batchCounter.await()
    if(failSome) {
      assert(availableChannelSlots(channel) === 3000)
    } else {
      assertChannelIsEmpty(channel)
    }
    sink.stop()
    channel.stop()
    transAndClient.foreach(x => x._1.close())
  }

  def initializeChannelAndSink(overrides: Option[Map[String, String]]):
  (MemoryChannel, SparkSink) = {
    val channel = new MemoryChannel()
    val channelContext = new Context()

    channelContext.put("capacity", channelCapacity.toString)
    channelContext.put("transactionCapacity", 1000.toString)
    channelContext.put("keep-alive", 0.toString)
    overrides.foreach(channelContext.putAll(_))
    channel.configure(channelContext)

    val sink = new SparkSink()
    val sinkContext = new Context()
    sinkContext.put(SparkSinkConfig.CONF_HOSTNAME, "0.0.0.0")
    sinkContext.put(SparkSinkConfig.CONF_PORT, 0.toString)
    sink.configure(sinkContext)
    sink.setChannel(channel)
    (channel, sink)
  }

  private def putEvents(ch: MemoryChannel, count: Int): Unit = {
    val tx = ch.getTransaction
    tx.begin()
    (1 to count).map(x => ch.put(EventBuilder.withBody(x.toString.getBytes)))
    tx.commit()
    tx.close()
  }

  private def getTransceiverAndClient(address: InetSocketAddress, count: Int):
  Seq[(NettyTransceiver, SparkFlumeProtocol.Callback)] = {

    (1 to count).map(_ => {
      lazy val channelFactoryExecutor =
        Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).
          setNameFormat("Flume Receiver Channel Thread - %d").build())
      lazy val channelFactory =
        new NioClientSocketChannelFactory(channelFactoryExecutor, channelFactoryExecutor)
      val transceiver = new NettyTransceiver(address, channelFactory)
      val client = SpecificRequestor.getClient(classOf[SparkFlumeProtocol.Callback], transceiver)
      (transceiver, client)
    })
  }

  private def assertChannelIsEmpty(channel: MemoryChannel) = {
    assert(availableChannelSlots(channel) === 5000)
  }

  private def availableChannelSlots(channel: MemoryChannel): Int = {
    val queueRemaining = channel.getClass.getDeclaredField("queueRemaining")
    queueRemaining.setAccessible(true)
    val m = queueRemaining.get(channel).getClass.getDeclaredMethod("availablePermits")
    m.invoke(queueRemaining.get(channel)).asInstanceOf[Int]
  }

}
