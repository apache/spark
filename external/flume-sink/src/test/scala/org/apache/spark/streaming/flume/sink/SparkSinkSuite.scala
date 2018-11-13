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
import java.nio.charset.StandardCharsets
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

import org.apache.avro.ipc.NettyTransceiver
import org.apache.avro.ipc.specific.SpecificRequestor
import org.apache.flume.Context
import org.apache.flume.channel.MemoryChannel
import org.apache.flume.event.EventBuilder
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory

// Due to MNG-1378, there is not a way to include test dependencies transitively.
// We cannot include Spark core tests as a dependency here because it depends on
// Spark core main, which has too many dependencies to require here manually.
// For this reason, we continue to use FunSuite and ignore the scalastyle checks
// that fail if this is detected.
// scalastyle:off
import org.scalatest.FunSuite

class SparkSinkSuite extends FunSuite {
// scalastyle:on

  val eventsPerBatch = 1000
  val channelCapacity = 5000

  test("Success with ack") {
    val (channel, sink, latch) = initializeChannelAndSink()
    channel.start()
    sink.start()

    putEvents(channel, eventsPerBatch)

    val port = sink.getPort
    val address = new InetSocketAddress("0.0.0.0", port)

    val (transceiver, client) = getTransceiverAndClient(address, 1)(0)
    val events = client.getEventBatch(1000)
    client.ack(events.getSequenceNumber)
    assert(events.getEvents.size() === 1000)
    latch.await(1, TimeUnit.SECONDS)
    assertChannelIsEmpty(channel)
    sink.stop()
    channel.stop()
    transceiver.close()
  }

  test("Failure with nack") {
    val (channel, sink, latch) = initializeChannelAndSink()
    channel.start()
    sink.start()
    putEvents(channel, eventsPerBatch)

    val port = sink.getPort
    val address = new InetSocketAddress("0.0.0.0", port)

    val (transceiver, client) = getTransceiverAndClient(address, 1)(0)
    val events = client.getEventBatch(1000)
    assert(events.getEvents.size() === 1000)
    client.nack(events.getSequenceNumber)
    latch.await(1, TimeUnit.SECONDS)
    assert(availableChannelSlots(channel) === 4000)
    sink.stop()
    channel.stop()
    transceiver.close()
  }

  test("Failure with timeout") {
    val (channel, sink, latch) = initializeChannelAndSink(Map(SparkSinkConfig
      .CONF_TRANSACTION_TIMEOUT -> 1.toString))
    channel.start()
    sink.start()
    putEvents(channel, eventsPerBatch)
    val port = sink.getPort
    val address = new InetSocketAddress("0.0.0.0", port)

    val (transceiver, client) = getTransceiverAndClient(address, 1)(0)
    val events = client.getEventBatch(1000)
    assert(events.getEvents.size() === 1000)
    latch.await(1, TimeUnit.SECONDS)
    assert(availableChannelSlots(channel) === 4000)
    sink.stop()
    channel.stop()
    transceiver.close()
  }

  test("Multiple consumers") {
    testMultipleConsumers(failSome = false)
  }

  test("Multiple consumers with some failures") {
    testMultipleConsumers(failSome = true)
  }

  def testMultipleConsumers(failSome: Boolean): Unit = {
    implicit val executorContext = ExecutionContext
      .fromExecutorService(Executors.newFixedThreadPool(5))
    val (channel, sink, latch) = initializeChannelAndSink(Map.empty, 5)
    channel.start()
    sink.start()
    (1 to 5).foreach(_ => putEvents(channel, eventsPerBatch))
    val port = sink.getPort
    val address = new InetSocketAddress("0.0.0.0", port)
    val transceiversAndClients = getTransceiverAndClient(address, 5)
    val batchCounter = new CountDownLatch(5)
    val counter = new AtomicInteger(0)
    transceiversAndClients.foreach(x => {
      Future {
        val client = x._2
        val events = client.getEventBatch(1000)
        if (!failSome || counter.getAndIncrement() % 2 == 0) {
          client.ack(events.getSequenceNumber)
        } else {
          client.nack(events.getSequenceNumber)
          throw new RuntimeException("Sending NACK for failure!")
        }
        events
      }.onComplete {
        case Success(events) =>
          assert(events.getEvents.size() === 1000)
          batchCounter.countDown()
        case Failure(t) =>
          // Don't re-throw the exception, causes a nasty unnecessary stack trace on stdout
          batchCounter.countDown()
      }
    })
    batchCounter.await()
    latch.await(1, TimeUnit.SECONDS)
    executorContext.shutdown()
    if(failSome) {
      assert(availableChannelSlots(channel) === 3000)
    } else {
      assertChannelIsEmpty(channel)
    }
    sink.stop()
    channel.stop()
    transceiversAndClients.foreach(x => x._1.close())
  }

  private def initializeChannelAndSink(overrides: Map[String, String] = Map.empty,
    batchCounter: Int = 1): (MemoryChannel, SparkSink, CountDownLatch) = {
    val channel = new MemoryChannel()
    val channelContext = new Context()

    channelContext.put("capacity", channelCapacity.toString)
    channelContext.put("transactionCapacity", 1000.toString)
    channelContext.put("keep-alive", 0.toString)
    channelContext.putAll(overrides.asJava)
    channel.setName(scala.util.Random.nextString(10))
    channel.configure(channelContext)

    val sink = new SparkSink()
    val sinkContext = new Context()
    sinkContext.put(SparkSinkConfig.CONF_HOSTNAME, "0.0.0.0")
    sinkContext.put(SparkSinkConfig.CONF_PORT, 0.toString)
    sink.configure(sinkContext)
    sink.setChannel(channel)
    val latch = new CountDownLatch(batchCounter)
    sink.countdownWhenBatchReceived(latch)
    (channel, sink, latch)
  }

  private def putEvents(ch: MemoryChannel, count: Int): Unit = {
    val tx = ch.getTransaction
    tx.begin()
    (1 to count).foreach(x =>
      ch.put(EventBuilder.withBody(x.toString.getBytes(StandardCharsets.UTF_8))))
    tx.commit()
    tx.close()
  }

  private def getTransceiverAndClient(address: InetSocketAddress,
    count: Int): Seq[(NettyTransceiver, SparkFlumeProtocol.Callback)] = {

    (1 to count).map(_ => {
      lazy val channelFactoryExecutor = Executors.newCachedThreadPool(
        new SparkSinkThreadFactory("Flume Receiver Channel Thread - %d"))
      lazy val channelFactory =
        new NioClientSocketChannelFactory(channelFactoryExecutor, channelFactoryExecutor)
      val transceiver = new NettyTransceiver(address, channelFactory)
      val client = SpecificRequestor.getClient(classOf[SparkFlumeProtocol.Callback], transceiver)
      (transceiver, client)
    })
  }

  private def assertChannelIsEmpty(channel: MemoryChannel): Unit = {
    assert(availableChannelSlots(channel) === channelCapacity)
  }

  private def availableChannelSlots(channel: MemoryChannel): Int = {
    val queueRemaining = channel.getClass.getDeclaredField("queueRemaining")
    queueRemaining.setAccessible(true)
    val m = queueRemaining.get(channel).getClass.getDeclaredMethod("availablePermits")
    m.invoke(queueRemaining.get(channel)).asInstanceOf[Int]
  }
}
