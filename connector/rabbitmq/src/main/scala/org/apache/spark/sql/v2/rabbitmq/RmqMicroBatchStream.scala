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
package org.apache.spark.sql.v2.rabbitmq

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.convert.ImplicitConversions.`map AsScala`
import scala.util.control.NonFatal

import com.rabbitmq.stream._
import com.rabbitmq.stream.MessageHandler.Context
import org.apache.qpid.proton.amqp.messaging.Data

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String





class RmqMicroBatchStream(opts: CaseInsensitiveStringMap, chk: String)
  extends MicroBatchStream with Logging {

  // ---- config
  private val queueName = req("rmq.queuename")
  private val host = req("rmq.host")
  private val port = opt("rmq.port").map(_.toInt).getOrElse(5552) // default stream port
  private val vhost = opt("rmq.vhost").getOrElse("/")
  private val username = opt("rmq.username").getOrElse("guest")
  private val password = opt("rmq.password").getOrElse("guest")
  private val fetchSize = opt("rmq.fetchsize").map(_.toLong).getOrElse(2000L)
  private val readTimeoutSec = opt("rmq.readtimeout").map(_.toLong).getOrElse(300L)
  private val maxBatch = opt("rmq.maxbatchsize").map(_.toLong).getOrElse(1000L)

  private val checkpointFile: Path = {
    val base = opt("rmq.offsetcheckpointpath").map(Paths.get(_)).getOrElse(Paths.get(chk))
    base.resolve("rmq_v2_offset.txt")
  }

  // ---- state
  private val env: Environment = Environment.builder()
    .host(host).port(port).virtualHost(vhost)
    .username(username).password(password)
    .build()

  private val buffer: ConcurrentNavigableMap[Long, InternalRow] =
    new ConcurrentSkipListMap[Long, InternalRow]()
  private val fetched = new AtomicLong(0L)
  private val lastDelivered = new AtomicLong(-1L)
  private val running = new AtomicBoolean(true)
  private val consumer: Consumer = startConsumer()

  // persisted last committed (driver-side)
  private val persistedOffsetOnStart: Long = readPersistedOrMinusOne()
  private val initial: Long = persistedOffsetOnStart + 1 // next to read
  logInfo(s"[RMQ V2] init: queue=$queueName host=$host:$port vhost=$vhost user=$username")
  logInfo(s"[RMQ V2] init: fetchSize=$fetchSize maxBatch=$maxBatch readTimeoutSec=$readTimeoutSec")
  logInfo(s"[RMQ V2] init: checkpointFile=$checkpointFile " +
    s"persistedOffset=$persistedOffsetOnStart initialNext=$initial")

  override def initialOffset(): Offset = {
    logInfo(s"[RMQ V2] initialOffset -> $persistedOffsetOnStart")
    RmqLongOffset(persistedOffsetOnStart)
  }

  override def latestOffset(): Offset = {
    val top = Option(buffer.lastKey()).getOrElse(lastDelivered.get())
    val latest = math.max(top, persistedOffsetOnStart)
    logDebug(s"[RMQ V2] latestOffset -> topInBuffer=$top," +
      s" persisted=$persistedOffsetOnStart => $latest")
    RmqLongOffset(latest)
  }

  // ---- MicroBatchStream API

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val from = start.asInstanceOf[RmqLongOffset].value
    val to = end.asInstanceOf[RmqLongOffset].value

    val sub = buffer.subMap(from + 1, true, to, true)

    val cap = math.min(sub.size(), maxBatch).toInt
    val iter = sub.entrySet().iterator()
    val rows = new util.ArrayList[InternalRow](cap)
    var i = 0
    var lastSeen = -1L
    while (iter.hasNext && i < cap) {
      val e = iter.next()
      rows.add(e.getValue)
      lastSeen = e.getKey
      i += 1
    }
    if (lastSeen >= 0) buffer.subMap(from + 1, true, lastSeen, true).clear()
    fetched.addAndGet(-i)

    logInfo(s"[RMQ V2] planInputPartitions: requested=($from,$to]," +
      s" prepared=$i rows, lastSeen=$lastSeen," +
      s" bufferNow=${buffer.size()}, fetchedNow=${fetched.get()}")

    Array(RmqInputPartition(rows))
  }

  override def createReaderFactory(): PartitionReaderFactory =
    RmqReaderFactory

  override def commit(end: Offset): Unit = {
    val off = end.asInstanceOf[RmqLongOffset].value
    persist(off)
    logInfo(s"[RMQ V2] commit: persisted endOffset=$off")
  }

  private def persist(value: Long): Unit = {
    try {
      val parent = checkpointFile.getParent
      if (parent != null && !Files.exists(parent)) Files.createDirectories(parent)
      Files.write(checkpointFile, value.toString.getBytes(StandardCharsets.UTF_8))
    } catch {
      case NonFatal(e) =>
        logWarning(s"[RMQ V2] persist offset failed (value=$value, file=$checkpointFile)", e)
    }
  }

  override def stop(): Unit = {
    logInfo("[RMQ V2] stop: closing consumer + environment")
    running.set(false)
    try consumer.close() catch {
      case NonFatal(e) => logWarning("consumer.close failed", e)
    }
    try env.close() catch {
      case NonFatal(e) => logWarning("env.close failed", e)
    }
  }

  override def deserializeOffset(s: String): Offset = RmqLongOffset(s.trim.toLong)

  // ---- helpers to read options
  private def req(k: String): String = {
    val v = opts.get(k)
    if (v == null) throw new IllegalArgumentException(s"Missing option: $k")
    v
  }

  private def opt(k: String): Option[String] = Option(opts.get(k))

  // ---- IO for offset
  private def readPersistedOrMinusOne(): Long = {
    try {
      if (Files.exists(checkpointFile)) {
        val str = new String(Files.readAllBytes(checkpointFile), StandardCharsets.UTF_8).trim
        val v = str.toLong
        logInfo(s"[RMQ V2] readPersisted: $v from $checkpointFile")
        v
      } else {
        logInfo(s"[RMQ V2] readPersisted: file not found, return -1")
        -1L
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"[RMQ V2] readPersisted: failed, return -1 (file=$checkpointFile)", e)
        -1L
    }
  }

  // ---- Consumer & conversion

  private def startConsumer(): Consumer = {
    logInfo(s"[RMQ V2] startConsumer: stream=$queueName from offset=$initial, fetchSize=$fetchSize")
    env.consumerBuilder()
      .stream(queueName)
      .offset(OffsetSpecification.offset(initial))
      .flow()
      .strategy(ConsumerFlowStrategy.creditWhenHalfMessagesProcessed(3))
      .builder()
      .messageHandler((ctx: Context, msg: Message) => {
        if (!running.get()) ctx.processed()
        try {
          val off = ctx.offset()


          val row = toInternalRow(msg, ctx)
          buffer.put(off, row)
          lastDelivered.set(Math.max(lastDelivered.get(), off))
          val cur = fetched.incrementAndGet()

          if (cur > fetchSize) {
            val over = cur - fetchSize
            val sleepMs = Math.min(200L, 5L * over)
            logDebug(s"[RMQ V2] handler: fetched=$cur > fetchSize=$fetchSize," +
              s" sleep=${sleepMs}ms, buffer=${buffer.size()}")
            Thread.sleep(sleepMs)
          }

          ctx.processed()

          if ((off % 10000) == 0) {
            logInfo(s"[RMQ V2] handler: milestone offset=$off," +
              s" buffer=${buffer.size()}, fetched=${fetched.get()}")
          }
        } catch {
          case NonFatal(e) =>
            logError(s"[RMQ V2] handler: exception at offset=${ctx.offset()}," +
              s" NOT calling processed()", e)
        }
      })
      .build()
  }

  private def toInternalRow(message: Message, context: Context): InternalRow = {
    val routingKey: UTF8String = getRoutingKey(message)
    val headers: ArrayBasedMapData = getHeaders(message).orNull
    val body: UTF8String = getBody(message)
    InternalRow(routingKey, headers, body, context.timestamp() * 1000) // ms
  }

  private def getBody(message: Message): UTF8String = {
    message.getBody match {
      case data: Data => UTF8String.fromBytes(data.getValue.getArray)
      case other =>
        logWarning(s"[RMQ V2] getBody: unexpected type=${Option(other)
          .map(_.getClass.getName).orNull}, returning null")
        null
    }
  }

  private def getRoutingKey(message: Message): UTF8String = {
    message.getMessageAnnotations.get("x-routing-key") match {
      case null => null
      case s: String => UTF8String.fromString(s)
      case other =>
        logWarning(s"[RMQ V2] getRoutingKey: unexpected type=${other.getClass.getName}," +
          s" returning null")
        null
    }
  }

  private def getHeaders(message: Message): Option[ArrayBasedMapData] = {
    Option(message.getApplicationProperties).map { props =>
      val tuples = props.toList.map { case (k, v) =>
        (UTF8String.fromString(k), UTF8String.fromString(String.valueOf(v)))
      }
      val keys = ArrayData.toArrayData(tuples.map(_._1))
      val vals = ArrayData.toArrayData(tuples.map(_._2))
      new ArrayBasedMapData(keys, vals)
    }
  }
}
