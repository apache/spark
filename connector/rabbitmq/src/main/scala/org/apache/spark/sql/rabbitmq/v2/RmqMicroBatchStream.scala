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
package org.apache.spark.sql.rabbitmq.v2

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.util.control.NonFatal

import com.rabbitmq.stream._
import com.rabbitmq.stream.MessageHandler.Context

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.rabbitmq.common.{RmqPropsHolder, RmqUtils}
import org.apache.spark.sql.util.CaseInsensitiveStringMap





class RmqMicroBatchStream(opts: CaseInsensitiveStringMap, path: String)
  extends MicroBatchStream with Logging  {
  val props: RmqPropsHolder = RmqPropsHolder(opts.asInstanceOf, path)

  // ---- state
  private val env: Environment = Environment.builder()
    .host(props.getHost).port(props.getPort).virtualHost(props.getVhost)
    .username(props.getUsername).password(props.getPassword)
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
  logInfo(s"[RMQ V2] init: queue=$props.getQueueName host=$props.getHost:$props.getPort" +
    s" vhost=$props.getVhost user=$props.getUsername")
  logInfo(s"[RMQ V2] init: fetchSize=$props.getFetchSize" +
    s" maxBatch=$props.getMaxBatch readTimeoutSec=$props.getReadTimeoutSec")
  logInfo(s"[RMQ V2] init: checkpointFile=$props.getCheckpointFile " +
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

    val cap = math.min(sub.size(), props.getMaxBatch).toInt
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
      val parent = props.getCheckpointFile.getParent
      if (parent != null && !Files.exists(parent)) Files.createDirectories(parent)
      Files.write(props.getCheckpointFile, value.toString.getBytes(StandardCharsets.UTF_8))
    } catch {
      case NonFatal(e) =>
        logWarning(s"[RMQ V2] persist offset failed (value=$value," +
          s" file=$props.getCheckpointFile)", e)
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



  // ---- IO for offset
  private def readPersistedOrMinusOne(): Long = {
    try {
      if (Files.exists(props.getCheckpointFile)) {
        val str = new String(Files.readAllBytes(props.getCheckpointFile),
          StandardCharsets.UTF_8).trim
        val v = str.toLong
        logInfo(s"[RMQ V2] readPersisted: $v from $props.getCheckpointFile")
        v
      } else {
        logInfo(s"[RMQ V2] readPersisted: file not found, return -1")
        -1L
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"[RMQ V2] readPersisted: failed, return -1 (file=$props.getCheckpointFile)", e)
        -1L
    }
  }

  // ---- Consumer & conversion

  private def startConsumer(): Consumer = {
    logInfo(s"[RMQ V2] startConsumer: stream=$props.getQueueName from offset=$initial," +
      s" fetchSize=$props.getFetchSize")
    env.consumerBuilder()
      .stream(props.getQueueName)
      .offset(OffsetSpecification.offset(initial))
      .flow()
      .strategy(ConsumerFlowStrategy.creditWhenHalfMessagesProcessed(3))
      .builder()
      .messageHandler((ctx: Context, msg: Message) => {
        if (!running.get()) ctx.processed()
        try {
          val off = ctx.offset()
          val row = RmqUtils.toInternalRow(msg, ctx)
          buffer.put(off, row)
          lastDelivered.set(Math.max(lastDelivered.get(), off))
          val cur = fetched.incrementAndGet()

          if (cur > props.getFetchSize) {
            val over = cur - props.getFetchSize
            val sleepMs = Math.min(200L, 5L * over)
            logDebug(s"[RMQ V2] handler: fetched=$cur > fetchSize=$props.getFetchSize," +
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




}
