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
package org.apache.spark.sql.rabbitmq


import java.net.URI
import java.nio.file.Paths
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.convert.ImplicitConversions.{`map AsJavaMap`, `map AsScala`, `map AsScalaConcurrentMap`}

import com.rabbitmq.stream._
import com.rabbitmq.stream.MessageHandler.Context
import org.apache.qpid.proton.amqp.messaging.Data

import org.apache.spark.api.java.JavaSparkContext.fromSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.classic.ClassicConversions.castToImpl
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.execution.streaming.runtime.LongOffset
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String



class RmqStreamingSource(sqlContext: SQLContext,
                         metadataPath: String, parameters: Map[String, String])
  extends Source with Logging {
  private val prefetch: Long = parameters.getOrElse("rmq.fetchsize", "2001").toLong
  private val readLimit: Long = parameters.getOrElse("rmq.maxbatchsize", "1000").toLong
  private val queueName: String = parameters("rmq.queuename")
  private val fetchedCount: AtomicLong = new AtomicLong(0)
  private val readTimeoutSecond: Long = parameters.getOrElse("rmq.readtimeout", "300").toLong

  private val checkpointPath: String =
    PathUtil.convertToSystemPath(
      parameters.getOrDefault("rmq.offsetcheckpointpath",
        new URI(metadataPath).getPath))
  private val customOffsetCheckpointPath =
    Paths.get(checkpointPath).resolve("customOffset")
  private val offsetManager: RmqOffsetManagerTrait =
    new RmqFileSystemRmqOffsetManager(customOffsetCheckpointPath)

  private val rmqEnv: Environment = RmqEnvironmentFactory.getEnvironment(parameters)
  private val rmqRetryEnv: Environment = RmqEnvironmentFactory.getEnvironment(parameters)

  private val buffer: ConcurrentNavigableMap[Long, (Message, Context)]
  = new ConcurrentSkipListMap[Long, (Message, Context)]()
  private val deliveredTotal: AtomicLong = new AtomicLong(0)
  private val consumer: Consumer = startConsume(lastReadOffset + 1)
  logInfo(s"[RMQ] init: queue=$queueName, prefetch=$prefetch, readLimit=$readLimit," +
    s" readTimeoutSec=$readTimeoutSecond")
  logInfo(s"[RMQ] init: checkpointPath=$checkpointPath," +
    s" offsetFile=$customOffsetCheckpointPath, persistedLastReadOffset=$lastReadOffset")
  @volatile private var lastReadOffset: Long = offsetManager.readLongFromFile().getOrElse(-1L)

  override def schema: StructType = RmqStreamingSchema.default

  override def getOffset: Option[Offset] = {
    val unreadMsg: ConcurrentNavigableMap[Long, (Message, Context)] =
      buffer.tailMap(lastReadOffset, false)
    val unread = unreadMsg.size()
    logDebug(s"[RMQ] getOffset: lastReadOffset=$lastReadOffset," +
      s" bufferSize=${buffer.size()}, unread=$unread")
    getKeyByIndexOrLast(unreadMsg, readLimit) match {
      case None => None
      case Some(offsetValue) =>
        logDebug(s"[RMQ] getOffset: selecting endOffset=$offsetValue (limit=$readLimit)")
        Some(LongOffset(offsetValue))
    }
  }

  private def getKeyByIndexOrLast(map: ConcurrentNavigableMap[Long, (Message, Context)],
                                  index: Long): Option[Long] = {
    if (map.isEmpty) return None
    var count = 0
    var lastKey: Option[Long] = None
    val iterator = map.entrySet.iterator
    while (iterator.hasNext) {
      val entry = iterator.next
      lastKey = Some(entry.getKey) // Keep updating lastKey until the end of the loop
      count += 1
      if (count == index) {
        logDebug(s"[RMQ] getKeyByIndexOrLast: hit index=$index -> key=${lastKey.get}")
        return lastKey // Return the 500th entry if it exists
      }
    }
    // If the loop completes without finding the 500th entry, return the last key found
    logDebug(s"[RMQ] getKeyByIndexOrLast: " +
      s"map smaller than index=$index, using lastKey=${lastKey.get}")
    lastKey
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val fromOffset: Long = start match {
      case Some(offset) => offset.json().toLong
      case None => -1L
    }
    val toOffset: Long = end.json().toLong
    logInfo(s"[RMQ] getBatch: requested=($fromOffset,$toOffset]," +
      s" lastReadOffset=$lastReadOffset, bufferSize=${buffer.size()}," +
      s" fetched=${fetchedCount.get()}")

    if (fromOffset < lastReadOffset) {
      logWarning(s"[RMQ] getBatch: cache miss " +
        s"(fromOffset=$fromOffset < lastReadOffset=$lastReadOffset), fallback to re-read")
      readAgainFromSource(fromOffset, toOffset)
    } else {
      readFromBuffer(fromOffset, toOffset)
    }
  }

  override def commit(end: Offset): Unit = {
    val endOffset: Long = end.json().toLong
    logInfo(s"[RMQ] commit: endOffset=$endOffset (prev persisted lastReadOffset=$lastReadOffset)")
    offsetManager.saveLongToFile(endOffset)
  }

  override def stop(): Unit = {
    logInfo("[RMQ] stop: closing consumer")
    try consumer.close() catch {
      case e: Throwable => logWarning("[RMQ] stop: consumer.close failed", e)
    }
  }

  private def readAgainFromSource(fromOffset: Long, toOffset: Long) = {
    logError("cache missed, read again from: " + fromOffset +
      " to: " + toOffset + ". Huge performance impact if this happened frequently")
    logInfo(s"[RMQ] re-read: start from offset=${fromOffset + 1}" +
      s" up to $toOffset, timeoutSec=$readTimeoutSecond")
    val latch: CountDownLatch = new CountDownLatch(1)
    val tempBuffer: ConcurrentNavigableMap[Long, (Message, Context)] =
      new ConcurrentSkipListMap[Long, (Message, Context)]()
    val tempConsumer = rmqRetryEnv.consumerBuilder()
      .offset(OffsetSpecification.offset(fromOffset + 1))
      .stream(queueName)
      .flow()
      .strategy(ConsumerFlowStrategy.creditOnProcessedMessageCount(1, 1))
      .builder()
      .messageHandler((context: Context, message: Message) => {
        if (context.offset() <= toOffset) {
          tempBuffer.put(context.offset(), (message, context))
          context.processed()
        } else {
          latch.countDown()
        }
      })
      .build()
    val awaited = latch.await(readTimeoutSecond, TimeUnit.SECONDS)
    if (!awaited) logWarning(s"[RMQ] re-read:" +
      s" timeout after $readTimeoutSecond sec; tempBufferSize=${tempBuffer.size()}")
    tempConsumer.close()
    logInfo(s"[RMQ] re-read: collected=${tempBuffer.size()} messages")
    toInternalDf(tempBuffer)
  }

  // (fromOffset, toOffset]
  private def readFromBuffer(fromOffset: Long, toOffset: Long) = {
    logDebug("processing from " + fromOffset + " to " + toOffset
      + " total " + (toOffset - fromOffset))
    val subMap: ConcurrentNavigableMap[Long, (Message, Context)] =
      buffer.subMap(fromOffset + 1, toOffset + 1)
    logInfo(s"[RMQ] readFromBuffer: subMapSize=${subMap.size()}," +
      s" bufferSize=${buffer.size()}, fetched=${fetchedCount.get()}")
    val internalDf: DataFrame = toInternalDf(subMap)
    lastReadOffset = toOffset

    fetchedCount.addAndGet(-1L * subMap.size().toLong)
    subMap.clear()
    logDebug(s"[RMQ] readFromBuffer: advanced lastReadOffset -> " +
      s"$lastReadOffset, fetchedNow=${fetchedCount.get()}")
    internalDf
  }

  private def toInternalDf(subMap: ConcurrentNavigableMap[Long, (Message, Context)]) = {
    if (!subMap.isEmpty) {
      logDebug(s"[RMQ] toInternalDf: " +
        s"range=[${subMap.firstKey()}..${subMap.lastKey()}], size=${subMap.size()}")
    } else {
      logDebug(s"[RMQ] toInternalDf: empty subMap")
    }
    val internalRows = subMap.map(item => {
      val message: Message = item._2._1
      val context: Context = item._2._2
      val routingKey: UTF8String = getRoutingKey(message)
      val headers: ArrayBasedMapData = getHeaders(message).orNull
      val body: UTF8String = getBody(message)
      InternalRow(routingKey, headers, body, context.timestamp() * 1000)
    })
    val internalRdd = sqlContext.sparkContext parallelize(internalRows.toList, internalRows.size)
    sqlContext.internalCreateDataFrame(internalRdd, RmqStreamingSchema.default, isStreaming = true)
  }

  private def getBody(message: Message) = {
    val body: UTF8String = message.getBody match {
      case data: Data => UTF8String.fromBytes(data.getValue.getArray)
      case other =>
        logWarning(s"[RMQ] getBody: unexpected body type=${Option(other)
          .map(_.getClass.getName).orNull}, returning null")
        null
    }
    body
  }

  private def getRoutingKey(message: Message) = {
    val routingKey: UTF8String = message.getMessageAnnotations.get("x-routing-key") match {
      case null => null
      case key: String => UTF8String.fromString(key)
      case other =>
        logWarning(s"[RMQ] getRoutingKey: unexpected type=${other.getClass.getName}," +
          s" value=$other; returning null")
        null
      case _ => null
    }
    routingKey
  }

  private def getHeaders(message: Message): Option[ArrayBasedMapData] = {
    Option(message.getApplicationProperties).map { properties =>
      val headerTuples = properties.toList.map { case (key, value) =>
        val utf8Key = UTF8String.fromString(key)
        val utf8Value = UTF8String.fromString(value.toString)
        (utf8Key, utf8Value)
      }
      val keyArray = ArrayData.toArrayData(headerTuples.map(_._1))
      val valueArray = ArrayData.toArrayData(headerTuples.map(_._2))
      new ArrayBasedMapData(keyArray, valueArray)
    }
  }

  private def startConsume(startOffset: Long): Consumer = {
    logInfo(s"[RMQ] startConsume: stream=$queueName, startOffset=$startOffset, prefetch=$prefetch")
    rmqEnv.consumerBuilder()
      .offset(OffsetSpecification.offset(startOffset))
      .stream(queueName)
      .flow()
      .strategy(ConsumerFlowStrategy.creditWhenHalfMessagesProcessed(3))
      .builder()
      .messageHandler((context: Context, message: Message) => {
        val off = context.offset()
        buffer.put(off, (message, context))
        val cur = fetchedCount.incrementAndGet()
        deliveredTotal.incrementAndGet()
        if (cur >= prefetch) {
          val over = cur - prefetch
          val sleepMs = 100L * Math.max(0, over)
          logDebug(s"[RMQ] handler: fetched=$cur > prefetch=$prefetch," +
            s" sleep=${sleepMs}ms, lastReadOffset=$lastReadOffset, bufferSize=${buffer.size()}")
          Thread.sleep(sleepMs)
        }
        if ((off % 10000) == 0) {
          logInfo(s"[RMQ] handler: milestone offset=$off, " +
            s"bufferSize=${buffer.size()}, fetched=${fetchedCount.get()}," +
            s" deliveredTotal=${deliveredTotal.get()}")
        }
        context.processed()
      })
      .build()
  }

}