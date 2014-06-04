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
import java.io.{ObjectInput, ObjectOutput, Externalizable}
import java.nio.ByteBuffer

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.apache.flume.source.avro.AvroSourceProtocol
import org.apache.flume.source.avro.AvroFlumeEvent
import org.apache.flume.source.avro.Status
import org.apache.avro.ipc.specific.SpecificResponder
import org.apache.avro.ipc.NettyServer

import org.apache.spark.util.Utils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.Logging
import org.apache.spark.streaming.receiver.Receiver

private[streaming]
class FlumeInputDStream[T: ClassTag](
  @transient ssc_ : StreamingContext,
  host: String,
  port: Int,
  storageLevel: StorageLevel
) extends ReceiverInputDStream[SparkFlumeEvent](ssc_) {

  override def getReceiver(): Receiver[SparkFlumeEvent] = {
    new FlumeReceiver(host, port, storageLevel)
  }
}

/**
 * A wrapper class for AvroFlumeEvent's with a custom serialization format.
 *
 * This is necessary because AvroFlumeEvent uses inner data structures
 * which are not serializable.
 */
class SparkFlumeEvent() extends Externalizable {
  var event : AvroFlumeEvent = new AvroFlumeEvent()

  /* De-serialize from bytes. */
  def readExternal(in: ObjectInput) {
    val bodyLength = in.readInt()
    val bodyBuff = new Array[Byte](bodyLength)
    in.readFully(bodyBuff)

    val numHeaders = in.readInt()
    val headers = new java.util.HashMap[CharSequence, CharSequence]

    for (i <- 0 until numHeaders) {
      val keyLength = in.readInt()
      val keyBuff = new Array[Byte](keyLength)
      in.read(keyBuff)
      val key : String = Utils.deserialize(keyBuff)

      val valLength = in.readInt()
      val valBuff = new Array[Byte](valLength)
      in.read(valBuff)
      val value : String = Utils.deserialize(valBuff)

      headers.put(key, value)
    }

    event.setBody(ByteBuffer.wrap(bodyBuff))
    event.setHeaders(headers)
  }

  /* Serialize to bytes. */
  def writeExternal(out: ObjectOutput) {
    val body = event.getBody.array()
    out.writeInt(body.length)
    out.write(body)

    val numHeaders = event.getHeaders.size()
    out.writeInt(numHeaders)
    for ((k, v) <- event.getHeaders) {
      val keyBuff = Utils.serialize(k.toString)
      out.writeInt(keyBuff.length)
      out.write(keyBuff)
      val valBuff = Utils.serialize(v.toString)
      out.writeInt(valBuff.length)
      out.write(valBuff)
    }
  }
}

private[streaming] object SparkFlumeEvent {
  def fromAvroFlumeEvent(in : AvroFlumeEvent) : SparkFlumeEvent = {
    val event = new SparkFlumeEvent
    event.event = in
    event
  }
}

/** A simple server that implements Flume's Avro protocol. */
private[streaming]
class FlumeEventServer(receiver : FlumeReceiver) extends AvroSourceProtocol {
  override def append(event : AvroFlumeEvent) : Status = {
    receiver.store(SparkFlumeEvent.fromAvroFlumeEvent(event))
    Status.OK
  }

  override def appendBatch(events : java.util.List[AvroFlumeEvent]) : Status = {
    events.foreach (event =>
      receiver.store(SparkFlumeEvent.fromAvroFlumeEvent(event)))
    Status.OK
  }
}

/** A NetworkReceiver which listens for events using the
  * Flume Avro interface. */
private[streaming]
class FlumeReceiver(
    host: String,
    port: Int,
    storageLevel: StorageLevel
  ) extends Receiver[SparkFlumeEvent](storageLevel) with Logging {

  lazy val responder = new SpecificResponder(
    classOf[AvroSourceProtocol], new FlumeEventServer(this))
  lazy val server = new NettyServer(responder, new InetSocketAddress(host, port))

  def onStart() {
    server.start()
    logInfo("Flume receiver started")
  }

  def onStop() {
    server.close()
    logInfo("Flume receiver stopped")
  }

  override def preferredLocation = Some(host)
}
