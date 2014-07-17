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
import java.io.FileInputStream;
import java.nio.ByteBuffer
import java.security.KeyStore;
import java.security.Security;
import java.util.concurrent.Executors

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.apache.flume.source.avro.AvroSourceProtocol
import org.apache.flume.source.avro.AvroFlumeEvent
import org.apache.flume.source.avro.Status
import org.apache.avro.ipc.specific.SpecificResponder
import org.apache.avro.ipc.NettyServer
import org.apache.spark.Logging
import org.apache.spark.util.Utils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver.Receiver

import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.channel.ChannelFactory
import org.jboss.netty.handler.codec.compression._
import org.jboss.netty.handler.execution.ExecutionHandler
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.handler.ssl.SslHandler;

private[streaming]
class FlumeInputDStream[T: ClassTag](
  @transient ssc_ : StreamingContext,
  host: String,
  port: Int,
  storageLevel: StorageLevel,
  enableDecompression: Boolean,
  enableSsl: Boolean,
  keystore: String,
  keystorePassword: String, 
  keystoreType: String
) extends ReceiverInputDStream[SparkFlumeEvent](ssc_) {

  def this (ssc_ : StreamingContext,
    host: String,
    port: Int,
    storageLevel: StorageLevel,
    enableDecompression: Boolean) = this(ssc_,
        host, 
        port, 
        storageLevel, 
        enableDecompression, 
        false, 
        null, 
        null, 
        null)
  
  override def getReceiver(): Receiver[SparkFlumeEvent] = {
    new FlumeReceiver(
        host, 
        port, 
        storageLevel, 
        enableDecompression, 
        enableSsl, 
        keystore, 
        keystorePassword, 
        keystoreType)
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
      in.readFully(keyBuff)
      val key : String = Utils.deserialize(keyBuff)

      val valLength = in.readInt()
      val valBuff = new Array[Byte](valLength)
      in.readFully(valBuff)
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
    storageLevel: StorageLevel,
    enableDecompression: Boolean,
    enableSsl: Boolean,
    keystore: String,
    keystorePassword: String, 
    keystoreType: String
  ) extends Receiver[SparkFlumeEvent](storageLevel) with Logging {

  lazy val responder = new SpecificResponder(
    classOf[AvroSourceProtocol], new FlumeEventServer(this))
  var server: NettyServer = null

  private def initServer() = {
    if (enableDecompression || enableSsl) {
      val channelFactory = new NioServerSocketChannelFactory
        (Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
      val channelPipelieFactory = new SSLCompressionChannelPipelineFactory(
          enableDecompression,
          enableSsl,
          keystore,
          keystorePassword, 
          keystoreType)
      
      new NettyServer(
        responder, 
        new InetSocketAddress(host, port),
        channelFactory, 
        channelPipelieFactory, 
        null)
    } else {
      new NettyServer(responder, new InetSocketAddress(host, port))
    }
  }

  def onStart() {
    synchronized {
      if (server == null) {
        server = initServer()
        server.start()
      } else {
        logWarning("Flume receiver being asked to start more then once with out close")
      }
    }
    logInfo("Flume receiver started")
  }

  def onStop() {
    synchronized {
      if (server != null) {
        server.close()
        server = null
      }
    }
    logInfo("Flume receiver stopped")
  }

  override def preferredLocation = Some(host)
  
  /** A Netty Pipeline factory that will decompress incoming data from 
    * and the Netty client and compress data going back to the client.
    *
    * The compression on the return is required because Flume requires
    * a successful response to indicate it can remove the event/batch 
    * from the configured channel 
    */
  private[streaming]
  class SSLCompressionChannelPipelineFactory (
    enableDecompression: Boolean,
    enableSsl: Boolean,
    keystore: String,
    keystorePassword: String, 
    keystoreType: String) 
    extends ChannelPipelineFactory {

    def createServerSSLContext():SSLContext = {
      try {
        val ks = KeyStore.getInstance(keystoreType)
        ks.load(new FileInputStream(keystore), keystorePassword.toCharArray())
	
        // Set up key manager factory to use our key store
        val kmf = KeyManagerFactory.getInstance(getAlgorithm())
        kmf.init(ks, keystorePassword.toCharArray())
	
        val serverContext = SSLContext.getInstance("TLS")
        serverContext.init(kmf.getKeyManagers(), null, null)
	      
        serverContext
      } catch {
        case e: Exception => {
          throw new RuntimeException(e);
        }
      }
      
    }
    
    def  getAlgorithm():String = {
      var algorithm = Security.getProperty("ssl.KeyManagerFactory.algorithm")   
      if (algorithm == null) {
        algorithm = "SunX509"
      }
      algorithm
    }
    
    def getPipeline() = {
      val pipeline = Channels.pipeline()
      if (enableDecompression) {
        val encoder = new ZlibEncoder(6)
        pipeline.addFirst("deflater", encoder)
        pipeline.addFirst("inflater", new ZlibDecoder())
        
      } 
      if (enableSsl) {
        val sslEngine = createServerSSLContext().createSSLEngine();
        sslEngine.setUseClientMode(false);
        pipeline.addFirst("ssl", new SslHandler(sslEngine));
      }
      pipeline
    }
  }
}
