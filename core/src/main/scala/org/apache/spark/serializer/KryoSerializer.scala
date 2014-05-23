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

package org.apache.spark.serializer

import java.io.{EOFException, InputStream, OutputStream}
import java.nio.ByteBuffer

import com.esotericsoftware.kryo.{Kryo, KryoException}
import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer}
import com.twitter.chill.{AllScalaRegistrar, EmptyScalaKryoInstantiator}

import org.apache.spark._
import org.apache.spark.broadcast.HttpBroadcast
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.storage._
import org.apache.spark.storage.{GetBlock, GotBlock, PutBlock}

import scala.reflect.ClassTag

/**
 * A Spark serializer that uses the [[https://code.google.com/p/kryo/ Kryo serialization library]].
 *
 * Note that this serializer is not guaranteed to be wire-compatible across different versions of
 * Spark. It is intended to be used to serialize/de-serialize data within a single
 * Spark application.
 */
class KryoSerializer(conf: SparkConf)
  extends org.apache.spark.serializer.Serializer
  with Logging
  with Serializable {

  private val bufferSize = conf.getInt("spark.kryoserializer.buffer.mb", 2) * 1024 * 1024
  private val referenceTracking = conf.getBoolean("spark.kryo.referenceTracking", true)
  private val registrator = conf.getOption("spark.kryo.registrator")

  def newKryoOutput() = new KryoOutput(bufferSize)

  def newKryo(): Kryo = {
    val instantiator = new EmptyScalaKryoInstantiator
    val kryo = instantiator.newKryo()
    val classLoader = Thread.currentThread.getContextClassLoader

    // Allow disabling Kryo reference tracking if user knows their object graphs don't have loops.
    // Do this before we invoke the user registrator so the user registrator can override this.
    kryo.setReferences(referenceTracking)

    for (cls <- KryoSerializer.toRegister) {
      kryo.register(cls)
    }

    // Allow sending SerializableWritable
    kryo.register(classOf[SerializableWritable[_]], new KryoJavaSerializer())
    kryo.register(classOf[HttpBroadcast[_]], new KryoJavaSerializer())

    // Allow the user to register their own classes by setting spark.kryo.registrator
    try {
      for (regCls <- registrator) {
        logDebug("Running user registrator: " + regCls)
        val reg = Class.forName(regCls, true, classLoader).newInstance()
          .asInstanceOf[KryoRegistrator]
        reg.registerClasses(kryo)
      }
    } catch {
      case e: Exception => logError("Failed to run spark.kryo.registrator", e)
    }

    // Register Chill's classes; we do this after our ranges and the user's own classes to let
    // our code override the generic serializers in Chill for things like Seq
    new AllScalaRegistrar().apply(kryo)

    kryo.setClassLoader(classLoader)
    kryo
  }

  def newInstance(): SerializerInstance = {
    new KryoSerializerInstance(this)
  }
}

private[spark]
class KryoSerializationStream(kryo: Kryo, outStream: OutputStream) extends SerializationStream {
  val output = new KryoOutput(outStream)

  def writeObject[T: ClassTag](t: T): SerializationStream = {
    kryo.writeClassAndObject(output, t)
    this
  }

  def flush() { output.flush() }
  def close() { output.close() }
}

private[spark]
class KryoDeserializationStream(kryo: Kryo, inStream: InputStream) extends DeserializationStream {
  val input = new KryoInput(inStream)

  def readObject[T: ClassTag](): T = {
    try {
      kryo.readClassAndObject(input).asInstanceOf[T]
    } catch {
      // DeserializationStream uses the EOF exception to indicate stopping condition.
      case e: KryoException if e.getMessage.toLowerCase.contains("buffer underflow") =>
        throw new EOFException
    }
  }

  def close() {
    // Kryo's Input automatically closes the input stream it is using.
    input.close()
  }
}

private[spark] class KryoSerializerInstance(ks: KryoSerializer) extends SerializerInstance {
  val kryo = ks.newKryo()

  // Make these lazy vals to avoid creating a buffer unless we use them
  lazy val output = ks.newKryoOutput()
  lazy val input = new KryoInput()

  def serialize[T: ClassTag](t: T): ByteBuffer = {
    output.clear()
    kryo.writeClassAndObject(output, t)
    ByteBuffer.wrap(output.toBytes)
  }

  def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    input.setBuffer(bytes.array)
    kryo.readClassAndObject(input).asInstanceOf[T]
  }

  def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    val oldClassLoader = kryo.getClassLoader
    kryo.setClassLoader(loader)
    input.setBuffer(bytes.array)
    val obj = kryo.readClassAndObject(input).asInstanceOf[T]
    kryo.setClassLoader(oldClassLoader)
    obj
  }

  def serializeStream(s: OutputStream): SerializationStream = {
    new KryoSerializationStream(kryo, s)
  }

  def deserializeStream(s: InputStream): DeserializationStream = {
    new KryoDeserializationStream(kryo, s)
  }
}

/**
 * Interface implemented by clients to register their classes with Kryo when using Kryo
 * serialization.
 */
trait KryoRegistrator {
  def registerClasses(kryo: Kryo)
}

private[serializer] object KryoSerializer {
  // Commonly used classes.
  private val toRegister: Seq[Class[_]] = Seq(
    ByteBuffer.allocate(1).getClass,
    classOf[StorageLevel],
    classOf[PutBlock],
    classOf[GotBlock],
    classOf[GetBlock],
    classOf[MapStatus],
    classOf[BlockManagerId],
    classOf[Array[Byte]]
  )
}
