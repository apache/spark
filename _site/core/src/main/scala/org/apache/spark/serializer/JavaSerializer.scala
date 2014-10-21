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

import java.io._
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.ByteBufferInputStream
import org.apache.spark.util.Utils

private[spark] class JavaSerializationStream(out: OutputStream, counterReset: Int)
  extends SerializationStream {
  private val objOut = new ObjectOutputStream(out)
  private var counter = 0

  /**
   * Calling reset to avoid memory leak:
   * http://stackoverflow.com/questions/1281549/memory-leak-traps-in-the-java-standard-api
   * But only call it every 100th time to avoid bloated serialization streams (when
   * the stream 'resets' object class descriptions have to be re-written)
   */
  def writeObject[T: ClassTag](t: T): SerializationStream = {
    objOut.writeObject(t)
    counter += 1
    if (counterReset > 0 && counter >= counterReset) {
      objOut.reset()
      counter = 0
    }
    this
  }

  def flush() { objOut.flush() }
  def close() { objOut.close() }
}

private[spark] class JavaDeserializationStream(in: InputStream, loader: ClassLoader)
extends DeserializationStream {
  private val objIn = new ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass) =
      Class.forName(desc.getName, false, loader)
  }

  def readObject[T: ClassTag](): T = objIn.readObject().asInstanceOf[T]
  def close() { objIn.close() }
}


private[spark] class JavaSerializerInstance(counterReset: Int, defaultClassLoader: ClassLoader)
  extends SerializerInstance {

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    val bos = new ByteArrayOutputStream()
    val out = serializeStream(bos)
    out.writeObject(t)
    out.close()
    ByteBuffer.wrap(bos.toByteArray)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis)
    in.readObject()
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis, loader)
    in.readObject()
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new JavaSerializationStream(s, counterReset)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new JavaDeserializationStream(s, Utils.getContextOrSparkClassLoader)
  }

  def deserializeStream(s: InputStream, loader: ClassLoader): DeserializationStream = {
    new JavaDeserializationStream(s, loader)
  }
}

/**
 * :: DeveloperApi ::
 * A Spark serializer that uses Java's built-in serialization.
 *
 * Note that this serializer is not guaranteed to be wire-compatible across different versions of
 * Spark. It is intended to be used to serialize/de-serialize data within a single
 * Spark application.
 */
@DeveloperApi
class JavaSerializer(conf: SparkConf) extends Serializer with Externalizable {
  private var counterReset = conf.getInt("spark.serializer.objectStreamReset", 100)

  override def newInstance(): SerializerInstance = {
    val classLoader = defaultClassLoader.getOrElse(Thread.currentThread.getContextClassLoader)
    new JavaSerializerInstance(counterReset, classLoader)
  }

  override def writeExternal(out: ObjectOutput) {
    out.writeInt(counterReset)
  }

  override def readExternal(in: ObjectInput) {
    counterReset = in.readInt()
  }
}
