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
import org.apache.spark.util.{ByteBufferInputStream, ByteBufferOutputStream, Utils}

private[spark] class JavaSerializationStream(
    out: OutputStream, counterReset: Int, extraDebugInfo: Boolean)
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
    try {
      objOut.writeObject(t)
    } catch {
      case e: NotSerializableException if extraDebugInfo =>
        throw SerializationDebugger.improveException(t, e)
    }
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
    override def resolveClass(desc: ObjectStreamClass): Class[_] =
      try {
        // scalastyle:off classforname
        Class.forName(desc.getName, false, loader)
        // scalastyle:on classforname
      } catch {
        case e: ClassNotFoundException =>
          JavaDeserializationStream.primitiveMappings.getOrElse(desc.getName, throw e)
      }
  }

  def readObject[T: ClassTag](): T = objIn.readObject().asInstanceOf[T]
  def close() { objIn.close() }
}

private[spark] object JavaDeserializationStream {
  val primitiveMappings = Map[String, Class[_]](
    "boolean" -> classOf[Boolean],
    "byte" -> classOf[Byte],
    "char" -> classOf[Char],
    "short" -> classOf[Short],
    "int" -> classOf[Int],
    "long" -> classOf[Long],
    "float" -> classOf[Float],
    "double" -> classOf[Double],
    "void" -> classOf[Void]
  )
}

private[spark] class JavaClassSpecificSerializationStream[T: ClassTag](
    out: OutputStream, counterReset: Int, extraDebugInfo: Boolean
  ) extends ClassSpecificSerializationStream[T] {

  private val objOut = new ObjectOutputStream(out)
  private var counter = 0

  override def writeObject(t: T): ClassSpecificSerializationStream[T] = {
    try {
      objOut.writeObject(t)
    } catch {
      case e: NotSerializableException if extraDebugInfo =>
        throw SerializationDebugger.improveException(t, e)
    }
    counter += 1
    if (counterReset > 0 && counter >= counterReset) {
      objOut.reset()
      counter = 0
    }
    this
  }

  override def flush(): Unit = {objOut.flush()}

  override def close(): Unit = {objOut.close()}

  override protected def classWrote: Boolean = throw new UnsupportedOperationException
  override protected def writeClass(clazz: Class[T]): ClassSpecificSerializationStream[T] = {
    throw new UnsupportedOperationException
  }
  override protected def writeObjectWithoutClass(t: T): ClassSpecificSerializationStream[T] = {
    throw new UnsupportedOperationException
  }
}

private[spark] class JavaClassSpecificDeserializationStream[T: ClassTag](
    in: InputStream, loader: ClassLoader)
  extends ClassSpecificDeserializationStream[T] {

  private val objIn = new ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass): Class[_] =
      try {
        // scalastyle:off classforname
        Class.forName(desc.getName, false, loader)
        // scalastyle:on classforname
      } catch {
        case e: ClassNotFoundException =>
          JavaDeserializationStream.primitiveMappings.getOrElse(desc.getName, throw e)
      }
  }

  override def readObject(): T = objIn.readObject().asInstanceOf[T]
  def close() { objIn.close() }

  override protected def classRead: Boolean = throw new UnsupportedOperationException
  override protected def classInfo: Class[T] = throw new UnsupportedOperationException
  override protected def readClass(): Class[T] = throw new UnsupportedOperationException
  override protected def readObjectWithoutClass(clazz: Class[T]): T = {
    throw new UnsupportedOperationException
  }
}

private[spark] class JavaKVClassSpecificSerializationStream[K: ClassTag, V: ClassTag](
    out: OutputStream, counterReset: Int, extraDebugInfo: Boolean
    ) extends KVClassSpecificSerializationStream[K, V] {

  private val objOut = new ObjectOutputStream(out)
  private var counter = 0

  override def writeKey(t: K): KVClassSpecificSerializationStream[K, V] = {
    writeObject(t)
  }

  override def writeValue(t: V): KVClassSpecificSerializationStream[K, V] = {
    writeObject(t)
  }

  private[this] def writeObject[T: ClassTag](t: T): KVClassSpecificSerializationStream[K, V] = {
    try {
      objOut.writeObject(t)
    } catch {
      case e: NotSerializableException if extraDebugInfo =>
        throw SerializationDebugger.improveException(t, e)
    }
    counter += 1
    if (counterReset > 0 && counter >= counterReset) {
      objOut.reset()
      counter = 0
    }
    this
  }

  override def flush(): Unit = {objOut.flush()}

  override def close(): Unit = {objOut.close()}

  override protected def keyClassWrote: Boolean = throw new UnsupportedOperationException
  override protected def valueClassWrote: Boolean = throw new UnsupportedOperationException
  override protected def writeClass[T](
      clazz: Class[T]): KVClassSpecificSerializationStream[K, V] = {
    throw new UnsupportedOperationException
  }
  override protected def writeObjectWithoutClass[T](
      t: T): KVClassSpecificSerializationStream[K, V] = {
    throw new UnsupportedOperationException
  }
}

private[spark] class JavaKVClassSpecificDeserializationStream[K: ClassTag, V: ClassTag](
    in: InputStream, loader: ClassLoader) extends KVClassSpecificDeserializationStream[K, V] {

  private val objIn = new ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass): Class[_] =
      try {
        // scalastyle:off classforname
        Class.forName(desc.getName, false, loader)
        // scalastyle:on classforname
      } catch {
        case e: ClassNotFoundException =>
          JavaDeserializationStream.primitiveMappings.getOrElse(desc.getName, throw e)
      }
  }

  override def readKey(): K = readObject[K]()

  override def readValue(): V = readObject[V]()

  private[this] def readObject[T](): T = objIn.readObject().asInstanceOf[T]
  def close() { objIn.close() }

  override protected def keyClassRead: Boolean = throw new UnsupportedOperationException
  override protected def valueClassRead: Boolean = throw new UnsupportedOperationException
  override protected def keyClassInfo: Class[K] = throw new UnsupportedOperationException
  override protected def valueClassInfo: Class[V] = throw new UnsupportedOperationException
  override protected def readClass[T](): Class[T] = throw new UnsupportedOperationException
  override protected def readObjectWithoutClass[T](clazz: Class[T]): T = {
    throw new UnsupportedOperationException
  }
}


private[spark] class JavaSerializerInstance(
    counterReset: Int, extraDebugInfo: Boolean, defaultClassLoader: ClassLoader)
  extends SerializerInstance {

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    val bos = new ByteBufferOutputStream()
    val out = serializeStream(bos)
    out.writeObject(t)
    out.close()
    bos.toByteBuffer
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
    new JavaSerializationStream(s, counterReset, extraDebugInfo)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new JavaDeserializationStream(s, defaultClassLoader)
  }

  def deserializeStream(s: InputStream, loader: ClassLoader): DeserializationStream = {
    new JavaDeserializationStream(s, loader)
  }

  override def serializeStreamForClass[T: ClassTag](
      s: OutputStream): ClassSpecificSerializationStream[T] = {
    new JavaClassSpecificSerializationStream[T](s, counterReset, extraDebugInfo)
  }

  override def serializeStreamForKVClass[K: ClassTag, V: ClassTag](
      s: OutputStream): KVClassSpecificSerializationStream[K, V] = {
    new JavaKVClassSpecificSerializationStream[K, V](s, counterReset, extraDebugInfo)
  }

  override def deserializeStreamForClass[T: ClassTag](
      s: InputStream): ClassSpecificDeserializationStream[T] = {
    new JavaClassSpecificDeserializationStream[T](s, defaultClassLoader)
  }

  override def deserializeStreamForKVClass[K: ClassTag, V: ClassTag](
      s: InputStream): KVClassSpecificDeserializationStream[K, V] = {
    new JavaKVClassSpecificDeserializationStream[K, V](s, defaultClassLoader)
  }
}

/**
 * :: DeveloperApi ::
 * A Spark serializer that uses Java's built-in serialization.
 *
 * @note This serializer is not guaranteed to be wire-compatible across different versions of
 * Spark. It is intended to be used to serialize/de-serialize data within a single
 * Spark application.
 */
@DeveloperApi
class JavaSerializer(conf: SparkConf) extends Serializer with Externalizable {
  private var counterReset = conf.getInt("spark.serializer.objectStreamReset", 100)
  private var extraDebugInfo = conf.getBoolean("spark.serializer.extraDebugInfo", true)

  protected def this() = this(new SparkConf())  // For deserialization only

  override def newInstance(): SerializerInstance = {
    val classLoader = defaultClassLoader.getOrElse(Thread.currentThread.getContextClassLoader)
    new JavaSerializerInstance(counterReset, extraDebugInfo, classLoader)
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeInt(counterReset)
    out.writeBoolean(extraDebugInfo)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    counterReset = in.readInt()
    extraDebugInfo = in.readBoolean()
  }
}
