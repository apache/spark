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
import javax.annotation.concurrent.NotThreadSafe

import scala.reflect._

import org.apache.spark.annotation.{DeveloperApi, Private}
import org.apache.spark.util.NextIterator

/**
 * :: DeveloperApi ::
 * A serializer. Because some serialization libraries are not thread safe, this class is used to
 * create [[org.apache.spark.serializer.SerializerInstance]] objects that do the actual
 * serialization and are guaranteed to only be called from one thread at a time.
 *
 * Implementations of this trait should implement:
 *
 * 1. a zero-arg constructor or a constructor that accepts a [[org.apache.spark.SparkConf]]
 * as parameter. If both constructors are defined, the latter takes precedence.
 *
 * 2. Java serialization interface.
 *
 * @note Serializers are not required to be wire-compatible across different versions of Spark.
 * They are intended to be used to serialize/de-serialize data within a single Spark application.
 */
@DeveloperApi
abstract class Serializer {

  /**
   * Default ClassLoader to use in deserialization. Implementations of [[Serializer]] should
   * make sure it is using this when set.
   */
  @volatile protected var defaultClassLoader: Option[ClassLoader] = None

  /**
   * Sets a class loader for the serializer to use in deserialization.
   *
   * @return this Serializer object
   */
  def setDefaultClassLoader(classLoader: ClassLoader): Serializer = {
    defaultClassLoader = Some(classLoader)
    this
  }

  /** Creates a new [[SerializerInstance]]. */
  def newInstance(): SerializerInstance

  /**
   * :: Private ::
   * Returns true if this serializer supports relocation of its serialized objects and false
   * otherwise. This should return true if and only if reordering the bytes of serialized objects
   * in serialization stream output is equivalent to having re-ordered those elements prior to
   * serializing them. More specifically, the following should hold if a serializer supports
   * relocation:
   *
   * {{{
   * serOut.open()
   * position = 0
   * serOut.write(obj1)
   * serOut.flush()
   * position = # of bytes written to stream so far
   * obj1Bytes = output[0:position-1]
   * serOut.write(obj2)
   * serOut.flush()
   * position2 = # of bytes written to stream so far
   * obj2Bytes = output[position:position2-1]
   * serIn.open([obj2bytes] concatenate [obj1bytes]) should return (obj2, obj1)
   * }}}
   *
   * In general, this property should hold for serializers that are stateless and that do not
   * write special metadata at the beginning or end of the serialization stream.
   *
   * This API is private to Spark; this method should not be overridden in third-party subclasses
   * or called in user code and is subject to removal in future Spark releases.
   *
   * See SPARK-7311 for more details.
   */
  @Private
  private[spark] def supportsRelocationOfSerializedObjects: Boolean = false
}


/**
 * :: DeveloperApi ::
 * An instance of a serializer, for use by one thread at a time.
 *
 * It is legal to create multiple serialization / deserialization streams from the same
 * SerializerInstance as long as those streams are all used within the same thread.
 */
@DeveloperApi
@NotThreadSafe
abstract class SerializerInstance {
  def serialize[T: ClassTag](t: T): ByteBuffer

  def deserialize[T: ClassTag](bytes: ByteBuffer): T

  def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T

  def serializeStream(s: OutputStream): SerializationStream

  def serializeStreamForClass[T: ClassTag](s: OutputStream): ClassSpecificSerializationStream[T]

  def serializeStreamForKVClass[K: ClassTag, V: ClassTag](
      s: OutputStream): KVClassSpecificSerializationStream[K, V]

  def deserializeStream(s: InputStream): DeserializationStream

  def deserializeStreamForClass[T: ClassTag](s: InputStream): ClassSpecificDeserializationStream[T]

  def deserializeStreamForKVClass[K: ClassTag, V: ClassTag](
      s: InputStream): KVClassSpecificDeserializationStream[K, V]
}

/**
 * :: DeveloperApi ::
 * A stream for writing serialized objects.
 */
@DeveloperApi
abstract class SerializationStream extends Closeable {
  /** The most general-purpose method to write an object. */
  def writeObject[T: ClassTag](t: T): SerializationStream
  /** Writes the object representing the key of a key-value pair. */
  def writeKey[T: ClassTag](key: T): SerializationStream = writeObject(key)
  /** Writes the object representing the value of a key-value pair. */
  def writeValue[T: ClassTag](value: T): SerializationStream = writeObject(value)
  def flush(): Unit
  override def close(): Unit

  def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
    while (iter.hasNext) {
      writeObject(iter.next())
    }
    this
  }
}


/**
 * :: DeveloperApi ::
 * A stream for reading serialized objects.
 */
@DeveloperApi
abstract class DeserializationStream extends Closeable {
  /** The most general-purpose method to read an object. */
  def readObject[T: ClassTag](): T
  /** Reads the object representing the key of a key-value pair. */
  def readKey[T: ClassTag](): T = readObject[T]()
  /** Reads the object representing the value of a key-value pair. */
  def readValue[T: ClassTag](): T = readObject[T]()
  override def close(): Unit

  /**
   * Read the elements of this stream through an iterator. This can only be called once, as
   * reading each element will consume data from the input source.
   */
  def asIterator: Iterator[Any] = new NextIterator[Any] {
    override protected def getNext() = {
      try {
        readObject[Any]()
      } catch {
        case eof: EOFException =>
          finished = true
          null
      }
    }

    override protected def close() {
      DeserializationStream.this.close()
    }
  }

  /**
   * Read the elements of this stream through an iterator over key-value pairs. This can only be
   * called once, as reading each element will consume data from the input source.
   */
  def asKeyValueIterator: Iterator[(Any, Any)] = new NextIterator[(Any, Any)] {
    override protected def getNext() = {
      try {
        (readKey[Any](), readValue[Any]())
      } catch {
        case eof: EOFException =>
          finished = true
          null
      }
    }

    override protected def close() {
      DeserializationStream.this.close()
    }
  }
}

abstract class ClassSpecificSerializationStream[T: ClassTag] extends Closeable{
  /** Indicates whether the class has been wrote. */
  protected def classWrote: Boolean
  /** Writes the class. */
  protected def writeClass(clazz: Class[T]): ClassSpecificSerializationStream[T]
  /** Writes the object without class. */
  protected def writeObjectWithoutClass(t: T): ClassSpecificSerializationStream[T]
  /** The most general-purpose method to write an object. */
  def writeObject(t: T): ClassSpecificSerializationStream[T] = {
    if (!classWrote) {
      writeClass(classTag[T].runtimeClass.asInstanceOf[Class[T]])
    }

    writeObjectWithoutClass(t)
    this
  }

  def writeAll(iter: Iterator[T]): ClassSpecificSerializationStream[T] = {
    while (iter.hasNext) {
      writeObject(iter.next())
    }
    this
  }

  def flush(): Unit

  override def close(): Unit
}

abstract class ClassSpecificDeserializationStream[T: ClassTag] extends Closeable {
  /** Indicates whether the class has read. */
  protected def classRead: Boolean
  /** The read classes. */
  protected def classInfo: Class[T]
  /** Reads the object class. */
  protected def readClass(): Class[T]
  /** Reads the object without class. */
  protected def readObjectWithoutClass(clazz: Class[T]): T
  /** The most general-purpose method to read an object. */
  def readObject(): T = {
    if (!classRead) {
      readClass()
      assert(classInfo != null)
    }

    readObjectWithoutClass(classInfo)
  }

  /**
    * Read the elements of this stream through an iterator. This can only be called once, as
    * reading each element will consume data from the input source.
    */
  def asIterator: Iterator[Any] = new NextIterator[Any] {
    override protected def getNext() = {
      try {
        readObject()
      } catch {
        case eof: EOFException =>
          finished = true
          null
      }
    }

    override protected def close() {
      ClassSpecificDeserializationStream.this.close()
    }
  }

  override def close(): Unit
}

abstract class KVClassSpecificSerializationStream[K: ClassTag, V: ClassTag] extends Closeable {
  /** Indicates whether the key class has been wrote. */
  protected def keyClassWrote: Boolean
  /** Indicates whether the value class has been wrote. */
  protected def valueClassWrote: Boolean
  /** Writes the class. */
  protected def writeClass[T](clazz: Class[T]): KVClassSpecificSerializationStream[K, V]
  /** Writes the object without class. */
  protected def writeObjectWithoutClass[T](t: T): KVClassSpecificSerializationStream[K, V]
  /** Writes the key object, and only writes the key class once. */
  def writeKey(t: K): KVClassSpecificSerializationStream[K, V] = {
    if (!keyClassWrote) {
      writeClass[K](classTag[K].runtimeClass.asInstanceOf[Class[K]])
    }

    writeObjectWithoutClass[K](t)
    this
  }
  /** Writes the value object, and only writes the value class once. */
  def writeValue(t: V): KVClassSpecificSerializationStream[K, V] = {
    if (!valueClassWrote) {
      writeClass[V](classTag[V].runtimeClass.asInstanceOf[Class[V]])
    }

    writeObjectWithoutClass[V](t)
    this
  }

  def flush(): Unit

  override def close(): Unit
}

abstract class KVClassSpecificDeserializationStream[K: ClassTag, V: ClassTag] extends Closeable {
  /** Indicate whether the key class has read. */
  protected def keyClassRead: Boolean
  /** Indicate whether the value class has read. */
  protected def valueClassRead: Boolean
  /** The read key class. */
  protected def keyClassInfo: Class[K]
  /** The read value class. */
  protected def valueClassInfo: Class[V]
  /** Reads the object class. */
  protected def readClass[T](): Class[T]
  /** Reads the object without class. */
  protected def readObjectWithoutClass[T](clazz: Class[T]): T
  /** Reads the key object, and only reads the key class once. */
  def readKey(): K = {
    if (!keyClassRead) {
      readClass[K]()
      assert(keyClassInfo != null)
    }

    readObjectWithoutClass[K](keyClassInfo)
  }
  /** Reads the value object, and only reads the value class once. */
  def readValue(): V = {
    if (!valueClassRead) {
      readClass[V]()
      assert(valueClassInfo != null)
    }

    readObjectWithoutClass[V](valueClassInfo)
  }

  /**
    * Read the elements of this stream through an iterator over key-value pairs. This can only be
    * called once, as reading each element will consume data from the input source.
    */
  def asKeyValueIterator: Iterator[(Any, Any)] = new NextIterator[(Any, Any)] {
    override protected def getNext() = {
      try {
        (readKey(), readValue())
      } catch {
        case eof: EOFException =>
          finished = true
          null
      }
    }

    override protected def close() {
      KVClassSpecificDeserializationStream.this.close()
    }
  }

  override def close(): Unit
}
