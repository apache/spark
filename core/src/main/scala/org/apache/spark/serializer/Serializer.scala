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

import java.io.{ByteArrayOutputStream, EOFException, InputStream, OutputStream}
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.spark.SparkEnv
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.{ByteBufferInputStream, NextIterator}

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
 * Note that serializers are not required to be wire-compatible across different versions of Spark.
 * They are intended to be used to serialize/de-serialize data within a single Spark application.
 */
@DeveloperApi
trait Serializer {
  def newInstance(): SerializerInstance
}


object Serializer {
  def getSerializer(serializer: Serializer): Serializer = {
    if (serializer == null) SparkEnv.get.serializer else serializer
  }

  def getSerializer(serializer: Option[Serializer]): Serializer = {
    serializer.getOrElse(SparkEnv.get.serializer)
  }
}


/**
 * :: DeveloperApi ::
 * An instance of a serializer, for use by one thread at a time.
 */
@DeveloperApi
trait SerializerInstance {
  def serialize[T: ClassTag](t: T): ByteBuffer

  def deserialize[T: ClassTag](bytes: ByteBuffer): T

  def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T

  def serializeStream(s: OutputStream): SerializationStream

  def deserializeStream(s: InputStream): DeserializationStream

  def serializeMany[T: ClassTag](iterator: Iterator[T]): ByteBuffer = {
    // Default implementation uses serializeStream
    val stream = new ByteArrayOutputStream()
    serializeStream(stream).writeAll(iterator)
    val buffer = ByteBuffer.wrap(stream.toByteArray)
    buffer.flip()
    buffer
  }

  def deserializeMany(buffer: ByteBuffer): Iterator[Any] = {
    // Default implementation uses deserializeStream
    buffer.rewind()
    deserializeStream(new ByteBufferInputStream(buffer)).asIterator
  }
}

/**
 * :: DeveloperApi ::
 * A stream for writing serialized objects.
 */
@DeveloperApi
trait SerializationStream {
  def writeObject[T: ClassTag](t: T): SerializationStream
  def flush(): Unit
  def close(): Unit

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
trait DeserializationStream {
  def readObject[T: ClassTag](): T
  def close(): Unit

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
      }
    }

    override protected def close() {
      DeserializationStream.this.close()
    }
  }
}
