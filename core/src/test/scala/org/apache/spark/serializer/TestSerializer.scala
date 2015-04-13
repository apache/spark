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

import java.io.{EOFException, OutputStream, InputStream}
import java.nio.ByteBuffer

import scala.reflect.ClassTag


/**
 * A serializer implementation that always return a single element in a deserialization stream.
 */
class TestSerializer extends Serializer {
  override def newInstance(): TestSerializerInstance = new TestSerializerInstance
}


class TestSerializerInstance extends SerializerInstance {
  override def serialize[T: ClassTag](t: T): ByteBuffer = ???

  override def serializeStream(s: OutputStream): SerializationStream = ???

  override def deserializeStream(s: InputStream): TestDeserializationStream =
    new TestDeserializationStream

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = ???

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = ???
}


class TestDeserializationStream extends DeserializationStream {

  private var count = 0

  override def readObject[T: ClassTag](): T = {
    count += 1
    if (count == 2) {
      throw new EOFException
    }
    new Object().asInstanceOf[T]
  }

  override def close(): Unit = {}
}
