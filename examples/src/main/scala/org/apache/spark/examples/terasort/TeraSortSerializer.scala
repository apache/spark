/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.terasort

import org.apache.spark.serializer.{SerializationStream, DeserializationStream, SerializerInstance, Serializer}
import scala.reflect.ClassTag
import java.nio.ByteBuffer
import java.io.{OutputStream, InputStream}


class TeraSortSerializer extends Serializer with Serializable {
  override def newInstance(): SerializerInstance = new TeraSortSerializerInstance
}


class TeraSortSerializerInstance extends SerializerInstance {
  override def serialize[T: ClassTag](t: T) = throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException

  override def serializeStream(s: OutputStream) = new TeraSortSerializationStream(s)

  override def deserializeStream(s: InputStream) = new TeraSortDeserializationStream(s)
}


class TeraSortSerializationStream(stream: OutputStream) extends SerializationStream {
  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    stream.write(t.asInstanceOf[RecordWrapper]._2)
    this
  }

  override def flush(): Unit = stream.flush()

  override def close(): Unit = stream.close()
}


class TeraSortDeserializationStream(stream: InputStream) extends DeserializationStream {
  override def readObject[T: ClassTag](): T = {
    val record = new RecordWrapper(new Array[Byte](100))
    readFully(stream, record.bytes, 100)
    record.asInstanceOf[T]
  }

  override def close(): Unit = stream.close()

  def readFully(stream: InputStream, bytes: Array[Byte], length: Int) {
    var read = 0
    while (read < length) {
      val inc = stream.read(bytes, read, length - read)
      if (inc < 0) throw new java.io.EOFException
      read += inc
    }
  }

}
