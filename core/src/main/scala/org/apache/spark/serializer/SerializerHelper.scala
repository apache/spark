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

import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

private[spark] object SerializerHelper extends Logging {

  /**
   *
   * @param serializerInstance instance of SerializerInstance
   * @param objectToSerialize the object to serialize, of type `T`
   * @param estimatedSize estimated size of `t`, used as a hint to choose proper chunk size
   */
  def serializeToChunkedBuffer[T: ClassTag](
      serializerInstance: SerializerInstance,
      objectToSerialize: T,
      estimatedSize: Long = -1): ChunkedByteBuffer = {
    val chunkSize = ChunkedByteBuffer.estimateBufferChunkSize(estimatedSize)
    val cbbos = new ChunkedByteBufferOutputStream(chunkSize, ByteBuffer.allocate)
    val out = serializerInstance.serializeStream(cbbos)
    out.writeObject(objectToSerialize)
    out.close()
    cbbos.close()
    cbbos.toChunkedByteBuffer
  }

  def deserializeFromChunkedBuffer[T: ClassTag](
      serializerInstance: SerializerInstance,
      bytes: ChunkedByteBuffer): T = {
    val in = serializerInstance.deserializeStream(bytes.toInputStream())
    val res = in.readObject()
    in.close()
    res
  }
}
