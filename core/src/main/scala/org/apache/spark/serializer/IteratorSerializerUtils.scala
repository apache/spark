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

import org.apache.spark.util.ByteBufferOutputStream


object IteratorSerializerUtils {

  def serialize(iterator: Iterator[ByteBuffer], count: Int): ByteBuffer = {
    val array = iterator.toArray
    val lengthSum = array.map(_.array().length).sum + 4
    val bos = new ByteBufferOutputStream(lengthSum)
    bos.write(intToByteArrayBigEndian(count))
    for (buffer <- array) {
      bos.write(buffer.array())
    }
    bos.close()
    bos.toByteBuffer
  }

  def deserialize(byteIterator: Iterator[ByteBuffer]): (Iterator[ByteBuffer], Int) = {
    val firstBuffer = byteIterator.next().array()
    val countBytes = firstBuffer.slice(0, 4)
    val size = byteArrayToIntBigEndian(countBytes)
    val bufferIterator = Array(ByteBuffer.wrap(firstBuffer, 4,
      firstBuffer.length - 4)).iterator ++ byteIterator
    (bufferIterator, size)
  }

  def intToByteArrayBigEndian(x: Int): Array[Byte] = {
    val bytes = new Array[Byte](4)
    bytes(0) = (x >> 24).toByte
    bytes(1) = (x >> 16).toByte
    bytes(2) = (x >> 8).toByte
    bytes(3) = x.toByte
    bytes
  }

  def byteArrayToIntBigEndian(bytes: Array[Byte]): Int = {
    var x = 0
    var i = 0
    for (i <- 0 to 3) {
      x <<= 8
      val b = bytes(i) & 0xFF
      x |= b
    }
    x
  }
}
