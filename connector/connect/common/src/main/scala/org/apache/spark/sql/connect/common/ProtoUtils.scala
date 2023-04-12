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

package org.apache.spark.sql.connect.common

import scala.collection.JavaConverters._

import com.google.protobuf.{ByteString, Message}
import com.google.protobuf.Descriptors.FieldDescriptor

private[connect] object ProtoUtils {
  private val format = java.text.NumberFormat.getInstance()
  private val NUM_FIRST_BYTES = 8

  def abbreviateBytes(message: Message): Message = {
    val builder = message.toBuilder

    message.getAllFields.asScala.iterator.foreach {
      case (field: FieldDescriptor, byteString: ByteString)
          if field.getJavaType == FieldDescriptor.JavaType.BYTE_STRING && byteString != null =>
        val size = byteString.size()
        if (size > NUM_FIRST_BYTES) {
          val bytes = Array.ofDim[Byte](NUM_FIRST_BYTES)
          var i = 0
          while (i < NUM_FIRST_BYTES) {
            bytes(i) = byteString.byteAt(i)
            i += 1
          }
          builder.setField(field, createByteString(Some(bytes), size))
        } else {
          builder.setField(field, createByteString(None, size))
        }

      case (field: FieldDescriptor, byteArray: Array[Byte])
          if field.getJavaType == FieldDescriptor.JavaType.BYTE_STRING && byteArray != null =>
        val size = byteArray.length
        if (size > NUM_FIRST_BYTES) {
          builder.setField(field, createByteString(Some(byteArray.take(NUM_FIRST_BYTES)), size))
        } else {
          builder.setField(field, createByteString(None, size))
        }

      // TODO: should also support 1, repeated msg; 2, map<xxx, msg>
      case (field: FieldDescriptor, msg: Message)
          if field.getJavaType == FieldDescriptor.JavaType.MESSAGE && msg != null =>
        builder.setField(field, abbreviateBytes(msg))

      case (field: FieldDescriptor, value: Any) => builder.setField(field, value)
    }

    builder.build()
  }

  private def createByteString(firstBytes: Option[Array[Byte]], size: Int): ByteString = {
    var byteStrings = Array.empty[ByteString]
    firstBytes.foreach { bytes =>
      byteStrings :+= ByteString.copyFrom(bytes)
    }
    byteStrings :+= ByteString.copyFromUtf8(s"*********(redacted, size=${format.format(size)})")
    ByteString.copyFrom(byteStrings.toIterable.asJava)
  }
}
