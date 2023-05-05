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
  private val MAX_BYTES_SIZE = 8
  private val MAX_STRING_SIZE = 1024

  def abbreviate(message: Message): Message = {
    val builder = message.toBuilder

    message.getAllFields.asScala.iterator.foreach {
      case (field: FieldDescriptor, string: String)
          if field.getJavaType == FieldDescriptor.JavaType.STRING && string != null =>
        val size = string.size
        if (size > MAX_STRING_SIZE) {
          builder.setField(field, createString(string.take(MAX_STRING_SIZE), size))
        } else {
          builder.setField(field, string)
        }

      case (field: FieldDescriptor, byteString: ByteString)
          if field.getJavaType == FieldDescriptor.JavaType.BYTE_STRING && byteString != null =>
        val size = byteString.size
        if (size > MAX_BYTES_SIZE) {
          val prefix = Array.tabulate(MAX_BYTES_SIZE)(byteString.byteAt)
          builder.setField(field, createByteString(prefix, size))
        } else {
          builder.setField(field, byteString)
        }

      case (field: FieldDescriptor, byteArray: Array[Byte])
          if field.getJavaType == FieldDescriptor.JavaType.BYTE_STRING && byteArray != null =>
        val size = byteArray.size
        if (size > MAX_BYTES_SIZE) {
          val prefix = byteArray.take(MAX_BYTES_SIZE)
          builder.setField(field, createByteString(prefix, size))
        } else {
          builder.setField(field, byteArray)
        }

      // TODO(SPARK-43117): should also support 1, repeated msg; 2, map<xxx, msg>
      case (field: FieldDescriptor, msg: Message)
          if field.getJavaType == FieldDescriptor.JavaType.MESSAGE && msg != null =>
        builder.setField(field, abbreviate(msg))

      case (field: FieldDescriptor, value: Any) => builder.setField(field, value)
    }

    builder.build()
  }

  private def createByteString(prefix: Array[Byte], size: Int): ByteString = {
    ByteString.copyFrom(
      List(
        ByteString.copyFrom(prefix),
        ByteString.copyFromUtf8(s"[truncated(size=${format.format(size)})]")).asJava)
  }

  private def createString(prefix: String, size: Int): String = {
    s"$prefix[truncated(size=${format.format(size)})]"
  }
}
