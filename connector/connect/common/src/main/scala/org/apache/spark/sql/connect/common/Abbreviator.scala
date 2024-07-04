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

import scala.jdk.CollectionConverters._

import com.google.protobuf.{ByteString, Message}
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType

private[connect] class Abbreviator(thresholds: Map[String, Int]) extends Serializable {
  private val format = java.text.NumberFormat.getInstance()
  private val MAX_BYTES_SIZE = 8
  private val MAX_STRING_SIZE = 1024

  private val maxStringSize = thresholds.getOrElse("STRING", MAX_STRING_SIZE)
  private val maxBytesSize = thresholds.getOrElse("BYTES", MAX_BYTES_SIZE)

  def abbreviate[T <: Message](message: T): T = {
    val builder = message.toBuilder

    message.getAllFields.asScala.iterator.foreach {
      case (field, string: String)
          if field.getJavaType == JavaType.STRING && !field.isRepeated
            && string != null && string.length > maxStringSize =>
        builder.setField(field, truncateString(string))

      case (field, strings: java.util.List[_])
          if field.getJavaType == JavaType.STRING && field.isRepeated
            && strings != null && !strings.isEmpty =>
        strings.iterator().asScala.zipWithIndex.foreach {
          case (string: String, i) if string != null && string.length > maxStringSize =>
            builder.setRepeatedField(field, i, truncateString(string))
          case _ =>
        }

      case (field, bytes: ByteString)
          if field.getJavaType == JavaType.BYTE_STRING && !field.isRepeated
            && bytes != null && bytes.size() > maxBytesSize =>
        builder.setField(field, truncateBytes(bytes))

      case (field, bytes: Array[Byte])
          if field.getJavaType == JavaType.BYTE_STRING && !field.isRepeated
            && bytes != null && bytes.length > maxBytesSize =>
        builder.setField(field, truncateBytes(bytes))

      case (field, msg: Message)
          if field.getJavaType == JavaType.MESSAGE && !field.isRepeated
            && msg != null =>
        builder.setField(field, abbreviate(msg))

      case (field, msgs: java.util.List[_])
          if field.getJavaType == JavaType.MESSAGE && field.isRepeated
            && msgs != null && !msgs.isEmpty =>
        msgs.iterator().asScala.zipWithIndex.foreach {
          case (msg: Message, i) if msg != null =>
            builder.setRepeatedField(field, i, abbreviate(msg))
          case _ =>
        }

      case _ =>
    }

    builder.build().asInstanceOf[T]
  }

  private def truncateString(str: String): String = {
    s"${str.take(maxStringSize)}[truncated(size=${format.format(str.length)})]"
  }

  private def truncateBytes(bytes: ByteString): ByteString = {
    bytes.substring(0, maxBytesSize).concat(byteStringSuffix(bytes.size))
  }

  private def truncateBytes(bytes: Array[Byte]): ByteString = {
    ByteString.copyFrom(bytes, 0, maxBytesSize).concat(byteStringSuffix(bytes.length))
  }

  private def byteStringSuffix(size: Int): ByteString = {
    ByteString.copyFromUtf8(s"[truncated(size=${format.format(size)})]")
  }
}
