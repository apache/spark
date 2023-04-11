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

  def redact(message: Message): Message = {
    val builder = message.toBuilder

    message.getAllFields.asScala.iterator.foreach {
      case (field: FieldDescriptor, bytes: ByteString)
          if field.getJavaType == FieldDescriptor.JavaType.BYTE_STRING && bytes != null =>
        builder.setField(
          field,
          ByteString.copyFromUtf8(s"bytes (size=${format.format(bytes.size)})"))

      case (field: FieldDescriptor, bytes: Array[_])
          if field.getJavaType == FieldDescriptor.JavaType.BYTE_STRING && bytes != null =>
        builder.setField(
          field,
          ByteString.copyFromUtf8(s"bytes (size=${format.format(bytes.length)})"))

      // TODO: should also take 1, repeated msg; 2, map<xxx, msg> into account
      case (field: FieldDescriptor, msg: Message)
          if field.getJavaType == FieldDescriptor.JavaType.MESSAGE && msg != null =>
        builder.setField(field, redact(msg))

      case (field: FieldDescriptor, value: Any) => builder.setField(field, value)
    }

    builder.build()
  }
}
