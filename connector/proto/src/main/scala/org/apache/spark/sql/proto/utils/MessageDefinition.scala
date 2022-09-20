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
package org.apache.spark.sql.proto.utils

import com.google.protobuf.DescriptorProtos.{DescriptorProto, FieldDescriptorProto}

class MessageDefinition(val messageType: DescriptorProto = null) {

  def newBuilder(msgTypeName: String): Builder = {
    new Builder(msgTypeName)
  }

  def getMessageType(): DescriptorProto = {
    messageType
  }

  class Builder(msgTypeName: String) {
    var messageTypeBuilder: DescriptorProto.Builder = DescriptorProto.newBuilder()
    messageTypeBuilder.setName(msgTypeName)

    def addField(label: String, typeName: String, name: String, num: Int): Builder = {
      addField(label, typeName, name, num, null)
    }

    def addField(label: String, typeName: String, name: String, num: Int,
                 defaultVal: String): Builder = {
      val protoLabel: FieldDescriptorProto.Label = protoLabelMap.getOrElse(label, null)
      if (protoLabel == null) {
        throw new IllegalArgumentException("Illegal label: " + label)
      }
      addField(protoLabel, typeName, name, num, defaultVal)
      this
    }

    def addMessageDefinition(msgDef: MessageDefinition): Builder = {
      messageTypeBuilder.addNestedType(msgDef.getMessageType())
      this
    }

    def build(): MessageDefinition = {
      new MessageDefinition(messageTypeBuilder.build())
    }

    def addField(label: FieldDescriptorProto.Label, typeName: String, name: String, num: Int,
                 defaultVal: String) {
      val fieldBuilder: FieldDescriptorProto.Builder = FieldDescriptorProto.newBuilder()
      fieldBuilder.setLabel(label)
      val primType: FieldDescriptorProto.Type = protoTypeMap.getOrElse(typeName, null)
      if (primType != null) {
        fieldBuilder.setType(primType)
      } else {
        fieldBuilder.setTypeName(typeName)
      }

      fieldBuilder.setName(name).setNumber(num);
      if (defaultVal != null) fieldBuilder.setDefaultValue(defaultVal);
      messageTypeBuilder.addField(fieldBuilder.build());
    }
  }


  private val protoLabelMap: Map[String, FieldDescriptorProto.Label] =
    Map("optional" -> FieldDescriptorProto.Label.LABEL_OPTIONAL,
      "required" -> FieldDescriptorProto.Label.LABEL_REQUIRED,
      "repeated" -> FieldDescriptorProto.Label.LABEL_REPEATED
    )

  private val protoTypeMap: Map[String, FieldDescriptorProto.Type] =
    Map("double" -> FieldDescriptorProto.Type.TYPE_DOUBLE,
    "float" -> FieldDescriptorProto.Type.TYPE_FLOAT,
    "int32" -> FieldDescriptorProto.Type.TYPE_INT32,
    "int64" -> FieldDescriptorProto.Type.TYPE_INT64,
    "uint32" -> FieldDescriptorProto.Type.TYPE_UINT32,
    "uint64" -> FieldDescriptorProto.Type.TYPE_UINT64,
    "sint32" -> FieldDescriptorProto.Type.TYPE_SINT32,
    "sint64" -> FieldDescriptorProto.Type.TYPE_SINT64,
    "fixed32" -> FieldDescriptorProto.Type.TYPE_FIXED32,
    "fixed64" -> FieldDescriptorProto.Type.TYPE_FIXED64,
    "sfixed32" -> FieldDescriptorProto.Type.TYPE_SFIXED32,
    "sfixed64" -> FieldDescriptorProto.Type.TYPE_SFIXED64,
    "bool" -> FieldDescriptorProto.Type.TYPE_BOOL,
    "string" -> FieldDescriptorProto.Type.TYPE_STRING,
    "bytes" -> FieldDescriptorProto.Type.TYPE_BYTES)
}

