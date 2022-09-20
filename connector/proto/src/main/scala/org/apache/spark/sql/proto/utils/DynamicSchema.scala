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

import java.util

import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}

import com.google.protobuf.DescriptorProtos.{FileDescriptorProto, FileDescriptorSet}
import com.google.protobuf.Descriptors.{Descriptor, FileDescriptor}

class DynamicSchema {
  var fileDescSet: FileDescriptorSet = null

  def this(fileDescSet: FileDescriptorSet) {
    this
    this.fileDescSet = fileDescSet;
    val fileDescMap: util.Map[String, FileDescriptor] = init(fileDescSet)
    val msgDupes: util.Set[String] = new util.HashSet[String]()
    for (fileDesc: FileDescriptor <- fileDescMap.values().asScala) {
      for (msgType: Descriptor <- fileDesc.getMessageTypes().asScala) {
        addMessageType(msgType, null, msgDupes)
      }
    }

    for (msgName: String <- msgDupes.asScala) {
      messageMsgDescriptorMapShort.remove(msgName)
    }
  }

  val messageDescriptorMapFull: util.Map[String, Descriptor] =
    new util.HashMap[String, Descriptor]()
  val messageMsgDescriptorMapShort: util.Map[String, Descriptor] =
    new util.HashMap[String, Descriptor]()

  def newBuilder(): Builder = {
    new Builder()
  }

  def addMessageType(msgType: Descriptor, scope: String, msgDupes: util.Set[String]) {
    val msgTypeNameFull: String = msgType.getFullName()
    val msgTypeNameShort: String = {
      if (scope == null) {
        msgType.getName()
      } else {
        scope + "." + msgType.getName ()
      }
    }

    if (messageDescriptorMapFull.containsKey(msgTypeNameFull)) {
      throw new IllegalArgumentException("duplicate name: " + msgTypeNameFull)
    }
    if (messageMsgDescriptorMapShort.containsKey(msgTypeNameShort)) {
      msgDupes.add(msgTypeNameShort)
    }
    messageDescriptorMapFull.put(msgTypeNameFull, msgType);
    messageMsgDescriptorMapShort.put(msgTypeNameShort, msgType);

    for (nestedType <- msgType.getNestedTypes().asScala)  {
      addMessageType(nestedType, msgTypeNameShort, msgDupes)
    }
  }

  def  init(fileDescSet: FileDescriptorSet) : util.Map[String, FileDescriptor] = {
    // check for dupes
    val allFdProtoNames: util.Set[String] = new util.HashSet[String]()
    for (fdProto: FileDescriptorProto <- fileDescSet.getFileList().asScala) {
      if (allFdProtoNames.contains(fdProto.getName())) {
        throw new IllegalArgumentException("duplicate name: " + fdProto.getName())
      }
      allFdProtoNames.add(fdProto.getName())
    }

    // build FileDescriptors, resolve dependencies (imports) if any
    val resolvedFileDescMap: util.Map[String, FileDescriptor] =
      new util.HashMap[String, FileDescriptor]()
    while (resolvedFileDescMap.size() < fileDescSet.getFileCount()) {
      for (fdProto : FileDescriptorProto <- fileDescSet.getFileList().asScala) {
        breakable {
          if (resolvedFileDescMap.containsKey(fdProto.getName())) {
            break
          }

          val dependencyList: util.List[String] = fdProto.getDependencyList();
          val resolvedFdList: util.List[FileDescriptor] =
            new util.ArrayList[FileDescriptor]()
          for (depName: String <- dependencyList.asScala) {
            if (!allFdProtoNames.contains(depName)) {
              throw new IllegalArgumentException("cannot resolve import " + depName + " in " +
                fdProto.getName())
            }
            val fd: FileDescriptor = resolvedFileDescMap.get(depName)
            if (fd != null) resolvedFdList.add(fd)
          }

          if (resolvedFdList.size() == dependencyList.size()) {
            val fds = new Array[FileDescriptor](resolvedFdList.size)
            val fd: FileDescriptor = FileDescriptor.buildFrom(fdProto, resolvedFdList.toArray(fds))
            resolvedFileDescMap.put(fdProto.getName(), fd)
          }
        }
      }
    }

    resolvedFileDescMap
  }

  override def toString() : String = {
    val msgTypes: util.Set[String] = getMessageTypes()
    ("types: " + msgTypes + "\n" + fileDescSet)
  }

  def toByteArray() : Array[Byte] = {
    fileDescSet.toByteArray()
  }

  def getMessageTypes(): util.Set[String] = {
    new util.TreeSet[String](messageDescriptorMapFull.keySet())
  }


  def getMessageDescriptor(messageTypeName: String) : Descriptor = {
    var messageType: Descriptor = messageMsgDescriptorMapShort.get(messageTypeName)
    if (messageType == null) {
      messageType = messageDescriptorMapFull.get(messageTypeName)
    }
    messageType
  }

  class Builder {
    val messageFileDescProtoBuilder: FileDescriptorProto.Builder = FileDescriptorProto.newBuilder()
    val messageFileDescSetBuilder: FileDescriptorSet.Builder = FileDescriptorSet.newBuilder()

    def  build(): DynamicSchema = {
      val fileDescSetBuilder: FileDescriptorSet.Builder = FileDescriptorSet.newBuilder()
      fileDescSetBuilder.addFile(messageFileDescProtoBuilder.build())
      fileDescSetBuilder.mergeFrom(messageFileDescSetBuilder.build())
      new DynamicSchema(fileDescSetBuilder.build())
    }

    def setName(name: String) : Builder = {
      messageFileDescProtoBuilder.setName(name)
      this
    }

    def setPackage(name: String): Builder = {
      messageFileDescProtoBuilder.setPackage(name)
      this
    }

    def addMessageDefinition(msgDef: MessageDefinition) : Builder = {
      messageFileDescProtoBuilder.addMessageType(msgDef.getMessageType())
      this
    }
  }
}
