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

package org.apache.spark.status.protobuf

import org.apache.spark.status.ResourceProfileWrapper

class ResourceProfileWrapperSerializer extends ProtobufSerDe {

  private val appEnvSerializer = new ApplicationEnvironmentInfoWrapperSerializer

  override val supportClass: Class[_] = classOf[ResourceProfileWrapper]

  override def serialize(input: Any): Array[Byte] =
    serialize(input.asInstanceOf[ResourceProfileWrapper])

  private def serialize(input: ResourceProfileWrapper): Array[Byte] = {
    val builder = StoreTypes.ResourceProfileWrapper.newBuilder()
    builder.setRpInfo(appEnvSerializer.serializeResourceProfileInfo(input.rpInfo))
    builder.build().toByteArray
  }

  def deserialize(bytes: Array[Byte]): ResourceProfileWrapper = {
    val wrapper = StoreTypes.ResourceProfileWrapper.parseFrom(bytes)
    new ResourceProfileWrapper(
      rpInfo = appEnvSerializer.deserializeResourceProfileInfo(wrapper.getRpInfo)
    )
  }
}
