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

import scala.jdk.CollectionConverters._

import org.apache.spark.status.PoolData
import org.apache.spark.status.protobuf.Utils.{getStringField, setStringField}

private[protobuf] class PoolDataSerializer extends ProtobufSerDe[PoolData] {

  override def serialize(input: PoolData): Array[Byte] = {
    val builder = StoreTypes.PoolData.newBuilder()
    setStringField(input.name, builder.setName)
    input.stageIds.foreach(id => builder.addStageIds(id.toLong))
    builder.build().toByteArray
  }

  override def deserialize(bytes: Array[Byte]): PoolData = {
    val poolData = StoreTypes.PoolData.parseFrom(bytes)
    new PoolData(
      name = getStringField(poolData.hasName, poolData.getName),
      stageIds = poolData.getStageIdsList.asScala.map(_.toInt).toSet
    )
  }
}
