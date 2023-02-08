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

import java.util.{HashMap => JHashMap, Map => JMap}

import com.google.protobuf.ByteString

import org.apache.spark.status.protobuf.Utils.setJMapField
import org.apache.spark.util.kvstore.RocksDB

class RockDBTypeAliasesSerializer extends ProtobufSerDe[RocksDB.TypeAliases] {

  override def serialize(input: RocksDB.TypeAliases): Array[Byte] = {
    val builder = StoreTypes.RockDBTypeAliases.newBuilder()
    setJMapField(input.aliases, putAllAliases(builder, _))
    builder.build().toByteArray
  }

  override def deserialize(bytes: Array[Byte]): RocksDB.TypeAliases = {
    val typeAliases = StoreTypes.RockDBTypeAliases.parseFrom(bytes)
    RocksDB.TypeAliases.of(convertToAliases(typeAliases.getAliasesMap))
  }

  private def putAllAliases(builder: StoreTypes.RockDBTypeAliases.Builder,
      aliases: JMap[String, Array[Byte]]): Unit = {
    aliases.forEach {
      case (k, v) => builder.putAliases(k, ByteString.copyFrom(v))
    }
  }

  private def convertToAliases(input: JMap[String, ByteString]): JHashMap[String, Array[Byte]] = {
    val aliases = new JHashMap[String, Array[Byte]](input.size())
    input.forEach {
      case (k, v) =>
        aliases.put(k, v.toByteArray)
    }
    aliases
  }
}
