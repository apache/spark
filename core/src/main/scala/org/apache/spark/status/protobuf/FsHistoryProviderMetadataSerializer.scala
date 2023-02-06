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

import org.apache.spark.deploy.history.FsHistoryProviderMetadata
import org.apache.spark.status.protobuf.Utils.{getStringField, setStringField}
import org.apache.spark.util.Utils.weakIntern

class FsHistoryProviderMetadataSerializer
  extends ProtobufSerDe[FsHistoryProviderMetadata] {

  override def serialize(input: FsHistoryProviderMetadata): Array[Byte] = {
    val builder = StoreTypes.FsHistoryProviderMetadata.newBuilder()
      .setVersion(input.version)
      .setUiVersion(input.uiVersion)
    setStringField(input.logDir, builder.setLogDir)
    builder.build().toByteArray
  }

  override def deserialize(bytes: Array[Byte]): FsHistoryProviderMetadata = {
    val metadata = StoreTypes.FsHistoryProviderMetadata.parseFrom(bytes)
    FsHistoryProviderMetadata(
      version = metadata.getVersion,
      uiVersion = metadata.getUiVersion,
      logDir = getStringField(metadata.hasLogDir, () => weakIntern(metadata.getLogDir))
    )
  }
}
