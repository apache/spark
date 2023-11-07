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

package org.apache.spark.network.shuffle.protocol;

import java.util.Arrays;
import java.util.Objects;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import org.apache.spark.network.protocol.Encoders;

// Needed by ScalaDoc. See SPARK-7726
import static org.apache.spark.network.shuffle.protocol.BlockTransferMessage.Type;


/** Request to upload a block with a certain StorageLevel. Returns nothing (empty byte array). */
public class UploadBlock extends BlockTransferMessage {
  public final String appId;
  public final String execId;
  public final String blockId;
  // TODO: StorageLevel is serialized separately in here because StorageLevel is not available in
  // this package. We should avoid this hack.
  public final byte[] metadata;
  public final byte[] blockData;

  /**
   * @param metadata Meta-information about block, typically StorageLevel.
   * @param blockData The actual block's bytes.
   */
  public UploadBlock(
      String appId,
      String execId,
      String blockId,
      byte[] metadata,
      byte[] blockData) {
    this.appId = appId;
    this.execId = execId;
    this.blockId = blockId;
    this.metadata = metadata;
    this.blockData = blockData;
  }

  @Override
  protected Type type() { return Type.UPLOAD_BLOCK; }

  @Override
  public int hashCode() {
    int objectsHashCode = Objects.hash(appId, execId, blockId);
    return (objectsHashCode * 41 + Arrays.hashCode(metadata)) * 41 + Arrays.hashCode(blockData);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("appId", appId)
      .append("execId", execId)
      .append("blockId", blockId)
      .append("metadata size", metadata.length)
      .append("block size", blockData.length)
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof UploadBlock o) {
      return Objects.equals(appId, o.appId)
        && Objects.equals(execId, o.execId)
        && Objects.equals(blockId, o.blockId)
        && Arrays.equals(metadata, o.metadata)
        && Arrays.equals(blockData, o.blockData);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId)
      + Encoders.Strings.encodedLength(execId)
      + Encoders.Strings.encodedLength(blockId)
      + Encoders.ByteArrays.encodedLength(metadata)
      + Encoders.ByteArrays.encodedLength(blockData);
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    Encoders.Strings.encode(buf, execId);
    Encoders.Strings.encode(buf, blockId);
    Encoders.ByteArrays.encode(buf, metadata);
    Encoders.ByteArrays.encode(buf, blockData);
  }

  public static UploadBlock decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    String execId = Encoders.Strings.decode(buf);
    String blockId = Encoders.Strings.decode(buf);
    byte[] metadata = Encoders.ByteArrays.decode(buf);
    byte[] blockData = Encoders.ByteArrays.decode(buf);
    return new UploadBlock(appId, execId, blockId, metadata, blockData);
  }
}
