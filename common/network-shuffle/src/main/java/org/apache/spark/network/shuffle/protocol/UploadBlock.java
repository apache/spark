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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.util.Arrays;

import com.google.common.base.Objects;
import com.google.common.io.ByteStreams;

import org.apache.spark.network.buffer.ChunkedByteBufferOutputStream;
import org.apache.spark.network.buffer.InputStreamManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
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
  public final ManagedBuffer blockData;

  /**
   * @param metadata Meta-information about block, typically StorageLevel.
   * @param blockData The actual block's bytes.
   */
  public UploadBlock(
      String appId,
      String execId,
      String blockId,
      byte[] metadata,
      ManagedBuffer blockData) {
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
    int objectsHashCode = Objects.hashCode(appId, execId, blockId);
    return (objectsHashCode * 41 + Arrays.hashCode(metadata)) * 41 + (int) blockData.size();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("appId", appId)
      .add("execId", execId)
      .add("blockId", blockId)
      .add("metadata size", metadata.length)
      .add("block size", blockData.size())
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof UploadBlock) {
      UploadBlock o = (UploadBlock) other;
      return Objects.equal(appId, o.appId)
        && Objects.equal(execId, o.execId)
        && Objects.equal(blockId, o.blockId)
        && Arrays.equals(metadata, o.metadata)
        && Objects.equal(blockData, o.blockData);
    }
    return false;
  }

  @Override
  public long encodedLength() {
    return Encoders.Strings.encodedLength(appId)
      + Encoders.Strings.encodedLength(execId)
      + Encoders.Strings.encodedLength(blockId)
      + Encoders.ByteArrays.encodedLength(metadata)
      + blockData.size() + 8;
  }

  @Override
  public void encode(OutputStream out) throws IOException {
    Encoders.Strings.encode(out, appId);
    Encoders.Strings.encode(out, execId);
    Encoders.Strings.encode(out, blockId);
    Encoders.ByteArrays.encode(out, metadata);
    long bl = blockData.size();
    Encoders.Longs.encode(out, bl);
    copy(blockData.createInputStream(), out, bl);
  }

  private void encodeWithoutBlockData(OutputStream out) throws IOException {
    Encoders.Strings.encode(out, appId);
    Encoders.Strings.encode(out, execId);
    Encoders.Strings.encode(out, blockId);
    Encoders.ByteArrays.encode(out, metadata);
    long bl = blockData.size();
    Encoders.Longs.encode(out, bl);
  }

  public InputStream toInputStream() throws IOException {
    ChunkedByteBufferOutputStream out = ChunkedByteBufferOutputStream.newInstance();
    // Allow room for encoded message, plus the type byte
    Encoders.Bytes.encode(out, type().id());
    encodeWithoutBlockData(out);
    out.close();
    return new SequenceInputStream(out.toChunkedByteBuffer().toInputStream(),
        blockData.createInputStream());
  }

  public static UploadBlock decode(InputStream in) throws IOException {
    String appId = Encoders.Strings.decode(in);
    String execId = Encoders.Strings.decode(in);
    String blockId = Encoders.Strings.decode(in);
    byte[] metadata = Encoders.ByteArrays.decode(in);
    long bl = Encoders.Longs.decode(in);
    ManagedBuffer buffer = new InputStreamManagedBuffer(in, bl);
    return new UploadBlock(appId, execId, blockId, metadata, buffer);
  }

  private static int BUF_SIZE = 4 * 1024;

  public static void copy(InputStream from, OutputStream to, long total) throws IOException {
    byte[] buf = new byte[BUF_SIZE];
    while (total > 0) {
      int len = (int) Math.min(BUF_SIZE, total);
      ByteStreams.readFully(from, buf, 0, len);
      to.write(buf, 0, len);
      total -= len;
    }
  }
}
