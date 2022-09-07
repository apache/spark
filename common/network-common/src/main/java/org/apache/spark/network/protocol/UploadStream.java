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

package org.apache.spark.network.protocol;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NettyManagedBuffer;

/**
 * An RPC with data that is sent outside of the frame, so it can be read as a stream.
 */
public final class UploadStream extends AbstractMessage implements RequestMessage {
  /** Used to link an RPC request with its response. */
  public final long requestId;
  public final ManagedBuffer meta;
  public final long bodyByteCount;

  public UploadStream(long requestId, ManagedBuffer meta, ManagedBuffer body) {
    super(body, false); // body is *not* included in the frame
    this.requestId = requestId;
    this.meta = meta;
    bodyByteCount = body.size();
  }

  // this version is called when decoding the bytes on the receiving end.  The body is handled
  // separately.
  private UploadStream(long requestId, ManagedBuffer meta, long bodyByteCount) {
    super(null, false);
    this.requestId = requestId;
    this.meta = meta;
    this.bodyByteCount = bodyByteCount;
  }

  @Override
  public Message.Type type() { return Type.UploadStream; }

  @Override
  public int encodedLength() {
    // the requestId, meta size, meta and bodyByteCount (body is not included)
    return 8 + 4 + ((int) meta.size()) + 8;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(requestId);
    try {
      ByteBuffer metaBuf = meta.nioByteBuffer();
      buf.writeInt(metaBuf.remaining());
      buf.writeBytes(metaBuf);
    } catch (IOException io) {
      throw new RuntimeException(io);
    }
    buf.writeLong(bodyByteCount);
  }

  public static UploadStream decode(ByteBuf buf) {
    long requestId = buf.readLong();
    int metaSize = buf.readInt();
    ManagedBuffer meta = new NettyManagedBuffer(buf.readRetainedSlice(metaSize));
    long bodyByteCount = buf.readLong();
    // This is called by the frame decoder, so the data is still null.  We need a StreamInterceptor
    // to read the data.
    return new UploadStream(requestId, meta, bodyByteCount);
  }

  @Override
  public int hashCode() {
    return Long.hashCode(requestId);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof UploadStream) {
      UploadStream o = (UploadStream) other;
      return requestId == o.requestId && super.equals(o);
    }
    return false;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("requestId", requestId)
      .append("body", body())
      .toString();
  }
}
