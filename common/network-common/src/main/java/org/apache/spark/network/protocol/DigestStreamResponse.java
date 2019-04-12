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

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.spark.network.buffer.ManagedBuffer;

/**
 * Response to {@link StreamRequest} with digest when the stream has been successfully opened.
 * <p>
 * Note the message itself does not contain the stream data. That is written separately by the
 * sender. The receiver is expected to set a temporary channel handler that will consume the
 * number of bytes this message says the stream has.
 */
public final class DigestStreamResponse extends AbstractResponseMessage {
  public final String streamId;
  public final long byteCount;
  public final long digest;

  public DigestStreamResponse(String streamId, long byteCount, ManagedBuffer buffer, long digest) {
    super(buffer, false);
    this.streamId = streamId;
    this.byteCount = byteCount;
    this.digest = digest;
  }

  @Override
  public Type type() { return Type.DigestStreamResponse; }

  @Override
  public int encodedLength() {
    return 8 + Encoders.Strings.encodedLength(streamId) + 8;
  }

  /** Encoding does NOT include 'buffer' itself. See {@link MessageEncoder}. */
  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, streamId);
    buf.writeLong(byteCount);
    buf.writeLong(digest);
  }

  @Override
  public ResponseMessage createFailureResponse(String error) {
    return new StreamFailure(streamId, error);
  }

  public static DigestStreamResponse decode(ByteBuf buf) {
    String streamId = Encoders.Strings.decode(buf);
    long byteCount = buf.readLong();
    long digest = buf.readLong();
    return new DigestStreamResponse(streamId, byteCount, null, digest);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(byteCount, streamId, body(), digest);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof DigestStreamResponse) {
      DigestStreamResponse o = (DigestStreamResponse) other;
      return byteCount == o.byteCount && streamId.equals(o.streamId) && digest == o.digest;
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamId", streamId)
      .add("byteCount", byteCount)
      .add("digest", digest)
      .add("body", body())
      .toString();
  }

}
