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
import java.io.InputStream;
import java.io.OutputStream;

import com.google.common.base.Objects;

import org.apache.spark.network.buffer.ManagedBuffer;

/**
 * Response to {@link StreamRequest} when the stream has been successfully opened.
 * <p>
 * Note the message itself does not contain the stream data. That is written separately by the
 * sender. The receiver is expected to set a temporary channel handler that will consume the
 * number of bytes this message says the stream has.
 */
public final class StreamResponse extends AbstractResponseMessage {
  public final String streamId;
  public final long byteCount;

  public StreamResponse(String streamId, long byteCount, ManagedBuffer buffer) {
    super(buffer, false);
    this.streamId = streamId;
    this.byteCount = byteCount;
  }

  @Override
  public Type type() { return Type.StreamResponse; }

  @Override
  public long encodedLength() {
    return 8 + Encoders.Strings.encodedLength(streamId);
  }

  /** Encoding does NOT include 'buffer' itself. See {@link MessageEncoder}. */
  @Override
  public void encode(OutputStream out) throws IOException {
    Encoders.Strings.encode(out, streamId);
    Encoders.Longs.encode(out, byteCount);
  }

  @Override
  public ResponseMessage createFailureResponse(String error) {
    return new StreamFailure(streamId, error);
  }

  public static StreamResponse decode(InputStream in) throws IOException {
    String streamId = Encoders.Strings.decode(in);
    long byteCount = Encoders.Longs.decode(in);
    return new StreamResponse(streamId, byteCount, null);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(byteCount, streamId, body());
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof StreamResponse) {
      StreamResponse o = (StreamResponse) other;
      return byteCount == o.byteCount && streamId.equals(o.streamId);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamId", streamId)
      .add("bodySize", byteCount)
      .add("body", body())
      .toString();
  }

}
