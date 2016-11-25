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

/**
 * Request to stream data from the remote end.
 * <p>
 * The stream ID is an arbitrary string that needs to be negotiated between the two endpoints before
 * the data can be streamed.
 */
public final class StreamRequest extends AbstractMessage implements RequestMessage {
   public final String streamId;

   public StreamRequest(String streamId) {
     this.streamId = streamId;
   }

  @Override
  public Type type() { return Type.StreamRequest; }

  @Override
  public long encodedLength() {
    return Encoders.Strings.encodedLength(streamId);
  }

  @Override
  public void encode(OutputStream out) throws IOException {
    Encoders.Strings.encode(out, streamId);
  }

  public static StreamRequest decode(InputStream in) throws IOException {
    String streamId = Encoders.Strings.decode(in);
    return new StreamRequest(streamId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(streamId);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof StreamRequest) {
      StreamRequest o = (StreamRequest) other;
      return streamId.equals(o.streamId);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamId", streamId)
      .toString();
  }

}
