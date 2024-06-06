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

import java.util.Objects;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Message indicating an error when transferring a stream.
 */
public final class StreamFailure extends AbstractMessage implements ResponseMessage {
  public final String streamId;
  public final String error;

  public StreamFailure(String streamId, String error) {
    this.streamId = streamId;
    this.error = error;
  }

  @Override
  public Message.Type type() { return Type.StreamFailure; }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(streamId) + Encoders.Strings.encodedLength(error);
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, streamId);
    Encoders.Strings.encode(buf, error);
  }

  public static StreamFailure decode(ByteBuf buf) {
    String streamId = Encoders.Strings.decode(buf);
    String error = Encoders.Strings.decode(buf);
    return new StreamFailure(streamId, error);
  }

  @Override
  public int hashCode() {
    return Objects.hash(streamId, error);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof StreamFailure o) {
      return streamId.equals(o.streamId) && error.equals(o.error);
    }
    return false;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("streamId", streamId)
      .append("error", error)
      .toString();
  }

}
