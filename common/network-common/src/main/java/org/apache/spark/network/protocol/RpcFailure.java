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

/** Response to {@link RpcRequest} for a failed RPC. */
public final class RpcFailure extends AbstractMessage implements ResponseMessage {
  public final long requestId;
  public final String errorString;

  public RpcFailure(long requestId, String errorString) {
    this.requestId = requestId;
    this.errorString = errorString;
  }

  @Override
  public Type type() { return Type.RpcFailure; }

  @Override
  public long encodedLength() {
    return 8 + Encoders.Strings.encodedLength(errorString);
  }

  @Override
  public void encode(OutputStream out) throws IOException {
    Encoders.Longs.encode(out, requestId);
    Encoders.Strings.encode(out, errorString);
  }

  public static RpcFailure decode(InputStream in) throws IOException {
    long requestId = Encoders.Longs.decode(in);
    String errorString = Encoders.Strings.decode(in);
    return new RpcFailure(requestId, errorString);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(requestId, errorString);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RpcFailure) {
      RpcFailure o = (RpcFailure) other;
      return requestId == o.requestId && errorString.equals(o.errorString);
    }
    return false;
  }

  @Override
   public String toString() {
    return Objects.toStringHelper(this)
      .add("requestId", requestId)
      .add("errorString", errorString)
      .toString();
  }
}
