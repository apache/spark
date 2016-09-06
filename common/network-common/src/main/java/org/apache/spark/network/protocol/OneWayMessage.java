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

import org.apache.spark.network.buffer.InputStreamManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;

/**
 * A RPC that does not expect a reply, which is handled by a remote
 * {@link org.apache.spark.network.server.RpcHandler}.
 */
public final class OneWayMessage extends AbstractMessage implements RequestMessage {

  public OneWayMessage(ManagedBuffer body) {
    super(body, true);
  }

  @Override
  public Type type() { return Type.OneWayMessage; }

  @Override
  public long encodedLength() {
    return 8;
  }

  @Override
  public void encode(OutputStream out) throws IOException {
    Encoders.Longs.encode(out, body().size());
  }

  public static OneWayMessage decode(InputStream in) throws IOException {
    long limit = Encoders.Longs.decode(in);
    ManagedBuffer managedBuf = new InputStreamManagedBuffer(in, limit, false);
    return new OneWayMessage(managedBuf);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(body());
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof OneWayMessage) {
      OneWayMessage o = (OneWayMessage) other;
      return super.equals(o);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("body", body())
      .toString();
  }
}
