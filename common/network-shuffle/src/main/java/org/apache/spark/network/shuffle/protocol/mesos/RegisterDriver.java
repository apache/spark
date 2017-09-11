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

package org.apache.spark.network.shuffle.protocol.mesos;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;

// Needed by ScalaDoc. See SPARK-7726
import static org.apache.spark.network.shuffle.protocol.BlockTransferMessage.Type;

/**
 * A message sent from the driver to register with the MesosExternalShuffleService.
 */
public class RegisterDriver extends BlockTransferMessage {
  private final String appId;
  private final long heartbeatTimeoutMs;

  public RegisterDriver(String appId, long heartbeatTimeoutMs) {
    this.appId = appId;
    this.heartbeatTimeoutMs = heartbeatTimeoutMs;
  }

  public String getAppId() { return appId; }

  public long getHeartbeatTimeoutMs() { return heartbeatTimeoutMs; }

  @Override
  protected Type type() { return Type.REGISTER_DRIVER; }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId) + Long.SIZE / Byte.SIZE;
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    buf.writeLong(heartbeatTimeoutMs);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(appId, heartbeatTimeoutMs);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof RegisterDriver)) {
      return false;
    }
    return Objects.equal(appId, ((RegisterDriver) o).appId);
  }

  public static RegisterDriver decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    long heartbeatTimeout = buf.readLong();
    return new RegisterDriver(appId, heartbeatTimeout);
  }
}
