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

import java.util.Objects;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import org.apache.spark.network.protocol.Encoders;

// Needed by ScalaDoc. See SPARK-7726
import static org.apache.spark.network.shuffle.protocol.BlockTransferMessage.Type;

/**
 * Initial registration message between an executor and its local shuffle server.
 * Returns nothing (empty byte array).
 */
public class RegisterExecutor extends BlockTransferMessage {
  public final String appId;
  public final String execId;
  public final ExecutorShuffleInfo executorInfo;

  public RegisterExecutor(
      String appId,
      String execId,
      ExecutorShuffleInfo executorInfo) {
    this.appId = appId;
    this.execId = execId;
    this.executorInfo = executorInfo;
  }

  @Override
  protected Type type() { return Type.REGISTER_EXECUTOR; }

  @Override
  public int hashCode() {
    return Objects.hash(appId, execId, executorInfo);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("appId", appId)
      .append("execId", execId)
      .append("executorInfo", executorInfo)
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RegisterExecutor o) {
      return Objects.equals(appId, o.appId)
        && Objects.equals(execId, o.execId)
        && Objects.equals(executorInfo, o.executorInfo);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId)
      + Encoders.Strings.encodedLength(execId)
      + executorInfo.encodedLength();
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    Encoders.Strings.encode(buf, execId);
    executorInfo.encode(buf);
  }

  public static RegisterExecutor decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    String execId = Encoders.Strings.decode(buf);
    ExecutorShuffleInfo executorShuffleInfo = ExecutorShuffleInfo.decode(buf);
    return new RegisterExecutor(appId, execId, executorShuffleInfo);
  }
}
