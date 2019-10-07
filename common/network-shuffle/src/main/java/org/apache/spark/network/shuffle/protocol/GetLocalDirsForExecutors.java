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

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;

import java.util.Arrays;

// Needed by ScalaDoc. See SPARK-7726


/** Request to get the local dirs for the given executors. */
public class GetLocalDirsForExecutors extends BlockTransferMessage {
  public final String appId;

  public final String[] execIds;

  public GetLocalDirsForExecutors(String appId, String[] execIds) {
    this.appId = appId;
    this.execIds = execIds;
  }

  @Override
  protected Type type() { return Type.GET_LOCAL_DIRS_FOR_EXECUTORS; }

  @Override
  public int hashCode() {
    return Objects.hashCode(appId) * 41 + Arrays.hashCode(execIds);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("appId", appId)
      .add("execIds", Arrays.toString(execIds))
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof GetLocalDirsForExecutors) {
      GetLocalDirsForExecutors o = (GetLocalDirsForExecutors) other;
      return appId.equals(o.appId) && Arrays.equals(execIds, o.execIds);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId) + Encoders.StringArrays.encodedLength(execIds);
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    Encoders.StringArrays.encode(buf, execIds);
  }

  public static GetLocalDirsForExecutors decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    String[] execIds = Encoders.StringArrays.decode(buf);
    return new GetLocalDirsForExecutors(appId, execIds);
  }
}
