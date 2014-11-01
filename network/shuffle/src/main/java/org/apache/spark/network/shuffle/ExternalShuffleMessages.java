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

package org.apache.spark.network.shuffle;

import java.io.Serializable;
import java.util.Arrays;

import com.google.common.base.Objects;

/** Messages handled by the {@link ExternalShuffleBlockHandler}. */
public class ExternalShuffleMessages {

  /** Request to read a set of shuffle blocks. Returns [[ShuffleStreamHandle]]. */
  public static class OpenShuffleBlocks implements Serializable {
    public final String appId;
    public final String execId;
    public final String[] blockIds;

    public OpenShuffleBlocks(String appId, String execId, String[] blockIds) {
      this.appId = appId;
      this.execId = execId;
      this.blockIds = blockIds;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(appId, execId) * 41 + Arrays.hashCode(blockIds);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("appId", appId)
        .add("execId", execId)
        .add("blockIds", Arrays.toString(blockIds))
        .toString();
    }

    @Override
    public boolean equals(Object other) {
      if (other != null && other instanceof OpenShuffleBlocks) {
        OpenShuffleBlocks o = (OpenShuffleBlocks) other;
        return Objects.equal(appId, o.appId)
          && Objects.equal(execId, o.execId)
          && Arrays.equals(blockIds, o.blockIds);
      }
      return false;
    }
  }

  /** Initial registration message between an executor and its local shuffle server. */
  public static class RegisterExecutor implements Serializable {
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
    public int hashCode() {
      return Objects.hashCode(appId, execId, executorInfo);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("appId", appId)
        .add("execId", execId)
        .add("executorInfo", executorInfo)
        .toString();
    }

    @Override
    public boolean equals(Object other) {
      if (other != null && other instanceof RegisterExecutor) {
        RegisterExecutor o = (RegisterExecutor) other;
        return Objects.equal(appId, o.appId)
          && Objects.equal(execId, o.execId)
          && Objects.equal(executorInfo, o.executorInfo);
      }
      return false;
    }
  }
}
