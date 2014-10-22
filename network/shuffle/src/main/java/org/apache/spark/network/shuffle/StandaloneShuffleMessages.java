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

/** Messages handled by the {@link StandaloneShuffleBlockHandler}. */
public class StandaloneShuffleMessages {

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
  }

  /** Initial registration message between an executor and its local standalone shuffle server. */
  public static class RegisterExecutor implements Serializable {
    public final String appId;
    public final String execId;
    public final ExecutorShuffleConfig executorConfig;

    public RegisterExecutor(
        String appId,
        String execId,
        ExecutorShuffleConfig executorConfig) {
      this.appId = appId;
      this.execId = execId;
      this.executorConfig = executorConfig;
    }
  }
}
