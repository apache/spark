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

package org.apache.spark.shuffle.sort.io;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.shuffle.ShuffleExecutorComponents;
import org.apache.spark.api.shuffle.ShuffleWriteSupport;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.storage.BlockManager;

public class DefaultShuffleExecutorComponents implements ShuffleExecutorComponents {

  private final SparkConf sparkConf;
  private BlockManager blockManager;
  private IndexShuffleBlockResolver blockResolver;

  public DefaultShuffleExecutorComponents(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
  }

  @Override
  public void initializeExecutor(String appId, String execId) {
    blockManager = SparkEnv.get().blockManager();
    blockResolver = new IndexShuffleBlockResolver(sparkConf, blockManager);
  }

  @Override
  public ShuffleWriteSupport writes() {
    if (blockResolver == null) {
      throw new IllegalStateException(
        "Executor components must be initialized before getting writers.");
    }
    return new DefaultShuffleWriteSupport(sparkConf, blockResolver, blockManager.shuffleServerId());
  }
}
