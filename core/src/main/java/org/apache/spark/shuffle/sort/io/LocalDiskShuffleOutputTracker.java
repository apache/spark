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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.api.metadata.MapOutputMetadata;
import org.apache.spark.shuffle.api.metadata.ShuffleOutputTracker;
import org.apache.spark.storage.BlockManagerMaster;
import org.apache.spark.util.Utils;

public final class LocalDiskShuffleOutputTracker implements ShuffleOutputTracker {

  private final Supplier<SparkEnv> env = Suppliers.memoize(SparkEnv::get);
  private final Supplier<BlockManagerMaster> blockManagerMaster = Suppliers.memoize(
      () -> {
        SparkEnv env = SparkEnv.get();
        if (env == null) {
          throw new IllegalStateException("SparkEnv should not be null here.");
        }
        return env.blockManager().master();
      });
  private final SparkConf sparkConf;

  public LocalDiskShuffleOutputTracker(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
  }

  @Override
  public void registerShuffle(int shuffleId) {}

  @Override
  public void unregisterShuffle(int shuffleId, boolean blocking) {
    // In local mode, we don't want to route to the block manager master, since we end
    // up with a cycle: The BlockManagerMaster routes to the MapOutputTracker, which in
    // local mode is the MapOutputTrackerMaster, but that in turn will route to this
    // ShuffleOutputTracker.
    if (Utils.isLocalMaster(sparkConf)) {
      env.get().shuffleManager().unregisterShuffle(shuffleId);
    } else {
      blockManagerMaster.get().removeShuffle(shuffleId, blocking);
    }
  }

  @Override
  public void registerMapOutput(
      int shuffleId, int mapIndex, long mapId, MapOutputMetadata mapOutputMetadata) {}

  @Override
  public void removeMapOutput(int shuffleId, int mapId, long mapTaskAttemptId) {}
}
