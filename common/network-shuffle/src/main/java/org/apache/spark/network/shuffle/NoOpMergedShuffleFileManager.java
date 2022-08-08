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

import java.io.File;
import java.io.IOException;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.shuffle.protocol.FinalizeShuffleMerge;
import org.apache.spark.network.shuffle.protocol.MergeStatuses;
import org.apache.spark.network.shuffle.protocol.PushBlockStream;
import org.apache.spark.network.util.TransportConf;

/**
 * Dummy implementation of merged shuffle file manager. Suitable for when push-based shuffle
 * is not enabled.
 *
 * @since 3.1.0
 */
public class NoOpMergedShuffleFileManager implements MergedShuffleFileManager {

  // This constructor is needed because we use this constructor to instantiate an implementation
  // of MergedShuffleFileManager using reflection.
  // See YarnShuffleService#newMergedShuffleFileManagerInstance.
  public NoOpMergedShuffleFileManager(TransportConf transportConf, File recoveryFile) {}

  @Override
  public StreamCallbackWithID receiveBlockDataAsStream(PushBlockStream msg) {
    throw new UnsupportedOperationException("Cannot handle shuffle block merge");
  }

  @Override
  public MergeStatuses finalizeShuffleMerge(FinalizeShuffleMerge msg) throws IOException {
    throw new UnsupportedOperationException("Cannot handle shuffle block merge");
  }

  @Override
  public void registerExecutor(String appId, ExecutorShuffleInfo executorInfo) {
    // No-Op. Do nothing.
  }

  @Override
  public void applicationRemoved(String appId, boolean cleanupLocalDirs) {
    // No-Op. Do nothing.
  }

  @Override
  public ManagedBuffer getMergedBlockData(
      String appId,
      int shuffleId,
      int shuffleMergeId,
      int reduceId,
      int chunkId) {
    throw new UnsupportedOperationException("Cannot handle shuffle block merge");
  }

  @Override
  public MergedBlockMeta getMergedBlockMeta(
      String appId,
      int shuffleId,
      int shuffleMergeId,
      int reduceId) {
    throw new UnsupportedOperationException("Cannot handle shuffle block merge");
  }

  @Override
  public String[] getMergedBlockDirs(String appId) {
    throw new UnsupportedOperationException("Cannot handle shuffle block merge");
  }
}
