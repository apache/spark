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

import java.io.IOException;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.shuffle.protocol.FinalizeShuffleMerge;
import org.apache.spark.network.shuffle.protocol.MergeStatuses;
import org.apache.spark.network.shuffle.protocol.PushBlockStream;

/**
 * The MergedShuffleFileManager is used to process push based shuffle when enabled. It works
 * along side {@link ExternalBlockHandler} and serves as an RPCHandler for
 * {@link org.apache.spark.network.server.RpcHandler#receiveStream}, where it processes the
 * remotely pushed streams of shuffle blocks to merge them into merged shuffle files. Right
 * now, support for push based shuffle is only implemented for external shuffle service in
 * YARN mode.
 *
 * @since 3.1.0
 */
@Evolving
public interface MergedShuffleFileManager {
  /**
   * Provides the stream callback used to process a remotely pushed block. The callback is
   * used by the {@link org.apache.spark.network.client.StreamInterceptor} installed on the
   * channel to process the block data in the channel outside of the message frame.
   *
   * @param msg metadata of the remotely pushed blocks. This is processed inside the message frame
   * @return A stream callback to process the block data in streaming fashion as it arrives
   */
  StreamCallbackWithID receiveBlockDataAsStream(PushBlockStream msg);

  /**
   * Handles the request to finalize shuffle merge for a given shuffle.
   *
   * @param msg contains appId and shuffleId to uniquely identify a shuffle to be finalized
   * @return The statuses of the merged shuffle partitions for the given shuffle on this
   *         shuffle service
   * @throws IOException
   */
  MergeStatuses finalizeShuffleMerge(FinalizeShuffleMerge msg) throws IOException;

  /**
   * Registers an executor with MergedShuffleFileManager. This executor-info provides
   * the directories and number of sub-dirs per dir so that MergedShuffleFileManager knows where to
   * store and look for shuffle data for a given application. It is invoked by the RPC call when
   * executor tries to register with the local shuffle service.
   *
   * @param appId application ID
   * @param executorInfo The list of local dirs that this executor gets granted from NodeManager
   */
  void registerExecutor(String appId, ExecutorShuffleInfo executorInfo);

  /**
   * Invoked when an application finishes. This cleans up any remaining metadata associated with
   * this application, and optionally deletes the application specific directory path.
   *
   * @param appId application ID
   * @param cleanupLocalDirs flag indicating whether MergedShuffleFileManager should handle
   *                         deletion of local dirs itself.
   */
  void applicationRemoved(String appId, boolean cleanupLocalDirs);

  /**
   * Get the buffer for a given merged shuffle chunk when serving merged shuffle to reducers
   *
   * @param appId application ID
   * @param shuffleId shuffle ID
   * @param shuffleMergeId shuffleMergeId is used to uniquely identify merging process
   *                       of shuffle by an indeterminate stage attempt.
   * @param reduceId reducer ID
   * @param chunkId merged shuffle file chunk ID
   * @return The {@link ManagedBuffer} for the given merged shuffle chunk
   */
  ManagedBuffer getMergedBlockData(
      String appId,
      int shuffleId,
      int shuffleMergeId,
      int reduceId,
      int chunkId);

  /**
   * Get the meta information of a merged block.
   *
   * @param appId application ID
   * @param shuffleId shuffle ID
   * @param shuffleMergeId shuffleMergeId is used to uniquely identify merging process
   *                       of shuffle by an indeterminate stage attempt.
   * @param reduceId reducer ID
   * @return meta information of a merged block
   */
  MergedBlockMeta getMergedBlockMeta(
      String appId,
      int shuffleId,
      int shuffleMergeId,
      int reduceId);

  /**
   * Get the local directories which stores the merged shuffle files.
   *
   * @param appId application ID
   */
  String[] getMergedBlockDirs(String appId);
}
