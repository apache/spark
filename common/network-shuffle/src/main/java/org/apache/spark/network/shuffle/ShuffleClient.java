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

import java.io.Closeable;

/** Provides an interface for reading shuffle files, either from an Executor or external service. */
public abstract class ShuffleClient implements Closeable {

  /**
   * Initializes the ShuffleClient, specifying this Executor's appId.
   * Must be called before any other method on the ShuffleClient.
   */
  public void init(String appId) { }

  /**
   * Fetch a sequence of blocks from a remote node asynchronously,
   *
   * Note that this API takes a sequence so the implementation can batch requests, and does not
   * return a future so the underlying implementation can invoke onBlockFetchSuccess as soon as
   * the data of a block is fetched, rather than waiting for all blocks to be fetched.
   *
   * @param host the host of the remote node.
   * @param port the port of the remote node.
   * @param execId the executor id.
   * @param blockIds block ids to fetch.
   * @param listener the listener to receive block fetching status.
   * @param tempShuffleFileManager TempShuffleFileManager to create and clean temp shuffle files.
   *                               If it's not <code>null</code>, the remote blocks will be streamed
   *                               into temp shuffle files to reduce the memory usage, otherwise,
   *                               they will be kept in memory.
   */
  public abstract void fetchBlocks(
      String host,
      int port,
      String execId,
      String[] blockIds,
      BlockFetchingListener listener,
      TempShuffleFileManager tempShuffleFileManager);
}
