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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.shuffle.StandaloneShuffleMessages.RegisterExecutor;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.TransportConf;

public class StandaloneShuffleClient implements ShuffleClient {
  private final Logger logger = LoggerFactory.getLogger(StandaloneShuffleClient.class);

  private final TransportClientFactory clientFactory;
  private final String appId;

  public StandaloneShuffleClient(TransportConf conf, String appId) {
    TransportContext context = new TransportContext(conf, new NoOpRpcHandler());
    this.clientFactory = context.createClientFactory();
    this.appId = appId;
  }

  @Override
  public void fetchBlocks(
      String host,
      int port,
      String execId,
      String[] blockIds,
      BlockFetchingListener listener) {
    logger.debug("Standalone shuffle fetch from {}:{} (executor id {})", host, port, execId);
    try {
      TransportClient client = clientFactory.createClient(host, port);
      new OneForOneBlockFetcher(client, blockIds, listener)
        .start(new StandaloneShuffleMessages.OpenShuffleBlocks(appId, execId, blockIds));
    } catch (Exception e) {
      logger.error("Exception while beginning fetchBlocks", e);
      for (String blockId : blockIds) {
        listener.onBlockFetchFailure(blockId, e);
      }
    }
  }

  /**
   * Registers this executor with a standalone shuffle server. This registration is required to
   * inform the shuffle server about where and how we store our shuffle files.
   *
   * @param host Host of standalone shuffle server.
   * @param port Port of standalone shuffle server.
   * @param execId This Executor's id.
   * @param executorConfig Contains all config necessary for the service to find our shuffle files.
   */
  public void registerWithStandaloneShuffleService(
      String host,
      int port,
      String execId,
      ExecutorShuffleConfig executorConfig) {
    TransportClient client = clientFactory.createClient(host, port);
    byte[] registerExecutorMessage =
      JavaUtils.serialize(new RegisterExecutor(appId, execId, executorConfig));
    client.sendRpcSync(registerExecutorMessage, 3000);
  }
}
