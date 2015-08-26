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
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.sasl.SaslClientBootstrap;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.shuffle.protocol.RegisterExecutor;
import org.apache.spark.network.util.TransportConf;

/**
 * Client for reading shuffle blocks which points to an external (outside of executor) server.
 * This is instead of reading shuffle blocks directly from other executors (via
 * BlockTransferService), which has the downside of losing the shuffle data if we lose the
 * executors.
 */
public class ExternalShuffleClient extends ShuffleClient {
  private final Logger logger = LoggerFactory.getLogger(ExternalShuffleClient.class);

  private final TransportConf conf;
  private final boolean saslEnabled;
  private final boolean saslEncryptionEnabled;
  private final SecretKeyHolder secretKeyHolder;

  protected TransportClientFactory clientFactory;
  protected String appId;

  /**
   * Creates an external shuffle client, with SASL optionally enabled. If SASL is not enabled,
   * then secretKeyHolder may be null.
   */
  public ExternalShuffleClient(
      TransportConf conf,
      SecretKeyHolder secretKeyHolder,
      boolean saslEnabled,
      boolean saslEncryptionEnabled) {
    Preconditions.checkArgument(
      !saslEncryptionEnabled || saslEnabled,
      "SASL encryption can only be enabled if SASL is also enabled.");
    this.conf = conf;
    this.secretKeyHolder = secretKeyHolder;
    this.saslEnabled = saslEnabled;
    this.saslEncryptionEnabled = saslEncryptionEnabled;
  }

  protected void checkInit() {
    assert appId != null : "Called before init()";
  }

  @Override
  public void init(String appId) {
    this.appId = appId;
    TransportContext context = new TransportContext(conf, new NoOpRpcHandler());
    List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
    if (saslEnabled) {
      bootstraps.add(new SaslClientBootstrap(conf, appId, secretKeyHolder, saslEncryptionEnabled));
    }
    clientFactory = context.createClientFactory(bootstraps);
  }

  @Override
  public void fetchBlocks(
      final String host,
      final int port,
      final String execId,
      String[] blockIds,
      BlockFetchingListener listener) {
    checkInit();
    logger.debug("External shuffle fetch from {}:{} (executor id {})", host, port, execId);
    try {
      RetryingBlockFetcher.BlockFetchStarter blockFetchStarter =
        new RetryingBlockFetcher.BlockFetchStarter() {
          @Override
          public void createAndStart(String[] blockIds, BlockFetchingListener listener)
              throws IOException {
            TransportClient client = clientFactory.createClient(host, port);
            new OneForOneBlockFetcher(client, appId, execId, blockIds, listener).start();
          }
        };

      int maxRetries = conf.maxIORetries();
      if (maxRetries > 0) {
        // Note this Fetcher will correctly handle maxRetries == 0; we avoid it just in case there's
        // a bug in this code. We should remove the if statement once we're sure of the stability.
        new RetryingBlockFetcher(conf, blockFetchStarter, blockIds, listener).start();
      } else {
        blockFetchStarter.createAndStart(blockIds, listener);
      }
    } catch (Exception e) {
      logger.error("Exception while beginning fetchBlocks", e);
      for (String blockId : blockIds) {
        listener.onBlockFetchFailure(blockId, e);
      }
    }
  }

  /**
   * Registers this executor with an external shuffle server. This registration is required to
   * inform the shuffle server about where and how we store our shuffle files.
   *
   * @param host Host of shuffle server.
   * @param port Port of shuffle server.
   * @param execId This Executor's id.
   * @param executorInfo Contains all info necessary for the service to find our shuffle files.
   */
  public void registerWithShuffleServer(
      String host,
      int port,
      String execId,
      ExecutorShuffleInfo executorInfo) throws IOException {
    checkInit();
    TransportClient client = clientFactory.createClient(host, port);
    byte[] registerMessage = new RegisterExecutor(appId, execId, executorInfo).toByteArray();
    client.sendRpcSync(registerMessage, 5000 /* timeoutMs */);
  }

  @Override
  public void close() {
    clientFactory.close();
  }
}
