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

package org.apache.spark.network.remoteshuffle;

import com.google.common.collect.Lists;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.remoteshuffle.protocol.ConnectWriteRequest;
import org.apache.spark.network.remoteshuffle.protocol.ConnectWriteResponse;
import org.apache.spark.network.remoteshuffle.protocol.RemoteShuffleMessage;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Client for writing shuffle data to remote shuffle server.
 */
public class WriteClient implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(WriteClient.class);

  private final TransportConf conf;
  private final long timeoutMs;

  private TransportClientFactory clientFactory;
  private final String host;
  private final int port;
  private final ShuffleStageFqid shuffleStageFqid;
  private final int numMaps;

  /**
   * Creates an external shuffle client, with SASL optionally enabled. If SASL is not enabled,
   * then secretKeyHolder may be null.
   */
  public WriteClient(
      String host,
      int port,
      long timeoutMs,
      ShuffleStageFqid shuffleStageFqid,
      int numMaps,
      Map<String, String> config) {
    this.host = host;
    this.port = port;
    this.timeoutMs = timeoutMs;
    this.shuffleStageFqid = shuffleStageFqid;
    this.numMaps = numMaps;
    this.conf = new TransportConf("remoteShuffle", new MapConfigProvider(config));
  }

  public void connect() throws IOException, InterruptedException {
    TransportContext context = new TransportContext(conf, new NoOpRpcHandler(), true);
    List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
    clientFactory = context.createClientFactory(bootstraps);
    try (TransportClient client = clientFactory.createUnmanagedClient(host, port)) {
      ByteBuffer connectWriteRequest = new ConnectWriteRequest(
          shuffleStageFqid.getAppId(),
          shuffleStageFqid.getExecId(),
          shuffleStageFqid.getShuffleId(),
          shuffleStageFqid.getStageAttempt(),
          numMaps).toByteBuffer();
      ByteBuffer connectWriteResponse = client.sendRpcSync(connectWriteRequest, timeoutMs);
      ConnectWriteResponse msg =
          (ConnectWriteResponse)RemoteShuffleMessage.Decoder.fromByteBuffer(connectWriteResponse);
      logger.info("Write client connected to shuffle server: {}", msg);
    }
  }

  @Override
  public void close() {
    if (clientFactory != null) {
      clientFactory.close();
      clientFactory = null;
    }
  }
}
