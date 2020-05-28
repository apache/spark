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

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Remote shuffle server.
 */
public class ShuffleServer {

  private static final String SPARK_SHUFFLE_SERVICE_PORT_KEY = "spark.shuffle.service.port";
  private static final int DEFAULT_SPARK_SHUFFLE_SERVICE_PORT = 7337;

  private final Map<String, String> config;

  private ShuffleServerHandler shuffleHandler;
  private TransportServer shuffleServer;
  private int boundPort;

  public ShuffleServer(Map<String, String> config) {
    this.config = Collections.unmodifiableMap(config);
  }

  public void start() {
    String portConfigValue = config.getOrDefault(SPARK_SHUFFLE_SERVICE_PORT_KEY, String.valueOf(DEFAULT_SPARK_SHUFFLE_SERVICE_PORT));
    int port = Integer.parseInt(portConfigValue);

    TransportConf transportConf = new TransportConf("remoteShuffle", new MapConfigProvider(config));
    shuffleHandler = new ShuffleServerHandler();
    TransportContext transportContext = new TransportContext(transportConf, shuffleHandler);
    List<TransportServerBootstrap> bootstraps = Collections.emptyList();
    shuffleServer = transportContext.createServer(port, bootstraps);
    boundPort = shuffleServer.getPort();
  }

  public void stop() {
    if (shuffleServer != null) {
      shuffleServer.close();
    }
  }

  public int getBoundPort() {
    return boundPort;
  }

  public static void main(String[] args) {
  }
}
