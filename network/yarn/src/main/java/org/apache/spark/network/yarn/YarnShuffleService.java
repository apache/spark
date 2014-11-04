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

package org.apache.spark.network.yarn;

import java.lang.Override;
import java.nio.ByteBuffer;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.util.SystemPropertyConfigProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.ContainerInitializationContext;
import org.apache.hadoop.yarn.server.api.ContainerTerminationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * External shuffle service used by Spark on Yarn.
 */
public class YarnShuffleService extends AuxiliaryService {
  private final Logger logger = LoggerFactory.getLogger(YarnShuffleService.class);

  private static final String SPARK_SHUFFLE_SERVICE_PORT_KEY = "spark.shuffle.service.port";
  private static final int DEFAULT_SPARK_SHUFFLE_SERVICE_PORT = 7337;

  // Actual server that serves the shuffle files
  private TransportServer shuffleServer = null;

  public YarnShuffleService() {
    super("spark_shuffle");
    logger.info("Initializing Yarn shuffle service for Spark");
  }

  /**
   * Start the shuffle server with the given configuration.
   */
  @Override
  protected void serviceInit(Configuration conf) {
    try {
      int port = conf.getInt(
        SPARK_SHUFFLE_SERVICE_PORT_KEY, DEFAULT_SPARK_SHUFFLE_SERVICE_PORT);
      TransportConf transportConf = new TransportConf(new SystemPropertyConfigProvider());
      RpcHandler rpcHandler = new ExternalShuffleBlockHandler();
      TransportContext transportContext = new TransportContext(transportConf, rpcHandler);
      shuffleServer = transportContext.createServer(port);
      logger.info("Started Yarn shuffle service for Spark on port " + port);
    } catch (Exception e) {
      logger.error("Exception in starting Yarn shuffle service for Spark", e);
    }
  }

  @Override
  public void initializeApplication(ApplicationInitializationContext context) {
    ApplicationId appId = context.getApplicationId();
    logger.debug("Initializing application " + appId + "!");
  }

  @Override
  public void stopApplication(ApplicationTerminationContext context) {
    ApplicationId appId = context.getApplicationId();
    logger.debug("Stopping application " + appId + "!");
  }

  @Override
  public ByteBuffer getMetaData() {
    logger.debug("Getting meta data");
    return ByteBuffer.allocate(0);
  }

  @Override
  public void initializeContainer(ContainerInitializationContext context) {
    ContainerId containerId = context.getContainerId();
    logger.debug("Initializing container " + containerId + "!");
  }

  @Override
  public void stopContainer(ContainerTerminationContext context) {
    ContainerId containerId = context.getContainerId();
    logger.debug("Stopping container " + containerId + "!");
  }

  @Override
  protected void serviceStop() {
    if (shuffleServer != null) {
      shuffleServer.close();
    }
  }
}
