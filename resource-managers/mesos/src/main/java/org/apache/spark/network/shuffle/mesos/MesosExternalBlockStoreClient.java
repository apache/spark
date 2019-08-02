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

package org.apache.spark.network.shuffle.mesos;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.spark.network.shuffle.protocol.mesos.ShuffleServiceHeartbeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.shuffle.ExternalBlockStoreClient;
import org.apache.spark.network.shuffle.protocol.mesos.RegisterDriver;
import org.apache.spark.network.util.TransportConf;

/**
 * A client for talking to the external shuffle service in Mesos coarse-grained mode.
 *
 * This is used by the Spark driver to register with each external shuffle service on the cluster.
 * The reason why the driver has to talk to the service is for cleaning up shuffle files reliably
 * after the application exits. Mesos does not provide a great alternative to do this, so Spark
 * has to detect this itself.
 */
public class MesosExternalBlockStoreClient extends ExternalBlockStoreClient {
  private static final Logger logger =
      LoggerFactory.getLogger(MesosExternalBlockStoreClient.class);

  private final ScheduledExecutorService heartbeaterThread =
      Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat("mesos-external-shuffle-client-heartbeater")
          .build());

  /**
   * Creates an Mesos external shuffle client that wraps the {@link ExternalBlockStoreClient}.
   * Please refer to docs on {@link ExternalBlockStoreClient} for more information.
   */
  public MesosExternalBlockStoreClient(
      TransportConf conf,
      SecretKeyHolder secretKeyHolder,
      boolean authEnabled,
      long registrationTimeoutMs) {
    super(conf, secretKeyHolder, authEnabled, registrationTimeoutMs);
  }

  public void registerDriverWithShuffleService(
      String host,
      int port,
      long heartbeatTimeoutMs,
      long heartbeatIntervalMs) throws IOException, InterruptedException {

    checkInit();
    ByteBuffer registerDriver = new RegisterDriver(appId, heartbeatTimeoutMs).toByteBuffer();
    TransportClient client = clientFactory.createClient(host, port);
    client.sendRpc(registerDriver, new RegisterDriverCallback(client, heartbeatIntervalMs));
  }

  private class RegisterDriverCallback implements RpcResponseCallback {
    private final TransportClient client;
    private final long heartbeatIntervalMs;

    private RegisterDriverCallback(TransportClient client, long heartbeatIntervalMs) {
      this.client = client;
      this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    @Override
    public void onSuccess(ByteBuffer response) {
      heartbeaterThread.scheduleAtFixedRate(
          new Heartbeater(client), 0, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
      logger.info("Successfully registered app " + appId + " with external shuffle service.");
    }

    @Override
    public void onFailure(Throwable e) {
      logger.warn("Unable to register app " + appId + " with external shuffle service. " +
          "Please manually remove shuffle data after driver exit. Error: " + e);
    }
  }

  @Override
  public void close() {
    heartbeaterThread.shutdownNow();
    super.close();
  }

  private class Heartbeater implements Runnable {

    private final TransportClient client;

    private Heartbeater(TransportClient client) {
      this.client = client;
    }

    @Override
    public void run() {
      // TODO: Stop sending heartbeats if the shuffle service has lost the app due to timeout
      client.send(new ShuffleServiceHeartbeat(appId).toByteBuffer());
    }
  }
}
