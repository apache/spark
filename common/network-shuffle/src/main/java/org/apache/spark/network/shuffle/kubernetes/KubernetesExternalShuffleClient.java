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

package org.apache.spark.network.shuffle.kubernetes;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.shuffle.ExternalShuffleClient;
import org.apache.spark.network.shuffle.protocol.RegisterDriver;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A client for talking to the external shuffle service in Kubernetes cluster mode.
 *
 * This is used by the each Spark executor to register with a corresponding external
 * shuffle service on the cluster. The purpose is for cleaning up shuffle files
 * reliably if the application exits unexpectedly.
 */
public class KubernetesExternalShuffleClient extends ExternalShuffleClient {
  private static final Logger logger = LoggerFactory
          .getLogger(KubernetesExternalShuffleClient.class);

  /**
   * Creates an Kubernetes external shuffle client that wraps the {@link ExternalShuffleClient}.
   * Please refer to docs on {@link ExternalShuffleClient} for more information.
   */
  public KubernetesExternalShuffleClient(
      TransportConf conf,
      SecretKeyHolder secretKeyHolder,
      boolean authEnabled,
      long registrationTimeoutMs) {
    super(conf, secretKeyHolder, authEnabled, registrationTimeoutMs);
  }

  public void registerDriverWithShuffleService(String host, int port)
      throws IOException, InterruptedException {
    checkInit();
    ByteBuffer registerDriver = new RegisterDriver(appId, 0).toByteBuffer();
    TransportClient client = clientFactory.createClient(host, port);
    client.sendRpc(registerDriver, new RegisterDriverCallback());
  }

  private class RegisterDriverCallback implements RpcResponseCallback {
    @Override
    public void onSuccess(ByteBuffer response) {
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
    super.close();
  }
}
