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

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.shuffle.ExternalShuffleClient;
import org.apache.spark.network.shuffle.protocol.mesos.RegisterDriver;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MesosExternalShuffleClient extends ExternalShuffleClient {
  private final Logger logger = LoggerFactory.getLogger(MesosExternalShuffleClient.class);

  /**
   * Creates an Mesos external shuffle client that wraps the [[ExternalShuffleClient]].
   * Please refer to docs on [[ExternalShuffleClient]] for more information.
   */
  public MesosExternalShuffleClient(
      TransportConf conf,
      SecretKeyHolder secretKeyHolder,
      boolean saslEnabled,
      boolean saslEncryptionEnabled) {
    super(conf, secretKeyHolder, saslEnabled, saslEncryptionEnabled);
  }

  public void registerDriverWithShuffleService(String host, int port) throws IOException {
    checkInit();
    byte[] registerDriver = new RegisterDriver(appId).toByteArray();
    TransportClient client = clientFactory.createClient(host, port);
    client.sendRpc(registerDriver, new RpcResponseCallback() {
      @Override
      public void onSuccess(byte[] response) {
        logger.info("Successfully registered app " + appId);
      }

      @Override
      public void onFailure(Throwable e) {
        logger.warn("Unable to register app " + appId + " with external shuffle service. " +
            "Please manually remove shuffle data after driver exit. Error: " + e);
      }
    });
  }
}
