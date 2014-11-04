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

package org.apache.spark.network.sasl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;

public class SaslBootstrap implements TransportClientBootstrap {
  private final Logger logger = LoggerFactory.getLogger(SaslBootstrap.class);

  private final String secretKeyId;
  private final SecretKeyHolder secretKeyHolder;

  public SaslBootstrap(String secretKeyId, SecretKeyHolder secretKeyHolder) {
    this.secretKeyId = secretKeyId;
    this.secretKeyHolder = secretKeyHolder;
  }

  public void doBootstrap(TransportClient client) {
    SparkSaslClient saslClient = new SparkSaslClient(secretKeyId, secretKeyHolder);
    try {
      byte[] payload = saslClient.firstToken();

      while (!saslClient.isComplete()) {
        SaslMessage msg = new SaslMessage(secretKeyId, payload);
        logger.info("Sending msg {} {}", secretKeyId, payload.length);
        ByteBuf buf = Unpooled.buffer(msg.encodedLength());
        msg.encode(buf);

        byte[] response = client.sendRpcSync(buf.array(), 300000);
        logger.info("Got response {} {}", secretKeyId, response.length);
        payload = saslClient.response(response);
      }
    } finally {
      try {
        // Once authentication is complete, the server will trust all remaining communication.
        saslClient.dispose();
      } catch (RuntimeException e) {
        logger.error("Error while disposing SASL client", e);
      }
    }
  }
}
