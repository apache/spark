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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;


/**
 * Jointly tests SparkSaslClient and SparkSaslServer, as both are black boxes.
 */
public class SparkSaslSuite {

  /** Provides a secret key holder which returns secret key == appId */
  private SecretKeyHolder secretKeyHolder = new SecretKeyHolder() {
    @Override
    public String getSaslUser(String appId) {
      return "user";
    }

    @Override
    public String getSecretKey(String appId) {
      return appId;
    }
  };

  @Test
  public void testMatching() {
    SparkSaslClient client = new SparkSaslClient("shared-secret", secretKeyHolder);
    SparkSaslServer server = new SparkSaslServer("shared-secret", secretKeyHolder);

    assertFalse(client.isComplete());
    assertFalse(server.isComplete());

    byte[] clientMessage = client.firstToken();

    while (!client.isComplete()) {
      clientMessage = client.response(server.response(clientMessage));
    }
    assertTrue(server.isComplete());

    // Disposal should invalidate
    server.dispose();
    assertFalse(server.isComplete());
    client.dispose();
    assertFalse(client.isComplete());
  }


  @Test
  public void testNonMatching() {
    SparkSaslClient client = new SparkSaslClient("my-secret", secretKeyHolder);
    SparkSaslServer server = new SparkSaslServer("your-secret", secretKeyHolder);

    assertFalse(client.isComplete());
    assertFalse(server.isComplete());

    byte[] clientMessage = client.firstToken();

    try {
      while (!client.isComplete()) {
        clientMessage = client.response(server.response(clientMessage));
      }
      fail("Should not have completed");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Mismatched response"));
      assertFalse(client.isComplete());
      assertFalse(server.isComplete());
    }
  }
}
