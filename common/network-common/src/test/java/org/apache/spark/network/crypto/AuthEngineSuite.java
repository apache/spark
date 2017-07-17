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

package org.apache.spark.network.crypto;

import java.util.Arrays;
import static java.nio.charset.StandardCharsets.UTF_8;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class AuthEngineSuite {

  private static TransportConf conf;

  @BeforeClass
  public static void setUp() {
    conf = new TransportConf("rpc", MapConfigProvider.EMPTY);
  }

  @Test
  public void testAuthEngine() throws Exception {
    AuthEngine client = new AuthEngine("appId", "secret", conf);
    AuthEngine server = new AuthEngine("appId", "secret", conf);

    try {
      ClientChallenge clientChallenge = client.challenge();
      ServerResponse serverResponse = server.respond(clientChallenge);
      client.validate(serverResponse);

      TransportCipher serverCipher = server.sessionCipher();
      TransportCipher clientCipher = client.sessionCipher();

      assertTrue(Arrays.equals(serverCipher.getInputIv(), clientCipher.getOutputIv()));
      assertTrue(Arrays.equals(serverCipher.getOutputIv(), clientCipher.getInputIv()));
      assertEquals(serverCipher.getKey(), clientCipher.getKey());
    } finally {
      client.close();
      server.close();
    }
  }

  @Test
  public void testMismatchedSecret() throws Exception {
    AuthEngine client = new AuthEngine("appId", "secret", conf);
    AuthEngine server = new AuthEngine("appId", "different_secret", conf);

    ClientChallenge clientChallenge = client.challenge();
    try {
      server.respond(clientChallenge);
      fail("Should have failed to validate response.");
    } catch (IllegalArgumentException e) {
      // Expected.
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWrongAppId() throws Exception {
    AuthEngine engine = new AuthEngine("appId", "secret", conf);
    ClientChallenge challenge = engine.challenge();

    byte[] badChallenge = engine.challenge(new byte[] { 0x00 }, challenge.nonce,
      engine.rawResponse(engine.challenge));
    engine.respond(new ClientChallenge(challenge.appId, challenge.kdf, challenge.iterations,
      challenge.cipher, challenge.keyLength, challenge.nonce, badChallenge));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWrongNonce() throws Exception {
    AuthEngine engine = new AuthEngine("appId", "secret", conf);
    ClientChallenge challenge = engine.challenge();

    byte[] badChallenge = engine.challenge(challenge.appId.getBytes(UTF_8), new byte[] { 0x00 },
      engine.rawResponse(engine.challenge));
    engine.respond(new ClientChallenge(challenge.appId, challenge.kdf, challenge.iterations,
      challenge.cipher, challenge.keyLength, challenge.nonce, badChallenge));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadChallenge() throws Exception {
    AuthEngine engine = new AuthEngine("appId", "secret", conf);
    ClientChallenge challenge = engine.challenge();

    byte[] badChallenge = new byte[challenge.challenge.length];
    engine.respond(new ClientChallenge(challenge.appId, challenge.kdf, challenge.iterations,
      challenge.cipher, challenge.keyLength, challenge.nonce, badChallenge));
  }

}
