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

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.crypto.tink.subtle.Hex;
import org.apache.spark.network.util.*;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

abstract class AuthEngineSuite {
  static final String clientPrivate =
          "efe6b68b3fce92158e3637f6ef9d937e75558928dd4b401de04b43d300a73186";
  static final String clientChallengeHex =
          "fb00000005617070496400000010890b6e960f48e998777267a7e4e623220000003c48ad7dc7ec9466da9" +
          "3bda9f11488dc9404050e02c661d87d67c782444944c6e369b27e0a416c30845a2d9e64271511ca98b41d" +
          "65f8c426e18ff380f6";
  static final String serverResponseHex =
          "fb00000005617070496400000010708451c9dd2792c97c1ca66e6df449ef0000003c64fe899ecdaf458d4" +
          "e25e9d5c5a380b8e6d1a184692fac065ed84f8592c18e9629f9c636809dca2ffc041f20346eb53db78738" +
          "08ecad08b46b5ee3ff";
  static final String derivedKeyId =
          "de04fd52d71040ed9d260579dacfdf4f5695f991ce8ddb1dde05a7335880906e";
  // This key would have been derived for version 1.0 protocol that did not run a final HKDF round.
  static final String unsafeDerivedKey =
          "31963f15a320d5c90333f7ecf5cf3a31c7eaf151de07fef8494663a9f47cfd31";
  static TransportConf conf;

  static TransportConf getConf(int authEngineVerison, boolean useCtr) {
    String authEngineVersion = (authEngineVerison == 1) ? "1" : "2";
    String mode = useCtr ? "AES/CTR/NoPadding" : "AES/GCM/NoPadding";
    Map<String, String> confMap = ImmutableMap.of(
            "spark.network.crypto.enabled", "true",
            "spark.network.crypto.authEngineVersion", authEngineVersion,
            "spark.network.crypto.cipher", mode
    );
    ConfigProvider v2Provider = new MapConfigProvider(confMap);
    return new TransportConf("rpc", v2Provider);
  }

  @Test
  public void testAuthEngine() throws Exception {
    try (AuthEngine client = new AuthEngine("appId", "secret", conf);
         AuthEngine server = new AuthEngine("appId", "secret", conf)) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      client.deriveSessionCipher(clientChallenge, serverResponse);
      TransportCipher serverCipher = server.sessionCipher();
      TransportCipher clientCipher = client.sessionCipher();
      assertEquals(clientCipher.getKeyId(), serverCipher.getKeyId());
    }
  }

  @Test
  public void testFixedChallengeResponse() throws Exception {
    try (AuthEngine client = new AuthEngine("appId", "secret", conf)) {
      byte[] clientPrivateKey = Hex.decode(clientPrivate);
      client.setClientPrivateKey(clientPrivateKey);
      AuthMessage clientChallenge =
              AuthMessage.decodeMessage(ByteBuffer.wrap(Hex.decode(clientChallengeHex)));
      AuthMessage serverResponse =
              AuthMessage.decodeMessage(ByteBuffer.wrap(Hex.decode(serverResponseHex)));
      // Verify that the client will accept an old transcript.
      client.deriveSessionCipher(clientChallenge, serverResponse);
      assertEquals(client.sessionCipher().getKeyId(), derivedKeyId);
    }
  }

  @Test
  public void testCorruptChallengeAppId() throws Exception {
    try (AuthEngine client = new AuthEngine("appId", "secret", conf);
         AuthEngine server = new AuthEngine("appId", "secret", conf)) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage corruptChallenge =
              new AuthMessage("junk", clientChallenge.salt(), clientChallenge.ciphertext());
      assertThrows(IllegalArgumentException.class, () -> server.response(corruptChallenge));
    }
  }

  @Test
  public void testCorruptChallengeSalt() throws Exception {
    try (AuthEngine client = new AuthEngine("appId", "secret", conf);
         AuthEngine server = new AuthEngine("appId", "secret", conf)) {
      AuthMessage clientChallenge = client.challenge();
      clientChallenge.salt()[0] ^= 1;
      assertThrows(GeneralSecurityException.class, () -> server.response(clientChallenge));
    }
  }

  @Test
  public void testCorruptChallengeCiphertext() throws Exception {
    try (AuthEngine client = new AuthEngine("appId", "secret", conf);
         AuthEngine server = new AuthEngine("appId", "secret", conf)) {
      AuthMessage clientChallenge = client.challenge();
      clientChallenge.ciphertext()[0] ^= 1;
      assertThrows(GeneralSecurityException.class, () -> server.response(clientChallenge));
    }
  }

  @Test
  public void testCorruptResponseAppId() throws Exception {
    try (AuthEngine client = new AuthEngine("appId", "secret", conf);
         AuthEngine server = new AuthEngine("appId", "secret", conf)) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      AuthMessage corruptResponse =
              new AuthMessage("junk", serverResponse.salt(), serverResponse.ciphertext());
      assertThrows(IllegalArgumentException.class,
        () -> client.deriveSessionCipher(clientChallenge, corruptResponse));
    }
  }

  @Test
  public void testCorruptResponseSalt() throws Exception {
    try (AuthEngine client = new AuthEngine("appId", "secret", conf);
         AuthEngine server = new AuthEngine("appId", "secret", conf)) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      serverResponse.salt()[0] ^= 1;
      assertThrows(GeneralSecurityException.class,
              () -> client.deriveSessionCipher(clientChallenge, serverResponse));
    }
  }

  @Test
  public void testCorruptServerCiphertext() throws Exception {
    try (AuthEngine client = new AuthEngine("appId", "secret", conf);
         AuthEngine server = new AuthEngine("appId", "secret", conf)) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      serverResponse.ciphertext()[0] ^= 1;
      assertThrows(GeneralSecurityException.class,
        () -> client.deriveSessionCipher(clientChallenge, serverResponse));
    }
  }

  @Test
  public void testFixedChallenge() throws Exception {
    try (AuthEngine server = new AuthEngine("appId", "secret", conf)) {
      AuthMessage clientChallenge =
              AuthMessage.decodeMessage(ByteBuffer.wrap(Hex.decode(clientChallengeHex)));
      // This tests that the server will accept an old challenge as expected. However,
      // it will generate a fresh ephemeral keypair, so we can't replay an old session.
      server.response(clientChallenge);
    }
  }

  @Test
  public void testMismatchedSecret() throws Exception {
    try (AuthEngine client = new AuthEngine("appId", "secret", conf);
         AuthEngine server = new AuthEngine("appId", "different_secret", conf)) {
      AuthMessage clientChallenge = client.challenge();
      assertThrows(GeneralSecurityException.class, () -> server.response(clientChallenge));
    }
  }
}
