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
import java.nio.channels.WritableByteChannel;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.Random;

import com.google.crypto.tink.subtle.Hex;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.FileRegion;
import org.apache.spark.network.util.ByteArrayWritableChannel;
import org.apache.spark.network.util.ConfigProvider;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class AuthEngineSuite {

  private static final String clientPrivate =
      "efe6b68b3fce92158e3637f6ef9d937e75558928dd4b401de04b43d300a73186";
  private static final String clientChallengeHex =
      "fb00000005617070496400000010890b6e960f48e998777267a7e4e623220000003c48ad7dc7ec9466da9" +
      "3bda9f11488dc9404050e02c661d87d67c782444944c6e369b27e0a416c30845a2d9e64271511ca98b41d" +
      "65f8c426e18ff380f6";
  private static final String serverResponseHex =
      "fb00000005617070496400000010708451c9dd2792c97c1ca66e6df449ef0000003c64fe899ecdaf458d4" +
      "e25e9d5c5a380b8e6d1a184692fac065ed84f8592c18e9629f9c636809dca2ffc041f20346eb53db78738" +
      "08ecad08b46b5ee3ff";
  private static final String derivedKey = "2d6e7a9048c8265c33a8f3747bfcc84c";
  // This key would have been derived for version 1.0 protocol that did not run a final HKDF round.
  private static final String unsafeDerivedKey =
      "31963f15a320d5c90333f7ecf5cf3a31c7eaf151de07fef8494663a9f47cfd31";

  private static final String inputIv = "fc6a5dc8b90a9dad8f54f08b51a59ed2";
  private static final String outputIv = "a72709baf00785cad6329ce09f631f71";
  private static TransportConf conf;

  @BeforeAll
  public static void setUp() {
    ConfigProvider v2Provider = new MapConfigProvider(Collections.singletonMap(
            "spark.network.crypto.authEngineVersion", "2"));
    conf = new TransportConf("rpc", v2Provider);
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

      assertArrayEquals(serverCipher.getInputIv(), clientCipher.getOutputIv());
      assertArrayEquals(serverCipher.getOutputIv(), clientCipher.getInputIv());
      assertEquals(serverCipher.getKey(), clientCipher.getKey());
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
      TransportCipher clientCipher = client.sessionCipher();
      assertEquals(Hex.encode(clientCipher.getKey().getEncoded()), derivedKey);
      assertEquals(Hex.encode(clientCipher.getInputIv()), inputIv);
      assertEquals(Hex.encode(clientCipher.getOutputIv()), outputIv);
    }
  }

  @Test
  public void testFixedChallengeResponseUnsafeVersion() throws Exception {
    ConfigProvider v1Provider = new MapConfigProvider(Collections.singletonMap(
            "spark.network.crypto.authEngineVersion", "1"));
    TransportConf v1Conf = new TransportConf("rpc", v1Provider);
    try (AuthEngine client = new AuthEngine("appId", "secret", v1Conf)) {
      byte[] clientPrivateKey = Hex.decode(clientPrivate);
      client.setClientPrivateKey(clientPrivateKey);
      AuthMessage clientChallenge =
              AuthMessage.decodeMessage(ByteBuffer.wrap(Hex.decode(clientChallengeHex)));
      AuthMessage serverResponse =
              AuthMessage.decodeMessage(ByteBuffer.wrap(Hex.decode(serverResponseHex)));
      // Verify that the client will accept an old transcript.
      client.deriveSessionCipher(clientChallenge, serverResponse);
      TransportCipher clientCipher = client.sessionCipher();
      assertEquals(Hex.encode(clientCipher.getKey().getEncoded()), unsafeDerivedKey);
      assertEquals(Hex.encode(clientCipher.getInputIv()), inputIv);
      assertEquals(Hex.encode(clientCipher.getOutputIv()), outputIv);
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

  @Test
  public void testEncryptedMessage() throws Exception {
    try (AuthEngine client = new AuthEngine("appId", "secret", conf);
         AuthEngine server = new AuthEngine("appId", "secret", conf)) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      client.deriveSessionCipher(clientChallenge, serverResponse);

      TransportCipher cipher = server.sessionCipher();
      TransportCipher.EncryptionHandler handler = new TransportCipher.EncryptionHandler(cipher);

      byte[] data = new byte[TransportCipher.STREAM_BUFFER_SIZE + 1];
      new Random().nextBytes(data);
      ByteBuf buf = Unpooled.wrappedBuffer(data);

      ByteArrayWritableChannel channel = new ByteArrayWritableChannel(data.length);
      TransportCipher.EncryptedMessage emsg = handler.createEncryptedMessage(buf);
      while (emsg.transferred() < emsg.count()) {
        emsg.transferTo(channel, emsg.transferred());
      }
      assertEquals(data.length, channel.length());
    }
  }

  @Test
  public void testEncryptedMessageWhenTransferringZeroBytes() throws Exception {
    try (AuthEngine client = new AuthEngine("appId", "secret", conf);
         AuthEngine server = new AuthEngine("appId", "secret", conf)) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      client.deriveSessionCipher(clientChallenge, serverResponse);

      TransportCipher cipher = server.sessionCipher();
      TransportCipher.EncryptionHandler handler = new TransportCipher.EncryptionHandler(cipher);

      int testDataLength = 4;
      FileRegion region = mock(FileRegion.class);
      when(region.count()).thenReturn((long) testDataLength);
      // Make `region.transferTo` do nothing in first call and transfer 4 bytes in the second one.
      when(region.transferTo(any(), anyLong())).thenAnswer(new Answer<Long>() {

        private boolean firstTime = true;

        @Override
        public Long answer(InvocationOnMock invocationOnMock) throws Throwable {
          if (firstTime) {
            firstTime = false;
            return 0L;
          } else {
            WritableByteChannel channel = invocationOnMock.getArgument(0);
            channel.write(ByteBuffer.wrap(new byte[testDataLength]));
            return (long) testDataLength;
          }
        }
      });

      TransportCipher.EncryptedMessage emsg = handler.createEncryptedMessage(region);
      ByteArrayWritableChannel channel = new ByteArrayWritableChannel(testDataLength);
      // "transferTo" should act correctly when the underlying FileRegion transfers 0 bytes.
      assertEquals(0L, emsg.transferTo(channel, emsg.transferred()));
      assertEquals(testDataLength, emsg.transferTo(channel, emsg.transferred()));
      assertEquals(emsg.transferred(), emsg.count());
      assertEquals(4, channel.length());
    }
  }
}
