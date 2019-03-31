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
import java.util.Arrays;
import java.util.Map;
import java.security.InvalidKeyException;
import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.FileRegion;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.spark.network.util.ByteArrayWritableChannel;
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

  @Test(expected = InvalidKeyException.class)
  public void testBadKeySize() throws Exception {
    Map<String, String> mconf = ImmutableMap.of("spark.network.crypto.keyLength", "42");
    TransportConf conf = new TransportConf("rpc", new MapConfigProvider(mconf));

    try (AuthEngine engine = new AuthEngine("appId", "secret", conf)) {
      engine.challenge();
      fail("Should have failed to create challenge message.");

      // Call close explicitly to make sure it's idempotent.
      engine.close();
    }
  }

  @Test
  public void testEncryptedMessage() throws Exception {
    AuthEngine client = new AuthEngine("appId", "secret", conf);
    AuthEngine server = new AuthEngine("appId", "secret", conf);
    try {
      ClientChallenge clientChallenge = client.challenge();
      ServerResponse serverResponse = server.respond(clientChallenge);
      client.validate(serverResponse);

      TransportCipher cipher = server.sessionCipher();
      TransportCipher.EncryptionHandler handler = new TransportCipher.EncryptionHandler(cipher);

      byte[] data = new byte[TransportCipher.STREAM_BUFFER_SIZE + 1];
      new Random().nextBytes(data);
      ByteBuf buf = Unpooled.wrappedBuffer(data);

      ByteArrayWritableChannel channel = new ByteArrayWritableChannel(data.length);
      TransportCipher.EncryptedMessage emsg = handler.createEncryptedMessage(buf);
      while (emsg.transfered() < emsg.count()) {
        emsg.transferTo(channel, emsg.transfered());
      }
      assertEquals(data.length, channel.length());
    } finally {
      client.close();
      server.close();
    }
  }

  @Test
  public void testEncryptedMessageWhenTransferringZeroBytes() throws Exception {
    AuthEngine client = new AuthEngine("appId", "secret", conf);
    AuthEngine server = new AuthEngine("appId", "secret", conf);
    try {
      ClientChallenge clientChallenge = client.challenge();
      ServerResponse serverResponse = server.respond(clientChallenge);
      client.validate(serverResponse);

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
      assertEquals(0L, emsg.transferTo(channel, emsg.transfered()));
      assertEquals(testDataLength, emsg.transferTo(channel, emsg.transfered()));
      assertEquals(emsg.transfered(), emsg.count());
      assertEquals(4, channel.length());
    } finally {
      client.close();
      server.close();
    }
  }
}
