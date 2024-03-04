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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;

import com.google.crypto.tink.subtle.Hex;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.FileRegion;
import org.apache.spark.network.util.ByteArrayWritableChannel;
import static org.junit.jupiter.api.Assertions.*;

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
  private static final String expectedTransportCipherId =
      "TransportCipher(id=bzYVxiQSeBV4oGiLKV0BnFkrAOp0ijld4h47pVh6Sh8)";

  @Test
  public void testAuthEngine() throws Exception {

    try (AuthEngine client = new AuthEngine("appId", "secret");
         AuthEngine server = new AuthEngine("appId", "secret")) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      client.deriveSessionCipher(clientChallenge, serverResponse);

      TransportCipher serverCipher = server.sessionCipher();
      TransportCipher clientCipher = client.sessionCipher();

      // Encrypt a message from the server
      String plaintext = "Hello world";
      ByteArrayOutputStream ciphertext = new ByteArrayOutputStream(100);
      OutputStream encryptStream = serverCipher.getGcmStreamer().newEncryptingStream(ciphertext, new byte[0]);
      encryptStream.write(plaintext.getBytes(StandardCharsets.UTF_8));
      encryptStream.close();

      // Decrypt it on the client
      ByteArrayInputStream ciphertextInputStream = new ByteArrayInputStream(ciphertext.toByteArray());
      InputStream decryptStream = clientCipher.getGcmStreamer().newDecryptingStream(ciphertextInputStream, new byte[0]);
      byte[] decrypted = decryptStream.readAllBytes();
      String decryptedString = new String(decrypted, StandardCharsets.UTF_8);
      assertEquals(plaintext, decryptedString);
    }
  }

  @Test
  public void testCorruptChallengeAppId() throws Exception {

    try (AuthEngine client = new AuthEngine("appId", "secret");
         AuthEngine server = new AuthEngine("appId", "secret")) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage corruptChallenge =
              new AuthMessage("junk", clientChallenge.salt(), clientChallenge.ciphertext());
      assertThrows(IllegalArgumentException.class, () -> server.response(corruptChallenge));
    }
  }

  @Test
  public void testCorruptChallengeSalt() throws Exception {

    try (AuthEngine client = new AuthEngine("appId", "secret");
         AuthEngine server = new AuthEngine("appId", "secret")) {
      AuthMessage clientChallenge = client.challenge();
      clientChallenge.salt()[0] ^= 1;
      assertThrows(GeneralSecurityException.class, () -> server.response(clientChallenge));
    }
  }

  @Test
  public void testCorruptChallengeCiphertext() throws Exception {

    try (AuthEngine client = new AuthEngine("appId", "secret");
         AuthEngine server = new AuthEngine("appId", "secret")) {
      AuthMessage clientChallenge = client.challenge();
      clientChallenge.ciphertext()[0] ^= 1;
      assertThrows(GeneralSecurityException.class, () -> server.response(clientChallenge));
    }
  }

  @Test
  public void testCorruptResponseAppId() throws Exception {

    try (AuthEngine client = new AuthEngine("appId", "secret");
         AuthEngine server = new AuthEngine("appId", "secret")) {
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

    try (AuthEngine client = new AuthEngine("appId", "secret");
         AuthEngine server = new AuthEngine("appId", "secret")) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      serverResponse.salt()[0] ^= 1;
      assertThrows(GeneralSecurityException.class,
        () -> client.deriveSessionCipher(clientChallenge, serverResponse));
    }
  }

  @Test
  public void testCorruptServerCiphertext() throws Exception {

    try (AuthEngine client = new AuthEngine("appId", "secret");
         AuthEngine server = new AuthEngine("appId", "secret")) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      serverResponse.ciphertext()[0] ^= 1;
      assertThrows(GeneralSecurityException.class,
        () -> client.deriveSessionCipher(clientChallenge, serverResponse));
    }
  }

  @Test
  public void testFixedChallenge() throws Exception {
    try (AuthEngine server = new AuthEngine("appId", "secret")) {
      AuthMessage clientChallenge =
              AuthMessage.decodeMessage(ByteBuffer.wrap(Hex.decode(clientChallengeHex)));
      // This tests that the server will accept an old challenge as expected. However,
      // it will generate a fresh ephemeral keypair, so we can't replay an old session.
      server.response(clientChallenge);
    }
  }

  @Test
  public void testFixedChallengeResponse() throws Exception {
    try (AuthEngine client = new AuthEngine("appId", "secret")) {
      byte[] clientPrivateKey = Hex.decode(clientPrivate);
      client.setClientPrivateKey(clientPrivateKey);
      AuthMessage clientChallenge =
              AuthMessage.decodeMessage(ByteBuffer.wrap(Hex.decode(clientChallengeHex)));
      AuthMessage serverResponse =
              AuthMessage.decodeMessage(ByteBuffer.wrap(Hex.decode(serverResponseHex)));
      // Verify that the client will accept an old transcript.
      client.deriveSessionCipher(clientChallenge, serverResponse);
      TransportCipher clientCipher = client.sessionCipher();
      String id = clientCipher.toString();
      assertEquals(expectedTransportCipherId, id);
    }
  }

  @Test
  public void testMismatchedSecret() throws Exception {
    try (AuthEngine client = new AuthEngine("appId", "secret");
         AuthEngine server = new AuthEngine("appId", "different_secret")) {
      AuthMessage clientChallenge = client.challenge();
      assertThrows(GeneralSecurityException.class, () -> server.response(clientChallenge));
    }
  }

  @Test
  public void testEncryptedMessage() throws Exception {
    try (AuthEngine client = new AuthEngine("appId", "secret");
         AuthEngine server = new AuthEngine("appId", "secret")) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      client.deriveSessionCipher(clientChallenge, serverResponse);

      TransportCipher transportCipher = server.sessionCipher();
      TransportCipher.EncryptionHandler handler = new TransportCipher.EncryptionHandler(transportCipher);
      int[] TEST_PLAINTEXT_LENGTHS = {
              10,
              100,
              TransportCipher.PLAINTEXT_SEGMENT_SIZE_32K_BYTES,
              TransportCipher.PLAINTEXT_SEGMENT_SIZE_32K_BYTES + 1,
              TransportCipher.PLAINTEXT_SEGMENT_SIZE_32K_BYTES * 2,
              TransportCipher.PLAINTEXT_SEGMENT_SIZE_32K_BYTES * 3 + 100
      };

      for (int dataLength : TEST_PLAINTEXT_LENGTHS) {
        byte[] data = new byte[dataLength];
        Arrays.fill(data, (byte) 'x');
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        ByteArrayWritableChannel channel = new ByteArrayWritableChannel((int) transportCipher.expectedCiphertextSize(dataLength));
        TransportCipher.EncryptedMessage<ByteBuf> emsg = handler.createEncryptedMessage(buf);
        while (emsg.transferred() < emsg.count()) {
          emsg.transferTo(channel, emsg.transferred());
        }
        assertEquals(dataLength, emsg.transferred());
      }
    }
  }

  @Test
  public void testDestinationTooSmall() throws Exception {
    try (AuthEngine client = new AuthEngine("appId", "secret");
         AuthEngine server = new AuthEngine("appId", "secret")) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      client.deriveSessionCipher(clientChallenge, serverResponse);

      TransportCipher transportCipher = server.sessionCipher();
      TransportCipher.EncryptionHandler handler = new TransportCipher.EncryptionHandler(transportCipher);
      int fixedDataLength = TransportCipher.PLAINTEXT_SEGMENT_SIZE_32K_BYTES * 3 + 100;
      byte[] data = new byte[fixedDataLength];
      Arrays.fill(data, (byte) 'x');
      ByteBuf buf = Unpooled.wrappedBuffer(data);
      // This output channel will be too short since it will not have space for headers and authentication tags
      ByteArrayWritableChannel channel = new ByteArrayWritableChannel(fixedDataLength);
      TransportCipher.EncryptedMessage<ByteBuf> emsg = handler.createEncryptedMessage(buf);

      assertThrows(IOException.class, () -> {
        while (emsg.transferred() < emsg.count()) {
          emsg.transferTo(channel, emsg.transferred());
        }
      });
      assertEquals(fixedDataLength, emsg.transferred());
    }
  }

  @Test
  public void testEncryptedMessageWhenTransferringZeroBytes() throws Exception {
    try (AuthEngine client = new AuthEngine("appId", "secret");
         AuthEngine server = new AuthEngine("appId", "secret")) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      client.deriveSessionCipher(clientChallenge, serverResponse);

      TransportCipher transportCipher = server.sessionCipher();
      TransportCipher.EncryptionHandler handler = new TransportCipher.EncryptionHandler(transportCipher);

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
            byte[] testData = new byte[testDataLength];
            Arrays.fill(testData, (byte) 'x');
            channel.write(ByteBuffer.wrap(testData));
            return (long) testDataLength;
          }
        }
      });

      TransportCipher.EncryptedMessage<FileRegion> emsg = handler.createEncryptedMessage(region);
      int expectedCiphertextLength = (int) emsg.expectedCiphertextSize(testDataLength);
      ByteArrayWritableChannel channel = new ByteArrayWritableChannel(expectedCiphertextLength);
      // "transferTo" should act correctly when the underlying FileRegion transfers 0 bytes.
      assertEquals(0L, emsg.transferTo(channel, emsg.transferred()));
      assertEquals(expectedCiphertextLength, emsg.transferTo(channel, emsg.transferred()));
      assertEquals(emsg.transferred(), emsg.count());
      assertEquals(expectedCiphertextLength, channel.length());
    }
  }
}
