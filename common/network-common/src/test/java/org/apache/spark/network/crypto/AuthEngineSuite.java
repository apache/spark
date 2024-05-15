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
import java.util.Map;
import java.util.Random;

import com.google.common.collect.ImmutableMap;
import com.google.crypto.tink.subtle.Hex;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.FileRegion;
import org.apache.spark.network.crypto.GcmTransportCipher.ByteBufferWriteableChannel;
import org.apache.spark.network.util.ByteArrayWritableChannel;
import org.apache.spark.network.util.ConfigProvider;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.*;

import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.crypto.AEADBadTagException;

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

  private static TransportConf getConf(int authEngineVerison, boolean useCtr) {
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

  @BeforeAll
  public static void setUp() {
    Map<String, String> confMap = ImmutableMap.of(
      "spark.network.crypto.enabled", "true",
      "spark.network.crypto.authEngineVersion", "2",
      "spark.network.crypto.cipher", "AES/CTR/NoPadding"
    );
    ConfigProvider v2Provider = new MapConfigProvider(confMap);
    conf = getConf(2, true);
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
      assert(clientCipher instanceof CtrTransportCipher);
      assert(serverCipher instanceof CtrTransportCipher);
      CtrTransportCipher ctrClient = (CtrTransportCipher) clientCipher;
      CtrTransportCipher ctrServer = (CtrTransportCipher) serverCipher;
      assertArrayEquals(ctrServer.getInputIv(), ctrClient.getOutputIv());
      assertArrayEquals(ctrServer.getOutputIv(), ctrClient.getInputIv());
      assertEquals(ctrServer.getKey(), ctrClient.getKey());
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
      assert(clientCipher instanceof CtrTransportCipher);
      CtrTransportCipher ctrTransportCipher = (CtrTransportCipher) clientCipher;
      assertEquals(Hex.encode(ctrTransportCipher.getKey().getEncoded()), derivedKey);
      assertEquals(Hex.encode(ctrTransportCipher.getInputIv()), inputIv);
      assertEquals(Hex.encode(ctrTransportCipher.getOutputIv()), outputIv);
    }
  }

  @Test
  public void testFixedChallengeResponseUnsafeVersion() throws Exception {
    TransportConf v1Conf = getConf(1, true);
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
      assert(clientCipher instanceof CtrTransportCipher);
      CtrTransportCipher ctrTransportCipher = (CtrTransportCipher) clientCipher;
      assertEquals(Hex.encode(ctrTransportCipher.getKey().getEncoded()), unsafeDerivedKey);
      assertEquals(Hex.encode(ctrTransportCipher.getInputIv()), inputIv);
      assertEquals(Hex.encode(ctrTransportCipher.getOutputIv()), outputIv);
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
  public void testCtrEncryptedMessage() throws Exception {
    try (AuthEngine client = new AuthEngine("appId", "secret", conf);
         AuthEngine server = new AuthEngine("appId", "secret", conf)) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      client.deriveSessionCipher(clientChallenge, serverResponse);

      TransportCipher clientCipher = server.sessionCipher();
      assert(clientCipher instanceof CtrTransportCipher);
      CtrTransportCipher ctrTransportCipher = (CtrTransportCipher) clientCipher;
      CtrTransportCipher.EncryptionHandler handler =
              new CtrTransportCipher.EncryptionHandler(ctrTransportCipher);

      byte[] data = new byte[CtrTransportCipher.STREAM_BUFFER_SIZE + 1];
      new Random().nextBytes(data);
      ByteBuf buf = Unpooled.wrappedBuffer(data);

      ByteArrayWritableChannel channel = new ByteArrayWritableChannel(data.length);
      CtrTransportCipher.EncryptedMessage emsg = handler.createEncryptedMessage(buf);
      while (emsg.transferred() < emsg.count()) {
        emsg.transferTo(channel, emsg.transferred());
      }
      assertEquals(data.length, channel.length());
    }
  }

  private ChannelHandlerContext mockChannelHandlerContext() {
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.fireChannelRead(any())).thenAnswer(in -> {
      ByteBuf buf = (ByteBuf) in.getArguments()[0];
      buf.release();
      return null;
    });
    return ctx;
  }

  @Test
  public void testGcmEncryptedMessage() throws Exception {
    TransportConf gcmConf = getConf(2, false);
    try (AuthEngine client = new AuthEngine("appId", "secret", gcmConf);
         AuthEngine server = new AuthEngine("appId", "secret", gcmConf)) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      client.deriveSessionCipher(clientChallenge, serverResponse);
      TransportCipher clientCipher = server.sessionCipher();
      // Verify that it derives a GcmTransportCipher
      assert (clientCipher instanceof GcmTransportCipher);
      GcmTransportCipher gcmTransportCipher = (GcmTransportCipher) clientCipher;
      GcmTransportCipher.EncryptionHandler encryptionHandler =
              gcmTransportCipher.getEncryptionHandler();
      GcmTransportCipher.DecryptionHandler decryptionHandler =
              gcmTransportCipher.getDecryptionHandler();
      // Allocating 1.5x the buffer size to test multiple segments and a fractional segment.
      int plaintextSegmentSize = GcmTransportCipher.CIPHERTEXT_BUFFER_SIZE - 16;
      byte[] data = new byte[plaintextSegmentSize + (plaintextSegmentSize / 2)];
      // Just writing some bytes.
      data[0] = 'a';
      data[data.length / 2] = 'b';
      data[data.length - 10] = 'c';
      ByteBuf buf = Unpooled.wrappedBuffer(data);

      // Mock the context and capture the arguments passed to it
      ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
      ChannelPromise promise = mock(ChannelPromise.class);
      ArgumentCaptor<GcmTransportCipher.GcmEncryptedMessage> captorWrappedEncrypted =
              ArgumentCaptor.forClass(GcmTransportCipher.GcmEncryptedMessage.class);
      encryptionHandler.write(ctx, buf, promise);
      verify(ctx).write(captorWrappedEncrypted.capture(), eq(promise));

      // Get the encrypted value and pass it to the decryption handler
      GcmTransportCipher.GcmEncryptedMessage encrypted =
              captorWrappedEncrypted.getValue();
      ByteBuffer ciphertextBuffer =
              ByteBuffer.allocate((int) encrypted.count());
      ByteBufferWriteableChannel channel =
              new ByteBufferWriteableChannel(ciphertextBuffer);
      encrypted.transferTo(channel, 0);
      ciphertextBuffer.flip();
      ByteBuf ciphertext = Unpooled.wrappedBuffer(ciphertextBuffer);

      // Capture the decrypted values and verify them
      ArgumentCaptor<ByteBuf> captorPlaintext = ArgumentCaptor.forClass(ByteBuf.class);
      decryptionHandler.channelRead(ctx, ciphertext);
      verify(ctx, times(2))
              .fireChannelRead(captorPlaintext.capture());
      ByteBuf lastPlaintextSegment = captorPlaintext.getValue();
      assertEquals(plaintextSegmentSize/2,
              lastPlaintextSegment.readableBytes());
      assertEquals('c',
              lastPlaintextSegment.getByte((plaintextSegmentSize/2) - 10));
    }
  }

  @Test
  public void testCorruptGcmEncryptedMessage() throws Exception {
    TransportConf gcmConf = getConf(2, false);

    try (AuthEngine client = new AuthEngine("appId", "secret", gcmConf);
         AuthEngine server = new AuthEngine("appId", "secret", gcmConf)) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      client.deriveSessionCipher(clientChallenge, serverResponse);

      TransportCipher clientCipher = server.sessionCipher();
      assert (clientCipher instanceof GcmTransportCipher);

      GcmTransportCipher gcmTransportCipher = (GcmTransportCipher) clientCipher;
      GcmTransportCipher.EncryptionHandler encryptionHandler =
              gcmTransportCipher.getEncryptionHandler();
      GcmTransportCipher.DecryptionHandler decryptionHandler =
              gcmTransportCipher.getDecryptionHandler();
      byte[] zeroData = new byte[1024 * 32];
      // Just writing some bytes.
      ByteBuf buf = Unpooled.wrappedBuffer(zeroData);

      // Mock the context and capture the arguments passed to it
      ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
      ChannelPromise promise = mock(ChannelPromise.class);
      ArgumentCaptor<GcmTransportCipher.GcmEncryptedMessage> captorWrappedEncrypted =
              ArgumentCaptor.forClass(GcmTransportCipher.GcmEncryptedMessage.class);
      encryptionHandler.write(ctx, buf, promise);
      verify(ctx).write(captorWrappedEncrypted.capture(), eq(promise));

      GcmTransportCipher.GcmEncryptedMessage encrypted =
              captorWrappedEncrypted.getValue();
      ByteBuffer ciphertextBuffer =
              ByteBuffer.allocate((int) encrypted.count());
      ByteBufferWriteableChannel channel =
              new ByteBufferWriteableChannel(ciphertextBuffer);
      encrypted.transferTo(channel, 0);
      ciphertextBuffer.flip();
      ByteBuf ciphertext = Unpooled.wrappedBuffer(ciphertextBuffer);

      byte b = ciphertext.getByte(100);
      // Inverting the bits of the 100th bit
      ciphertext.setByte(100, ~b & 0xFF);
      assertThrows(AEADBadTagException.class, () -> decryptionHandler.channelRead(ctx, ciphertext));
    }
  }

  @Test
  public void testCtrEncryptedMessageWhenTransferringZeroBytes() throws Exception {
    try (AuthEngine client = new AuthEngine("appId", "secret", conf);
         AuthEngine server = new AuthEngine("appId", "secret", conf)) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      client.deriveSessionCipher(clientChallenge, serverResponse);
      TransportCipher clientCipher = server.sessionCipher();
      assert(clientCipher instanceof CtrTransportCipher);
      CtrTransportCipher ctrTransportCipher = (CtrTransportCipher) clientCipher;
      CtrTransportCipher.EncryptionHandler handler =
              new CtrTransportCipher.EncryptionHandler(ctrTransportCipher);
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

      CtrTransportCipher.EncryptedMessage emsg = handler.createEncryptedMessage(region);
      ByteArrayWritableChannel channel = new ByteArrayWritableChannel(testDataLength);
      // "transferTo" should act correctly when the underlying FileRegion transfers 0 bytes.
      assertEquals(0L, emsg.transferTo(channel, emsg.transferred()));
      assertEquals(testDataLength, emsg.transferTo(channel, emsg.transferred()));
      assertEquals(emsg.transferred(), emsg.count());
      assertEquals(4, channel.length());
    }
  }
}
