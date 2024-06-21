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

import com.google.crypto.tink.subtle.Hex;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.FileRegion;
import org.apache.spark.network.util.ByteArrayWritableChannel;
import org.apache.spark.network.util.TransportConf;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class CtrAuthEngineSuite extends AuthEngineSuite {
  private static final String inputIv = "fc6a5dc8b90a9dad8f54f08b51a59ed2";
  private static final String outputIv = "a72709baf00785cad6329ce09f631f71";

  @Before
  public void setUp() {
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
  public void testCtrFixedChallengeIvResponse() throws Exception {
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
      assertEquals(clientCipher.getKeyId(), derivedKeyId);
      assert(clientCipher instanceof CtrTransportCipher);
      CtrTransportCipher ctrTransportCipher = (CtrTransportCipher) clientCipher;
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
