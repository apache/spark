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
import java.util.Arrays;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.spark.network.protocol.Encodable;

public class AuthMessagesSuite {

  private static int COUNTER = 0;

  private static String string() {
    return String.valueOf(COUNTER++);
  }

  private static byte[] byteArray() {
    byte[] bytes = new byte[COUNTER++];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) COUNTER;
    } return bytes;
  }

  private static int integer() {
    return COUNTER++;
  }

  @Test
  public void testClientChallenge() {
    ClientChallenge msg = new ClientChallenge(string(), string(), integer(), string(), integer(),
      byteArray(), byteArray());
    ClientChallenge decoded = ClientChallenge.decodeMessage(encode(msg));

    assertEquals(msg.appId, decoded.appId);
    assertEquals(msg.kdf, decoded.kdf);
    assertEquals(msg.iterations, decoded.iterations);
    assertEquals(msg.cipher, decoded.cipher);
    assertEquals(msg.keyLength, decoded.keyLength);
    assertTrue(Arrays.equals(msg.nonce, decoded.nonce));
    assertTrue(Arrays.equals(msg.challenge, decoded.challenge));
  }

  @Test
  public void testServerResponse() {
    ServerResponse msg = new ServerResponse(byteArray(), byteArray(), byteArray(), byteArray());
    ServerResponse decoded = ServerResponse.decodeMessage(encode(msg));
    assertTrue(Arrays.equals(msg.response, decoded.response));
    assertTrue(Arrays.equals(msg.nonce, decoded.nonce));
    assertTrue(Arrays.equals(msg.inputIv, decoded.inputIv));
    assertTrue(Arrays.equals(msg.outputIv, decoded.outputIv));
  }

  private ByteBuffer encode(Encodable msg) {
    ByteBuf buf = Unpooled.buffer();
    msg.encode(buf);
    return buf.nioBuffer();
  }

}
