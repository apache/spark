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

import java.io.IOException;
import java.security.GeneralSecurityException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import javax.crypto.spec.SecretKeySpec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TransportCipherSuite {

  @Test
  public void testBufferNotLeaksOnInternalError() throws GeneralSecurityException {
    SecretKeySpec fakeAesKey = new SecretKeySpec(new byte[16], "AES");
    TransportCipher cipher = new TransportCipher("ID", fakeAesKey);

    EmbeddedChannel channel = new EmbeddedChannel();
    cipher.addToChannel(channel);

    ByteBuf buffer = Unpooled.wrappedBuffer(new byte[] { 1, 2 });
    ByteBuf buffer2 = Unpooled.wrappedBuffer(new byte[] { 1, 2 });

    assertThrows(InternalError.class, () -> channel.writeInbound(buffer));
    assertEquals(0, buffer.refCnt());

    Throwable expected = assertThrows(Throwable.class, () -> channel.writeInbound(buffer2));
    assertEquals(expected.getClass(), IOException.class);
    assertEquals(0, buffer2.refCnt());

    // Simulate closing the connection
    assertFalse(channel.finish());
  }
}
