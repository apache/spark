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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.apache.spark.network.util.AbstractFileRegion;
import org.apache.spark.network.util.ByteBufferWriteableChannel;
import org.apache.spark.network.util.TransportConf;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import javax.crypto.AEADBadTagException;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

public class GcmAuthEngineSuite extends AuthEngineSuite {

  @Before
  public void setUp() {
    // Uses GCM mode
    conf = getConf(2, false);
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
      ((Buffer) ciphertextBuffer).flip();
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

  static class FakeRegion extends AbstractFileRegion {
    private final ByteBuffer[] source;
    private int sourcePosition;
    private final long count;

    FakeRegion(ByteBuffer... source) {
      this.source = source;
      sourcePosition = 0;
      count = remaining();
    }

    private long remaining() {
      long remaining = 0;
      for (ByteBuffer buffer : source) {
        remaining += buffer.remaining();
      }
      return remaining;
    }

    @Override
    public long position() {
      return 0;
    }

    @Override
    public long transferred() {
      return count - remaining();
    }

    @Override
    public long count() {
      return count;
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
      if (sourcePosition < source.length) {
        ByteBuffer currentBuffer = source[sourcePosition];
        long written = target.write(currentBuffer);
        if (!currentBuffer.hasRemaining()) {
          sourcePosition++;
        }
        return written;
      } else {
        return 0;
      }
    }

    @Override
    protected void deallocate() {
    }
  }

  private static ByteBuffer getTestByteBuf(int size, byte fill) {
    byte[] data = new byte[size];
    Arrays.fill(data, fill);
    return ByteBuffer.wrap(data);
  }

  @Test
  public void testGcmEncryptedMessageFileRegion() throws Exception {
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
      int halfSegmentSize = plaintextSegmentSize / 2;
      int totalSize = plaintextSegmentSize + halfSegmentSize;

      // Set up some fragmented segments to test
      ByteBuffer halfSegment = getTestByteBuf(halfSegmentSize, (byte) 'a');
      int smallFragmentSize = 128;
      ByteBuffer smallFragment = getTestByteBuf(smallFragmentSize, (byte) 'b');
      int remainderSize = totalSize - halfSegmentSize - smallFragmentSize;
      ByteBuffer remainder = getTestByteBuf(remainderSize, (byte) 'c');
      FakeRegion fakeRegion = new FakeRegion(halfSegment, smallFragment, remainder);
      assertEquals(totalSize, fakeRegion.count());

      ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
      ChannelPromise promise = mock(ChannelPromise.class);
      ArgumentCaptor<GcmTransportCipher.GcmEncryptedMessage> captorWrappedEncrypted =
              ArgumentCaptor.forClass(GcmTransportCipher.GcmEncryptedMessage.class);
      encryptionHandler.write(ctx, fakeRegion, promise);
      verify(ctx).write(captorWrappedEncrypted.capture(), eq(promise));

      // Get the encrypted value and pass it to the decryption handler
      GcmTransportCipher.GcmEncryptedMessage encrypted =
              captorWrappedEncrypted.getValue();
      ByteBuffer ciphertextBuffer =
              ByteBuffer.allocate((int) encrypted.count());
      ByteBufferWriteableChannel channel =
              new ByteBufferWriteableChannel(ciphertextBuffer);

      // We'll simulate the FileRegion only transferring half a segment.
      // The encrypted message should buffer the partial segment plaintext.
      long ciphertextTransferred = 0;
      while (ciphertextTransferred < encrypted.count()) {
        long chunkTransferred = encrypted.transferTo(channel, 0);
        ciphertextTransferred += chunkTransferred;
      }
      assertEquals(encrypted.count(), ciphertextTransferred);

      ((Buffer) ciphertextBuffer).flip();
      ByteBuf ciphertext = Unpooled.wrappedBuffer(ciphertextBuffer);

      // Capture the decrypted values and verify them
      ArgumentCaptor<ByteBuf> captorPlaintext = ArgumentCaptor.forClass(ByteBuf.class);
      decryptionHandler.channelRead(ctx, ciphertext);
      verify(ctx, times(2)).fireChannelRead(captorPlaintext.capture());
      ByteBuf plaintext = captorPlaintext.getValue();
      // We expect this to be the last partial plaintext segment
      int expectedLength = totalSize % plaintextSegmentSize;
      assertEquals(expectedLength, plaintext.readableBytes());
      // This will be the "remainder" segment that is filled to 'c'
      assertEquals('c', plaintext.getByte(0));
    }
  }


  @Test
  public void testGcmUnalignedDecryption() throws Exception {
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
      int plaintextSize = plaintextSegmentSize + (plaintextSegmentSize / 2);
      byte[] data = new byte[plaintextSize];
      Arrays.fill(data, (byte) 'x');
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
      ((Buffer) ciphertextBuffer).flip();
      ByteBuf ciphertext = Unpooled.wrappedBuffer(ciphertextBuffer);

      // Split up the ciphertext into some different sized chunks
      int firstChunkSize = plaintextSize / 2;
      ByteBuf mockCiphertext = spy(ciphertext);
      when(mockCiphertext.readableBytes())
              .thenReturn(firstChunkSize, firstChunkSize).thenCallRealMethod();

      // Capture the decrypted values and verify them
      ArgumentCaptor<ByteBuf> captorPlaintext = ArgumentCaptor.forClass(ByteBuf.class);
      decryptionHandler.channelRead(ctx, mockCiphertext);
      verify(ctx, times(2)).fireChannelRead(captorPlaintext.capture());
      ByteBuf lastPlaintextSegment = captorPlaintext.getValue();
      assertEquals(plaintextSegmentSize/2,
              lastPlaintextSegment.readableBytes());
      assertEquals('x',
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
      ((Buffer) ciphertextBuffer).flip();
      ByteBuf ciphertext = Unpooled.wrappedBuffer(ciphertextBuffer);

      byte b = ciphertext.getByte(100);
      // Inverting the bits of the 100th bit
      ciphertext.setByte(100, ~b & 0xFF);
      assertThrows(AEADBadTagException.class, () -> decryptionHandler.channelRead(ctx, ciphertext));
    }
  }
}
