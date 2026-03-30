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
import org.apache.spark.network.util.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import javax.crypto.AEADBadTagException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class GcmAuthEngineSuite extends AuthEngineSuite {

  @BeforeAll
  public static void setUp() {
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

      ciphertextBuffer.flip();
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
      ciphertextBuffer.flip();
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

  /**
   * Verifies that the same DecryptionHandler instance correctly decodes multiple independent
   * GCM-encrypted messages sent over the same channel. This is the regression test for the
   * bug where DecryptionHandler.completed was never reset, causing every message after the
   * first to be silently dropped — which manifested as YARN container launch failures.
   */
  @Test
  public void testMultipleMessages() throws Exception {
    TransportConf gcmConf = getConf(2, false);
    try (AuthEngine client = new AuthEngine("appId", "secret", gcmConf);
         AuthEngine server = new AuthEngine("appId", "secret", gcmConf)) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      client.deriveSessionCipher(clientChallenge, serverResponse);
      TransportCipher cipher = server.sessionCipher();
      assert (cipher instanceof GcmTransportCipher);
      GcmTransportCipher gcmTransportCipher = (GcmTransportCipher) cipher;

      GcmTransportCipher.EncryptionHandler encryptionHandler =
              gcmTransportCipher.getEncryptionHandler();
      GcmTransportCipher.DecryptionHandler decryptionHandler =
              gcmTransportCipher.getDecryptionHandler();

      ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
      ChannelPromise promise = mock(ChannelPromise.class);

      // --- First message ---
      byte[] data1 = new byte[1024];
      Arrays.fill(data1, (byte) 'A');
      ArgumentCaptor<GcmTransportCipher.GcmEncryptedMessage> captor1 =
              ArgumentCaptor.forClass(GcmTransportCipher.GcmEncryptedMessage.class);
      encryptionHandler.write(ctx, Unpooled.wrappedBuffer(data1), promise);
      verify(ctx).write(captor1.capture(), eq(promise));
      ByteBuffer ct1 = ByteBuffer.allocate((int) captor1.getValue().count());
      captor1.getValue().transferTo(new ByteBufferWriteableChannel(ct1), 0);
      ct1.flip();

      ArgumentCaptor<ByteBuf> plaintextCaptor1 = ArgumentCaptor.forClass(ByteBuf.class);
      decryptionHandler.channelRead(ctx, Unpooled.wrappedBuffer(ct1));
      verify(ctx, atLeastOnce()).fireChannelRead(plaintextCaptor1.capture());
      byte[] decrypted1 = new byte[data1.length];
      int offset = 0;
      for (ByteBuf segment : plaintextCaptor1.getAllValues()) {
        int len = segment.readableBytes();
        segment.readBytes(decrypted1, offset, len);
        offset += len;
      }
      assertEquals(data1.length, offset);
      assertArrayEquals(data1, decrypted1);

      // --- Second message (same handler, different content) ---
      reset(ctx);
      byte[] data2 = new byte[2048];
      Arrays.fill(data2, (byte) 'B');
      ArgumentCaptor<GcmTransportCipher.GcmEncryptedMessage> captor2 =
              ArgumentCaptor.forClass(GcmTransportCipher.GcmEncryptedMessage.class);
      encryptionHandler.write(ctx, Unpooled.wrappedBuffer(data2), promise);
      verify(ctx).write(captor2.capture(), eq(promise));
      ByteBuffer ct2 = ByteBuffer.allocate((int) captor2.getValue().count());
      captor2.getValue().transferTo(new ByteBufferWriteableChannel(ct2), 0);
      ct2.flip();

      ArgumentCaptor<ByteBuf> plaintextCaptor2 = ArgumentCaptor.forClass(ByteBuf.class);
      decryptionHandler.channelRead(ctx, Unpooled.wrappedBuffer(ct2));
      verify(ctx, atLeastOnce()).fireChannelRead(plaintextCaptor2.capture());
      byte[] decrypted2 = new byte[data2.length];
      offset = 0;
      for (ByteBuf segment : plaintextCaptor2.getAllValues()) {
        int len = segment.readableBytes();
        segment.readBytes(decrypted2, offset, len);
        offset += len;
      }
      assertEquals(data2.length, offset);
      assertArrayEquals(data2, decrypted2);
    }
  }

  /**
   * Verifies that multiple GCM-encrypted messages delivered inside a single channelRead()
   * call (TCP coalescing) are all decoded correctly. This is the regression test for the
   * IllegalStateException("Invalid expected ciphertext length") observed under SparkSQL
   * shuffle load: when Netty batches two messages into one ByteBuf, the old code released
   * the buffer after the first message, discarding remaining bytes. The next channelRead()
   * then read bytes from the middle of the second message as a length header.
   */
  @Test
  public void testBatchedMessages() throws Exception {
    TransportConf gcmConf = getConf(2, false);
    try (AuthEngine client = new AuthEngine("appId", "secret", gcmConf);
         AuthEngine server = new AuthEngine("appId", "secret", gcmConf)) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      client.deriveSessionCipher(clientChallenge, serverResponse);
      TransportCipher cipher = server.sessionCipher();
      assert (cipher instanceof GcmTransportCipher);
      GcmTransportCipher gcmTransportCipher = (GcmTransportCipher) cipher;

      GcmTransportCipher.EncryptionHandler encryptionHandler =
              gcmTransportCipher.getEncryptionHandler();
      GcmTransportCipher.DecryptionHandler decryptionHandler =
              gcmTransportCipher.getDecryptionHandler();

      ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
      ChannelPromise promise = mock(ChannelPromise.class);

      byte[] data1 = new byte[1024];
      Arrays.fill(data1, (byte) 'A');
      ArgumentCaptor<GcmTransportCipher.GcmEncryptedMessage> captor1 =
              ArgumentCaptor.forClass(GcmTransportCipher.GcmEncryptedMessage.class);
      encryptionHandler.write(ctx, Unpooled.wrappedBuffer(data1), promise);
      verify(ctx).write(captor1.capture(), eq(promise));
      ByteBuffer ct1 = ByteBuffer.allocate((int) captor1.getValue().count());
      captor1.getValue().transferTo(new ByteBufferWriteableChannel(ct1), 0);
      ct1.flip();

      reset(ctx);
      byte[] data2 = new byte[2048];
      Arrays.fill(data2, (byte) 'B');
      ArgumentCaptor<GcmTransportCipher.GcmEncryptedMessage> captor2 =
              ArgumentCaptor.forClass(GcmTransportCipher.GcmEncryptedMessage.class);
      encryptionHandler.write(ctx, Unpooled.wrappedBuffer(data2), promise);
      verify(ctx).write(captor2.capture(), eq(promise));
      ByteBuffer ct2 = ByteBuffer.allocate((int) captor2.getValue().count());
      captor2.getValue().transferTo(new ByteBufferWriteableChannel(ct2), 0);
      ct2.flip();

      // Simulate TCP coalescing: deliver both ciphertexts in one channelRead() call.
      reset(ctx);
      ByteBuf batched = Unpooled.wrappedBuffer(ct1, ct2);
      ArgumentCaptor<ByteBuf> plaintextCaptor = ArgumentCaptor.forClass(ByteBuf.class);
      decryptionHandler.channelRead(ctx, batched);
      verify(ctx, atLeastOnce()).fireChannelRead(plaintextCaptor.capture());

      byte[] decrypted = new byte[data1.length + data2.length];
      int offset = 0;
      for (ByteBuf segment : plaintextCaptor.getAllValues()) {
        int len = segment.readableBytes();
        segment.readBytes(decrypted, offset, len);
        offset += len;
      }
      assertEquals(data1.length + data2.length, offset);
      assertArrayEquals(data1, Arrays.copyOfRange(decrypted, 0, data1.length));
      assertArrayEquals(data2, Arrays.copyOfRange(decrypted, data1.length, decrypted.length));
    }
  }

  /**
   * Verifies that DecryptionHandler correctly handles a GCM message whose framing header
   * is split across two channelRead() calls. This is the regression test for the
   * IndexOutOfBoundsException in initializeDecrypter observed in benchmarking: when only
   * 4 bytes of the 24-byte GCM internal header arrived in one Netty buffer,
   * ByteBuf.readBytes(ByteBuffer) threw because it requires all dst.remaining() bytes to
   * be available rather than performing a partial fill.
   */
  @Test
  public void testSplitHeader() throws Exception {
    TransportConf gcmConf = getConf(2, false);
    try (AuthEngine client = new AuthEngine("appId", "secret", gcmConf);
         AuthEngine server = new AuthEngine("appId", "secret", gcmConf)) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      client.deriveSessionCipher(clientChallenge, serverResponse);
      TransportCipher cipher = server.sessionCipher();
      assert (cipher instanceof GcmTransportCipher);
      GcmTransportCipher gcmTransportCipher = (GcmTransportCipher) cipher;

      GcmTransportCipher.EncryptionHandler encryptionHandler =
              gcmTransportCipher.getEncryptionHandler();
      GcmTransportCipher.DecryptionHandler decryptionHandler =
              gcmTransportCipher.getDecryptionHandler();

      ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
      ChannelPromise promise = mock(ChannelPromise.class);

      byte[] data = new byte[1024];
      Arrays.fill(data, (byte) 'X');
      ArgumentCaptor<GcmTransportCipher.GcmEncryptedMessage> captor =
              ArgumentCaptor.forClass(GcmTransportCipher.GcmEncryptedMessage.class);
      encryptionHandler.write(ctx, Unpooled.wrappedBuffer(data), promise);
      verify(ctx).write(captor.capture(), eq(promise));

      ByteBuffer ciphertextBuffer = ByteBuffer.allocate((int) captor.getValue().count());
      captor.getValue().transferTo(new ByteBufferWriteableChannel(ciphertextBuffer), 0);
      ciphertextBuffer.flip();
      byte[] ciphertext = new byte[ciphertextBuffer.remaining()];
      ciphertextBuffer.get(ciphertext);

      // Split in the middle of the 24-byte GCM internal header:
      // chunk1 = [8-byte length field][4 bytes of GCM header]
      // chunk2 = [remaining 20 bytes of GCM header][full ciphertext]
      int splitPoint = 8 + 4;
      ByteBuf chunk1 = Unpooled.wrappedBuffer(ciphertext, 0, splitPoint);
      ByteBuf chunk2 = Unpooled.wrappedBuffer(
              ciphertext, splitPoint, ciphertext.length - splitPoint);

      decryptionHandler.channelRead(ctx, chunk1);
      // Only a partial header was delivered; no plaintext should be emitted yet.
      verify(ctx, never()).fireChannelRead(any());

      ArgumentCaptor<ByteBuf> plaintextCaptor = ArgumentCaptor.forClass(ByteBuf.class);
      decryptionHandler.channelRead(ctx, chunk2);
      verify(ctx, atLeastOnce()).fireChannelRead(plaintextCaptor.capture());

      byte[] decrypted = new byte[data.length];
      int offset = 0;
      for (ByteBuf segment : plaintextCaptor.getAllValues()) {
        int len = segment.readableBytes();
        segment.readBytes(decrypted, offset, len);
        offset += len;
      }
      assertEquals(data.length, offset);
      assertArrayEquals(data, decrypted);
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
}
