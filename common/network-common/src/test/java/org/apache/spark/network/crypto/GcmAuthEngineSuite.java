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

import com.google.common.primitives.Longs;
import com.google.crypto.tink.subtle.AesGcmHkdfStreaming;
import com.google.crypto.tink.subtle.StreamSegmentEncrypter;
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
import java.security.GeneralSecurityException;
import java.time.Duration;
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
      verify(ctx, times(1))
              .fireChannelRead(captorPlaintext.capture());
      ByteBuf plaintext = captorPlaintext.getValue();
      assertEquals(plaintextSegmentSize + (plaintextSegmentSize / 2),
              plaintext.readableBytes());
      assertEquals('c',
              plaintext.getByte(plaintext.readableBytes() - 10));
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
      verify(ctx, times(1)).fireChannelRead(captorPlaintext.capture());
      ByteBuf plaintext = captorPlaintext.getValue();
      assertEquals(totalSize, plaintext.readableBytes());
      // 'c' starts at the second plaintext segment (offset plaintextSegmentSize)
      assertEquals('c', plaintext.getByte(plaintextSegmentSize));
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
      verify(ctx, times(1)).fireChannelRead(captorPlaintext.capture());
      ByteBuf plaintext = captorPlaintext.getValue();
      assertEquals(plaintextSize, plaintext.readableBytes());
      assertEquals('x', plaintext.getByte(plaintextSize - 10));
    }
  }

  /**
   * Verifies that the same DecryptionHandler instance correctly decodes multiple independent
   * GCM-encrypted messages sent over the same channel. This is the regression test for the
   * bug where DecryptionHandler.completed was never reset, causing every message after the
   * first to be silently dropped - which manifested as YARN container launch failures.
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
  public void testSplitGcmHeader() throws Exception {
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

  /**
   * Verifies that DecryptionHandler correctly handles a GCM message whose 8-byte Spark length
   * prefix is split across two channelRead() calls. This exercises the partial-fill path in
   * {@code initializeExpectedLength}: when fewer than 8 bytes of the length prefix arrive,
   * the method narrows {@code expectedLengthBuffer}'s limit so that {@code readBytes()} does
   * not throw, accumulates the partial result, and returns {@code false} to signal that the
   * handler must wait for the rest. The second call completes the prefix and proceeds normally.
   */
  @Test
  public void testSplitLengthPrefix() throws Exception {
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
      Arrays.fill(data, (byte) 'Y');
      ArgumentCaptor<GcmTransportCipher.GcmEncryptedMessage> captor =
              ArgumentCaptor.forClass(GcmTransportCipher.GcmEncryptedMessage.class);
      encryptionHandler.write(ctx, Unpooled.wrappedBuffer(data), promise);
      verify(ctx).write(captor.capture(), eq(promise));

      ByteBuffer ciphertextBuffer = ByteBuffer.allocate((int) captor.getValue().count());
      captor.getValue().transferTo(new ByteBufferWriteableChannel(ciphertextBuffer), 0);
      ciphertextBuffer.flip();
      byte[] ciphertext = new byte[ciphertextBuffer.remaining()];
      ciphertextBuffer.get(ciphertext);

      // Split in the middle of the 8-byte Spark length prefix:
      // chunk1 = [4 bytes of length prefix]
      // chunk2 = [remaining 4 bytes of length prefix][GCM header][ciphertext]
      int splitPoint = 4;
      ByteBuf chunk1 = Unpooled.wrappedBuffer(ciphertext, 0, splitPoint);
      ByteBuf chunk2 = Unpooled.wrappedBuffer(
              ciphertext, splitPoint, ciphertext.length - splitPoint);

      decryptionHandler.channelRead(ctx, chunk1);
      // Only a partial length prefix was delivered; no plaintext should be emitted yet.
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

  /**
   * Writes {@code numSegments} full plaintext segments into {@code out}, encrypted with the
   * given {@code encrypter} so that the segments are bound to the length AAD it was created
   * with. The plaintext bytes are all 'p'.
   */
  private static void writeEncryptedSegments(
      AesGcmHkdfStreaming streaming,
      StreamSegmentEncrypter encrypter,
      int numSegments,
      ByteBuf out) throws GeneralSecurityException {
    int plaintextSegmentSize = streaming.getPlaintextSegmentSize();
    byte[] fill = new byte[plaintextSegmentSize];
    Arrays.fill(fill, (byte) 'p');
    ByteBuffer plaintext = ByteBuffer.allocate(plaintextSegmentSize);
    ByteBuffer ciphertext = ByteBuffer.allocate(streaming.getCiphertextSegmentSize());
    for (int i = 0; i < numSegments; i++) {
      plaintext.clear();
      plaintext.put(fill);
      plaintext.flip();
      ciphertext.clear();
      encrypter.encryptSegment(plaintext, false, ciphertext);
      ciphertext.flip();
      out.writeBytes(ciphertext);
    }
  }

  /**
   * Feeds a message that declares {@code remainingCiphertext} bytes after the length prefix
   * and the Tink header, and verifies that the first real segments decrypt and flush
   * normally. The declared length is never reached, which is what a receiver observes while
   * a large message is still in flight.
   */
  private void assertDeclaredLengthDecrypts(long remainingCiphertext) throws Exception {
    TransportConf gcmConf = getConf(2, false);
    try (AuthEngine client = new AuthEngine("appId", "secret", gcmConf);
         AuthEngine server = new AuthEngine("appId", "secret", gcmConf)) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      client.deriveSessionCipher(clientChallenge, serverResponse);
      GcmTransportCipher cipher = (GcmTransportCipher) server.sessionCipher();
      GcmTransportCipher.DecryptionHandler decryptionHandler = cipher.getDecryptionHandler();
      AesGcmHkdfStreaming streaming = cipher.getAesGcmHkdfStreaming();

      long expectedLength = GcmTransportCipher.LENGTH_HEADER_BYTES +
              streaming.getHeaderLength() + remainingCiphertext;
      StreamSegmentEncrypter encrypter = streaming.newStreamSegmentEncrypter(
              Longs.toByteArray(expectedLength));
      ByteBuffer header = encrypter.getHeader();
      // Enough segments to cross MAX_PLAINTEXT_BATCH_BYTES so the mid-message flush runs too.
      int numSegments = GcmTransportCipher.MAX_PLAINTEXT_BATCH_BYTES /
              streaming.getPlaintextSegmentSize() + 1;
      ByteBuf ciphertext = Unpooled.buffer(GcmTransportCipher.LENGTH_HEADER_BYTES +
              header.remaining() + numSegments * streaming.getCiphertextSegmentSize())
              .writeLong(expectedLength)
              .writeBytes(header);
      writeEncryptedSegments(streaming, encrypter, numSegments, ciphertext);

      ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
      ArgumentCaptor<ByteBuf> plaintextCaptor = ArgumentCaptor.forClass(ByteBuf.class);
      decryptionHandler.channelRead(ctx, ciphertext);
      verify(ctx).fireChannelRead(plaintextCaptor.capture());
      ByteBuf plaintext = plaintextCaptor.getValue();
      int expectedPlaintext = numSegments * streaming.getPlaintextSegmentSize();
      assertEquals(expectedPlaintext, plaintext.readableBytes());
      assertEquals((byte) 'p', plaintext.getByte(0));
      assertEquals((byte) 'p', plaintext.getByte(expectedPlaintext - 1));
    }
  }

  @Test
  public void testCiphertextLengthLargerThanMaxInt() throws Exception {
    // The remaining ciphertext is Integer.MAX_VALUE + 1, which the old (int) cast turned
    // into Integer.MIN_VALUE and made ciphertextBuffer.limit() throw.
    assertTimeoutPreemptively(Duration.ofSeconds(10),
            () -> assertDeclaredLengthDecrypts((long) Integer.MAX_VALUE + 1));
  }

  @Test
  public void testCiphertextLengthAtFourGiB() throws Exception {
    // At exactly 2^32 remaining, the old (int) cast produced 0, so channelRead() consumed
    // nothing and spun forever on the EventLoop. Between 2^32 and 2^32 + 2^31 the cast was
    // positive, so the handler read until exactly 2^32 remained and then spun the same way.
    assertTimeoutPreemptively(Duration.ofSeconds(10),
            () -> assertDeclaredLengthDecrypts(1L << 32));
    assertTimeoutPreemptively(Duration.ofSeconds(10),
            () -> assertDeclaredLengthDecrypts((1L << 32) + 1));
  }

  @Test
  public void testInvalidExpectedCiphertextLength() throws Exception {
    TransportConf gcmConf = getConf(2, false);
    try (AuthEngine client = new AuthEngine("appId", "secret", gcmConf);
         AuthEngine server = new AuthEngine("appId", "secret", gcmConf)) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      client.deriveSessionCipher(clientChallenge, serverResponse);
      GcmTransportCipher cipher = (GcmTransportCipher) server.sessionCipher();
      long minimumLength = GcmTransportCipher.LENGTH_HEADER_BYTES +
              cipher.getAesGcmHkdfStreaming().getHeaderLength();
      long[] invalidLengths = {
          Long.MIN_VALUE,
          -1,
          0,
          minimumLength - 1,
          minimumLength + 1,
          minimumLength + GcmTransportCipher.TAG_SIZE_BYTES - 1
      };
      for (long invalidLength : invalidLengths) {
        GcmTransportCipher.DecryptionHandler decryptionHandler = cipher.getDecryptionHandler();
        ByteBuf ciphertext = Unpooled.buffer(GcmTransportCipher.LENGTH_HEADER_BYTES)
                .writeLong(invalidLength);

        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> decryptionHandler.channelRead(mock(ChannelHandlerContext.class), ciphertext),
                "length " + invalidLength);

        assertEquals("Invalid expected ciphertext length: " + invalidLength, error.getMessage());
      }
    }
  }

  @Test
  public void testRejectedExpectedLengthIsNotRetained() throws Exception {
    TransportConf gcmConf = getConf(2, false);
    try (AuthEngine client = new AuthEngine("appId", "secret", gcmConf);
         AuthEngine server = new AuthEngine("appId", "secret", gcmConf)) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      client.deriveSessionCipher(clientChallenge, serverResponse);
      GcmTransportCipher cipher = (GcmTransportCipher) server.sessionCipher();
      GcmTransportCipher.DecryptionHandler decryptionHandler = cipher.getDecryptionHandler();
      int headerLength = cipher.getAesGcmHkdfStreaming().getHeaderLength();
      long rejectedLength = GcmTransportCipher.LENGTH_HEADER_BYTES + headerLength - 1;
      ByteBuf first = Unpooled.buffer(GcmTransportCipher.LENGTH_HEADER_BYTES)
              .writeLong(rejectedLength);

      assertThrows(IllegalStateException.class,
              () -> decryptionHandler.channelRead(mock(ChannelHandlerContext.class), first));

      // The rejected value must not clear the "length not read yet" sentinel: the handler
      // must keep rejecting it instead of treating the next read as a new frame.
      ByteBuf second = Unpooled.buffer(GcmTransportCipher.LENGTH_HEADER_BYTES + headerLength)
              .writeLong(GcmTransportCipher.LENGTH_HEADER_BYTES + headerLength)
              .writeZero(headerLength);
      IllegalStateException error = assertThrows(IllegalStateException.class,
              () -> decryptionHandler.channelRead(mock(ChannelHandlerContext.class), second));

      assertEquals("Invalid expected ciphertext length: " + rejectedLength, error.getMessage());
    }
  }

  /**
   * The accepted minimum length is an empty message: the 8-byte length prefix plus the Tink
   * header, with no segments. Changing the lower bound check from {@code <} to {@code <=}
   * would make such a message close the connection.
   */
  @Test
  public void testEmptyMessage() throws Exception {
    TransportConf gcmConf = getConf(2, false);
    try (AuthEngine client = new AuthEngine("appId", "secret", gcmConf);
         AuthEngine server = new AuthEngine("appId", "secret", gcmConf)) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      client.deriveSessionCipher(clientChallenge, serverResponse);
      GcmTransportCipher cipher = (GcmTransportCipher) server.sessionCipher();
      GcmTransportCipher.EncryptionHandler encryptionHandler = cipher.getEncryptionHandler();
      GcmTransportCipher.DecryptionHandler decryptionHandler = cipher.getDecryptionHandler();
      ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
      ChannelPromise promise = mock(ChannelPromise.class);

      ArgumentCaptor<GcmTransportCipher.GcmEncryptedMessage> emptyCaptor =
              ArgumentCaptor.forClass(GcmTransportCipher.GcmEncryptedMessage.class);
      encryptionHandler.write(ctx, Unpooled.wrappedBuffer(new byte[0]), promise);
      verify(ctx).write(emptyCaptor.capture(), eq(promise));
      ByteBuffer emptyCiphertext = ByteBuffer.allocate((int) emptyCaptor.getValue().count());
      emptyCaptor.getValue().transferTo(new ByteBufferWriteableChannel(emptyCiphertext), 0);
      emptyCiphertext.flip();

      decryptionHandler.channelRead(ctx, Unpooled.wrappedBuffer(emptyCiphertext));
      verify(ctx, never()).fireChannelRead(any());

      // The empty message must not close the connection: the next message on the same
      // channel still decodes.
      reset(ctx);
      byte[] data = new byte[1024];
      Arrays.fill(data, (byte) 'E');
      ArgumentCaptor<GcmTransportCipher.GcmEncryptedMessage> captor =
              ArgumentCaptor.forClass(GcmTransportCipher.GcmEncryptedMessage.class);
      encryptionHandler.write(ctx, Unpooled.wrappedBuffer(data), promise);
      verify(ctx).write(captor.capture(), eq(promise));
      ByteBuffer ciphertextBuffer = ByteBuffer.allocate((int) captor.getValue().count());
      captor.getValue().transferTo(new ByteBufferWriteableChannel(ciphertextBuffer), 0);
      ciphertextBuffer.flip();

      ArgumentCaptor<ByteBuf> plaintextCaptor = ArgumentCaptor.forClass(ByteBuf.class);
      decryptionHandler.channelRead(ctx, Unpooled.wrappedBuffer(ciphertextBuffer));
      verify(ctx).fireChannelRead(plaintextCaptor.capture());
      ByteBuf plaintext = plaintextCaptor.getValue();
      assertEquals(data.length, plaintext.readableBytes());
      byte[] decrypted = new byte[data.length];
      plaintext.readBytes(decrypted);
      assertArrayEquals(data, decrypted);
    }
  }

  /**
   * A FileRegion that produces {@code count} bytes from a small, reused buffer, so that a
   * region larger than 2 GiB does not need a 2 GiB allocation.
   */
  private static final class RepeatingRegion extends AbstractFileRegion {
    private final long count;
    private final ByteBuffer pattern;
    private long transferred;

    RepeatingRegion(long count) {
      this.count = count;
      this.pattern = ByteBuffer.allocate(4096);
    }

    @Override
    public long position() {
      return 0;
    }

    @Override
    public long transferred() {
      return transferred;
    }

    @Override
    public long count() {
      return count;
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
      long remaining = count - transferred;
      if (remaining <= 0) {
        return 0;
      }
      // Fill the target until it stops accepting bytes, so a full plaintext segment is
      // always delivered and the caller keeps encrypting in the same transferTo() call.
      long written = 0;
      while (written < remaining) {
        pattern.clear();
        pattern.limit((int) Math.min(pattern.capacity(), remaining - written));
        int n = target.write(pattern);
        if (n == 0) {
          break;
        }
        written += n;
      }
      transferred += written;
      return written;
    }

    @Override
    protected void deallocate() {
    }
  }

  private static final WritableByteChannel DISCARDING_CHANNEL = new WritableByteChannel() {
    @Override
    public int write(ByteBuffer src) {
      int remaining = src.remaining();
      src.position(src.limit());
      return remaining;
    }

    @Override
    public boolean isOpen() {
      return true;
    }

    @Override
    public void close() {
    }
  };

  @Test
  public void testTransferToReturnsLargeByteCount() throws Exception {
    TransportConf gcmConf = getConf(2, false);
    try (AuthEngine client = new AuthEngine("appId", "secret", gcmConf);
         AuthEngine server = new AuthEngine("appId", "secret", gcmConf)) {
      AuthMessage clientChallenge = client.challenge();
      AuthMessage serverResponse = server.response(clientChallenge);
      client.deriveSessionCipher(clientChallenge, serverResponse);
      GcmTransportCipher cipher = (GcmTransportCipher) server.sessionCipher();
      GcmTransportCipher.EncryptionHandler encryptionHandler = cipher.getEncryptionHandler();
      ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
      ChannelPromise promise = mock(ChannelPromise.class);

      // More than 2^31 plaintext bytes, so the ciphertext count exceeds Integer.MAX_VALUE.
      ArgumentCaptor<GcmTransportCipher.GcmEncryptedMessage> captor =
              ArgumentCaptor.forClass(GcmTransportCipher.GcmEncryptedMessage.class);
      encryptionHandler.write(ctx, new RepeatingRegion((long) Integer.MAX_VALUE + 1), promise);
      verify(ctx).write(captor.capture(), eq(promise));
      GcmTransportCipher.GcmEncryptedMessage encrypted = captor.getValue();
      assertTrue(encrypted.count() > Integer.MAX_VALUE);

      // With an int counter this returned a negative value, which Netty treats as a full
      // send buffer and silently drops the rest of the message.
      long transferred = assertTimeoutPreemptively(Duration.ofMinutes(2),
              () -> encrypted.transferTo(DISCARDING_CHANNEL, 0));

      assertTrue(transferred > Integer.MAX_VALUE, "transferTo returned " + transferred);
      assertEquals(encrypted.transferred(), transferred);
      assertEquals(encrypted.count(), transferred);
    }
  }

  /**
   * Regression test for the encryptedCount miscalculation that caused shuffle fetch stalls
   * for plaintext sizes in (plaintextSegmentSize - getCiphertextOffset(), plaintextSegmentSize]
   * = (32728, 32752].
   *
   * Root cause: encryptedCount was computed using {@code expectedCiphertextSize(P)} directly.
   * Tink's formula internally adds {@code getCiphertextOffset()} = 24 to P before dividing by
   * plaintextSegmentSize to count segments. For P in (32728, 32752] this predicted two
   * ciphertext segments, but {@code transferTo()} writes the Tink header separately and passes
   * all P bytes to a single {@code encryptSegment()} call, producing only one segment. The
   * resulting encryptedCount was inflated by TAG_SIZE_IN_BYTES = 16, so
   * {@code count() > transferred()} after all ciphertext was written. Subsequent
   * {@code transferTo()} calls returned 0 and the receiver stalled waiting indefinitely for
   * 16 bytes that were never sent.
   */
  @Test
  public void testEncryptedCountBoundary() throws Exception {
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

      // plaintextSegmentSize = CIPHERTEXT_BUFFER_SIZE - TAG_SIZE = 32768 - 16 = 32752
      // getCiphertextOffset() = 24 (Tink streaming header, written separately by transferTo)
      // Buggy range: P in (32728, 32752] - test lower boundary, midpoint, and upper boundary.
      int plaintextSegmentSize = GcmTransportCipher.CIPHERTEXT_BUFFER_SIZE - 16;
      int[] plaintextSizes = {
          plaintextSegmentSize - 23, // 32729: one above the lower boundary
          plaintextSegmentSize - 12, // 32740: midpoint of the affected range
          plaintextSegmentSize       // 32752: exactly one full segment (upper boundary)
      };

      for (int plaintextSize : plaintextSizes) {
        reset(ctx);
        byte[] data = new byte[plaintextSize];
        Arrays.fill(data, (byte) 'Z');

        ArgumentCaptor<GcmTransportCipher.GcmEncryptedMessage> encryptedCaptor =
                ArgumentCaptor.forClass(GcmTransportCipher.GcmEncryptedMessage.class);
        encryptionHandler.write(ctx, Unpooled.wrappedBuffer(data), promise);
        verify(ctx).write(encryptedCaptor.capture(), eq(promise));

        GcmTransportCipher.GcmEncryptedMessage encrypted = encryptedCaptor.getValue();
        ByteBuffer ciphertextBuf = ByteBuffer.allocate((int) encrypted.count());
        encrypted.transferTo(new ByteBufferWriteableChannel(ciphertextBuf), 0);

        // Before the fix: count() was inflated by 16 bytes for these sizes, so
        // transferred() < count() after all plaintext was consumed. The channel stalled
        // because subsequent transferTo() calls returned 0 instead of completing.
        assertEquals(encrypted.count(), encrypted.transferred(),
                "count() != transferred() for plaintextSize=" + plaintextSize);

        // Verify the full round-trip also decrypts correctly.
        reset(ctx);
        ciphertextBuf.flip();
        ArgumentCaptor<ByteBuf> plaintextCaptor = ArgumentCaptor.forClass(ByteBuf.class);
        decryptionHandler.channelRead(ctx, Unpooled.wrappedBuffer(ciphertextBuf));
        verify(ctx, times(1)).fireChannelRead(plaintextCaptor.capture());
        ByteBuf plaintext = plaintextCaptor.getValue();
        assertEquals(plaintextSize, plaintext.readableBytes());
        assertEquals('Z', plaintext.getByte(0));
        assertEquals('Z', plaintext.getByte(plaintextSize - 1));
      }
    }
  }

  /**
   * Regression test for the executor heartbeat timeout caused by per-segment
   * {@code ctx.fireChannelRead()} calls in {@code DecryptionHandler}. The old code fired one
   * EventLoop callback per 32 KB ciphertext segment; decoding a large shuffle block produced
   * many synchronous callbacks inside a single {@code processSelectedKeys()} call,
   * monopolising the Netty EventLoop and starving the executor-driver heartbeat task.
   *
   * The fix accumulates all decrypted segments into a {@code CompositeByteBuf} and reduce
   * the number of {@code ctx.fireChannelRead()} calls. This test verifies that a
   * multi-segment plaintext below the 1 MB accumulator threshold
   * ({@code MAX_PLAINTEXT_BATCH_BYTES}) produces exactly one {@code fireChannelRead} call.
   * Above that threshold the accumulator flushes mid-message; see
   * {@code testLargeMessageBatchedFlush}.
   */
  @Test
  public void testSingleFirePerMessage() throws Exception {
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

      // Use a 5-segment plaintext so the old per-segment fire (5 calls) is clearly
      // distinguishable from the expected single-fire behaviour (1 call).
      int plaintextSegmentSize = GcmTransportCipher.CIPHERTEXT_BUFFER_SIZE - 16;
      int plaintextSize = plaintextSegmentSize * 5;
      byte[] data = new byte[plaintextSize];
      Arrays.fill(data, (byte) 'M');

      ArgumentCaptor<GcmTransportCipher.GcmEncryptedMessage> encryptedCaptor =
              ArgumentCaptor.forClass(GcmTransportCipher.GcmEncryptedMessage.class);
      encryptionHandler.write(ctx, Unpooled.wrappedBuffer(data), promise);
      verify(ctx).write(encryptedCaptor.capture(), eq(promise));

      GcmTransportCipher.GcmEncryptedMessage encrypted = encryptedCaptor.getValue();
      ByteBuffer ciphertextBuf = ByteBuffer.allocate((int) encrypted.count());
      encrypted.transferTo(new ByteBufferWriteableChannel(ciphertextBuf), 0);
      ciphertextBuf.flip();

      reset(ctx);
      ArgumentCaptor<ByteBuf> plaintextCaptor = ArgumentCaptor.forClass(ByteBuf.class);
      decryptionHandler.channelRead(ctx, Unpooled.wrappedBuffer(ciphertextBuf));
      // The old code fired once per 32 KB segment (5 times for this plaintext size).
      // The fix must fire exactly once for the whole message.
      verify(ctx, times(1)).fireChannelRead(plaintextCaptor.capture());
      ByteBuf plaintext = plaintextCaptor.getValue();
      assertEquals(plaintextSize, plaintext.readableBytes());
      assertEquals('M', plaintext.getByte(0));
      assertEquals('M', plaintext.getByte(plaintextSize - 1));
    }
  }

  /**
   * Verifies that DecryptionHandler flushes plaintext in multiple batches when the message
   * plaintext exceeds {@code MAX_PLAINTEXT_BATCH_BYTES} (1 MB). The bounded accumulation
   * prevents full on-heap materialisation of large shuffle blocks that route to disk via
   * {@code spark.maxRemoteBlockSizeFetchToMem}. This test uses a 2 MB plaintext, which
   * crosses the 1 MB threshold mid-message (triggering the first flush) and flushes once
   * more at message completion, producing at least two {@code fireChannelRead} calls whose
   * concatenated output must reproduce the original plaintext exactly.
   */
  @Test
  public void testLargeMessageBatchedFlush() throws Exception {
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

      // 2 MB plaintext exceeds MAX_PLAINTEXT_BATCH_BYTES (1 MB): the accumulator flushes
      // when the threshold is crossed (after ~33 segments) and again at completion,
      // so at least two fireChannelRead() calls are expected rather than one.
      int plaintextSize = 2 * GcmTransportCipher.MAX_PLAINTEXT_BATCH_BYTES;
      byte[] data = new byte[plaintextSize];
      Arrays.fill(data, (byte) 'L');

      ArgumentCaptor<GcmTransportCipher.GcmEncryptedMessage> encryptedCaptor =
              ArgumentCaptor.forClass(GcmTransportCipher.GcmEncryptedMessage.class);
      encryptionHandler.write(ctx, Unpooled.wrappedBuffer(data), promise);
      verify(ctx).write(encryptedCaptor.capture(), eq(promise));

      GcmTransportCipher.GcmEncryptedMessage encrypted = encryptedCaptor.getValue();
      ByteBuffer ciphertextBuf = ByteBuffer.allocate((int) encrypted.count());
      encrypted.transferTo(new ByteBufferWriteableChannel(ciphertextBuf), 0);
      ciphertextBuf.flip();

      reset(ctx);
      ArgumentCaptor<ByteBuf> plaintextCaptor = ArgumentCaptor.forClass(ByteBuf.class);
      decryptionHandler.channelRead(ctx, Unpooled.wrappedBuffer(ciphertextBuf));
      // At least two batches: one when the accumulator crosses 1 MB, one at completion.
      verify(ctx, atLeast(2)).fireChannelRead(plaintextCaptor.capture());

      byte[] decrypted = new byte[plaintextSize];
      int offset = 0;
      for (ByteBuf segment : plaintextCaptor.getAllValues()) {
        int len = segment.readableBytes();
        segment.readBytes(decrypted, offset, len);
        offset += len;
      }
      assertEquals(plaintextSize, offset);
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
