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

package org.apache.spark.network.sasl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.FileRegion;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.AbstractReferenceCounted;

import org.apache.spark.network.util.ByteArrayWritableChannel;
import org.apache.spark.network.util.NettyUtils;

/**
 * Provides SASL-based encription for transport channels. The single method exposed by this
 * class installs the needed channel handlers on a connected channel.
 */
class SaslEncryption {

  @VisibleForTesting
  static final String ENCRYPTION_HANDLER_NAME = "saslEncryption";

  /**
   * Adds channel handlers that perform encryption / decryption of data using SASL.
   *
   * @param channel The channel.
   * @param backend The SASL backend.
   * @param maxOutboundBlockSize Max size in bytes of outgoing encrypted blocks, to control
   *                             memory usage.
   */
  static void addToChannel(
      Channel channel,
      SaslEncryptionBackend backend,
      int maxOutboundBlockSize) {
    channel.pipeline()
      .addFirst(ENCRYPTION_HANDLER_NAME, new EncryptionHandler(backend, maxOutboundBlockSize))
      .addFirst("saslDecryption", new DecryptionHandler(backend))
      .addFirst("saslFrameDecoder", NettyUtils.createFrameDecoder());
  }

  private static class EncryptionHandler extends ChannelOutboundHandlerAdapter {

    private final int maxOutboundBlockSize;
    private final SaslEncryptionBackend backend;

    EncryptionHandler(SaslEncryptionBackend backend, int maxOutboundBlockSize) {
      this.backend = backend;
      this.maxOutboundBlockSize = maxOutboundBlockSize;
    }

    /**
     * Wrap the incoming message in an implementation that will perform encryption lazily. This is
     * needed to guarantee ordering of the outgoing encrypted packets - they need to be decrypted in
     * the same order, and netty doesn't have an atomic ChannelHandlerContext.write() API, so it
     * does not guarantee any ordering.
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {

      ctx.write(new EncryptedMessage(backend, msg, maxOutboundBlockSize), promise);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
      try {
        backend.dispose();
      } finally {
        super.handlerRemoved(ctx);
      }
    }

  }

  private static class DecryptionHandler extends MessageToMessageDecoder<ByteBuf> {

    private final SaslEncryptionBackend backend;

    DecryptionHandler(SaslEncryptionBackend backend) {
      this.backend = backend;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out)
      throws Exception {

      byte[] data;
      int offset;
      int length = msg.readableBytes();
      if (msg.hasArray()) {
        data = msg.array();
        offset = msg.arrayOffset();
        msg.skipBytes(length);
      } else {
        data = new byte[length];
        msg.readBytes(data);
        offset = 0;
      }

      out.add(Unpooled.wrappedBuffer(backend.unwrap(data, offset, length)));
    }

  }

  @VisibleForTesting
  static class EncryptedMessage extends AbstractReferenceCounted implements FileRegion {

    private final SaslEncryptionBackend backend;
    private final boolean isByteBuf;
    private final ByteBuf buf;
    private final FileRegion region;

    /**
     * A channel used to buffer input data for encryption. The channel has an upper size bound
     * so that if the input is larger than the allowed buffer, it will be broken into multiple
     * chunks.
     */
    private final ByteArrayWritableChannel byteChannel;

    private ByteBuf currentHeader;
    private ByteBuffer currentChunk;
    private long currentChunkSize;
    private long currentReportedBytes;
    private long unencryptedChunkSize;
    private long transferred;

    EncryptedMessage(SaslEncryptionBackend backend, Object msg, int maxOutboundBlockSize) {
      Preconditions.checkArgument(msg instanceof ByteBuf || msg instanceof FileRegion,
        "Unrecognized message type: %s", msg.getClass().getName());
      this.backend = backend;
      this.isByteBuf = msg instanceof ByteBuf;
      this.buf = isByteBuf ? (ByteBuf) msg : null;
      this.region = isByteBuf ? null : (FileRegion) msg;
      this.byteChannel = new ByteArrayWritableChannel(maxOutboundBlockSize);
    }

    /**
     * Returns the size of the original (unencrypted) message.
     *
     * This makes assumptions about how netty treats FileRegion instances, because there's no way
     * to know beforehand what will be the size of the encrypted message. Namely, it assumes
     * that netty will try to transfer data from this message while
     * <code>transfered() < count()</code>. So these two methods return, technically, wrong data,
     * but netty doesn't know better.
     */
    @Override
    public long count() {
      return isByteBuf ? buf.readableBytes() : region.count();
    }

    @Override
    public long position() {
      return 0;
    }

    /**
     * Returns an approximation of the amount of data transferred. See {@link #count()}.
     */
    @Override
    public long transfered() {
      return transferred;
    }

    /**
     * Transfers data from the original message to the channel, encrypting it in the process.
     *
     * This method also breaks down the original message into smaller chunks when needed. This
     * is done to keep memory usage under control. This avoids having to copy the whole message
     * data into memory at once, and can avoid ballooning memory usage when transferring large
     * messages such as shuffle blocks.
     *
     * The {@link #transfered()} counter also behaves a little funny, in that it won't go forward
     * until a whole chunk has been written. This is done because the code can't use the actual
     * number of bytes written to the channel as the transferred count (see {@link #count()}).
     * Instead, once an encrypted chunk is written to the output (including its header), the
     * size of the original block will be added to the {@link #transfered()} amount.
     */
    @Override
    public long transferTo(final WritableByteChannel target, final long position)
      throws IOException {

      Preconditions.checkArgument(position == transfered(), "Invalid position.");

      long reportedWritten = 0L;
      long actuallyWritten = 0L;
      do {
        if (currentChunk == null) {
          nextChunk();
        }

        if (currentHeader.readableBytes() > 0) {
          int bytesWritten = target.write(currentHeader.nioBuffer());
          currentHeader.skipBytes(bytesWritten);
          actuallyWritten += bytesWritten;
          if (currentHeader.readableBytes() > 0) {
            // Break out of loop if there are still header bytes left to write.
            break;
          }
        }

        actuallyWritten += target.write(currentChunk);
        if (!currentChunk.hasRemaining()) {
          // Only update the count of written bytes once a full chunk has been written.
          // See method javadoc.
          long chunkBytesRemaining = unencryptedChunkSize - currentReportedBytes;
          reportedWritten += chunkBytesRemaining;
          transferred += chunkBytesRemaining;
          currentHeader.release();
          currentHeader = null;
          currentChunk = null;
          currentChunkSize = 0;
          currentReportedBytes = 0;
        }
      } while (currentChunk == null && transfered() + reportedWritten < count());

      // Returning 0 triggers a backoff mechanism in netty which may harm performance. Instead,
      // we return 1 until we can (i.e. until the reported count would actually match the size
      // of the current chunk), at which point we resort to returning 0 so that the counts still
      // match, at the cost of some performance. That situation should be rare, though.
      if (reportedWritten != 0L) {
        return reportedWritten;
      }

      if (actuallyWritten > 0 && currentReportedBytes < currentChunkSize - 1) {
        transferred += 1L;
        currentReportedBytes += 1L;
        return 1L;
      }

      return 0L;
    }

    private void nextChunk() throws IOException {
      byteChannel.reset();
      if (isByteBuf) {
        int copied = byteChannel.write(buf.nioBuffer());
        buf.skipBytes(copied);
      } else {
        region.transferTo(byteChannel, region.transfered());
      }

      byte[] encrypted = backend.wrap(byteChannel.getData(), 0, byteChannel.length());
      this.currentChunk = ByteBuffer.wrap(encrypted);
      this.currentChunkSize = encrypted.length;
      this.currentHeader = Unpooled.copyLong(8 + currentChunkSize);
      this.unencryptedChunkSize = byteChannel.length();
    }

    @Override
    protected void deallocate() {
      if (currentHeader != null) {
        currentHeader.release();
      }
      if (buf != null) {
        buf.release();
      }
      if (region != null) {
        region.release();
      }
    }

  }

}
