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

package org.apache.spark.network.client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.primitives.UnsignedBytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.EmptyByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.protocol.StreamChunkId;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.network.util.TransportFrameDecoder;

public class ChunkFetchInputStream extends InputStream {
  private final Logger logger = LoggerFactory.getLogger(ChunkFetchInputStream.class);

  private final TransportResponseHandler handler;
  private final Channel channel;
  private final StreamChunkId streamId;
  private final long byteCount;
  private final ChunkReceivedCallback callback;
  private final LinkedBlockingQueue<ByteBuf> buffers = new LinkedBlockingQueue<>(1024);
  public final TransportFrameDecoder.Interceptor interceptor;

  private ByteBuf curChunk;
  private ByteBuf emptyByteBuf;
  private boolean isCallbacked = false;
  private long writerIndex = 0;


  private final AtomicReference<Throwable> cause = new AtomicReference<>(null);
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  public ChunkFetchInputStream(
      TransportResponseHandler handler,
      Channel channel,
      StreamChunkId streamId,
      long byteCount,
      ChunkReceivedCallback callback) {
    this.handler = handler;
    this.channel = channel;
    this.streamId = streamId;
    this.byteCount = byteCount;
    this.callback = callback;
    this.interceptor = new StreamInterceptor();
    this.emptyByteBuf = new EmptyByteBuf(channel.alloc());
  }

  @Override
  public int read() throws IOException {
    if (isClosed.get()) return -1;
    pullChunk();
    if (curChunk != null) {
      byte b = curChunk.readByte();
      return UnsignedBytes.toInt(b);
    } else {
      return -1;
    }
  }

  @Override
  public int read(byte[] dest, int offset, int length) throws IOException {
    if (isClosed.get()) return -1;
    pullChunk();
    if (curChunk != null) {
      int amountToGet = Math.min(curChunk.readableBytes(), length);
      curChunk.readBytes(dest, offset, amountToGet);
      return amountToGet;
    } else {
      return -1;
    }
  }

  @Override
  public long skip(long bytes) throws IOException {
    if (isClosed.get()) return 0L;
    pullChunk();
    if (curChunk != null) {
      int amountToSkip = (int) Math.min(bytes, curChunk.readableBytes());
      curChunk.skipBytes(amountToSkip);
      return amountToSkip;
    } else {
      return 0L;
    }
  }

  @Override
  public void close() throws IOException {
    if (!isClosed.get()) {
      releaseCurChunk();
      isClosed.set(true);
      resetChannel();
      Iterator<ByteBuf> itr = buffers.iterator();
      while (itr.hasNext()) {
        itr.next().release();
      }
      buffers.clear();
    }
  }

  private void pullChunk() throws IOException {
    if (curChunk != null && !curChunk.isReadable()) releaseCurChunk();
    if (curChunk == null && cause.get() == null && !isClosed.get()) {
      try {
        if (buffers.size() < 32 && !channel.config().isAutoRead()) {
          // if channel.read() will be not invoked automatically,
          // the method is called by here
          channel.config().setAutoRead(true);
          channel.read();
        }
        curChunk = buffers.take();
        if (curChunk == emptyByteBuf) {
          assert cause.get() != null;
        }
      } catch (Throwable e) {
        setCause(e);
      }
    }
    if (cause.get() != null) throw new IOException(cause.get());
  }

  private void setCause(Throwable e) throws IOException {
    if (cause.get() == null) {
      try {
        cause.set(e);
        close();
        buffers.put(emptyByteBuf);
      } catch (Throwable throwable) {
        // setCause(throwable);
      }
    }
  }

  private void releaseCurChunk() {
    if (curChunk != null) {
      curChunk.release();
      curChunk = null;
    }
  }

  private void onSuccess() throws IOException {
    if (isCallbacked) return;
    if (cause.get() != null) {
      callback.onFailure(streamId.chunkIndex, cause.get());
    } else {
      InputStream inputStream = new LimitedInputStream(this, byteCount);
      ManagedBuffer managedBuffer = new InputStreamManagedBuffer(inputStream, byteCount);
      callback.onSuccess(streamId.chunkIndex, managedBuffer);
    }
    isCallbacked = true;
  }

  private void resetChannel() {
    if (!channel.config().isAutoRead()) {
      channel.config().setAutoRead(true);
      channel.read();
    }
  }

  private class StreamInterceptor implements TransportFrameDecoder.Interceptor {
    @Override
    public void exceptionCaught(Throwable e) throws Exception {
      handler.deactivateStream();
      setCause(e);
      logger.trace("exceptionCaught", e);
      onSuccess();
      resetChannel();
    }

    @Override
    public void channelInactive() throws Exception {
      handler.deactivateStream();
      setCause(new ClosedChannelException());
      logger.trace("channelInactive", cause.get());
      onSuccess();
      resetChannel();
    }

    @Override
    public boolean handle(ByteBuf buf) throws Exception {
      try {
        ByteBuf frame = nextBufferForFrame(byteCount - writerIndex, buf);
        int available = frame.readableBytes();
        writerIndex += available;
        mayTrafficSuspension();
        if (!isClosed.get() && available > 0) {
          buffers.put(frame);
          if (writerIndex > byteCount) {
            setCause(new IllegalStateException(String.format(
                "Read too many bytes? Expected %d, but read %d.", byteCount, writerIndex)));
            handler.deactivateStream();
          } else if (writerIndex == byteCount) {
            handler.deactivateStream();
          }
        } else {
          frame.release();
        }
        logger.trace(streamId + ", writerIndex  " + writerIndex + " byteCount, " + byteCount);
        onSuccess();
      } catch (Exception e) {
        setCause(e);
        resetChannel();
      }
      return writerIndex != byteCount;
    }

    /**
     * Takes the first buffer in the internal list, and either adjust it to fit in the frame
     * (by taking a slice out of it) or remove it from the internal list.
     */
    private ByteBuf nextBufferForFrame(long bytesToRead, ByteBuf buf) {
      int slen = (int) Math.min(buf.readableBytes(), bytesToRead);
      ByteBuf frame;
      if (slen == buf.readableBytes()) {
        frame = buf.retain().readSlice(slen);
      } else {
        frame = buf.alloc().buffer(slen);
        buf.readBytes(frame);
        frame.retain();
      }
      return frame;
    }

    private void mayTrafficSuspension() {
      // If there is too much cached chunk, to manually call channel.read().
      if (channel.config().isAutoRead() && buffers.size() > 512) {
        channel.config().setAutoRead(false);
      }
      if (writerIndex >= byteCount) resetChannel();
    }
  }

  private class InputStreamManagedBuffer extends ManagedBuffer {
    private final InputStream inputStream;
    private final long byteCount;

    InputStreamManagedBuffer(InputStream inputStream, long byteCount) {
      this.inputStream = inputStream;
      this.byteCount = byteCount;
    }

    public long size() {
      return byteCount;
    }

    public ByteBuffer nioByteBuffer() throws IOException {
      throw new UnsupportedOperationException("nioByteBuffer");
    }

    public InputStream createInputStream() throws IOException {
      return inputStream;
    }

    public ManagedBuffer retain() {
      // throw new UnsupportedOperationException("retain");
      return this;
    }

    public ManagedBuffer release() {
      // throw new UnsupportedOperationException("release");
      return this;
    }

    public Object convertToNetty() throws IOException {
      throw new UnsupportedOperationException("convertToNetty");
    }
  }
}
