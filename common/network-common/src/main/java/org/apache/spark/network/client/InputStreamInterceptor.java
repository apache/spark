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
import java.nio.channels.ClosedChannelException;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedBytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.EmptyByteBuf;
import io.netty.channel.Channel;
import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.util.TransportFrameDecoder;

public class InputStreamInterceptor extends InputStream {
  private final Logger logger = LoggerFactory.getLogger(InputStreamInterceptor.class);

  private final Channel channel;
  private final long byteCount;
  private final InputStreamCallback callback;
  private final LinkedBlockingQueue<ByteBuf> buffers = new LinkedBlockingQueue<>(1024);
  public final TransportFrameDecoder.Interceptor interceptor;

  private ByteBuf curChunk;
  private ByteBuf emptyByteBuf;
  private boolean isCallbacked = false;
  private long writerIndex = 0;
  private long readerIndex = 0;

  private volatile int closedInt = 0;
  private volatile Throwable refCause = null;

  private static final AtomicIntegerFieldUpdater<InputStreamInterceptor> closedUpdater;
  private static final AtomicReferenceFieldUpdater<InputStreamInterceptor, Throwable> causeUpdater;

  static {
    AtomicIntegerFieldUpdater<InputStreamInterceptor> updater =
        PlatformDependent.newAtomicIntegerFieldUpdater(InputStreamInterceptor.class, "closedInt");
    if (updater == null) {
      updater = AtomicIntegerFieldUpdater.newUpdater(InputStreamInterceptor.class, "closedInt");
    }
    closedUpdater = updater;
    @SuppressWarnings({"rawtypes"})
    AtomicReferenceFieldUpdater<InputStreamInterceptor, Throwable> throwUpdater =
        PlatformDependent.newAtomicReferenceFieldUpdater(InputStreamInterceptor.class, "refCause");
    if (throwUpdater == null) {
      throwUpdater = AtomicReferenceFieldUpdater.newUpdater(InputStreamInterceptor.class,
          Throwable.class, "refCause");
    }
    causeUpdater = throwUpdater;
  }

  public InputStreamInterceptor(
      Channel channel,
      long byteCount,
      InputStreamCallback callback) {
    this.channel = channel;
    this.byteCount = byteCount;
    this.callback = callback;
    this.interceptor = new StreamInterceptor();
    this.emptyByteBuf = new EmptyByteBuf(channel.alloc());
  }

  @Override
  public int read() throws IOException {
    if (isClosed()) return -1;
    pullChunk();
    if (curChunk != null) {
      byte b = curChunk.readByte();
      readerIndex += 1;
      maybeReleaseCurChunk();
      return UnsignedBytes.toInt(b);
    } else {
      return -1;
    }
  }

  @Override
  public int read(byte[] dest, int offset, int length) throws IOException {
    if (isClosed()) return -1;
    pullChunk();
    if (curChunk != null) {
      int amountToGet = Math.min(curChunk.readableBytes(), length);
      curChunk.readBytes(dest, offset, amountToGet);
      readerIndex += amountToGet;
      maybeReleaseCurChunk();
      return amountToGet;
    } else {
      return -1;
    }
  }

  @Override
  public long skip(long bytes) throws IOException {
    if (isClosed()) return 0L;
    pullChunk();
    if (curChunk != null) {
      int amountToSkip = (int) Math.min(bytes, curChunk.readableBytes());
      curChunk.skipBytes(amountToSkip);
      amountToSkip += amountToSkip;
      maybeReleaseCurChunk();
      return amountToSkip;
    } else {
      return 0L;
    }
  }

  @Override
  public void close() throws IOException {
    for (; ; ) {
      if (closedUpdater.compareAndSet(this, 0, 1)) {
        if (logger.isTraceEnabled()) {
          logger.trace("Closed remoteAddress: " + channel.remoteAddress() +
              ", readerIndex: " + readerIndex + ", byteCount: " + byteCount);
        }
        releaseCurChunk();
        resetChannel();
        Iterator<ByteBuf> itr = buffers.iterator();
        while (itr.hasNext()) {
          itr.next().release();
        }
        buffers.clear();
        break;
      }
    }
  }

  private void pullChunk() throws IOException {
    if (logger.isTraceEnabled()) {
      logger.trace("RemoteAddress: " + channel.remoteAddress() +
          ", readerIndex: " + readerIndex + ", byteCount: " + byteCount);
    }
    if (readerIndex >= byteCount) {
      close();
    } else if (curChunk == null && cause() == null && !isClosed()) {
      try {
        if (!channel.config().isAutoRead()) {
          // if channel.read() will be not invoked automatically,
          // the method is called by here
          if (buffers.size() < 64) channel.config().setAutoRead(true);
          channel.read();
        }

        curChunk = buffers.take();

        if (curChunk == emptyByteBuf) {
          Preconditions.checkNotNull(cause());
        }
      } catch (Throwable e) {
        setCause(e);
      }
    }
    if (cause() != null) throw new IOException(cause());
  }

  private boolean isClosed() {
    return closedInt == 1;
  }

  private Throwable cause() {
    return refCause;
  }

  private void setCause(Throwable e) throws IOException {
    if (logger.isTraceEnabled()) {
      logger.trace("exceptionCaught", e);
    }
    if (causeUpdater.compareAndSet(this, null, e)) {
      try {
        close();
        buffers.put(emptyByteBuf);
      } catch (Throwable throwable) {
        logger.error("exceptionCaught", e);
        // setCause(throwable);
      }
    }
  }

  private void maybeReleaseCurChunk() {
    if (curChunk != null && !curChunk.isReadable()) releaseCurChunk();
  }

  private void releaseCurChunk() {
    if (curChunk != null) {
      curChunk.release();
      curChunk = null;
    }
  }

  private void onSuccess() throws IOException {
    if (isCallbacked) return;
    if (cause() != null) {
      callback.onFailure(cause());
    } else {
      callback.onSuccess(this);
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
      callback.onComplete();
      setCause(e);
      onSuccess();
      resetChannel();
    }

    @Override
    public void channelInactive() throws Exception {
      callback.onComplete();
      setCause(new ClosedChannelException());
      onSuccess();
      resetChannel();
    }

    @Override
    public boolean handle(ByteBuf buf) throws Exception {
      try {
        ByteBuf frame = nextBufferForFrame(byteCount - writerIndex, buf);
        int available = frame.readableBytes();
        writerIndex += available;
        if (logger.isTraceEnabled()) {
          logger.trace("RemoteAddress: " + channel.remoteAddress() +
              ", writerIndex: " + writerIndex + ", byteCount: " + byteCount);
        }
        mayTrafficSuspension();
        if (!isClosed() && available > 0) {
          buffers.put(frame);
          if (writerIndex > byteCount) {
            setCause(new IllegalStateException(String.format(
                "Read too many bytes? Expected %d, but read %d.", byteCount, writerIndex)));
            callback.onComplete();
          } else if (writerIndex == byteCount) {
            callback.onComplete();
          }
        } else {
          frame.release();
        }
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
      if (channel.config().isAutoRead() && buffers.size() > 1000) {
        channel.config().setAutoRead(false);
      }
      if (writerIndex >= byteCount) resetChannel();
    }
  }

  public static interface InputStreamCallback {
    /**
     * Called when all data from the stream has been received.
     */
    void onSuccess(InputStream inputStream) throws IOException;

    /**
     * Called if there's an error reading data from the InputStream.
     */
    void onFailure(Throwable cause) throws IOException;

    void onComplete();
  }

  public static InputStreamCallback emptyInputStreamCallback = new InputStreamCallback() {
    @Override
    public void onSuccess(InputStream inputStream) throws IOException {

    }

    @Override
    public void onFailure(Throwable cause) throws IOException {

    }

    @Override
    public void onComplete() {

    }
  };
}
