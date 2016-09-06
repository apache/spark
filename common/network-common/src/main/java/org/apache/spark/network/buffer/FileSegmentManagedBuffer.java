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

package org.apache.spark.network.buffer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.google.common.base.Objects;
import com.google.common.io.ByteStreams;
import io.netty.channel.DefaultFileRegion;

import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.network.util.TransportConf;

/**
 * A {@link ManagedBuffer} backed by a segment in a file.
 */
public final class FileSegmentManagedBuffer extends ManagedBuffer {

  private final File file;
  private final long offset;
  private final long length;
  private final long memoryMapBytes;
  private final boolean lazyFileDescriptor;

  public FileSegmentManagedBuffer(TransportConf conf, File file, long offset, long length) {
     this(conf.memoryMapBytes(), conf.lazyFileDescriptor(),file,offset,length);
  }

  public FileSegmentManagedBuffer(
      long memoryMapBytes,
      boolean lazyFileDescriptor,
      File file,
      long offset,
      long length) {
    this.memoryMapBytes = memoryMapBytes;
    this.lazyFileDescriptor = lazyFileDescriptor;
    this.file = file;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public long size() {
    return length;
  }

  @Override
  public ChunkedByteBuffer nioByteBuffer() throws IOException {
    FileChannel channel = null;
    try {
      channel = new RandomAccessFile(file, "r").getChannel();
      // Just copy the buffer if it's sufficiently small, as memory mapping has a high overhead.
      if (length < memoryMapBytes) {
        ByteBuffer buf = ByteBuffer.allocate((int) length);
        channel.position(offset);
        while (buf.remaining() != 0) {
          if (channel.read(buf) == -1) {
            throw new IOException(String.format("Reached EOF before filling buffer\n" +
                    "offset=%s\nfile=%s\nbuf.remaining=%s",
                offset, file.getAbsoluteFile(), buf.remaining()));
          }
        }
        buf.flip();
        return ChunkedByteBufferUtil.wrap(buf);
      } else {
        int pageSize = 128 * 1024 * 1024;
        int numPage = (int) Math.ceil((double) length / pageSize);
        ByteBuffer[] buffers = new ByteBuffer[numPage];
        long len = length;
        long off = offset;
        for (int i = 0; i < buffers.length; i++) {
          long pageLen = Math.min(len, pageSize);
          buffers[i] = channel.map(FileChannel.MapMode.READ_ONLY, off, pageLen);
          len -= pageLen;
          off += pageLen;
        }
        return ChunkedByteBufferUtil.wrap(buffers);
      }
    } catch (IOException e) {
      try {
        if (channel != null) {
          long size = channel.size();
          throw new IOException(String.format("Error in reading %s (actual file length %s)",
              this, size), e);
        }
      } catch (IOException ignored) {
        // ignore
      }
      throw new IOException("Error in opening " + this, e);
    } finally {
      JavaUtils.closeQuietly(channel);
    }
  }

  @Override
  public InputStream createInputStream() throws IOException {
    FileInputStream is = null;
    try {
      is = new FileInputStream(file);
      ByteStreams.skipFully(is, offset);
      return new LimitedInputStream(is, length);
    } catch (IOException e) {
      try {
        if (is != null) {
          long size = file.length();
          throw new IOException("Error in reading " + this + " (actual file length " + size + ")",
              e);
        }
      } catch (IOException ignored) {
        // ignore
      } finally {
        JavaUtils.closeQuietly(is);
      }
      throw new IOException("Error in opening " + this, e);
    } catch (RuntimeException e) {
      JavaUtils.closeQuietly(is);
      throw e;
    }
  }

  @Override
  public ManagedBuffer retain() {
    super.retain();
    return this;
  }

  @Override
  public Object convertToNetty() throws IOException {
    if (lazyFileDescriptor) {
      return new DefaultFileRegion(file, offset, length);
    } else {
      FileChannel fileChannel = new FileInputStream(file).getChannel();
      return new DefaultFileRegion(fileChannel, offset, length);
    }
  }

  public File getFile() { return file; }

  public long getOffset() { return offset; }

  public long getLength() { return length; }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("file", file)
      .add("offset", offset)
      .add("length", length)
      .toString();
  }
}
