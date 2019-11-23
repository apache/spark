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
import java.nio.file.StandardOpenOption;

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
  private final TransportConf conf;
  private final File file;
  private final long offset;
  private final long length;

  public FileSegmentManagedBuffer(TransportConf conf, File file, long offset, long length) {
    this.conf = conf;
    this.file = file;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public long size() {
    return length;
  }

  @Override
  public ByteBuffer nioByteBuffer() throws IOException {
    FileChannel channel = null;
    try {
      channel = new RandomAccessFile(file, "r").getChannel();
      // Just copy the buffer if it's sufficiently small, as memory mapping has a high overhead.
      if (length < conf.memoryMapBytes()) {
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
        return buf;
      } else {
        return channel.map(FileChannel.MapMode.READ_ONLY, offset, length);
      }
    } catch (IOException e) {
      String errorMessage = "Error in reading " + this;
      try {
        if (channel != null) {
          long size = channel.size();
          errorMessage = "Error in reading " + this + " (actual file length " + size + ")";
        }
      } catch (IOException ignored) {
        // ignore
      }
      throw new IOException(errorMessage, e);
    } finally {
      JavaUtils.closeQuietly(channel);
    }
  }

  @Override
  public InputStream createInputStream() throws IOException {
    FileInputStream is = null;
    boolean shouldClose = true;
    try {
      is = new FileInputStream(file);
      ByteStreams.skipFully(is, offset);
      InputStream r = new LimitedInputStream(is, length);
      shouldClose = false;
      return r;
    } catch (IOException e) {
      String errorMessage = "Error in reading " + this;
      if (is != null) {
        long size = file.length();
        errorMessage = "Error in reading " + this + " (actual file length " + size + ")";
      }
      throw new IOException(errorMessage, e);
    } finally {
      if (shouldClose) {
        JavaUtils.closeQuietly(is);
      }
    }
  }

  @Override
  public ManagedBuffer retain() {
    return this;
  }

  @Override
  public ManagedBuffer release() {
    return this;
  }

  @Override
  public Object convertToNetty() throws IOException {
    if (conf.lazyFileDescriptor()) {
      return new DefaultFileRegion(file, offset, length);
    } else {
      FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
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
