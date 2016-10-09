/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * {@link InputStream} implementation which uses direct buffer
 * to read a file to avoid extra copy of data between Java and
 * native memory which happens when using {@link java.io.BufferedInputStream}.
 * Unfortunately, this is not something already available in JDK,
 * {@link sun.nio.ch.ChannelInputStream} supports reading a file using nio,
 * but does not support buffering.
 *
 */
public final class NioBasedBufferedFileInputStream extends InputStream {

  private static int DEFAULT_BUFFER_SIZE = 8192;

  private final ByteBuffer bb;

  private final FileChannel ch;

  public NioBasedBufferedFileInputStream(File file, int bufferSize) throws IOException {
    bb = ByteBuffer.allocateDirect(bufferSize);
    ch = FileChannel.open(file.toPath(), StandardOpenOption.READ);
    ch.read(bb);
    bb.flip();
  }

  public NioBasedBufferedFileInputStream(File file) throws IOException {
    this(file, DEFAULT_BUFFER_SIZE);
  }

  private boolean refill() throws IOException {
    if (!bb.hasRemaining()) {
      bb.clear();
      int nRead = ch.read(bb);
      if (nRead == -1) {
        return false;
      }
      bb.flip();
    }
    return true;
  }

  @Override
  public int read() throws IOException {
    if (!refill()) {
      return -1;
    }
    return bb.get() & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (!refill()) {
      return -1;
    }
    len = Math.min(len, bb.remaining());
    bb.get(b, off, len);
    return len;
  }

  @Override
  public int available() throws IOException {
    return bb.remaining();
  }

  @Override
  public void close() throws IOException {
    ch.close();
  }
}
