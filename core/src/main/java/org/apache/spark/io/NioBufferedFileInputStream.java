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

import org.apache.spark.storage.StorageUtils;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
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
 * TODO: support {@link #mark(int)}/{@link #reset()}
 *
 */
@ThreadSafe
public final class NioBufferedFileInputStream extends InputStream {

  private static int DEFAULT_BUFFER_SIZE_BYTES = 8192;

  private final ByteBuffer byteBuffer;

  private final FileChannel fileChannel;

  public NioBufferedFileInputStream(File file, int bufferSizeInBytes) throws IOException {
    byteBuffer = ByteBuffer.allocateDirect(bufferSizeInBytes);
    fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
    byteBuffer.flip();
  }

  public NioBufferedFileInputStream(File file) throws IOException {
    this(file, DEFAULT_BUFFER_SIZE_BYTES);
  }

  /**
   * Checks weather data is left to be read from the input stream.
   * @return true if data is left, false otherwise
   * @throws IOException
   */
  private boolean refill() throws IOException {
    if (!byteBuffer.hasRemaining()) {
      byteBuffer.clear();
      int nRead = 0;
      while (nRead == 0) {
        nRead = fileChannel.read(byteBuffer);
      }
      if (nRead < 0) {
        return false;
      }
      byteBuffer.flip();
    }
    return true;
  }

  @Override
  public synchronized int read() throws IOException {
    if (!refill()) {
      return -1;
    }
    return byteBuffer.get() & 0xFF;
  }

  @Override
  public synchronized int read(byte[] b, int offset, int len) throws IOException {
    if ((offset | len | (offset + len) | (b.length - (offset + len))) < 0) {
      throw new IndexOutOfBoundsException();
    }
    if (!refill()) {
      return -1;
    }
    len = Math.min(len, byteBuffer.remaining());
    byteBuffer.get(b, offset, len);
    return len;
  }

  @Override
  public synchronized int available() throws IOException {
    return byteBuffer.remaining();
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    if (n <= 0L) {
      return 0L;
    }
    if (byteBuffer.remaining() >= n) {
      // The buffered content is enough to skip
      byteBuffer.position(byteBuffer.position() + (int) n);
      return n;
    }
    long skippedFromBuffer = byteBuffer.remaining();
    long toSkipFromFileChannel = n - skippedFromBuffer;
    // Discard everything we have read in the buffer.
    byteBuffer.position(0);
    byteBuffer.flip();
    return skippedFromBuffer + skipFromFileChannel(toSkipFromFileChannel);
  }

  private long skipFromFileChannel(long n) throws IOException {
    long currentFilePosition = fileChannel.position();
    long size = fileChannel.size();
    if (n > size - currentFilePosition) {
      fileChannel.position(size);
      return size - currentFilePosition;
    } else {
      fileChannel.position(currentFilePosition + n);
      return n;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    fileChannel.close();
    StorageUtils.dispose(byteBuffer);
  }

  @Override
  protected void finalize() throws IOException {
      close();
  }
}
