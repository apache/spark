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

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.Checksum;

import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.util.SafeUtils;
import net.jpountz.xxhash.XXHashFactory;

/**
 * {@link InputStream} implementation to decode data written with
 * {@link net.jpountz.lz4.LZ4BlockOutputStream}. This class is not thread-safe and does not
 * support {@link #mark(int)}/{@link #reset()}.
 * @see net.jpountz.lz4.LZ4BlockOutputStream
 *
 * This is based on net.jpountz.lz4.LZ4BlockInputStream
 *
 * changes: https://github.com/davies/lz4-java/commit/cc1fa940ac57cc66a0b937300f805d37e2bf8411
 *
 * TODO: merge this into upstream
 */
public final class LZ4BlockInputStream extends FilterInputStream {

  // Copied from net.jpountz.lz4.LZ4BlockOutputStream
  static final byte[] MAGIC = new byte[] { 'L', 'Z', '4', 'B', 'l', 'o', 'c', 'k' };
  static final int MAGIC_LENGTH = MAGIC.length;

  static final int HEADER_LENGTH =
    MAGIC_LENGTH // magic bytes
      + 1          // token
      + 4          // compressed length
      + 4          // decompressed length
      + 4;         // checksum

  static final int COMPRESSION_LEVEL_BASE = 10;

  static final int COMPRESSION_METHOD_RAW = 0x10;
  static final int COMPRESSION_METHOD_LZ4 = 0x20;

  static final int DEFAULT_SEED = 0x9747b28c;

  private final LZ4FastDecompressor decompressor;
  private final Checksum checksum;
  private byte[] buffer;
  private byte[] compressedBuffer;
  private int originalLen;
  private int o;
  private boolean finished;

  /**
   * Create a new {@link InputStream}.
   *
   * @param in            the {@link InputStream} to poll
   * @param decompressor  the {@link LZ4FastDecompressor decompressor} instance to
   *                      use
   * @param checksum      the {@link Checksum} instance to use, must be
   *                      equivalent to the instance which has been used to
   *                      write the stream
   */
  public LZ4BlockInputStream(InputStream in, LZ4FastDecompressor decompressor, Checksum checksum) {
    super(in);
    this.decompressor = decompressor;
    this.checksum = checksum;
    this.buffer = new byte[0];
    this.compressedBuffer = new byte[HEADER_LENGTH];
    o = originalLen = 0;
    finished = false;
  }

  /**
   * Create a new instance using {@link net.jpountz.xxhash.XXHash32} for checksuming.
   * @see #LZ4BlockInputStream(InputStream, LZ4FastDecompressor, Checksum)
   * @see net.jpountz.xxhash.StreamingXXHash32#asChecksum()
   */
  public LZ4BlockInputStream(InputStream in, LZ4FastDecompressor decompressor) {
    this(in, decompressor,
      XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum());
  }

  /**
   * Create a new instance which uses the fastest {@link LZ4FastDecompressor} available.
   * @see LZ4Factory#fastestInstance()
   * @see #LZ4BlockInputStream(InputStream, LZ4FastDecompressor)
   */
  public LZ4BlockInputStream(InputStream in) {
    this(in, LZ4Factory.fastestInstance().fastDecompressor());
  }

  @Override
  public int available() throws IOException {
    refill();
    return originalLen - o;
  }

  @Override
  public int read() throws IOException {
    refill();
    if (finished) {
      return -1;
    }
    return buffer[o++] & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    SafeUtils.checkRange(b, off, len);
    refill();
    if (finished) {
      return -1;
    }
    len = Math.min(len, originalLen - o);
    System.arraycopy(buffer, o, b, off, len);
    o += len;
    return len;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public long skip(long n) throws IOException {
    refill();
    if (finished) {
      return -1;
    }
    final int skipped = (int) Math.min(n, originalLen - o);
    o += skipped;
    return skipped;
  }

  private void refill() throws IOException {
    if (finished || o < originalLen) {
      return;
    }
    try {
      readFully(compressedBuffer, HEADER_LENGTH);
    } catch (EOFException e) {
      finished = true;
      return;
    }
    for (int i = 0; i < MAGIC_LENGTH; ++i) {
      if (compressedBuffer[i] != MAGIC[i]) {
        throw new IOException("Stream is corrupted");
      }
    }
    final int token = compressedBuffer[MAGIC_LENGTH] & 0xFF;
    final int compressionMethod = token & 0xF0;
    final int compressionLevel = COMPRESSION_LEVEL_BASE + (token & 0x0F);
    if (compressionMethod != COMPRESSION_METHOD_RAW && compressionMethod != COMPRESSION_METHOD_LZ4)
    {
      throw new IOException("Stream is corrupted");
    }
    final int compressedLen = SafeUtils.readIntLE(compressedBuffer, MAGIC_LENGTH + 1);
    originalLen = SafeUtils.readIntLE(compressedBuffer, MAGIC_LENGTH + 5);
    final int check = SafeUtils.readIntLE(compressedBuffer, MAGIC_LENGTH + 9);
    assert HEADER_LENGTH == MAGIC_LENGTH + 13;
    if (originalLen > 1 << compressionLevel
      || originalLen < 0
      || compressedLen < 0
      || (originalLen == 0 && compressedLen != 0)
      || (originalLen != 0 && compressedLen == 0)
      || (compressionMethod == COMPRESSION_METHOD_RAW && originalLen != compressedLen)) {
      throw new IOException("Stream is corrupted");
    }
    if (originalLen == 0 && compressedLen == 0) {
      if (check != 0) {
        throw new IOException("Stream is corrupted");
      }
      refill();
      return;
    }
    if (buffer.length < originalLen) {
      buffer = new byte[Math.max(originalLen, buffer.length * 3 / 2)];
    }
    switch (compressionMethod) {
      case COMPRESSION_METHOD_RAW:
        readFully(buffer, originalLen);
        break;
      case COMPRESSION_METHOD_LZ4:
        if (compressedBuffer.length < originalLen) {
          compressedBuffer = new byte[Math.max(compressedLen, compressedBuffer.length * 3 / 2)];
        }
        readFully(compressedBuffer, compressedLen);
        try {
          final int compressedLen2 =
            decompressor.decompress(compressedBuffer, 0, buffer, 0, originalLen);
          if (compressedLen != compressedLen2) {
            throw new IOException("Stream is corrupted");
          }
        } catch (LZ4Exception e) {
          throw new IOException("Stream is corrupted", e);
        }
        break;
      default:
        throw new AssertionError();
    }
    checksum.reset();
    checksum.update(buffer, 0, originalLen);
    if ((int) checksum.getValue() != check) {
      throw new IOException("Stream is corrupted");
    }
    o = 0;
  }

  private void readFully(byte[] b, int len) throws IOException {
    int read = 0;
    while (read < len) {
      final int r = in.read(b, read, len - read);
      if (r < 0) {
        throw new EOFException("Stream ended prematurely");
      }
      read += r;
    }
    assert len == read;
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @SuppressWarnings("sync-override")
  @Override
  public void mark(int readlimit) {
    // unsupported
  }

  @SuppressWarnings("sync-override")
  @Override
  public void reset() throws IOException {
    throw new IOException("mark/reset not supported");
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(in=" + in
      + ", decompressor=" + decompressor + ", checksum=" + checksum + ")";
  }

}
